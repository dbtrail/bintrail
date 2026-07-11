package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/shim"
)

// FlashbackConfig tunes the embedded time-travel port (issue #996). The zero
// value is the production default: strict (a coverage gap or archive-fetch
// failure aborts the client's query), a 5-minute per-query deadline, and at
// most 4 concurrent full-table reconstructions.
type FlashbackConfig struct {
	// AllowGaps mirrors shim.Config.AllowGaps: false (default) fails a query
	// loudly on a coverage gap / archive failure; true downgrades to a
	// server-side warning and returns partial rows.
	AllowGaps bool
	// QueryTimeout bounds each time-travel query end-to-end. It is the ONLY
	// backstop against a runaway query on a dropped connection here — unlike
	// the standalone shim, the embedded port does not run the client-disconnect
	// detection pump — so it must never be 0. ServeFlashback substitutes the
	// 5-minute default when this is 0.
	QueryTimeout time.Duration
	// MaxFullTable caps concurrent full-table reconstructions across every
	// connection of this port (0 → 4, the standalone default). A shared gate,
	// exactly like the standalone shim's --max-fulltable-queries.
	MaxFullTable int
	// AuthMethod selects the MySQL auth plugin the port advertises. Empty
	// (default) keeps mysql_native_password; see shim.NewMySQLServer for the
	// accepted values.
	AuthMethod string
}

const (
	defaultFlashbackQueryTimeout = 5 * time.Minute
	defaultFlashbackMaxFullTable = 4
)

// withDefaults resolves the zero-value fields to their production defaults.
// QueryTimeout is the load-bearing one: 0 means "unbounded", but the embedded
// port has no client-disconnect pump, so a runaway query on a dropped
// connection would never be reclaimed — the 5-minute default is the only
// backstop. ServeFlashback always calls this, so the dangerous zero never
// reaches the query path; keeping both substitutions here (rather than one
// inline and one in a side variable) is why they can't drift apart.
func (c FlashbackConfig) withDefaults() FlashbackConfig {
	if c.QueryTimeout == 0 {
		c.QueryTimeout = defaultFlashbackQueryTimeout
	}
	if c.MaxFullTable == 0 {
		c.MaxFullTable = defaultFlashbackMaxFullTable
	}
	return c
}

// ServeFlashback runs a MySQL-protocol time-travel server on ln, routing every
// connection to a monitored server's per-source index by the connection's
// USERNAME (issue #996). Reuse of the console's already-resolved connManager is
// the whole point: `_flashback` / `_snapshot` / `_diff` resolve against the
// same per-source index + baseline the console's Time-travel tab shows, with no
// separate container and no hand-built INDEX_DSN.
//
// Routing: the MySQL username selects the target server — its registry ID, its
// display Name, or "default" for the command-line boot entry. The console's
// static --token is the shared password for every server (the operator can see
// all servers in the UI, so server selection is not a security boundary; the
// token is). The connection's client library therefore connects as, e.g.,
// `-u <server-id> -p<token>`.
//
// Auth requires a token: go-mysql validates the handshake by recomputing the
// scramble from the cleartext GetCredential returns (the
// compareNativePasswordAuthData path in go-mysql v1.13.0 server/auth.go), and
// the console's bcrypt password store cannot produce that cleartext. Callers
// that leave --token empty get an error here rather than an unauthenticated port.
//
// Blocks until ctx is cancelled (which closes ln and drains in-flight
// connections) or ln fails unrecoverably.
func (s *Server) ServeFlashback(ctx context.Context, ln net.Listener, cfg FlashbackConfig) error {
	if s.token == "" {
		return errors.New("flashback port requires an automation token: set --token or BINTRAIL_CONSOLE_TOKEN (the console password store cannot drive MySQL-protocol authentication)")
	}
	cfg = cfg.withDefaults()

	// One *server.Server for the whole port: it owns the caching_sha2_password
	// cache and the RSA keypair (see shim.NewMySQLServer). One shared *Gate so
	// the full-table cap is process-wide, not per-connection.
	srv, err := shim.NewMySQLServer(cfg.AuthMethod)
	if err != nil {
		// ln is already bound by the caller; close it so a construction failure
		// (an unsupported auth method) doesn't leak the listener nor leave the
		// caller's "listening" banner pointing at a port that rejects every
		// handshake. Unreachable from `watch` today (it passes an empty
		// AuthMethod), but a latent trap if a --flashback-auth-method flag lands.
		_ = ln.Close()
		return fmt.Errorf("flashback: %w", err)
	}
	gate := shim.NewGate(cfg.MaxFullTable)
	creds := flashbackCreds{s: s}
	logger := slog.Default()

	// Close the listener when the daemon context ends so Accept unblocks and
	// the loop returns; the deferred wg.Wait then drains open connections.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	// No per-connection cap (the standalone shim's --max-connections has no
	// equivalent here): this is a loopback-default, token-gated operator port,
	// and the shared FullTableGate already bounds the memory-heavy queries.
	var wg sync.WaitGroup
	defer wg.Wait()

	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil // graceful shutdown
			}
			backoff = nextFlashbackBackoff(backoff)
			logger.Error("flashback accept failed", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			s.handleFlashbackConn(ctx, c, srv, creds, gate, cfg, logger)
		}(conn)
	}
}

// handleFlashbackConn performs the handshake, then binds the connection's
// Handler to the server the authenticated username selects. The Handler cannot
// be built before the handshake — the username is only known afterwards, yet
// go-mysql binds the Handler at construction — so a routingHandler proxy is
// passed to NewCustomizedConn and its inner *shim.Handler is set once
// GetUser() reveals the target. Commands are dispatched sequentially in this
// goroutine strictly after that, so no synchronisation is needed.
func (s *Server) handleFlashbackConn(ctx context.Context, c net.Conn, srv *server.Server, creds server.CredentialProvider, gate *shim.Gate, cfg FlashbackConfig, logger *slog.Logger) {
	defer c.Close()

	// Cancel + close the socket when the daemon context dies (SIGTERM) or this
	// goroutine returns, so a graceful shutdown and a stalled handshake can't
	// wedge ServeFlashback's wg.Wait. BindConnContext(connCtx) below ties any
	// in-flight fetch to the same context, so shutdown also aborts a running
	// query — QueryTimeout remains the backstop for a client that simply
	// disappears mid-query without a shutdown signal.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopCloser := context.AfterFunc(connCtx, func() { _ = c.Close() })
	defer stopCloser()

	proxy := &routingHandler{}
	mysqlConn, err := server.NewCustomizedConn(c, srv, creds, proxy)
	if err != nil {
		level := slog.LevelWarn
		if isFlashbackProbe(err) {
			// Bare TCP probes (health checks, port scanners) close before the
			// handshake completes; don't log those at WARN.
			level = slog.LevelDebug
		}
		logger.Log(context.Background(), level, "flashback handshake failed", "err", err, "remote", c.RemoteAddr())
		return
	}

	if err := s.bindFlashbackHandler(connCtx, proxy, mysqlConn.GetUser(), gate, cfg, logger); err != nil {
		// Auth already succeeded; surface the routing failure on the client's
		// first query (a typed MySQL error) rather than a bare disconnect.
		proxy.fail = err
	}

	for {
		if err := mysqlConn.HandleCommand(); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				logger.Debug("flashback connection ended", "err", err, "remote", c.RemoteAddr())
			}
			return
		}
	}
}

// bindFlashbackHandler resolves the target server from the authenticated
// username and wires proxy.inner to a shim.Handler bound to that server's
// per-source index + baseline. A returned error is a typed *mysql.MyError the
// caller stores on the proxy so the client sees it on the first query.
func (s *Server) bindFlashbackHandler(ctx context.Context, proxy *routingHandler, user string, gate *shim.Gate, cfg FlashbackConfig, logger *slog.Logger) error {
	id, ok := s.flashbackTarget(user)
	if !ok {
		// Auth is the token alone (CheckUsername accepts any username), so an
		// unknown or typo'd server name is the normal way we land here — report
		// it as a missing database on the client's first query.
		return gomysql.NewError(gomysql.ER_BAD_DB_ERROR, fmt.Sprintf("flashback: no such server %q", user))
	}
	b, err := s.cm.Resolve(ctx, id)
	if err != nil {
		// scrubDSNError has already stripped secrets from connManager errors.
		return gomysql.NewError(gomysql.ER_UNKNOWN_ERROR, fmt.Sprintf("flashback: cannot open server %q: %s", user, err))
	}

	shimCfg := shim.Config{
		IndexDBName:   b.dbName,
		NoArchive:     b.noArchive,
		AllowGaps:     cfg.AllowGaps,
		QueryTimeout:  cfg.QueryTimeout,
		FullTableGate: gate,
		AuthMethod:    cfg.AuthMethod,
	}
	// The console bundle's baselineSrc is already dir-preferred; map it onto the
	// shim's dir/S3 fields by scheme so `_snapshot` reads the same source the
	// console's Time-travel tab reads (see splitBaselineSource for the #766 edge).
	shimCfg.BaselineDir, shimCfg.BaselineS3 = splitBaselineSource(b.baselineSrc)

	h := shim.NewHandlerWithConfig(b.db, shimCfg, logger)
	h.BindConnContext(ctx)
	// Seed the source schema so fully qualified `_flashback.<table>` queries
	// work without a prior `USE <db>` (mirrors the standalone shim's #263
	// behaviour). Best-effort: the boot entry has no registry SourceDSN.
	if schema := s.flashbackDefaultSchema(id); schema != "" {
		_ = h.UseDB(schema)
	}
	// A default schema the client sent in the handshake (CLIENT_CONNECT_WITH_DB,
	// stashed on the proxy before inner was bound) wins over the SourceDSN seed,
	// matching the standalone shim where an explicit client USE overrides the
	// #263 seed.
	if proxy.pendingDB != "" {
		_ = h.UseDB(proxy.pendingDB)
	}
	proxy.inner = h
	return nil
}

// splitBaselineSource maps a resolved baseline source (the console bundle's
// already dir-preferred baselineSrc) onto the shim's dir/S3 config fields by
// scheme. The #766 local→S3 fallback the console bundle also carries is
// deliberately NOT represented — a documented single-source-parity limitation
// for the embedded port: a server with BOTH a local dir and an S3 copy reads
// `_snapshot` only from the local dir here (see docs/time-travel-sql.md).
func splitBaselineSource(src string) (dir, s3 string) {
	if strings.HasPrefix(src, "s3://") {
		return "", src
	}
	if src != "" {
		return src, ""
	}
	return "", ""
}

// flashbackTarget maps a connection username to a canonical server id: a
// registry ID, a registry display Name, or "default" for the boot entry. The
// registry is read live so servers added in the UI mid-session are reachable
// without restarting the port.
func (s *Server) flashbackTarget(selector string) (string, bool) {
	if selector == "" {
		return "", false
	}
	if _, ok := s.cm.reg.Get(selector); ok {
		return selector, true // matched by id
	}
	for _, e := range s.cm.reg.List() {
		if e.Name == selector {
			return e.ID, true // matched by display name
		}
	}
	if selector == bootServerID && s.cm.bootSelectable() {
		return bootServerID, true
	}
	return "", false
}

// flashbackDefaultSchema derives the source database name for a target server
// from its registry SourceDSN, for `USE`-less fully qualified queries. Empty
// for the boot entry (no registry SourceDSN) or an unparseable/absent DSN.
func (s *Server) flashbackDefaultSchema(id string) string {
	entry, ok := s.cm.reg.Get(id)
	if !ok || entry.SourceDSN == "" {
		return ""
	}
	cfg, err := drivermysql.ParseDSN(entry.SourceDSN)
	if err != nil {
		return ""
	}
	return cfg.DBName
}

// flashbackCreds authenticates the flashback port on the shared console token
// alone: every username is accepted at the handshake (so the error code cannot
// enumerate servers), and the target server is validated AFTER the handshake in
// bindFlashbackHandler, which reads the live registry via flashbackTarget. It
// holds *Server to read s.token directly rather than snapshotting it.
type flashbackCreds struct {
	s *Server
}

// CheckUsername accepts any username: authentication is the shared token, and
// the target server is validated AFTER the handshake (bindFlashbackHandler).
// Deciding validity here would leak which usernames name a real server through
// the handshake's error code (unknown-user vs bad-password), letting an
// unauthenticated client enumerate monitored servers. Returns false only when
// no token is configured — a uniform denial of every connection.
func (f flashbackCreds) CheckUsername(username string) (bool, error) {
	return f.s.token != "", nil
}

// GetCredential returns the shared console token for every username, so auth
// turns on the token alone. found=false on an empty token is defence in depth
// behind ServeFlashback's startup guard: an empty token must never authorise a
// passwordless MySQL handshake.
func (f flashbackCreds) GetCredential(username string) (password string, found bool, err error) {
	if f.s.token == "" {
		return "", false, nil
	}
	return f.s.token, true, nil
}

// routingHandler is the go-mysql Handler passed to NewCustomizedConn before the
// authenticated username (and thus the target server) is known. Its inner
// *shim.Handler is set by bindFlashbackHandler once the handshake completes; if
// routing failed, fail carries the typed error returned on the first command.
// server.EmptyHandler supplies the commands shim.Handler itself does not
// implement (field-list, prepared statements) — identical to a bare
// shim.Handler, which embeds the same EmptyHandler.
type routingHandler struct {
	server.EmptyHandler
	inner *shim.Handler
	fail  error
	// pendingDB holds a default schema the client sent in the handshake
	// (CLIENT_CONNECT_WITH_DB): go-mysql invokes UseDB DURING the handshake,
	// before bindFlashbackHandler binds inner. Stashing it (and returning nil)
	// lets the handshake complete instead of failing it; bindFlashbackHandler
	// replays it onto the real handler. Without this, any client that connects
	// with a default schema (mysql -D, a DSN /db path, JDBC) is rejected before
	// auth even runs.
	pendingDB string
}

func (r *routingHandler) UseDB(dbName string) error {
	switch {
	case r.inner != nil:
		return r.inner.UseDB(dbName)
	case r.fail != nil:
		// Routing failed after the handshake; reject a later USE too so the
		// failure is consistent across commands.
		return r.fail
	default:
		// Pre-bind: go-mysql calls UseDB during the handshake, before inner is
		// set. Stash it (bindFlashbackHandler replays it) and let the handshake
		// complete rather than aborting the connection.
		r.pendingDB = dbName
		return nil
	}
}

func (r *routingHandler) HandleQuery(query string) (*gomysql.Result, error) {
	if r.inner == nil {
		return nil, r.unresolved()
	}
	return r.inner.HandleQuery(query)
}

func (r *routingHandler) unresolved() error {
	if r.fail != nil {
		return r.fail
	}
	return gomysql.NewError(gomysql.ER_UNKNOWN_ERROR, "flashback: no server bound to this connection")
}

// isFlashbackProbe reports whether a handshake error is a bare TCP probe
// (health check / port scan) that closed before completing — logged at Debug
// rather than Warn. Mirrors the `bintrail shim` command's classifyHandshakeErr
// probe arm (internal/cli/shim.go) without the ProxySQL-monitor aggregator,
// which does not apply to an embedded loopback port.
func isFlashbackProbe(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, gomysql.ErrBadConn)
}

const (
	initialFlashbackBackoff = 100 * time.Millisecond
	maxFlashbackBackoff     = 5 * time.Second
)

// nextFlashbackBackoff doubles the accept-retry sleep up to a cap; the zero
// value seeds the first retry. Keeps a wedged listener from spinning the CPU or
// flooding the log without delaying a SIGTERM more than the cap.
func nextFlashbackBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return initialFlashbackBackoff
	}
	if next := current * 2; next < maxFlashbackBackoff {
		return next
	}
	return maxFlashbackBackoff
}
