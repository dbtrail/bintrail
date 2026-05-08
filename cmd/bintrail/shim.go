package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/shim"
)

// shimCmd serves the time-travel SQL endpoint as an in-process
// MySQL-protocol server. Operators run this alongside the streamer
// (stream or agent) on the same host; ProxySQL routes _flashback / _diff /
// _snapshot virtual-schema queries to its --listen address.
//
// Recognised statement shapes (handled by internal/shim/parser.go):
//   - SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>
//   - SELECT * FROM _snapshot.<table>  AS OF '<ts>' WHERE <col> = <value>
//   - SELECT * FROM _diff.<table>      BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>
//
// Time-range queries are answered against the live bintrail MySQL
// index plus any S3 archives auto-discovered via archive_state — the
// same merge pipeline `bintrail query` and `bintrail recover` use.
//
// Authentication: TenantAuth validates BOTH that the connecting
// username appears in shim.yaml AND that the client's
// mysql_native_password challenge response matches the cleartext
// stored in mysql_password. ProxySQL is still the outer gate
// (validating against mysql_pass_sha1 derived from the same
// cleartext); the shim's local validation closes the gap that would
// otherwise let any direct connection to :3308 with a known username
// in. The default --listen of 127.0.0.1:3308 keeps the shim
// unreachable from the network anyway.
var shimCmd = &cobra.Command{
	Use:   "shim",
	Short: "Run the time-travel SQL MySQL-protocol server",
	Long: `Run an in-process MySQL-protocol server that answers
time-travel SQL queries against the three virtual schemas
_flashback / _snapshot / _diff. Intended to sit behind ProxySQL on the
same host — see docs/time-travel-sql.md for the end-to-end setup.

The shim auto-discovers S3 archives via archive_state, so queries that
reach back beyond the live MySQL index's retention window resolve
transparently from Parquet. Use --no-archive to disable that and stay
index-only.

By default, an archive fetch failure or a planner-detected coverage
gap aborts the customer's query with a MySQL protocol error — the
client has no stderr channel, so silent partial results are worse than
a loud failure. Use --allow-gaps to fall back to warn-and-continue
(matches bintrail recover's behaviour).

Authentication validates both username and password against shim.yaml's
tenants block (mysql_user + mysql_password). ProxySQL is still the outer
password gate against the same cleartext. The default --listen of
127.0.0.1:3308 keeps the shim unreachable from the network anyway.`,
	RunE: runShim,
}

var (
	shListen     string
	shIndexDSN   string
	shShimConfig string
	shNoArchive  bool
	shAllowGaps  bool
	shAuthMethod string
)

func init() {
	shimCmd.Flags().StringVar(&shListen, "listen", "127.0.0.1:3308", "Listen address for the MySQL protocol port (default: localhost-only — keep ProxySQL as the auth gate)")
	shimCmd.Flags().StringVar(&shIndexDSN, "index-dsn", "", "DSN of the bintrail MySQL index")
	shimCmd.Flags().StringVar(&shShimConfig, "shim-config", "shim.yaml", "Path to shim.yaml (the file produced by 'bintrail init-shim')")
	shimCmd.Flags().BoolVar(&shNoArchive, "no-archive", false, "Skip archive auto-discovery; query only the live MySQL index")
	shimCmd.Flags().BoolVar(&shAllowGaps, "allow-gaps", false, "Warn and continue when an archive source fails or the planner detects a coverage gap, instead of returning a MySQL protocol error to the client (default: strict, fail loudly)")
	shimCmd.Flags().StringVar(&shAuthMethod, "auth-method", "", "MySQL auth plugin to advertise during the handshake. Empty (default) keeps mysql_native_password for backwards compatibility. Set to 'caching_sha2_password' or 'sha256_password' on MySQL 8.4+ instances where mysql_native_password is disabled by default. Requires ProxySQL 2.7+ upstream.")
	_ = shimCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(shimCmd)
	rootCmd.AddCommand(shimCmd)
}

func runShim(cmd *cobra.Command, args []string) error {
	tenantCfgs, err := shim.LoadTenantConfigs(shShimConfig)
	if err != nil {
		return err
	}
	users := make(map[string]string, len(tenantCfgs))
	for _, t := range tenantCfgs {
		users[t.MySQLUser] = t.MySQLPassword
	}
	// userSchemas seeds Handler.db at handshake time so fully qualified
	// time-travel queries like `SELECT * FROM _flashback.orders` work
	// without a prior `USE <db>` (issue #263). Per-tenant warnings are
	// emitted by buildUserSchemas; the partial-degradation summary
	// below catches the more interesting case where so many tenants
	// have unusable DSNs that the operator should notice at startup.
	userSchemas := buildUserSchemas(tenantCfgs)
	if missing := len(tenantCfgs) - len(userSchemas); missing > 0 {
		// Name the affected users so a 50-tenant deployment doesn't
		// force the operator to grep the prior per-tenant warnings.
		missingUsers := make([]string, 0, missing)
		for _, t := range tenantCfgs {
			if _, ok := userSchemas[t.MySQLUser]; !ok {
				missingUsers = append(missingUsers, t.MySQLUser)
			}
		}
		slog.Warn(
			"shim: some tenants have no usable default schema; queries from those tenants will require explicit `USE <db>`",
			"tenants", len(tenantCfgs),
			"with_default_schema", len(userSchemas),
			"missing", missing,
			"missing_users", missingUsers,
		)
	}
	auth, err := shim.NewTenantAuth(users)
	if err != nil {
		return err
	}

	db, err := config.Connect(shIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer db.Close()

	// Eager Ping so a misconfigured DSN fails at startup rather than at
	// the first _flashback query the customer actually runs. config.Connect
	// is lazy: it sets parseTime + a TCP timeout but does not exchange a
	// packet until the first query.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		pingCancel()
		return fmt.Errorf("ping index DB: %w", err)
	}
	pingCancel()

	listener, err := net.Listen("tcp", shListen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", shListen, err)
	}
	defer listener.Close()

	if !isLoopbackAddr(listener.Addr()) {
		slog.Warn(
			"shim is bound to a non-loopback address; ensure no other client has direct network access to this port",
			"addr", listener.Addr().String(),
		)
	}

	slog.Info("shim listening",
		"addr", listener.Addr().String(),
		"tenants", len(tenantCfgs),
		"tenants_with_default_schema", len(userSchemas),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SIGINT / SIGTERM → cancel ctx → close listener → accept loop returns.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		slog.Info("shim shutting down")
		cancel()
		listener.Close()
	}()

	// config.Connect already parsed and pinged shIndexDSN above, so a parse
	// failure here would be a programming defect, not a configuration error
	// — surface it loud rather than degrading silently. The empty-DBName
	// check catches the more common operator mistake of pointing the shim
	// at a DSN with no database path; without it, every customer query
	// would fail at FetchMerged.validate() with an opaque wire-protocol
	// error or, under AllowGaps=true, silently skip gap detection.
	dsnCfg, err := mysqldriver.ParseDSN(shIndexDSN)
	if err != nil {
		return fmt.Errorf("parse index DSN: %w", err)
	}
	if dsnCfg.DBName == "" {
		return fmt.Errorf("index DSN must include the database name (e.g. /bintrail_index)")
	}

	cfg := shim.Config{
		AllowGaps:   shAllowGaps,
		NoArchive:   shNoArchive,
		IndexDBName: dsnCfg.DBName,
		AuthMethod:  shAuthMethod,
	}
	// Build the *server.Server once at startup, not per connection:
	//
	//  - typo in --auth-method fails the daemon immediately rather than
	//    silently dropping every incoming connection (which would log
	//    server-side and look like a TCP close to the client),
	//  - the caching_sha2_password cache is per-Server (the
	//    Server.cacheShaPassword sync.Map field in go-mysql v1.13.0);
	//    a per-connection Server resets the cache on every accept and
	//    the "caching" in the plugin name silently does nothing,
	//  - RSA-2048 keypair generation runs once per process (~50ms) not
	//    once per connection. *server.Server is goroutine-safe by
	//    upstream design — per-connection state lives on *server.Conn.
	srv, err := shim.NewMySQLServer(cfg.AuthMethod)
	if err != nil {
		return fmt.Errorf("auth method: %w", err)
	}
	serveLoop(ctx, listener, db, srv, auth, cfg, userSchemas)
	return nil
}

// serveLoop accepts MySQL protocol connections one at a time. Each
// connection runs in its own goroutine with its own Handler instance
// (Handler holds per-connection state: the currently-selected
// database).
//
// Accept errors that aren't shutdown signals (ctx cancellation, listener
// closed) are retried with exponential backoff so a transient kernel
// hiccup doesn't burn CPU and a permanent listener wedge doesn't fill
// the log at ~10 lines/sec. The backoff resets to zero on every
// successful Accept so a brief spike doesn't poison the steady state.
func serveLoop(ctx context.Context, listener net.Listener, db *sql.DB, srv *server.Server, auth shim.TenantAuth, cfg shim.Config, userSchemas map[string]string) {
	var wg sync.WaitGroup
	defer wg.Wait()

	var backoff time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return // graceful shutdown
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			backoff = nextAcceptBackoff(backoff)
			slog.Error("accept failed", "err", err, "backoff", backoff)
			// Sleep via select so SIGTERM can interrupt a long
			// backoff — without this, a wedged listener at the
			// 5s cap would keep the process alive for up to 5s
			// after the operator pressed Ctrl+C.
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			handleConn(c, db, srv, auth, cfg, userSchemas)
		}(conn)
	}
}

const (
	// initialAcceptBackoff is the first sleep after a non-fatal Accept
	// error. Short enough that a single transient blip is invisible to
	// callers; long enough that a flapping error doesn't spin.
	initialAcceptBackoff = 100 * time.Millisecond
	// maxAcceptBackoff caps the exponential growth. 5s is the longest
	// we'll let an operator wait between "listener wedged" log lines
	// and the longest a SIGTERM can be delayed waiting on the sleep.
	maxAcceptBackoff = 5 * time.Second
)

// nextAcceptBackoff returns the next sleep interval given the current
// one. Doubles up to maxAcceptBackoff; the zero value (steady state
// after a successful Accept) seeds the first retry at
// initialAcceptBackoff.
//
// Pure function so the doubling + cap behaviour can be unit-tested
// without driving a real listener through error states.
func nextAcceptBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return initialAcceptBackoff
	}
	next := current * 2
	if next > maxAcceptBackoff {
		return maxAcceptBackoff
	}
	return next
}

// isLoopbackAddr reports whether addr resolves to a loopback address.
// Used at startup so the operator gets a loud warning if the shim is
// reachable from the network — the auth model assumes ProxySQL on the
// same host is the only legitimate caller.
func isLoopbackAddr(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return false
	}
	if tcp.IP == nil || tcp.IP.IsUnspecified() {
		return false
	}
	return tcp.IP.IsLoopback()
}

// classifyHandshakeErr maps an error returned by go-mysql/server's
// handshake into the slog level and message we want to log it at.
//
// ProxySQL's monitor opens a TCP socket on the shim's listen port,
// reads nothing, then closes. go-mysql surfaces the resulting read
// failure as either a raw io.EOF / io.ErrUnexpectedEOF (rare paths
// outside packet/conn.go) or a pingcap-wrapped mysql.ErrBadConn (the
// usual case — every read in packet/conn.go is wrapped this way, and
// pingcap's Unwrap chain is errors.Is-compatible). Treat all three
// as the expected probe shape and demote to Debug so the steady-
// state log isn't a stack trace per probe.
//
// Auth failures from TenantAuth.GetCredential propagate as a
// *mysql.MyError with code ER_ACCESS_DENIED_ERROR (1045). Note that
// (*Conn).handshake also rewrites ErrAccessDeniedNoPassword (1698)
// into 1045 before returning, so checking the single 1045 code
// covers both no-password and bad-password cases — adding a 1698
// branch here would be dead code. Log auth failures at Info so an
// operator can correlate ProxySQL alerts with the shim's view of the
// failure without an alarming ERROR line per monitor probe.
//
// Anything else is unexpected (bad packet, protocol mismatch, …) and
// stays at Error so it surfaces in alerting.
//
// Extracted as a pure function so the classification can be unit-
// tested with synthetic errors — without it, a future refactor that
// flips a branch would silently re-introduce the issue #262 log
// volume regression.
func classifyHandshakeErr(err error) (slog.Level, string) {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, gomysql.ErrBadConn):
		return slog.LevelDebug, "handshake aborted (likely TCP probe)"
	}
	var myErr *gomysql.MyError
	if errors.As(err, &myErr) && myErr.Code == gomysql.ER_ACCESS_DENIED_ERROR {
		return slog.LevelInfo, "mysql auth failed"
	}
	return slog.LevelError, "mysql handshake failed"
}

// buildUserSchemas derives the per-tenant default schema for shim
// connections by parsing each tenant's source_dsn /<db> path. The
// returned map is keyed by mysql_user — handleConn looks the schema
// up after handshake and seeds Handler.db so fully qualified
// time-travel queries work without a prior `USE <db>` (issue #263).
//
// A tenant whose source_dsn is empty, unparseable, or missing the
// /<db> path is omitted from the returned map (with a warning log).
// That keeps a single misconfigured tenant from blocking the rest;
// the affected user simply falls back to the pre-#263 behaviour of
// requiring an explicit USE. runShim emits a summary if the count of
// tenants-with-default-schema is below the total.
func buildUserSchemas(tenantCfgs []shim.TenantConfig) map[string]string {
	out := make(map[string]string, len(tenantCfgs))
	for _, t := range tenantCfgs {
		if t.SourceDSN == "" {
			continue
		}
		cfg, err := mysqldriver.ParseDSN(t.SourceDSN)
		if err != nil {
			slog.Warn(
				"shim.yaml: source_dsn is unparseable; fully qualified time-travel queries from this tenant will still require `USE <db>`",
				"mysql_user", t.MySQLUser, "err", err,
			)
			continue
		}
		if cfg.DBName == "" {
			slog.Warn(
				"shim.yaml: source_dsn has no database path; fully qualified time-travel queries from this tenant will still require `USE <db>`",
				"mysql_user", t.MySQLUser,
			)
			continue
		}
		out[t.MySQLUser] = cfg.DBName
	}
	return out
}

// handleConn wraps one accepted TCP connection in go-mysql/server's
// Conn (which performs the MySQL handshake + auth) and dispatches
// every COM_QUERY through our Handler.
//
// userSchemas seeds the handler's default DB from the authenticated
// tenant's source_dsn so fully qualified time-travel queries
// (`_flashback.<table>`) succeed without a prior `USE <db>` (issue
// #263). The seed runs only after a successful handshake — pre-auth
// we don't know the tenant — and an explicit `USE` from the client
// still wins because UseDB is called sequentially and overwrites the
// seeded value.
func handleConn(c net.Conn, db *sql.DB, srv *server.Server, auth shim.TenantAuth, cfg shim.Config, userSchemas map[string]string) {
	defer c.Close()

	handler := shim.NewHandlerWithConfig(db, cfg, slog.Default())
	mysqlConn, err := server.NewCustomizedConn(c, srv, auth, handler)
	if err != nil {
		level, msg := classifyHandshakeErr(err)
		slog.Log(context.Background(), level, msg, "err", err, "remote", c.RemoteAddr())
		return
	}
	if schema, ok := userSchemas[mysqlConn.GetUser()]; ok && schema != "" {
		_ = handler.UseDB(schema)
	}
	for {
		if err := mysqlConn.HandleCommand(); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Debug("connection ended", "err", err, "remote", c.RemoteAddr())
			}
			return
		}
	}
}
