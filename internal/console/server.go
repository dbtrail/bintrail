// Package console serves an embedded, read-only, single-operator web UI over a
// bintrail index. It is the MCP server with a web face: the same query,
// recovery, status, and metadata engines, reached over HTTP from a browser.
//
// The console exposes only the free query_explorer surface — event browsing and
// recovery-SQL generation. It deliberately stays out of the paid forensics
// surface: see dto.go, where connection_id (actor attribution) is dropped on the
// way out. No endpoint ever executes SQL; recover generates a script for the
// operator to review and apply by hand.
package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/query"
)

// Config configures a console Server. The caller (cmd/bintrail/console.go) is
// responsible for connecting the DB, running EnsureSchema, and resolving the
// profile's RBAC rules before constructing the server.
type Config struct {
	// DB is the index database. Required for a functional server; may be nil
	// in unit tests that exercise only middleware/asset routes.
	DB *sql.DB
	// DBName is the index database name (from the DSN), used by the query
	// planner and status collection.
	DBName string
	// Listen is the bind address (host:port). Default 127.0.0.1:8090.
	Listen string
	// Token gates the API. When empty and Listen is loopback, a random token
	// is generated. When empty and Listen is non-loopback, New returns an
	// error — exposing an unauthenticated console off-host is refused.
	Token string
	// NoArchive disables Parquet archive auto-discovery. The caller forces
	// this true when a profile is active (archives do not enforce RBAC).
	NoArchive bool
	// DenyTables and RedactColumns are the resolved profile RBAC rules,
	// applied to every query the console runs.
	DenyTables    []query.SchemaTable
	RedactColumns []query.SchemaTableColumn
	// AllowedHosts is an optional allowlist of hostnames accepted in the Host
	// header (in addition to IP literals and localhost), for operators who
	// front the console with a DNS name.
	AllowedHosts []string
	// BaselineDir / BaselineS3 enable point-in-time reconstruct (Phase 2). When
	// either is set (and no RBAC profile is active), the "Reconstruct" surface
	// is exposed. BaselineDir takes precedence; BaselineS3 is an s3:// prefix.
	BaselineDir string
	BaselineS3  string
}

// Server is a configured, ready-to-run console HTTP server.
type Server struct {
	db           *sql.DB
	dbName       string
	listen       string
	token        string
	noArchive    bool
	denyTables   []query.SchemaTable
	redactCols   []query.SchemaTableColumn
	engine       *query.Engine
	resolver     *metadata.Resolver
	allowedHosts []string
	// baselineSrc is the resolved reconstruct baseline source (local dir or
	// s3:// prefix), empty when reconstruct is not configured.
	baselineSrc string
	// baselineConfigured gates the reconstruct surface: a baseline is present
	// AND no RBAC profile is active (baseline reads bypass redaction).
	baselineConfigured bool
	mux                http.Handler
}

// New validates the config, loads the schema resolver, and assembles the
// middleware/route tree. It does no network I/O — call Run to listen.
func New(cfg Config) (*Server, error) {
	listen := cfg.Listen
	if listen == "" {
		listen = "127.0.0.1:8090"
	}

	token := cfg.Token
	if token == "" {
		if !isLoopbackAddr(listen) {
			return nil, fmt.Errorf("a token is required when binding to a non-loopback address %q: set --token or BINTRAIL_CONSOLE_TOKEN", listen)
		}
		t, err := generateToken()
		if err != nil {
			return nil, fmt.Errorf("generate token: %w", err)
		}
		token = t
	}

	s := &Server{
		db:           cfg.DB,
		dbName:       cfg.DBName,
		listen:       listen,
		token:        token,
		noArchive:    cfg.NoArchive,
		denyTables:   cfg.DenyTables,
		redactCols:   cfg.RedactColumns,
		engine:       query.New(cfg.DB),
		allowedHosts: cfg.AllowedHosts,
	}

	// Safety coupling enforced here so it holds for every caller, not just the
	// CLI: Parquet archives do not apply RBAC rules, so the presence of any
	// deny-table / redact-column rule must also disable archive auto-discovery
	// — otherwise redacted data could leak in from archives. cmd/bintrail also
	// sets NoArchive when a profile is active; this makes the invariant
	// structural rather than caller-dependent.
	if len(s.denyTables) > 0 || len(s.redactCols) > 0 {
		s.noArchive = true
	}

	// Reconstruct (Phase 2) is gated on three conditions; baselineSrc prefers the
	// local dir over S3:
	//   1. a baseline must be configured;
	//   2. no RBAC profile may be active — ReadBaselineRow reads the baseline
	//      Parquet directly (bypassing engine.Fetch redaction), so under a
	//      profile it could leak redacted columns; and
	//   3. archives must not be disabled — reconstruct fetches deltas with
	//      AllowGaps=false and relies on the planner to turn a coverage gap into
	//      a hard error, but the planner can only verify coverage of archived,
	//      rotated-out hours if those archives are actually fetched. Under
	//      --no-archive the planner would mark an archived-but-rotated-out hour
	//      "covered" while its deltas are skipped, yielding a silently-wrong
	//      reconstruction. Disabling reconstruct there keeps the fail-loud
	//      guarantee intact.
	// Conditions 2 and 3 both collapse into !s.noArchive: an active profile has
	// already forced noArchive=true above, and an explicit --no-archive sets it
	// too.
	s.baselineSrc = cfg.BaselineDir
	if s.baselineSrc == "" {
		s.baselineSrc = cfg.BaselineS3
	}
	s.baselineConfigured = s.baselineSrc != "" && !s.noArchive

	// Load the latest schema snapshot for recovery WHERE-clause generation.
	// Best-effort: a missing snapshot just means recovery falls back to
	// all-column WHERE clauses, which is correct (if more verbose).
	if cfg.DB != nil {
		r, err := metadata.NewResolver(cfg.DB, 0)
		switch {
		case err == nil:
			s.resolver = r
		case errors.Is(err, metadata.ErrNoSnapshots):
			slog.Debug("console: no schema snapshots; recovery will use all-column WHERE clauses")
		default:
			slog.Warn("console: failed to load schema resolver; recovery will use all-column WHERE clauses", "error", err)
		}
	}

	s.mux = s.buildHandler()
	return s, nil
}

// buildHandler wires the route tree and middleware chain:
//   - host guard on EVERY request (DNS-rebinding defense)
//   - the static shell and assets are served without a token, so a browser
//     can load the page and read its bootstrap token from the URL
//   - every /api/* route except healthz requires a bearer token
func (s *Server) buildHandler() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("GET /api/status", s.handleStatus)
	api.HandleFunc("GET /api/schemas", s.handleSchemas)
	api.HandleFunc("GET /api/events", s.handleEvents)
	api.HandleFunc("POST /api/recover", s.handleRecover)
	api.HandleFunc("GET /api/capabilities", s.handleCapabilities)
	api.HandleFunc("GET /api/reconstruct", s.handleReconstruct)

	root := http.NewServeMux()
	root.HandleFunc("GET /api/healthz", s.handleHealthz) // unauthenticated liveness
	root.Handle("/api/", s.tokenMiddleware(api))         // token on all other /api/*
	root.Handle("/", assetHandler())                     // static shell + assets

	return s.hostGuard(root)
}

// Handler returns the fully assembled HTTP handler. Exposed for tests.
func (s *Server) Handler() http.Handler { return s.mux }

// Token returns the active access token (supplied or generated).
func (s *Server) Token() string { return s.token }

// URL returns the jupyter-style bootstrap URL, including the token, that the
// operator opens in a browser.
func (s *Server) URL() string {
	return fmt.Sprintf("http://%s/?token=%s", displayHost(s.listen), s.token)
}

// Run starts the HTTP server and blocks until ctx is cancelled, then shuts the
// server down gracefully (5s drain). Returns the listen error, or nil on a
// clean shutdown.
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.listen,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	}
}

// displayHost turns a bind address into a browser-reachable host:port,
// rewriting wildcard binds to a concrete loopback address and bracketing IPv6.
func displayHost(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "[::1]"
	default:
		if ip := net.ParseIP(host); ip != nil && strings.Contains(host, ":") {
			host = "[" + host + "]" // bracket IPv6 literals
		}
	}
	return host + ":" + port
}
