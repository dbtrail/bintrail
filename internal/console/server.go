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
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/bintrail/internal/query"
)

// Config configures a console Server. The caller (cmd/bintrail/console.go) is
// responsible for connecting the boot DB (from --index-dsn), running
// EnsureSchema on it, and resolving the profile's RBAC rules before
// constructing the server. Registry servers are connected lazily by the
// console itself — and never schema-migrated (see connManager).
type Config struct {
	// DB is the boot index database, from the command-line DSN. May be nil
	// when the console is registry-only (servers come from Registry) or in
	// unit tests that exercise only middleware/asset routes.
	DB *sql.DB
	// DBName is the boot index database name (from the DSN), used by the
	// query planner and status collection.
	DBName string
	// BootDSN is the boot entry's DSN, used ONLY to render the masked
	// host/user/dbname view in /api/servers. Optional.
	BootDSN string
	// Registry is the named-server store (a local YAML file — the only thing
	// the console ever writes). nil means an empty in-memory registry.
	Registry *Registry
	// Listen is the bind address (host:port). Default 127.0.0.1:8090.
	Listen string
	// Token gates the API. When empty and Listen is loopback, a random token
	// is generated. When empty and Listen is non-loopback, New returns an
	// error — exposing an unauthenticated console off-host is refused.
	Token string
	// NoArchive disables Parquet archive auto-discovery on the boot entry.
	// The caller forces this true when a profile is active (archives do not
	// enforce RBAC). Registry entries carry their own per-server flag.
	NoArchive bool
	// DenyTables and RedactColumns are the resolved profile RBAC rules,
	// applied to every query the console runs — on EVERY server.
	DenyTables    []query.SchemaTable
	RedactColumns []query.SchemaTableColumn
	// AllowedHosts is an optional allowlist of hostnames accepted in the Host
	// header (in addition to IP literals and localhost), for operators who
	// front the console with a DNS name.
	AllowedHosts []string
	// BaselineDir / BaselineS3 enable point-in-time reconstruct (Phase 2) on
	// the boot entry. When either is set (and no RBAC profile is active), the
	// "Reconstruct" surface is exposed. BaselineDir takes precedence;
	// BaselineS3 is an s3:// prefix. Registry entries carry their own.
	BaselineDir string
	BaselineS3  string
}

// Server is a configured, ready-to-run console HTTP server. It holds only
// process-global state; everything per-server (db, engine, resolver, baseline
// gates) lives in a connManager bundle resolved per request from the
// X-Bintrail-Server header.
type Server struct {
	listen       string
	token        string
	denyTables   []query.SchemaTable
	redactCols   []query.SchemaTableColumn
	allowedHosts []string
	cm           *connManager
	mux          http.Handler
}

// serverHeader selects the target server per request. Selection is stateless —
// no server-side "active server" — so two browser tabs can each point at a
// different server without fighting. As a custom header it is also CSRF-safe
// for the same reason Authorization is: a cross-site form POST cannot set it.
const serverHeader = "X-Bintrail-Server"

// New validates the config, seeds the boot connection bundle, and assembles
// the middleware/route tree. It does no network I/O — call Run to listen.
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

	// Safety coupling enforced here so it holds for every caller, not just the
	// CLI: Parquet archives do not apply RBAC rules, so the presence of any
	// deny-table / redact-column rule must also disable archive auto-discovery
	// — on every server, not just the boot entry. cmd/bintrail also sets
	// NoArchive when a profile is active; this makes the invariant structural
	// rather than caller-dependent.
	profileActive := len(cfg.DenyTables) > 0 || len(cfg.RedactColumns) > 0

	s := &Server{
		listen:       listen,
		token:        token,
		denyTables:   cfg.DenyTables,
		redactCols:   cfg.RedactColumns,
		allowedHosts: cfg.AllowedHosts,
		cm:           newConnManager(cfg.Registry, profileActive),
	}

	// Seed the ephemeral boot bundle when the caller supplied a command-line
	// connection (or baseline config for it). Its derived state — noArchive
	// coupling and the three-condition reconstruct gate — is computed by
	// newBundleDerived, shared with lazily-opened registry bundles.
	if cfg.DB != nil || cfg.BaselineDir != "" || cfg.BaselineS3 != "" {
		boot := newBundleDerived(cfg.DB, cfg.DBName, ServerEntry{
			NoArchive:   cfg.NoArchive,
			BaselineDir: cfg.BaselineDir,
			BaselineS3:  cfg.BaselineS3,
		}, profileActive)
		boot.resolver = loadResolver(cfg.DB)
		s.cm.seedBoot(boot, cfg.BootDSN)
	}

	s.mux = s.buildHandler()
	return s, nil
}

// resolve returns the per-server bundle targeted by the request's
// X-Bintrail-Server header (empty = the default entry), lazily opening the
// connection for registry servers.
func (s *Server) resolve(r *http.Request) (*bundle, error) {
	return s.cm.Resolve(r.Context(), r.Header.Get(serverHeader))
}

// resolveOr resolves the request's bundle, writing the error response and
// returning nil when it cannot: 404 for an unknown id / empty console, 502
// for a server whose connection cannot be established (already scrubbed of
// DSN secrets by buildBundle).
func (s *Server) resolveOr(w http.ResponseWriter, r *http.Request) *bundle {
	b, err := s.resolve(r)
	if err == nil {
		return b
	}
	status := http.StatusBadGateway
	if errors.Is(err, ErrUnknownServer) || errors.Is(err, errNoServers) {
		status = http.StatusNotFound
	}
	writeJSONError(w, status, err.Error())
	return nil
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
	// Server management: CRUD over the local registry file (never a DB write)
	// plus a write-free test-connection probe. Same token + host guard as the
	// data endpoints.
	api.HandleFunc("GET /api/servers", s.handleServersList)
	api.HandleFunc("POST /api/servers", s.handleServersCreate)
	api.HandleFunc("POST /api/servers/test", s.handleServersTest)
	api.HandleFunc("GET /api/servers/{id}", s.handleServersGet)
	api.HandleFunc("PUT /api/servers/{id}", s.handleServersUpdate)
	api.HandleFunc("DELETE /api/servers/{id}", s.handleServersDelete)
	api.HandleFunc("POST /api/servers/{id}/test", s.handleServersTest)

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

// Listen binds the configured address and returns the listener synchronously,
// so a caller can fail fast on a port conflict before reporting the server as
// ready (the standalone command blocks on Run, but `bintrail up --console`
// starts the server in a goroutine and must surface a bind error up front).
func (s *Server) Listen() (net.Listener, error) {
	return net.Listen("tcp", s.listen)
}

// Serve serves on ln and blocks until ctx is cancelled, then shuts the server
// down gracefully (5s drain). It takes ownership of ln (Shutdown closes it).
// Lazily-opened registry connections are closed on the way out; the boot
// entry's db belongs to the cmd layer and its deferred Close.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	defer s.cm.CloseAll()
	srv := &http.Server{
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

// Run binds and serves, blocking until ctx is cancelled, then shuts down
// gracefully. Returns the bind/serve error, or nil on a clean shutdown.
func (s *Server) Run(ctx context.Context) error {
	ln, err := s.Listen()
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
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
