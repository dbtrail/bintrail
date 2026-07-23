// Package console serves an embedded, read-only, single-operator web UI over a
// bintrail index. It is the MCP server with a web face: the same query,
// recovery, status, and metadata engines, reached over HTTP from a browser.
//
// The console serves event browsing, recovery-SQL generation, status, and
// point-in-time reconstruction. The events API includes connection_id but
// never query_text/query_hash (see dto.go). No endpoint ever executes SQL;
// recover generates a script for the operator to review and apply by hand.
package console

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
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
	// HideBoot removes the boot entry from the UI entirely (selector, server
	// list, default selection). Set by source-less `bintrail-console watch`:
	// its boot index is only the control plane's anchor database — no stream
	// ever writes to it — and surfacing it as a "server" made fresh installs
	// look pre-populated. Header-less requests still resolve to the boot
	// bundle underneath, so the views render before the first server exists.
	HideBoot bool
	// Registry is the named-server store (a local YAML file — the only thing
	// the console ever writes). nil means an empty in-memory registry.
	Registry *Registry
	// Listen is the bind address (host:port). Default 127.0.0.1:8090.
	Listen string
	// Token is an OPT-IN static automation credential — set it explicitly for
	// scripts/curl. It is never generated. Password login is the primary path:
	// with no token and no password, a loopback bind (or AllowSetup) enters
	// first-run setup, and a non-loopback bind is refused.
	Token string
	// NoArchive disables Parquet archive auto-discovery on the boot entry.
	// The caller forces this true when a profile is active (archives do not
	// enforce RBAC). Registry entries carry their own per-server flag.
	NoArchive bool
	// DenyTables and RedactColumns are the resolved profile RBAC rules,
	// applied to every query the console runs — on EVERY server.
	DenyTables    []query.SchemaTable
	RedactColumns []query.SchemaTableColumn
	// ProfileActive is set by the caller whenever a profile NAME was supplied,
	// even if it resolved to ZERO deny/redact rules (an empty profile). It forces
	// query.Options.ProfileActive on every query so QueryText/QueryHash are
	// withheld under EVERY named profile (#699), and — since Parquet archives do
	// not run the redaction pass — couples with NoArchive so archive rows cannot
	// leak that statement text either (#838). Independent of the rule count,
	// which stays the signal for rbacActive() (cascade/reconstruct gating).
	ProfileActive bool
	// AllowedHosts is an optional allowlist of hostnames accepted in the Host
	// header (in addition to IP literals and localhost), for operators who
	// front the console with a DNS name.
	AllowedHosts []string
	// MonitorCtrl is the control-plane supervisor, wired in ONLY by
	// `bintrail-console watch` (the write-capable daemon). nil on the
	// standalone read-only console: /api/capabilities reports monitor:false
	// there and every monitor verb refuses at the endpoint with 403,
	// mirroring how reconstruct gates on baselineConfigured.
	MonitorCtrl MonitorController
	// BaselineCtrl runs in-process baseline snapshots (dump→convert→upload) for
	// a monitored server (#613). Wired in ONLY by `bintrail-console watch` when
	// the operator opts in (BINTRAIL_CONSOLE_BASELINE_TRIGGER=1); nil otherwise,
	// where the trigger endpoint refuses with 403 and /api/capabilities reports
	// baseline_trigger:false. Set together with MonitorCtrl (both control-plane).
	BaselineCtrl BaselineController
	// VerifyCtrl runs bintrail verify's engine in-process for a monitored
	// server (#677). Wired in ONLY by `bintrail-console watch` when the
	// operator opts in (BINTRAIL_CONSOLE_VERIFY_TRIGGER=1); nil otherwise,
	// where the trigger endpoint refuses with 403 and /api/capabilities reports
	// verify_trigger:false. Set together with MonitorCtrl (both control-plane).
	VerifyCtrl VerifyController
	// Telemetry is the live usage-telemetry client, wired in by
	// `bintrail-console watch` so the UI opt-out toggle stops the running
	// daemon's beacons immediately. nil on the read-only console (serve), where
	// the toggle still persists the machine-wide choice to the consent file.
	Telemetry TelemetryController
	// BaselineDir / BaselineS3 enable point-in-time reconstruct (Phase 2) on
	// the boot entry. When either is set (and no RBAC profile is active), the
	// "Reconstruct" surface is exposed. BaselineDir takes precedence;
	// BaselineS3 is an s3:// prefix. Registry entries carry their own.
	BaselineDir string
	BaselineS3  string
	// AuthPath locates the console credential file (username + bcrypt hash,
	// written by `bintrail-console user set-password`). Empty means
	// DefaultAuthPath(). A missing file means password login is not
	// configured; a corrupt one fails New loudly.
	AuthPath string
	// MCPTokenPath locates the managed MCP token file (SHA-256 only, written
	// by the Settings → Connect AI generate flow — #1052). Empty means
	// DefaultMCPTokenPath(). A missing file means no managed token; a corrupt
	// one is logged loudly and disables the managed token until regenerated —
	// unlike AuthPath, it deliberately never fails New (the daemon may be the
	// stream supervisor).
	MCPTokenPath string
	// TLSCert / TLSKey serve the console over HTTPS (both-or-neither). Static
	// files only — rotation is a restart; ACME is out of scope.
	TLSCert string
	TLSKey  string
	// AllowSetup permits browser first-run password setup on a NON-loopback
	// bind — an assertion that the bind is access-controlled by other means.
	// The compose stack sets it because it binds 0.0.0.0 inside the container
	// but publishes the port on the host's loopback only; the container can't
	// see that host mapping, so the operator asserts it. Loopback binds always
	// allow setup regardless. Off by default: an unguarded setup endpoint on a
	// truly public bind would let the first stranger claim the password.
	AllowSetup bool
	// RotationDefaults carries the daemon's --rotate-* flag/env values so
	// GET /api/rotation can report the effective policy (and the console panel
	// prefill it) before any console override is saved. Set by the watch
	// daemon; zero on the standalone serve (which runs no rotation loop and
	// hides the panel).
	RotationDefaults RotationDefaults
	// Version is the running build's version string ("0.36.0", or "dev" on
	// unversioned builds), reported in /api/capabilities so the frontend can
	// link release artifacts (the .mcpb bundle) matching the running binary.
	// Optional; empty reads as an unversioned build.
	Version string
}

// RotationDefaults is the daemon-side built-in-rotation policy, surfaced to the
// console read-only as the fallback shown when no override is saved.
type RotationDefaults struct {
	Retain    string
	Interval  string
	AddFuture int
	// Enabled is false when the daemon was started with rotation off
	// (--rotate-retain off). The loop is not running then, so a console-saved
	// override would need a restart — the panel warns in that state.
	Enabled bool
}

// Server is a configured, ready-to-run console HTTP server. It holds only
// process-global state; everything per-server (db, engine, resolver, baseline
// gates) lives in a connManager bundle resolved per request from the
// X-Bintrail-Server header.
type Server struct {
	listen     string
	token      string
	denyTables []query.SchemaTable
	redactCols []query.SchemaTableColumn
	// profileActive is true when a profile NAME was supplied (even zero-rule).
	// buildOptions threads it into query.Options.ProfileActive so query_text is
	// withheld under every named profile (#699/#838).
	profileActive bool
	allowedHosts  []string
	// monitorCtrl: non-nil only when this process is a control-plane
	// supervisor (see Config.MonitorCtrl).
	monitorCtrl MonitorController
	// baselineCtrl: non-nil only when the watch daemon opted into in-process
	// baseline creation (see Config.BaselineCtrl).
	baselineCtrl BaselineController
	// verifyCtrl: non-nil only when the watch daemon opted into in-process
	// verify runs (see Config.VerifyCtrl).
	verifyCtrl VerifyController
	// telemetry: non-nil only when a long-running console wired its live
	// telemetry client (see Config.Telemetry), so the UI opt-out reaches it.
	telemetry TelemetryController
	// rotationDefaults are the daemon's --rotate-* values, the fallback GET
	// /api/rotation reports when no console override is saved.
	rotationDefaults RotationDefaults
	// version is the running build's version string (Config.Version).
	version string
	cm      *connManager
	mux              http.Handler
	// Password login: authPath is the credential file (re-read per login so a
	// live `user set-password` applies without restart); passwordCfg is its
	// boot-time existence, which drives the bind gate and the printed banner.
	authPath     string
	passwordCfg  bool
	allowSetup   bool // assert a non-loopback bind is access-controlled (compose)
	sessions     *sessionStore
	loginLimiter *loginLimiter
	tlsConf      *tls.Config
	// Managed MCP token (#1052): mcpTokenPath is the on-disk hash file;
	// managedTok is the live credential, swapped in place by the
	// generate/rotate/revoke handlers so changes apply without a restart.
	mcpTokenPath string
	managedTok   managedMCPToken
}

// serverHeader selects the target server per request. Selection is stateless —
// no server-side "active server" — so two browser tabs can each point at a
// different server without fighting. As a custom header it is also CSRF-safe
// for the same reason Authorization is: a cross-site form POST cannot set it.
const serverHeader = "X-Bintrail-Server"

// extAuthPrefix is where an installed ext.ConsoleAuthProvider is mounted.
// The provider's login-initiation endpoint lives at extAuthPrefix + "start"
// (the login screen links there); see ext.ConsoleAuthProvider.Handler for
// the mount contract.
const extAuthPrefix = "/api/auth/ext/"

// New validates the config, seeds the boot connection bundle, and assembles
// the middleware/route tree. It does no network I/O — call Run to listen.
func New(cfg Config) (*Server, error) {
	listen := cfg.Listen
	if listen == "" {
		listen = "127.0.0.1:8090"
	}

	// Probe the credential file. Missing = password login not configured
	// (warned about when the path was explicit — a typo'd --auth-file must
	// not silently downgrade auth); corrupt = fail loud.
	authPath := cfg.AuthPath
	explicitAuthPath := authPath != ""
	if authPath == "" {
		authPath = DefaultAuthPath()
	}
	authFile, err := LoadAuthFile(authPath)
	if err != nil {
		return nil, err
	}
	passwordCfg := authFile != nil

	// Managed MCP token (#1052): an /mcp-ONLY credential minted from the UI.
	// Missing file = the normal not-configured state; an unreadable file is
	// logged loudly but never blocks startup — this daemon may be the stream
	// supervisor, and capture must not die over a UI-convenience credential
	// (regenerating from Settings → Connect AI overwrites the bad file). It
	// deliberately plays no part in the bind/setup policy below and is NOT
	// accepted by tokenMiddleware: its advertised scope is the read-only MCP
	// tools, so it must not unlock the browser API (registry CRUD, monitor
	// verbs, its own rotation).
	mcpTokenPath := cfg.MCPTokenPath
	if mcpTokenPath == "" {
		mcpTokenPath = DefaultMCPTokenPath()
	}
	mcpTokFile, err := LoadMCPTokenFile(mcpTokenPath)
	if err != nil {
		slog.Error("console: MCP token file unreadable; managed MCP token disabled until regenerated from Settings → Connect AI", "path", mcpTokenPath, "error", err)
		mcpTokFile = nil
	}

	// Bind/credential policy. Password login is the primary path; the static
	// token is an opt-in automation credential (set explicitly, never
	// generated). With neither a token nor a password:
	//   - loopback → first-run SETUP: no token is generated, and the
	//     unauthenticated /api/auth/setup endpoint lets the operator create the
	//     password in the browser. The rest of the API stays locked (token "" +
	//     no session ⇒ 401) until they do.
	//   - non-loopback → refused: an unauthenticated setup endpoint off-host
	//     would let the first stranger to reach it claim the password.
	// An explicit token always stands; a configured password makes
	// non-loopback binds legal. A non-loopback bind with no credential is
	// refused UNLESS the operator asserts the bind is access-controlled
	// (AllowSetup) — then it enters setup like a loopback bind would.
	// An installed external auth provider (ext.ConsoleAuth) is a valid sole
	// credential path — its login flow mints the same sessions password login
	// does — so it also lifts the non-loopback refusal. It does NOT change
	// willSetup/setupAllowed: browser first-run password setup stays gated on
	// loopback (or the explicit AllowSetup assertion) exactly as before.
	// An installed credential backend (ext.ConsoleCredential) likewise is
	// a credential path: it serves the login form, so it is folded into
	// noCredential — a backend makes the bind legal AND closes first-run setup
	// (a stray password file must not be creatable when a backend already holds
	// the credentials), matching passwordLoginEnabled().
	token := cfg.Token
	noCredential := token == "" && !passwordCfg && ext.ConsoleCredential() == nil
	willSetup := noCredential && (isLoopbackAddr(listen) || cfg.AllowSetup)
	if noCredential && !willSetup && ext.ConsoleAuth() == nil {
		return nil, fmt.Errorf("authentication is required when binding to a non-loopback address %q: set a console password with `bintrail-console user set-password`, set --token / BINTRAIL_CONSOLE_TOKEN for automation, or pass --allow-setup if this bind is access-controlled (e.g. published only on the host's loopback)", listen)
	}
	// A missing auth file is the EXPECTED first-run state (browser setup creates
	// it). Only warn about a missing explicit --auth-file when setup is NOT the
	// path — i.e. a token is configured (password login genuinely disabled until
	// the file exists), which usually means a typo'd path.
	if explicitAuthPath && !passwordCfg && !willSetup {
		slog.Warn("console auth file not found — password login disabled until it is created with `bintrail-console user set-password`", "path", authPath)
	}

	var tlsConf *tls.Config
	if cfg.TLSCert != "" || cfg.TLSKey != "" {
		if cfg.TLSCert == "" || cfg.TLSKey == "" {
			return nil, errors.New("both --tls-cert and --tls-key are required to serve HTTPS")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load TLS key pair: %w", err)
		}
		tlsConf = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	}

	// The LAN-plaintext warning: a password typed into a login form on plain
	// HTTP off-loopback transits cleartext. Warn, don't refuse — TLS
	// termination at a reverse proxy (with --allowed-hosts) is a legitimate,
	// documented topology.
	if passwordCfg && tlsConf == nil && !isLoopbackAddr(listen) {
		slog.Warn("the console password will transit plain HTTP on a non-loopback address — set --tls-cert/--tls-key or terminate TLS at a reverse proxy", "listen", listen)
	}
	// First-run setup is open until a password exists. On a non-loopback bind
	// that means anyone who can reach this port could claim the password — a
	// one-time, first-run-only message (it stops once the password is set).
	if noCredential && !isLoopbackAddr(listen) && cfg.AllowSetup {
		slog.Warn("first-run password setup is OPEN — create the console password before this port is reachable from untrusted networks", "listen", listen)
	}

	// Safety coupling enforced here so it holds for every caller, not just the
	// CLI: Parquet archives do not apply RBAC rules, so the presence of any
	// deny-table / redact-column rule must also disable archive auto-discovery
	// — on every server, not just the boot entry. cmd/bintrail also sets
	// NoArchive when a profile is active; this makes the invariant structural
	// rather than caller-dependent. A NAMED profile that resolved to zero rules
	// counts too (cfg.ProfileActive): its query_text withholding (#699) must not
	// be defeated by archive rows, which skip the redaction pass (#838).
	profileActive := cfg.ProfileActive || len(cfg.DenyTables) > 0 || len(cfg.RedactColumns) > 0

	s := &Server{
		listen:           listen,
		token:            token,
		denyTables:       cfg.DenyTables,
		redactCols:       cfg.RedactColumns,
		profileActive:    profileActive,
		allowedHosts:     cfg.AllowedHosts,
		monitorCtrl:      cfg.MonitorCtrl,
		baselineCtrl:     cfg.BaselineCtrl,
		verifyCtrl:       cfg.VerifyCtrl,
		telemetry:        cfg.Telemetry,
		rotationDefaults: cfg.RotationDefaults,
		version:          cfg.Version,
		cm:               newConnManager(cfg.Registry, profileActive),
		authPath:         authPath,
		passwordCfg:      passwordCfg,
		allowSetup:       cfg.AllowSetup,
		sessions:         newSessionStore(),
		loginLimiter:     newLoginLimiter(),
		tlsConf:          tlsConf,
		mcpTokenPath:     mcpTokenPath,
	}
	s.managedTok.initFromDisk(mcpTokenPath, mcpTokFile)
	s.cm.hideBoot = cfg.HideBoot

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

// selectedEntry returns the registry entry behind the request's effective
// server selection (header, or the default when absent), for capability
// hints that need registry-only fields (e.g. SourceDSN) a bundle doesn't
// carry. False for the boot entry (not in the registry) or an unresolvable
// selection — callers must treat that as "no hint available", not an error.
func (s *Server) selectedEntry(r *http.Request) (ServerEntry, bool) {
	id := r.Header.Get(serverHeader)
	if id == "" {
		id = s.cm.defaultID()
	}
	if id == "" || id == bootServerID {
		return ServerEntry{}, false
	}
	return s.cm.reg.Get(id)
}

// buildHandler wires the route tree and middleware chain:
//   - host guard on EVERY request (DNS-rebinding defense)
//   - three static security headers on every response
//   - the static shell and assets are served without a token, so a browser
//     can load the page and read its bootstrap token from the URL
//   - every /api/* route except healthz, the auth probe, and login requires a
//     bearer credential (static token or login session)
func (s *Server) buildHandler() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("GET /api/status", s.handleStatus)
	api.HandleFunc("GET /api/schemas", s.handleSchemas)
	api.HandleFunc("GET /api/events", s.handleEvents)
	api.HandleFunc("POST /api/recover", s.recordAction("recover", s.handleRecover))
	api.HandleFunc("POST /api/recover-cascade", s.recordAction("recover-cascade", s.handleRecoverCascade))
	api.HandleFunc("GET /api/capabilities", s.handleCapabilities)
	api.HandleFunc("GET /api/reconstruct", s.recordAction("reconstruct", s.handleReconstruct))
	// Storage surfaces (read-only): the selected server's baseline snapshot
	// listing, and the process's ambient AWS credential signals (presence
	// booleans and non-secret names — never values).
	api.HandleFunc("GET /api/baselines", s.handleBaselines)
	api.HandleFunc("GET /api/storage", s.handleStorageInfo)
	// Usage-telemetry opt-out: read the machine-wide state, and toggle it (a
	// local config write, not a data write). Available on any console; the UI
	// surfaces it on the watch daemon that actually beacons.
	api.HandleFunc("GET /api/telemetry", s.handleTelemetryGet)
	api.HandleFunc("POST /api/telemetry", s.handleTelemetrySet)
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
	// Monitor verbs: 403 unless this process is a control-plane supervisor
	// (`bintrail-console watch`). The standalone console stays read-only.
	api.HandleFunc("POST /api/servers/{id}/monitor/start", s.handleMonitorStart)
	api.HandleFunc("POST /api/servers/{id}/monitor/stop", s.handleMonitorStop)
	api.HandleFunc("GET /api/servers/{id}/monitor", s.handleMonitorStatus)
	// Baseline trigger: enqueue an in-process baseline (dump→convert→upload) for
	// a monitored server. 403 unless the watch daemon opted in
	// (BINTRAIL_CONSOLE_BASELINE_TRIGGER=1). GET polls the running/last state.
	api.HandleFunc("POST /api/servers/{id}/baseline", s.recordAction("baseline", s.handleBaselineTrigger))
	api.HandleFunc("GET /api/servers/{id}/baseline", s.handleBaselineStatus)
	api.HandleFunc("POST /api/servers/{id}/verify", s.recordAction("verify", s.handleVerifyTrigger))
	api.HandleFunc("GET /api/servers/{id}/verify", s.handleVerifyStatus)
	api.HandleFunc("GET /api/servers/{id}/verify/explain", s.handleVerifyExplain)
	// Global built-in-rotation policy: read the effective settings; PUT an
	// override (refused on the read-only console — only the watch daemon runs
	// the loop that consumes it).
	api.HandleFunc("GET /api/rotation", s.handleRotationGet)
	api.HandleFunc("PUT /api/rotation", s.handleRotationUpdate)
	// Authenticated auth verbs. Registered on the inner mux so a forgotten
	// root registration breaks login, never security (ServeMux specificity
	// keeps them under the tokenMiddleware-wrapped /api/ catch-all).
	api.HandleFunc("POST /api/auth/logout", s.handleLogout)
	api.HandleFunc("POST /api/auth/password", s.handlePasswordChange)
	// Managed MCP token (#1052): status / generate-or-rotate / revoke, all
	// behind the same credential as every /api route. Values never serialize
	// except the one-time plaintext in the generate response.
	api.HandleFunc("GET /api/mcp-token", s.handleMCPTokenGet)
	api.HandleFunc("POST /api/mcp-token", s.handleMCPTokenGenerate)
	api.HandleFunc("DELETE /api/mcp-token", s.handleMCPTokenRevoke)

	root := http.NewServeMux()
	root.HandleFunc("GET /api/healthz", s.handleHealthz) // unauthenticated liveness
	// Pre-auth surface (still behind hostGuard): the auth-mode probe the SPA
	// boots from, and login itself.
	root.HandleFunc("GET /api/auth", s.handleAuthInfo)
	root.HandleFunc("POST /api/auth/login", s.handleLogin)
	root.HandleFunc("POST /api/auth/setup", s.handleSetup) // first-run, loopback-only, self-disables
	// External auth provider (ext seam): mounted UNAUTHENTICATED at
	// extAuthPrefix, behind hostGuard and securityHeaders only. The provider
	// owns its own CSRF/state protection, and the console's login rate
	// limiter does not cover these routes. ServeMux specificity keeps this
	// subtree more specific than the tokenMiddleware-wrapped "/api/"
	// catch-all; with no provider installed the path falls into that
	// catch-all and 401s — the desired behavior for the stock binary.
	if p := ext.ConsoleAuth(); p != nil {
		root.Handle(extAuthPrefix, p.Handler(extAuthPrefix, s.extSessionIssuer()))
	}
	// Extension view (ext seam): an embedding distribution can contribute one
	// additional console view. Its static assets mount UNAUTHENTICATED on root at
	// /ext/<id>/ (behind hostGuard + securityHeaders only — code always ships,
	// only data is gated), and its data routes mount on the inner api mux at
	// /api/ext/<id>/ so they inherit tokenMiddleware, wrapped in rbacViewGuard so
	// the whole surface is refused (403) while an RBAC profile is active. The id
	// flows into both URL paths and a DOM route, so an invalid one is skipped
	// (logged, not mounted) rather than producing a broken/injectable route.
	if p := ext.ConsoleView(); p != nil {
		id := p.ID()
		if !ext.ValidConsoleViewID(id) {
			slog.Error("console: ignoring extension view with an invalid id (must match ^[a-z0-9-]+$)", "id", id)
		} else {
			staticPrefix := "/ext/" + id + "/"
			dataPrefix := "/api/ext/" + id + "/"
			root.Handle(staticPrefix, p.StaticHandler(staticPrefix))
			api.Handle(dataPrefix, s.rbacViewGuard(p.DataHandler(dataPrefix, s.consoleQueryContext)))
		}
	}
	// credential on all other /api/* (tokenMiddleware), then per-session
	// authorization (authzMiddleware). authz is inert for policy-less sessions —
	// the static token, the password login, and every OSS session — so this only
	// enforces when an EE build attaches a policy via the session-issuer seam.
	root.Handle("/api/", s.tokenMiddleware(s.authzMiddleware(api)))
	// MCP endpoint (#1039): the four read-only tools over Streamable HTTP,
	// token-authenticated (static or UI-managed — #1052), routed per server
	// by URL path. Carries its own auth check (tokens only, no sessions —
	// see mcp.go) instead of tokenMiddleware, and sits on root so it
	// inherits hostGuard + securityHeaders.
	mcpH := s.mcpHandler()
	root.Handle("/mcp", mcpH)
	root.Handle("/mcp/{server}", mcpH)
	root.Handle("/", assetHandler()) // static shell + assets

	return s.hostGuard(securityHeaders(root))
}

// Handler returns the fully assembled HTTP handler. Exposed for tests.
func (s *Server) Handler() http.Handler { return s.mux }

// Token returns the static automation token — never the UI-managed MCP
// token (the flashback port depends on that distinction). Empty in
// password-only mode.
func (s *Server) Token() string { return s.token }

// PasswordLogin reports whether a console password was configured at boot —
// the cmd layer keys its banner wording on it.
func (s *Server) PasswordLogin() bool { return s.passwordCfg }

// NeedsSetup reports whether the console is in first-run setup: no credential
// at all, on a loopback bind (or a non-loopback bind the operator marked
// access-controlled via --allow-setup), so the operator must create a password
// in the browser (via the unauthenticated /api/auth/setup endpoint). The cmd
// layer uses it to print "create your password" instead of a URL with a token.
func (s *Server) NeedsSetup() bool { return s.setupAllowed() }

// URL returns the bootstrap URL the operator opens in a browser. Token mode
// includes the jupyter-style ?token=; password mode omits it (printing a live
// credential into logs and shell history serves no one when the login form is
// the entry point). The scheme follows TLS.
func (s *Server) URL() string {
	scheme := "http"
	if s.tlsConf != nil {
		scheme = "https"
	}
	if s.passwordCfg || s.token == "" {
		return fmt.Sprintf("%s://%s/", scheme, displayHost(s.listen))
	}
	return fmt.Sprintf("%s://%s/?token=%s", scheme, displayHost(s.listen), s.token)
}

// Listen binds the configured address and returns the listener synchronously,
// so a caller can fail fast on a port conflict before reporting the server as
// ready (the standalone command blocks on Run, but `bintrail-console watch`
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
		TLSConfig:         s.tlsConf,
	}

	errCh := make(chan error, 1)
	go func() {
		var err error
		if s.tlsConf != nil {
			// Cert/key already live in TLSConfig; ServeTLS's file args are
			// unused. http/2 comes along automatically.
			err = srv.ServeTLS(ln, "", "")
		} else {
			err = srv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
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
