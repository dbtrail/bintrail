package ext

import (
	"net/http"
	"time"
)

// ConsoleSessionIssuer mints a console login session for an identity the
// external provider has already verified. identity is a display/log string
// (e.g. an email); it is logged, never stored. The returned token is the
// bearer credential the browser presents on every /api call — the same
// in-memory session a password login mints, with the same lifetime and
// revocation behavior.
type ConsoleSessionIssuer func(identity string) (token string, expiresAt time.Time, err error)

// ConsoleAuthProvider plugs an external authentication flow (e.g. OIDC
// single sign-on) into the web console's login surface. The console stays
// the authority over sessions: the provider only decides WHO may log in,
// then trades that decision for a console session via the issuer.
type ConsoleAuthProvider interface {
	// DisplayName labels the login screen's button ("Continue with <name>").
	DisplayName() string
	// Handler returns the provider's HTTP handler. The console mounts it
	// UNAUTHENTICATED at prefix (behind the host guard and security
	// headers only), so the provider must expose nothing state-changing
	// beyond its own login flow and owns its own CSRF/state protection —
	// the console's login rate limiter does not cover these routes.
	// Contract: the login-initiation endpoint MUST live at <prefix>start
	// (the login screen links there). On success the provider calls issue
	// and delivers the returned token to the browser; redirecting to
	// /?token=<token> reuses the SPA's existing bootstrap.
	Handler(prefix string, issue ConsoleSessionIssuer) http.Handler
}

// consoleAuth is nil in the OSS build — the console offers only its built-in
// credentials (password login and the opt-in static token).
var consoleAuth ConsoleAuthProvider

// SetConsoleAuth installs the process-wide console auth provider. Call once
// from main() before command dispatch, like SetAuditSink: the console reads
// it when the server is constructed, so a later install is never picked up.
func SetConsoleAuth(p ConsoleAuthProvider) {
	consoleAuth = p
}

// ConsoleAuth returns the installed provider, or nil when none is installed
// (the OSS build).
func ConsoleAuth() ConsoleAuthProvider {
	return consoleAuth
}
