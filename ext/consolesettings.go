package ext

import (
	"net/http"
	"slices"
)

// ConsoleSettingsContext is what a ConsoleSettingsProvider's data handler gets
// about the CALLING SESSION: who it is and what it may do. It deliberately
// carries no index access — a settings panel administers process-scoped
// configuration, so handing it the selected server's ConsoleQueryContext would
// imply a per-server semantics that does not exist, and would make a panel look
// like it changes meaning when the operator switches servers in the UI.
//
// It exists so a panel can enforce its OWN invariants server-side (the concrete
// case: refusing a self-demotion, which needs both the caller's identity and
// what the caller currently holds). Client-side checks are not enforcement.
type ConsoleSettingsContext struct {
	// Identity is the session's verified login identity — the string the auth
	// provider passed to ConsoleSessionIssuer. EMPTY for the static automation
	// token and for a session minted without one, so a panel that keys an
	// invariant on identity must treat "" as "unknown caller" and decide
	// explicitly (refuse, or allow as an unattributed operator) rather than
	// comparing "" against a stored record and matching the wrong row.
	Identity string
	// Permissions is a COPY of the effective permission set this session holds.
	// A copy on purpose: the live slice belongs to the session store and is
	// shared across every request that session makes, so a provider appending to
	// it would rewrite another request's grants.
	//
	// For a full-access session (see FullAccess) this is every permission the
	// core defines today. Prefer Allows over reading the slice.
	Permissions []Permission
	// FullAccess reports that the session carries NO access policy at all — the
	// static token, the built-in password login, and every OSS session. Such a
	// session also holds permissions the core adds in LATER releases, which a
	// materialized list cannot express: without this flag a full-access operator
	// would start being denied each newly defined permission until the panel was
	// rebuilt against the new core.
	FullAccess bool
}

// Allows reports whether the session may exercise perm. Use it rather than
// scanning Permissions, so the full-access case (a policy-less session, which
// holds everything including permissions defined after this build) stays correct.
func (c ConsoleSettingsContext) Allows(perm Permission) bool {
	return c.FullAccess || slices.Contains(c.Permissions, perm)
}

// ConsoleSettingsContextFunc resolves the calling session's identity and grants
// for a request. It cannot fail: the console has already authenticated the
// request (the settings data routes mount behind the bearer middleware) and the
// answer is read off the request context, so there is no error to report and no
// error case a provider could mishandle by ignoring.
type ConsoleSettingsContextFunc func(r *http.Request) ConsoleSettingsContext

// ConsoleSettingsProvider injects one administration panel into the web
// console's Settings surface: a nav item under Settings, a capability entry, an
// authenticated data API, and a frontend module that renders the panel. It is
// the sibling of ConsoleViewProvider for surfaces that administer local
// configuration instead of reading captured row data.
//
// The two seams differ in exactly one way that matters, and it is the reason
// this is a separate interface rather than a wider version of the other: an
// extension VIEW is refused outright (403) whenever an RBAC data profile is
// active, because the console cannot verify a third-party handler honors
// table-deny and column-redaction on the raw *sql.DB it is handed. A settings
// panel is handed no database at all, so that reasoning does not apply — and
// applying it anyway would lock out precisely the profile-carrying administrator
// who needs to administer. A settings panel is therefore gated by PERMISSION,
// the mechanism intended for it:
//
//   - Static assets at "/ext-settings/<ID>/" — served UNAUTHENTICATED behind the
//     host guard and security headers only, exactly like index.html and app.js.
//     Code always ships; only DATA is gated. A browser must be able to load the
//     module before it has authenticated.
//   - Data routes at "/api/ext-settings/<ID>/" — behind the console's
//     bearer-token middleware, then the per-session authorization middleware,
//     which requires PermSettingsRead for GET/HEAD and PermSettingsWrite for
//     every other method. Method-based because the core cannot let a provider
//     classify its own routes; see the console's route table for what that
//     cannot catch (a provider that mutates on GET).
type ConsoleSettingsProvider interface {
	// ID is a STABLE, lowercase, URL-safe key for this panel. It is used three
	// ways — as the capability key, as the "/ext-settings/<ID>/" +
	// "/api/ext-settings/<ID>/" mount segment, and as the SPA route
	// "extset-<ID>" — so it must match ^[a-z0-9-]+$ (see ValidConsoleViewID,
	// shared with the view seam: the constraint is on the URL/DOM segment, not on
	// the kind of provider). It must not change across releases: the operator's
	// bookmarks and the SPA route both key on it. A provider whose ID fails
	// validation, or collides with a panel already mounted, is skipped by the
	// console (logged, not mounted) so a typo degrades to "no panel" instead of a
	// broken route or a duplicate-pattern panic at startup.
	ID() string
	// Label is the human-readable nav text for the panel, shown in the console's
	// Settings group.
	Label() string
	// Script is the URL of an ES module the SPA import()s and then calls
	// render(mount, {apiBase, api}) on. Same-origin, same document — NOT an
	// iframe (X-Frame-Options: DENY is global). Serve it from this provider's own
	// StaticHandler subtree (e.g. "/ext-settings/<ID>/panel.js").
	Script() string
	// StaticHandler serves the panel's frontend assets. The console mounts it at
	// prefix ("/ext-settings/<ID>/") UNAUTHENTICATED — it must expose only
	// static, non-secret files (the module named by Script and whatever it
	// loads). The handler sees requests whose path still carries prefix.
	StaticHandler(prefix string) http.Handler
	// DataHandler serves the panel's authenticated data routes. The console
	// mounts it at prefix ("/api/ext-settings/<ID>/") behind the bearer-token and
	// authorization middleware described above. session yields the calling
	// session's identity and grants; the handler calls it to enforce its own
	// invariants (the permission floor is already enforced before the handler
	// runs, but a panel-specific rule — "an admin may not remove their own admin
	// role" — is the panel's to enforce). The handler sees requests whose path
	// still carries prefix.
	DataHandler(prefix string, session ConsoleSettingsContextFunc) http.Handler
}

// registeredConsoleSettings holds the installed settings-panel providers. A
// registry from the start, not a slot: the whole point of this seam is that a
// build can contribute a panel without displacing one another build already
// installed. Empty in the OSS build — the console ships no settings panels, so
// its behavior is unchanged.
var registeredConsoleSettings []ConsoleSettingsProvider

// RegisterConsoleSettings installs a console settings-panel provider. Additive:
// two calls install two panels. Call from main() before command dispatch — the
// console reads the registry when the server is constructed, so a later install
// is never picked up. A nil provider is ignored rather than appended, so an
// unconditional call in a build that computed no provider cannot produce a
// nil-dereference at mount time.
//
// IDs are neither validated nor deduplicated here (a setter that panicked on a
// caller's typo would take down the daemon at startup); the console skips an
// invalid or already-mounted ID at mount time and logs it.
func RegisterConsoleSettings(p ConsoleSettingsProvider) {
	if p == nil {
		return
	}
	registeredConsoleSettings = append(registeredConsoleSettings, p)
}

// ConsoleSettings returns every installed settings-panel provider, in install
// order, as a fresh slice the caller may keep. Empty in the OSS build. The order
// is stable so the console's Settings nav items do not shuffle between restarts.
func ConsoleSettings() []ConsoleSettingsProvider {
	out := make([]ConsoleSettingsProvider, len(registeredConsoleSettings))
	copy(out, registeredConsoleSettings)
	return out
}
