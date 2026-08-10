package console

import (
	"log/slog"
	"net/http"

	"github.com/dbtrail/dbtrail/ext"
)

// consoleSettingsContext resolves what an installed ext.ConsoleSettingsProvider
// learns about the calling session: its verified identity and its effective
// permission set. Deliberately NOT the per-server ConsoleQueryContext an
// extension VIEW gets — a settings panel administers process-scoped
// configuration, so handing it the selected server's index would imply a
// per-server semantics that does not exist.
//
// It cannot fail: the settings data routes mount behind tokenMiddleware, so the
// request is already authenticated and both values are read off its context.
func (s *Server) consoleSettingsContext(r *http.Request) ext.ConsoleSettingsContext {
	pol := policyFrom(r.Context())
	if pol == nil {
		// No policy = full access (the static token, the password login, every OSS
		// session). FullAccess is reported as its own flag rather than only as a
		// materialized list, so a permission the core defines in a LATER release is
		// still granted to such a session instead of silently starting to deny it.
		return ext.ConsoleSettingsContext{
			Identity:    identityFrom(r.Context()),
			Permissions: ext.AllPermissions(),
			FullAccess:  true,
		}
	}
	// COPY the policy's slice: it is owned by the session store and shared across
	// every request that session makes, so handing the provider the live slice
	// would let an append inside a third-party handler rewrite another request's
	// grants.
	perms := make([]ext.Permission, len(pol.Permissions))
	copy(perms, pol.Permissions)
	return ext.ConsoleSettingsContext{
		Identity:    identityFrom(r.Context()),
		Permissions: perms,
	}
}

// mountableExtensions filters a provider registry down to the entries the
// console will actually mount: a valid route ID, and the FIRST provider claiming
// a given ID. Both skips are load-bearing — an ID that is not ^[a-z0-9-]+$ flows
// into a URL path and a DOM route (broken or injectable mount), and a duplicate
// ID would hit http.ServeMux.Handle with a pattern already registered, which
// PANICS and takes the daemon down at construction.
//
// The mount path calls it with logSkips true; the capabilities path calls it
// with false (the same request is served over and over, and an operator does not
// need the same warning on every poll). Sharing one function is what keeps the
// advertised set and the mounted set from drifting into "advertised but 404s".
func mountableExtensions[T interface{ ID() string }](providers []T, kind string, logSkips bool) []T {
	if len(providers) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(providers))
	out := make([]T, 0, len(providers))
	for _, p := range providers {
		id := p.ID()
		if !ext.ValidConsoleViewID(id) {
			if logSkips {
				slog.Error("console: ignoring extension "+kind+" with an invalid id (must match ^[a-z0-9-]+$)", "id", id)
			}
			continue
		}
		if seen[id] {
			if logSkips {
				slog.Error("console: ignoring extension "+kind+" with a duplicate id (the first one installed is mounted)", "id", id)
			}
			continue
		}
		seen[id] = true
		out = append(out, p)
	}
	return out
}
