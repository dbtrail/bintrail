package console

import (
	"context"
	"net/http"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// consoleQueryContext resolves the per-request, per-selected-server index access
// an installed ext.ConsoleViewProvider's data handler receives. It uses the same
// bundle resolution the console's own endpoints do, so an extension view reads
// exactly the server the operator has selected (X-Bintrail-Server, or the
// default). The returned error is already DSN-scrubbed by the resolver, so it is
// safe for the provider to surface to the client.
func (s *Server) consoleQueryContext(r *http.Request) (ext.ConsoleQueryContext, error) {
	b, err := s.resolve(r)
	if err != nil {
		return ext.ConsoleQueryContext{}, err
	}
	// SourceDSN is a registry-only hint (the boot entry has none); "not found"
	// is not an error — the provider treats an empty DSN as "no source".
	sourceDSN := ""
	if e, ok := s.selectedEntry(r); ok {
		sourceDSN = e.SourceDSN
	}
	return ext.ConsoleQueryContext{
		DB:        b.db,
		Fetch:     s.consoleFetch(b),
		SourceDSN: sourceDSN,
	}, nil
}

// consoleFetch returns the cross-source fetch closure handed to an extension
// view, pre-bound to a selected server's bundle. It re-attaches the console's
// RBAC rules to every Options as defense-in-depth: the profile guard already
// refuses the whole extension surface while a profile is active (see
// rbacViewGuard), but if that ever regresses, a provider fetching through this
// closure still gets a redacted result set rather than raw rows. It also sets
// ProfileActive so query_text/query_hash stay withheld under a NAMED zero-rule
// profile (#699/#838) — the same signal the guard now keys on. The field type
// on ext.ConsoleQueryContext.Fetch is an alias of this signature, so the
// returned value assigns directly.
func (s *Server) consoleFetch(b *bundle) func(ctx context.Context, opts query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
	return func(ctx context.Context, opts query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
		opts.DenyTables = s.denyTables
		opts.RedactColumns = s.redactCols
		opts.ProfileActive = s.profileActive
		return s.fetch(ctx, b, opts)
	}
}

// rbacViewGuard wraps an extension view's data handler so it refuses with 403 —
// BEFORE the provider handler runs — whenever a profile is active. It keys on
// s.profileActive (a NAMED profile was supplied, even one that resolved to zero
// deny/redact rules — the #838 `--profile <typo>` state), NOT s.rbacActive()
// (rule count > 0). That distinction matters because the seam hands the provider
// a raw *sql.DB (ext.ConsoleQueryContext.DB): a handler could run
// `SELECT query_text, query_hash FROM binlog_events` directly and return the
// originating SQL literals a named profile is contracted to withhold (#699/#838)
// — the console's own /api/events blanks those under EVERY named profile via
// ProfileActive, so the raw-DB path must refuse under the same condition. The
// console cannot guarantee a third-party handler honors table-deny /
// column-redaction rules on its own queries, so it withholds the entire surface
// under any profile rather than risk a leak (the same posture recover-cascade
// takes). This is the enforcement backstop; the SPA also hides the nav item
// because capabilities omit the view under a profile.
func (s *Server) rbacViewGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.profileActive {
			writeJSONError(w, http.StatusForbidden,
				"this view is unavailable while an access-control profile is active")
			return
		}
		next.ServeHTTP(w, r)
	})
}
