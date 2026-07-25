package console

import (
	"net/http"

	"github.com/dbtrail/dbtrail/ext"
)

// tokenActor is the audit identity of a request authenticated with the static
// automation token rather than a login session. The console's token is a
// shared secret with no identity attached, so there is nothing more specific
// to record — but it must not be recorded as an empty Actor either, which
// reads like a session whose identity went missing.
const tokenActor = "token"

// consoleActor is the audit identity of one HTTP request: the session's
// verified login identity when the request carries a session, otherwise
// tokenActor. The console is a network surface with real authentication, so —
// like the shim — it records its authenticated user, never ext.ProcessActor
// (the daemon's OS owner says nothing about who made the request).
func consoleActor(r *http.Request) string {
	if id := identityFrom(r.Context()); id != "" {
		return id
	}
	return tokenActor
}

// recordConsoleAccess emits one historical-data-access or
// mutation-artifact event on the audit seam (a no-op with no sink
// installed — the OSS default).
//
// Call it on the SUCCESS path, once the response the operator asked for has
// been produced: a refused request read no rows, and a sink cannot fail the
// request anyway (ext.Record returns nothing — see ext/audit.go).
//
// Detail carries counts and filters only, never row data: audit records who
// read what, and a sink that also stored the rows would be a second copy of
// the customer's data outside the index.
func recordConsoleAccess(r *http.Request, action, schema, table string, detail map[string]string) {
	if detail == nil {
		detail = map[string]string{}
	}
	// Which monitored server the request selected. The console is
	// multi-server; without this an audit trail cannot tell two identical
	// reads of different databases apart. Empty header = the default
	// selection, recorded as such rather than guessed at.
	if id := r.Header.Get(serverHeader); id != "" {
		detail["server"] = id
	}
	ext.Record(r.Context(), ext.AuditEvent{
		Surface: "console",
		Action:  action,
		Actor:   consoleActor(r),
		Schema:  schema,
		Table:   table,
		Detail:  detail,
	})
}
