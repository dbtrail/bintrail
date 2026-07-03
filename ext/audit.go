package ext

import (
	"context"
	"time"
)

// AuditEvent describes one auditable operation. dbtrail never executes
// SQL against a user database, so events fall into two classes:
// historical data access (query, shim time-travel, reconstruct) and
// mutation-artifact generation (recover producing a reversal script).
type AuditEvent struct {
	Time    time.Time         // stamped by Record when zero
	Surface string            // "cli", "mcp", "shim", "console"
	Action  string            // e.g. "query.run", "recover.generate"
	Actor   string            // see ProcessActor; shim uses its authenticated user
	Schema  string            // schema filter/target, may be empty
	Table   string            // table filter/target, may be empty
	Detail  map[string]string // action-specific fields (statements, dry_run, gtid, ...)
}

// AuditSink receives audit events. Implementations must be safe for
// concurrent use and must not block: a slow or failing sink must never
// stall a recovery operation.
type AuditSink interface {
	Record(ctx context.Context, ev AuditEvent)
}

// sink is nil in the OSS build — auditing is a no-op.
var sink AuditSink

// SetAuditSink installs the process-wide audit sink. Call once from
// main() before command dispatch.
func SetAuditSink(s AuditSink) {
	sink = s
}

// Record forwards an event to the installed sink, stamping Time when
// unset. Safe to call with no sink installed.
func Record(ctx context.Context, ev AuditEvent) {
	if sink == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	sink.Record(ctx, ev)
}
