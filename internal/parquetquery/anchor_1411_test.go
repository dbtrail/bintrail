package parquetquery

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// The archive mirror of the anchor filter (#1411), and the one path the live
// short-circuit deliberately cannot cover.
//
// anchorSatisfiedLive elides the archives only when the anchored event came
// back from the live index. When it did not — it aged into an archived
// partition — the archives ARE read, and this filter is the only thing that
// keeps that read scoped to the one event. Without it every archived event for
// the row up to `until` enters MergeAndTrimReport and gets reversed, under a
// banner claiming exactly one change. Deleting the whole block left every
// suite in the repo green, which is why this exists.
func TestBuildFilters_eventAnchor(t *testing.T) {
	ts := time.Date(2026, 8, 21, 20, 8, 36, 0, time.UTC)
	where, args := buildFilters(query.Options{
		Schema:      "wordpress",
		Table:       "dbt_options",
		EventAnchor: &query.EventCursor{Timestamp: ts, EventID: 403440},
	}, nil)

	found := false
	for _, w := range where {
		if w == "event_timestamp = ? AND event_id = ?" {
			found = true
		}
	}
	if !found {
		t.Errorf("missing the anchor equality, so an anchored read that reaches the archives "+
			"returns every event for the row in the window instead of the one named: %v", where)
	}
	// Positions, not membership: the two binds have different types on the
	// live side but both are opaque `any` here, so a swap would compare
	// event_timestamp against the id and event_id against the timestamp —
	// zero rows, HTTP 200, an undo script that found nothing.
	if len(args) != 4 {
		t.Fatalf("args = %v, want 4 (schema, table, ts, event_id)", args)
	}
	if args[2] != ts {
		t.Errorf("args[2] = %v, want the anchor timestamp %v — the binds are out of order", args[2], ts)
	}
	if args[3] != uint64(403440) {
		t.Errorf("args[3] = %v, want the anchor event_id 403440 — the binds are out of order", args[3])
	}
}

// No anchor emits no anchor predicate. Without this the test above passes over
// an unconditional filter that would break every other archive read.
func TestBuildFilters_noEventAnchor(t *testing.T) {
	where, _ := buildFilters(query.Options{Schema: "wordpress"}, nil)
	for _, w := range where {
		if w == "event_timestamp = ? AND event_id = ?" {
			t.Errorf("an unanchored read emitted the anchor equality: %v", where)
		}
	}
}
