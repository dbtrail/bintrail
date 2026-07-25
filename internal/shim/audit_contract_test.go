package shim

import (
	"context"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/query"
)

// auditHandler builds a Handler over a sqlmock index whose binlog_events
// query answers with no rows, with archives disabled — enough for
// HandleQuery to complete a time-travel query and reach its audit emission
// without a live MySQL. resolverFn stays nil (validatePKColumn is
// deliberately permissive for a bare Handler), and archiveFetcher is a fake
// that must never be called under NoArchive.
func auditHandler(t *testing.T) *Handler {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	mock.MatchExpectationsInOrder(false)
	eventCols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
	}
	// Both the planner's partition probe and the event fetch can run more than
	// once across the table's cases; sqlmock replays a query as many times as
	// it is issued only when the expectation is not order-bound, so register
	// them generously.
	for range 8 {
		mock.ExpectQuery("information_schema.PARTITIONS").
			WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
		mock.ExpectQuery("FROM binlog_events").
			WillReturnRows(sqlmock.NewRows(eventCols))
	}

	h := &Handler{
		indexDB: db,
		// AllowGaps: an empty sqlmock index has no coverage, and a gap refusal
		// would abort before the audit emission — the point here is the
		// emission, not the planner.
		cfg:    Config{NoArchive: true, AllowGaps: true, IndexDBName: "bintrail_index"},
		logger: slog.Default(),
		archiveFetcher: func(context.Context, query.Options, string) ([]query.ResultRow, error) {
			t.Error("archiveFetcher called under NoArchive")
			return nil, nil
		},
	}
	if err := h.UseDB("myapp"); err != nil {
		t.Fatal(err)
	}
	return h
}

// TestAuditContract_Shim is the shim half of the #945 audit contract: every
// virtual schema that returns ROW IMAGES must emit shim/timetravel.query,
// attributed to the connection's authenticated tenant.
//
// It is behavioural on purpose — each case drives the real HandleQuery with a
// recording sink installed, so moving, renaming or dropping the emission
// fails here. A source-level check (grep, or an AST walk for the call) would
// still pass if the call survived on a branch the query never takes.
//
// No t.Parallel(): ext's sink is a process-wide variable (see
// audittest.Install), and HandleQuery is driven inline on this goroutine.
func TestAuditContract_Shim(t *testing.T) {
	rec := audittest.Install(t)

	cases := []struct {
		name      string
		query     string
		wantType  string
		wantScope string
	}{
		{
			name:      "flashback single row",
			query:     "SELECT * FROM _flashback.orders AS OF '2026-05-23 18:20:13' WHERE id = 1",
			wantType:  "_flashback",
			wantScope: "single_row",
		},
		{
			name:      "snapshot single row",
			query:     "SELECT * FROM _snapshot.orders AS OF '2026-05-23 18:20:13' WHERE id = 1",
			wantType:  "_snapshot",
			wantScope: "single_row",
		},
		{
			name:      "diff over a window",
			query:     "SELECT * FROM _diff.orders BETWEEN '2026-05-01 00:00:00' AND '2026-05-02 00:00:00' WHERE id = 1",
			wantType:  "_diff",
			wantScope: "single_row",
		},
	}

	var observed []audittest.Pair
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			h := auditHandler(t)
			h.BindActor("tenant_a")
			if _, err := h.HandleQuery(tc.query); err != nil {
				t.Fatalf("HandleQuery(%q): %v", tc.query, err)
			}
			evs := rec.Events()
			if len(evs) != 1 {
				t.Fatalf("recorded %d audit events, want exactly 1: %+v", len(evs), evs)
			}
			ev := evs[0]
			// The shim authenticates per tenant, so the actor must be that
			// tenant — not the daemon's OS owner (ext.ProcessActor), which
			// would attribute every customer's read to whoever started the
			// process.
			if ev.Actor != "tenant_a" {
				t.Errorf("actor = %q, want the authenticated tenant", ev.Actor)
			}
			if ev.Schema != "myapp" || ev.Table != "orders" {
				t.Errorf("schema/table = %q/%q, want myapp/orders", ev.Schema, ev.Table)
			}
			if got := ev.Detail["query_type"]; got != tc.wantType {
				t.Errorf("detail[query_type] = %q, want %q", got, tc.wantType)
			}
			if got := ev.Detail["scope"]; got != tc.wantScope {
				t.Errorf("detail[scope] = %q, want %q", got, tc.wantScope)
			}
			if ev.Time.IsZero() {
				t.Error("event Time not stamped")
			}
			observed = append(observed, audittest.Pair{Surface: ev.Surface, Action: ev.Action})
		})
	}

	audittest.CheckCoverage(t, audittest.OwnerShim, observed)
}

// TestAuditContract_ShimFullTableAndUnbound pins the two attribution edges:
// a full-table read (no WHERE) is audited with scope=full_table, and a
// Handler whose serving layer never called BindActor reports the unbound
// sentinel rather than an empty Actor that reads like a normal event.
func TestAuditContract_ShimFullTableAndUnbound(t *testing.T) {
	rec := audittest.Install(t)
	h := auditHandler(t)

	if _, err := h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-05-23 18:20:13'"); err != nil {
		t.Fatalf("full-table HandleQuery: %v", err)
	}
	evs := rec.Events()
	if len(evs) != 1 {
		t.Fatalf("recorded %d events, want 1: %+v", len(evs), evs)
	}
	if got := evs[0].Detail["scope"]; got != "full_table" {
		t.Errorf("detail[scope] = %q, want full_table", got)
	}
	if evs[0].Actor != unboundActor {
		t.Errorf("actor = %q, want the unbound sentinel %q — an empty actor would be indistinguishable from a real one", evs[0].Actor, unboundActor)
	}
}

// TestAuditContract_ShimSilentOnRefusal pins the failure semantics: a query
// the shim REFUSES read no rows, so it must not be recorded as a data
// access. (Auditing an unserved query would inflate every trail with reads
// that never happened.)
func TestAuditContract_ShimSilentOnRefusal(t *testing.T) {
	rec := audittest.Install(t)
	h := auditHandler(t)
	h.BindActor("tenant_a")

	if _, err := h.HandleQuery("SELECT * FROM orders WHERE id = 1"); err == nil {
		t.Fatal("expected a non-time-travel query to be refused")
	}
	if evs := rec.Events(); len(evs) != 0 {
		t.Errorf("a refused query recorded %d audit events, want 0: %+v", len(evs), evs)
	}
}

// TestAuditContract_ShimNoSinkNoAlloc documents the hot-path guard: with no
// sink installed (the OSS default) the emission must not build its Detail
// map. ext.Auditing() is what makes that true; this pins that the guard
// exists by measuring allocations of the emission helper itself.
func TestAuditContract_ShimNoSinkNoAlloc(t *testing.T) {
	h := auditHandler(t)
	h.BindActor("tenant_a")
	q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "orders", PKColumn: "id", PKValue: "1"}

	if n := testing.AllocsPerRun(100, func() { h.auditTimeTravel(q, nil) }); n != 0 {
		t.Errorf("auditTimeTravel allocated %.1f times per call with no sink installed; "+
			"the ext.Auditing() guard must keep the shim's per-query cost at one nil check", n)
	}
}
