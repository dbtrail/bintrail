//go:build integration

package reconstruct

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestFetchEventsAtomic_splitTransactionExcludedWhole is the load-bearing
// proof for #783: a two-statement transaction (single GTID) whose first
// statement executes before `--at` and whose second executes after it must
// be excluded WHOLE from a point-in-time reconstruction — never
// half-applied. This reproduces the bug against a real MySQL index (real
// SQL, real GTID grouping, real lookahead probe), then proves the fix.
func TestFetchEventsAtomic_splitTransactionExcludedWhole(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	_ = dbName

	const schema, table, pk = "shop", "orders", "1"
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:100"

	snapshotTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)  // first statement
	t2 := time.Date(2026, 1, 1, 12, 0, 10, 0, time.UTC) // second statement, same GTID
	at := time.Date(2026, 1, 1, 12, 0, 5, 0, time.UTC)  // strictly between t1 and t2

	rowBefore1 := []byte(`{"id":1,"status":"new"}`)
	rowAfter1 := []byte(`{"id":1,"status":"processing"}`)
	rowBefore2 := []byte(`{"id":1,"status":"processing"}`)
	rowAfter2 := []byte(`{"id":1,"status":"shipped"}`)

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		t1.Format("2006-01-02 15:04:05"), &gtid,
		schema, table, uint8(event.EventUpdate), pk,
		nil, rowBefore1, rowAfter1)
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300,
		t2.Format("2006-01-02 15:04:05"), &gtid,
		schema, table, uint8(event.EventUpdate), pk,
		nil, rowBefore2, rowAfter2)

	baselineState := map[string]any{"id": float64(1), "status": "new"}
	engine := query.New(db)
	opts := query.Options{
		Schema:   schema,
		Table:    table,
		PKValues: pk,
		Since:    &snapshotTime,
		Until:    &at,
	}
	fm := query.FetchMergedOptions{Opts: opts, NoArchive: true, AllowGaps: true}
	ctx := context.Background()

	// ── Before the fix: a naive row-level cut half-applies the transaction ──
	buggyEvents, _, err := query.FetchMerged(ctx, db, engine, fm)
	if err != nil {
		t.Fatalf("query.FetchMerged: %v", err)
	}
	if len(buggyEvents) != 1 {
		t.Fatalf("naive fetch: want exactly the T1 event (T2 is after `at`), got %d events", len(buggyEvents))
	}
	buggyState := mustApplyAt(t, baselineState, buggyEvents, at)
	if buggyState["status"] != "processing" {
		t.Fatalf("sanity check failed: expected the raw per-row cut to produce the never-existed half-applied state %q, got %v — the repro setup is wrong",
			"processing", buggyState["status"])
	}

	// ── After the fix: FetchEventsAtomic excludes the whole straddling txn ──
	fixedEvents, _, err := FetchEventsAtomic(ctx, db, engine, fm, at)
	if err != nil {
		t.Fatalf("FetchEventsAtomic: %v", err)
	}
	if len(fixedEvents) != 0 {
		t.Fatalf("FetchEventsAtomic: want the straddling transaction fully excluded (0 events), got %d", len(fixedEvents))
	}
	fixedState := mustApplyAt(t, baselineState, fixedEvents, at)
	if fixedState["status"] != "new" {
		t.Errorf("#783: reconstructed state at `at` (mid-transaction) = %v, want %q (the pre-transaction baseline state — the transaction never existed as of `at`)",
			fixedState["status"], "new")
	}
}

// TestFetchEventsAtomic_fractionalAtTruncatesToSecond pins the lookahead
// probe's lower bound: it must use `at.Truncate(time.Second).Add(time.Second)`,
// not `at.Add(time.Second)`. The index stores DATETIME(0) (whole seconds
// only), so with a fractional `at` (12:00:00.5) and the continuation event
// stored at exactly floor(at)+1 (12:00:01), `at.Add(1s)` would probe from
// 12:00:01.5 — strictly AFTER the stored continuation — and wrongly report
// "no continuation found", leaving the straddling transaction half-applied
// (the exact bug #783 fixes). Only the truncated bound (12:00:01) catches it.
func TestFetchEventsAtomic_fractionalAtTruncatesToSecond(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const schema, table, pk = "shop", "orders", "1"
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:300"

	snapshotTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 12, 0, 1, 0, time.UTC)           // floor(at)+1, exactly
	at := time.Date(2026, 1, 1, 12, 0, 0, 500_000_000, time.UTC) // fractional: 12:00:00.5

	rowBefore1 := []byte(`{"id":1,"status":"new"}`)
	rowAfter1 := []byte(`{"id":1,"status":"processing"}`)
	rowBefore2 := []byte(`{"id":1,"status":"processing"}`)
	rowAfter2 := []byte(`{"id":1,"status":"shipped"}`)

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		t1.Format("2006-01-02 15:04:05"), &gtid,
		schema, table, uint8(event.EventUpdate), pk,
		nil, rowBefore1, rowAfter1)
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300,
		t2.Format("2006-01-02 15:04:05"), &gtid,
		schema, table, uint8(event.EventUpdate), pk,
		nil, rowBefore2, rowAfter2)

	baselineState := map[string]any{"id": float64(1), "status": "new"}
	engine := query.New(db)
	opts := query.Options{
		Schema:   schema,
		Table:    table,
		PKValues: pk,
		Since:    &snapshotTime,
		Until:    &at,
	}
	fm := query.FetchMergedOptions{Opts: opts, NoArchive: true, AllowGaps: true}

	events, _, err := FetchEventsAtomic(context.Background(), db, engine, fm, at)
	if err != nil {
		t.Fatalf("FetchEventsAtomic: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("fractional `at`: want the straddling transaction fully excluded (0 events), got %d — the lookahead probe's lower bound is not truncating to the second correctly", len(events))
	}
	state := mustApplyAt(t, baselineState, events, at)
	if state["status"] != "new" {
		t.Errorf("fractional `at`: reconstructed state = %v, want %q", state["status"], "new")
	}
}

// TestFetchEventsAtomic_completedTransactionIncludedWhole is the regression
// guard: a transaction whose statements ALL execute at-or-before `at` must
// still be included in full — the fix must not over-trigger and drop
// legitimately complete transactions.
func TestFetchEventsAtomic_completedTransactionIncludedWhole(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const schema, table, pk = "shop", "orders", "1"
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:200"

	snapshotTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 12, 0, 10, 0, time.UTC)
	at := time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC) // after both statements

	rowBefore1 := []byte(`{"id":1,"status":"new"}`)
	rowAfter1 := []byte(`{"id":1,"status":"processing"}`)
	rowBefore2 := []byte(`{"id":1,"status":"processing"}`)
	rowAfter2 := []byte(`{"id":1,"status":"shipped"}`)

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		t1.Format("2006-01-02 15:04:05"), &gtid,
		schema, table, uint8(event.EventUpdate), pk,
		nil, rowBefore1, rowAfter1)
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300,
		t2.Format("2006-01-02 15:04:05"), &gtid,
		schema, table, uint8(event.EventUpdate), pk,
		nil, rowBefore2, rowAfter2)

	baselineState := map[string]any{"id": float64(1), "status": "new"}
	engine := query.New(db)
	opts := query.Options{
		Schema:   schema,
		Table:    table,
		PKValues: pk,
		Since:    &snapshotTime,
		Until:    &at,
	}
	fm := query.FetchMergedOptions{Opts: opts, NoArchive: true, AllowGaps: true}

	events, _, err := FetchEventsAtomic(context.Background(), db, engine, fm, at)
	if err != nil {
		t.Fatalf("FetchEventsAtomic: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("want both statements of the completed transaction included, got %d events", len(events))
	}
	state := mustApplyAt(t, baselineState, events, at)
	if state["status"] != "shipped" {
		t.Errorf("completed transaction: state at `at` = %v, want %q (both statements applied)", state["status"], "shipped")
	}
}

// TestFetchEventsAtomic_noGTIDPassesThrough confirms the documented
// degradation: events with no GTID (replication without GTIDs) can't be
// grouped into a transaction, so FetchEventsAtomic falls back to the
// pre-#783 per-row cut rather than erroring or dropping data.
func TestFetchEventsAtomic_noGTIDPassesThrough(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const schema, table, pk = "shop", "orders", "1"
	snapshotTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC)

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		t1.Format("2006-01-02 15:04:05"), nil, // no GTID
		schema, table, uint8(event.EventUpdate), pk,
		nil, []byte(`{"id":1,"status":"new"}`), []byte(`{"id":1,"status":"processing"}`))

	engine := query.New(db)
	opts := query.Options{
		Schema:   schema,
		Table:    table,
		PKValues: pk,
		Since:    &snapshotTime,
		Until:    &at,
	}
	fm := query.FetchMergedOptions{Opts: opts, NoArchive: true, AllowGaps: true}

	events, _, err := FetchEventsAtomic(context.Background(), db, engine, fm, at)
	if err != nil {
		t.Fatalf("FetchEventsAtomic: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("no-GTID event should pass through unchanged, got %d events", len(events))
	}
}
