package parser

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── Capture-time schema-drift cross-check (#700) ─────────────────────────────
//
// With binlog_row_metadata=FULL the TABLE_MAP event carries the table's real
// column names. handleRows must hold the schema snapshot against them and fail
// LOUD on a same-count divergence (rename, DROP+ADD) — the drift class the
// column-count guard cannot see, which would otherwise index row values under
// the wrong column names.

// driftResolver builds a two-column shop.orders resolver (id PK, amount).
func driftResolver() *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTables(9, map[string]*metadata.TableMeta{"shop.orders": tm})
}

// driftRowsEvent builds a WRITE_ROWS event for shop.orders with the given
// TABLE_MAP column names (nil = binlog_row_metadata=MINIMAL, no names).
func driftRowsEvent(columnNames []string) *replication.BinlogEvent {
	tme := &replication.TableMapEvent{
		Schema:      []byte("shop"),
		Table:       []byte("orders"),
		ColumnCount: 2,
	}
	for _, n := range columnNames {
		tme.ColumnName = append(tme.ColumnName, []byte(n))
	}
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: tme,
			Rows:  [][]any{{int64(1), int64(10)}},
		},
	}
}

func runHandleRows(t *testing.T, binlogEv *replication.BinlogEvent) ([]Event, error) {
	t.Helper()
	return runHandleRowsWith(t, driftResolver(), binlogEv, &bytes.Buffer{})
}

func runHandleRowsWith(t *testing.T, resolver *metadata.Resolver, binlogEv *replication.BinlogEvent, logBuf *bytes.Buffer) ([]Event, error) {
	t.Helper()
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(logBuf), resolver, &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, "", 9, out)
	close(out)
	var evs []Event
	for ev := range out {
		evs = append(evs, ev)
	}
	return evs, err
}

func TestHandleRows_matchingColumnNamesEmit(t *testing.T) {
	evs, err := runHandleRows(t, driftRowsEvent([]string{"id", "amount"}))
	if err != nil {
		t.Fatalf("matching names must not error: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	if evs[0].RowAfter["amount"] != int64(10) {
		t.Errorf("RowAfter[amount] = %v, want 10", evs[0].RowAfter["amount"])
	}
}

// The corruption case: same column COUNT, different NAME (a rename since the
// snapshot). Must return a hard error naming both sides — never emit.
func TestHandleRows_renamedColumnFailsLoud(t *testing.T) {
	evs, err := runHandleRows(t, driftRowsEvent([]string{"id", "total"}))
	if err == nil {
		t.Fatal("expected a schema-drift error for a renamed column, got nil")
	}
	for _, want := range []string{"schema drift", "total", "amount", "bintrail snapshot", "shop.orders", "binlog.000001"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("drift error missing %q: %v", want, err)
		}
	}
	if len(evs) != 0 {
		t.Errorf("drift must emit no events, got %d", len(evs))
	}
}

// A case-only difference is not drift: MySQL column names are
// case-insensitive, so the name→value mapping is unchanged.
func TestHandleRows_caseOnlyNameDifferenceIsNotDrift(t *testing.T) {
	evs, err := runHandleRows(t, driftRowsEvent([]string{"ID", "Amount"}))
	if err != nil {
		t.Fatalf("case-only difference must not error: %v", err)
	}
	if len(evs) != 1 {
		t.Errorf("expected 1 event, got %d", len(evs))
	}
}

// Under the default binlog_row_metadata=MINIMAL the TABLE_MAP carries no
// names — the check must degrade to a no-op (today's behavior, unchanged).
func TestHandleRows_noColumnNamesNoCheck(t *testing.T) {
	evs, err := runHandleRows(t, driftRowsEvent(nil))
	if err != nil {
		t.Fatalf("MINIMAL metadata (no names) must not error: %v", err)
	}
	if len(evs) != 1 {
		t.Errorf("expected 1 event, got %d", len(evs))
	}
}

// TestStreamParser_driftErrorPropagates pins that the drift error aborts the
// stream (fail closed) rather than being swallowed by the Run loop.
func TestStreamParser_driftErrorPropagates(t *testing.T) {
	sp := NewStreamParser(driftResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000001"),
		driftRowsEvent([]string{"id", "total"}),
	)

	err := sp.Run(ctx, streamer, out)
	if err == nil {
		t.Fatal("expected Run to surface the schema-drift error")
	}
	if !strings.Contains(err.Error(), "schema drift") {
		t.Errorf("error = %v, want schema-drift", err)
	}
}

// driftResolverAt is driftResolver with an explicit snapshot creation time,
// for the historical-vs-stale distinction tests.
func driftResolverAt(snapTime time.Time) *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTablesAt(9, snapTime, map[string]*metadata.TableMeta{"shop.orders": tm})
}

// driftRowsEventAt is driftRowsEvent with an explicit event timestamp.
func driftRowsEventAt(columnNames []string, ts time.Time) *replication.BinlogEvent {
	ev := driftRowsEvent(columnNames)
	ev.Header.Timestamp = uint32(ts.Unix())
	return ev
}

// A divergence on an event OLDER than the snapshot is a routine historical
// state (re-indexing history after a rename; stream backlog catch-up) — a
// hard error would be a permanent dead end whose remediation (re-snapshot)
// is a no-op. It must warn and proceed under the snapshot's names.
func TestHandleRows_preSnapshotDriftWarnsAndProceeds(t *testing.T) {
	snapTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	eventTime := snapTime.Add(-1 * time.Hour) // event predates the snapshot

	var logBuf bytes.Buffer
	evs, err := runHandleRowsWith(t, driftResolverAt(snapTime),
		driftRowsEventAt([]string{"id", "total"}, eventTime), &logBuf)
	if err != nil {
		t.Fatalf("pre-snapshot divergence must not hard-error: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("pre-snapshot event must still index, got %d events", len(evs))
	}
	if evs[0].RowAfter["amount"] != int64(10) {
		t.Errorf("values must index under the SNAPSHOT's names, got %v", evs[0].RowAfter)
	}
	logged := logBuf.String()
	for _, want := range []string{"pre-snapshot", "total", "amount"} {
		if !strings.Contains(logged, want) {
			t.Errorf("historical divergence must warn naming both sides; logs missing %q: %s", want, logged)
		}
	}
}

// A divergence on an event AT-OR-AFTER the snapshot is the genuine stale
// case — hard error, and re-snapshotting actually converges.
func TestHandleRows_postSnapshotDriftFailsLoud(t *testing.T) {
	snapTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	eventTime := snapTime.Add(1 * time.Hour) // event AFTER the snapshot

	evs, err := runHandleRowsWith(t, driftResolverAt(snapTime),
		driftRowsEventAt([]string{"id", "total"}, eventTime), &bytes.Buffer{})
	if err == nil {
		t.Fatal("post-snapshot divergence must hard-error (stale snapshot)")
	}
	if !strings.Contains(err.Error(), "schema drift") {
		t.Errorf("error = %v, want schema-drift", err)
	}
	if len(evs) != 0 {
		t.Errorf("stale-snapshot drift must emit no events, got %d", len(evs))
	}
}

// MySQL treats the Turkish dotted/dotless I pair (İ U+0130, ı U+0131) as
// equal to I/i in identifiers, but Unicode simple folding does not — a legal
// case-style rename İstanbul→istanbul must NOT be drift.
func TestMysqlIdentEqualFold_turkishDottedI(t *testing.T) {
	cases := [][2]string{
		{"İstanbul", "istanbul"},
		{"ıspanak", "ISPANAK"},
		{"amount", "AMOUNT"},
	}
	for _, c := range cases {
		if !mysqlIdentEqualFold(c[0], c[1]) {
			t.Errorf("mysqlIdentEqualFold(%q, %q) = false, want true (MySQL treats them as the same identifier)", c[0], c[1])
		}
	}
	if mysqlIdentEqualFold("amount", "total") {
		t.Error("distinct identifiers must not fold equal")
	}
}

// The boundary case: an event in the SAME second as the snapshot (1s binlog
// timestamp granularity makes this the common real tie) must take the
// hard-error side — "at-or-after" — so a refactor to eventTime.After() can't
// silently flip ties onto the warn-and-proceed (corruption) side.
func TestHandleRows_snapshotTimeTieFailsLoud(t *testing.T) {
	snapTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	evs, err := runHandleRowsWith(t, driftResolverAt(snapTime),
		driftRowsEventAt([]string{"id", "total"}, snapTime), &bytes.Buffer{})
	if err == nil {
		t.Fatal("event at exactly the snapshot time must hard-error (at-or-after = stale)")
	}
	if len(evs) != 0 {
		t.Errorf("tie must emit no events, got %d", len(evs))
	}
}

// A zero EVENT timestamp (tool-generated/rewritten binlogs) is an unknown
// age — it must stay strict, not classify as pre-snapshot lenient.
func TestHandleRows_zeroEventTimestampStaysStrict(t *testing.T) {
	snapTime := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	ev := driftRowsEvent([]string{"id", "total"})
	ev.Header.Timestamp = 0
	evs, err := runHandleRowsWith(t, driftResolverAt(snapTime), ev, &bytes.Buffer{})
	if err == nil {
		t.Fatal("zero event timestamp must stay strict (unknown age), got warn-and-proceed")
	}
	if len(evs) != 0 {
		t.Errorf("expected no events, got %d", len(evs))
	}
}

// Pins the deliberate asymmetry: names present but COUNT differing takes the
// pre-existing count-guard warn-and-skip (parser.go count validation), never
// the drift hard error — and the len(names)==len(tm.Columns) gate is what
// keeps the name loop in bounds.
func TestHandleRows_namesPresentCountDiffersTakesCountGuard(t *testing.T) {
	tme := &replication.TableMapEvent{
		Schema:      []byte("shop"),
		Table:       []byte("orders"),
		ColumnCount: 1, // table now has ONE column; snapshot has two
		ColumnName:  [][]byte{[]byte("id")},
	}
	binlogEv := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: tme,
			Rows:  [][]any{{int64(1)}},
		},
	}

	var logBuf bytes.Buffer
	evs, err := runHandleRowsWith(t, driftResolver(), binlogEv, &logBuf)
	if err != nil {
		t.Fatalf("count divergence must take the count-guard skip, not the drift error: %v", err)
	}
	if len(evs) != 0 {
		t.Errorf("count guard must skip the event, got %d emitted", len(evs))
	}
	if !strings.Contains(logBuf.String(), "column count mismatch") {
		t.Errorf("expected the count-guard warn, got logs: %s", logBuf.String())
	}
}
