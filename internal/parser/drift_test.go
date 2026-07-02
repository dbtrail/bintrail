package parser

import (
	"bytes"
	"context"
	"strings"
	"testing"

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
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 9, out)
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
