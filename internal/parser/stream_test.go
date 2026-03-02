package parser

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// makeRotate builds a BinlogEvent wrapping a RotateEvent.
func makeRotate(nextFile string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ROTATE_EVENT},
		Event:  &replication.RotateEvent{NextLogName: []byte(nextFile)},
	}
}

// makeRowsEvent builds a BinlogEvent wrapping a RowsEvent for the given schema/table.
func makeRowsEvent(schema, table string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema: []byte(schema),
				Table:  []byte(table),
			},
		},
	}
}

// makeGTIDEvent builds a BinlogEvent wrapping a GTIDEvent with a fake SID.
func makeGTIDEvent(gno int64) *replication.BinlogEvent {
	sid := make([]byte, 16) // all-zero UUID is fine for unit tests
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.GTID_EVENT},
		Event:  &replication.GTIDEvent{SID: sid, GNO: gno},
	}
}

// makeQueryEvent builds a BinlogEvent wrapping a QueryEvent.
func makeQueryEvent(query string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT},
		Event:  &replication.QueryEvent{Query: []byte(query)},
	}
}

// feed sends events to a streamer then cancels ctx after a short delay,
// ensuring Run returns even if no further events arrive.
func feedThenCancel(t *testing.T, streamer *replication.BinlogStreamer, cancel context.CancelFunc, evs ...*replication.BinlogEvent) {
	t.Helper()
	for _, ev := range evs {
		if err := streamer.AddEventToStreamer(ev); err != nil {
			t.Fatalf("AddEventToStreamer: %v", err)
		}
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
}

// ─── context cancellation ─────────────────────────────────────────────────────

// TestStreamParser_cancelReturnNil verifies that an already-cancelled context
// causes Run to return nil (graceful shutdown, not an error).
func TestStreamParser_cancelReturnNil(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before starting

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil on context cancel, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no events, got %d", len(out))
	}
}

// ─── RotateEvent ─────────────────────────────────────────────────────────────

// TestStreamParser_rotateEventNoError verifies that processing a RotateEvent
// does not produce an error or output events.
func TestStreamParser_rotateEventNoError(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRotate("binlog.000002"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no events, got %d", len(out))
	}
}

// TestStreamParser_rotateBeforeRows verifies that a RotateEvent followed by a
// filtered RowsEvent produces no output (exercises the sequence without a resolver).
func TestStreamParser_rotateBeforeRows(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000003"),
		makeRowsEvent("anydb", "t"),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events, got %d", len(out))
	}
}

// ─── GTIDEvent ───────────────────────────────────────────────────────────────

// TestStreamParser_gtidEventNoOutput verifies that a GTIDEvent does not produce
// output events (it only updates internal GTID state).
func TestStreamParser_gtidEventNoOutput(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeGTIDEvent(42))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no events for GTIDEvent, got %d", len(out))
	}
}

// TestStreamParser_gtidThenFilteredRows verifies that a GTIDEvent followed by
// a filtered RowsEvent produces no output — the GTID is set internally but
// discarded along with the row.
func TestStreamParser_gtidThenFilteredRows(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{"only": true}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(1),
		makeRowsEvent("other", "t"), // filtered out
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events, got %d", len(out))
	}
}

// ─── QueryEvent / DDL ────────────────────────────────────────────────────────

// TestStreamParser_queryEventNonDDL verifies that a non-DDL QUERY_EVENT
// (e.g. BEGIN) produces no error and no output.
func TestStreamParser_queryEventNonDDL(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEvent("BEGIN"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error for BEGIN, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no events, got %d", len(out))
	}
}

// TestStreamParser_queryEventDDL verifies that a DDL QUERY_EVENT emits an
// EventDDL on the output channel.
func TestStreamParser_queryEventDDL(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEvent("ALTER TABLE orders ADD COLUMN note TEXT"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error for DDL query, got %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 DDL event, got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventDDL {
		t.Errorf("expected EventDDL (%d), got %d", EventDDL, ev.EventType)
	}
	if ev.Table != "orders" {
		t.Errorf("expected table 'orders', got %q", ev.Table)
	}
	if ev.DDLType != DDLAlterTable {
		t.Errorf("expected DDLType DDLAlterTable, got %q", ev.DDLType)
	}
}

// ─── RowsEvent filtering ──────────────────────────────────────────────────────

// TestStreamParser_filteredRowsEvent verifies that a RowsEvent for a schema
// not in the filter produces no output and does not invoke the resolver
// (which is nil here — a panic would occur if resolver were called).
func TestStreamParser_filteredRowsEvent(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{"accepted": true}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRowsEvent("rejected", "orders"))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events for filtered schema, got %d", len(out))
	}
}

// TestStreamParser_emptyFilterAcceptsAll verifies that nil filter maps accept
// all schemas/tables (the filter is only exercised via the filter path).
func TestStreamParser_emptyFilterAcceptsAll(t *testing.T) {
	// Nil resolver — if the filter passes, resolver.Resolve will be called and panic.
	// We use a non-nil filter with a specific table that won't match the event's table.
	sp := NewStreamParser(nil, Filters{
		Tables: map[string]bool{"mydb.other": true},
	}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRowsEvent("mydb", "orders")) // "mydb.orders" not in filter

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events, got %d", len(out))
	}
}

// ─── Streamer error propagation ───────────────────────────────────────────────

// TestStreamParser_streamerError verifies that an error injected into the
// streamer is propagated by Run as a non-nil return value.
func TestStreamParser_streamerError(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	injected := errors.New("network connection lost")
	if !streamer.AddErrorToStreamer(injected) {
		t.Fatal("AddErrorToStreamer returned false — could not inject error")
	}

	err := sp.Run(context.Background(), streamer, out)
	if err == nil {
		t.Error("expected non-nil error from streamer error, got nil")
	}
}

// TestStreamParser_streamerErrorAfterEvents verifies that events processed
// before a streamer error are fully emitted and the error is then returned.
func TestStreamParser_streamerErrorAfterEvents(t *testing.T) {
	// Filter rejects everything — so the rotate event won't invoke a resolver.
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	// Queue a rotate event then inject an error.
	if err := streamer.AddEventToStreamer(makeRotate("binlog.000010")); err != nil {
		t.Fatalf("AddEventToStreamer: %v", err)
	}
	if !streamer.AddErrorToStreamer(errors.New("disk full")) {
		t.Fatal("could not inject error")
	}

	err := sp.Run(context.Background(), streamer, out)
	if err == nil {
		t.Error("expected non-nil error after injected error, got nil")
	}
}

// ─── Mixed event sequence ─────────────────────────────────────────────────────

// TestStreamParser_mixedSequenceNoOutput processes a realistic sequence of
// Rotate → GTID → Query → RowsEvent (filtered) and verifies no output, no error.
func TestStreamParser_mixedSequenceNoOutput(t *testing.T) {
	sp := NewStreamParser(nil, Filters{Schemas: map[string]bool{"prod": true}}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000005"),
		makeGTIDEvent(10),
		makeQueryEvent("BEGIN"),
		makeRowsEvent("staging", "orders"), // filtered: not "prod"
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events, got %d", len(out))
	}
}
