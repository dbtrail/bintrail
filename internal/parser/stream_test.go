package parser

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/bintrail/internal/metadata"
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

// TestStreamParser_gtidEventEmitsTrackingEvent verifies that a GTIDEvent emits
// an EventGTID tracking event so that the GTID is accumulated by the stream
// loop even when no row events follow (fix for issue #124).
func TestStreamParser_gtidEventEmitsTrackingEvent(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeGTIDEvent(42))

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 EventGTID tracking event, got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventGTID {
		t.Errorf("expected EventGTID (%d), got %d", EventGTID, ev.EventType)
	}
	if ev.GTID == "" {
		t.Error("expected non-empty GTID on tracking event")
	}
}

// TestStreamParser_gtidThenFilteredRows verifies that a GTIDEvent followed by
// a filtered RowsEvent emits only the GTID tracking event — the row is filtered
// but the GTID is preserved for accumulation (fix for issue #124).
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
	if len(out) != 1 {
		t.Fatalf("expected 1 EventGTID tracking event (rows filtered), got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventGTID {
		t.Errorf("expected EventGTID (%d), got %d", EventGTID, ev.EventType)
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

// TestStreamParser_mixedSequenceGTIDOnly processes a realistic sequence of
// Rotate → GTID → Query → RowsEvent (filtered) and verifies only a GTID
// tracking event is emitted (rows filtered but GTID preserved).
func TestStreamParser_mixedSequenceGTIDOnly(t *testing.T) {
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
	if len(out) != 1 {
		t.Fatalf("expected 1 EventGTID tracking event (rows filtered), got %d", len(out))
	}
	ev := <-out
	if ev.EventType != EventGTID {
		t.Errorf("expected EventGTID (%d), got %d", EventGTID, ev.EventType)
	}
}

// ─── Synchronous DDL hook (#396) ──────────────────────────────────────────────

// TestStreamParser_syncDDLHookOrdering locks the #396 fix: the hook runs
// synchronously inside Run — after the DDL is emitted, before ANY subsequent
// event is decoded. Observable two ways: (1) when the hook runs, the output
// channel holds exactly the DDL (the following event cannot have been
// processed yet); (2) a resolver swapped inside the hook is what stamps the
// NEXT event's SchemaVersion.
func TestStreamParser_syncDDLHookOrdering(t *testing.T) {
	r1 := metadata.NewResolverFromTables(1, nil)
	sp := NewStreamParser(r1, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	hookRuns := 0
	sp.SetSyncDDLHook(func(ev Event) {
		hookRuns++
		if ev.EventType != EventDDL {
			t.Errorf("hook received %v, want EventDDL", ev.EventType)
		}
		if ev.SchemaVersion != 1 {
			t.Errorf("first DDL SchemaVersion = %d, want pre-swap 1", ev.SchemaVersion)
		}
		// The DDL itself is emitted; the event AFTER it must not be yet.
		if got := len(out); got != 1 {
			t.Errorf("hook must run before the next event is processed; out holds %d events", got)
		}
		// The snapshot-refresh stand-in: the very next decode must see this.
		sp.SwapResolver(metadata.NewResolverFromTables(99, nil))
		sp.SetSyncDDLHook(nil) // only assert the first DDL
	})

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeQueryEvent("CREATE TABLE mydb.orders (id INT)"),
		makeQueryEvent("ALTER TABLE mydb.orders ADD COLUMN qty INT"),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if hookRuns != 1 {
		t.Fatalf("hook ran %d times, want 1 (unregistered after the first DDL)", hookRuns)
	}
	close(out)
	var evs []Event
	for ev := range out {
		evs = append(evs, ev)
	}
	if len(evs) != 2 {
		t.Fatalf("expected 2 DDL events, got %d", len(evs))
	}
	if evs[1].SchemaVersion != 99 {
		t.Errorf("post-hook event SchemaVersion = %d, want 99 (the in-hook swap must land before the next decode)", evs[1].SchemaVersion)
	}
}

// TestStreamParser_nilHookUnaffected: without a hook the DDL path behaves as
// before (agent and tests that never register one).
func TestStreamParser_nilHookUnaffected(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEvent("CREATE TABLE mydb.t (id INT)"))
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(out) != 1 {
		t.Errorf("expected the DDL event, got %d events", len(out))
	}
}

// ─── Transaction_payload events (binlog_transaction_compression=ON) ──────────

// makeOrdersResolver builds a resolver for shop.orders(id PK, amount) so a
// fabricated RowsEvent can clear every handleRows guard and actually emit.
func makeOrdersResolver() *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTables(7, map[string]*metadata.TableMeta{"shop.orders": tm})
}

// makePayloadEvent wraps inner events in a TransactionPayloadEvent, mimicking
// what go-mysql produces after decompressing a binlog_transaction_compression
// transaction: inner events pre-decoded in .Events, with headers that carry a
// genuine EventSize but no usable file position (real MySQL zeroes the inner
// end_log_pos; see rewriteInnerHeader).
func makePayloadEvent(logPos, eventSize uint32, inner ...*replication.BinlogEvent) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.TRANSACTION_PAYLOAD_EVENT,
			LogPos:    logPos,
			EventSize: eventSize,
		},
		Event: &replication.TransactionPayloadEvent{Events: inner},
	}
}

// TestStreamParser_transactionPayloadDispatchesInnerRows is the regression
// guard for the compressed-transaction bug: a Transaction_payload event must
// have its inner row events dispatched through the normal pipeline. Before the
// fix, the payload matched no switch case and every compressed transaction was
// silently dropped while the GTID checkpoint kept advancing.
func TestStreamParser_transactionPayloadDispatchesInnerRows(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	// Inner BEGIN carries the connection id; inner row events carry headers
	// with no usable file position (any LogPos < EventSize would make the
	// start_pos derivation underflow uint64; real MySQL zeroes LogPos).
	innerBegin := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT, LogPos: 0},
		Event:  &replication.QueryEvent{Query: []byte("BEGIN"), SlaveProxyID: 42},
	}
	innerInserts := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			Timestamp: 1770000000, // real commit-time timestamp survives dispatch
			LogPos:    0,
			EventSize: 500,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 2,
			},
			Rows: [][]any{{int64(1), int64(10)}, {int64(2), int64(20)}},
		},
	}
	innerDelete := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.DELETE_ROWS_EVENTv2,
			Timestamp: 1770000000,
			LogPos:    0,
			EventSize: 300,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 2,
			},
			Rows: [][]any{{int64(2), int64(20)}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(7), // GTID stays OUTSIDE the payload on the wire
		makePayloadEvent(5000, 900, innerBegin, innerInserts, innerDelete),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(out)
	var dml []Event
	for ev := range out {
		if ev.EventType == EventGTID {
			continue // outer GTID tracking event — not under test
		}
		dml = append(dml, ev)
	}

	// Positive count is the actual regression guard: zero emitted events is
	// exactly the bug this test exists to prevent.
	if len(dml) != 3 {
		t.Fatalf("expected 2 INSERT + 1 DELETE events from payload inner rows, got %d", len(dml))
	}
	wantTypes := []EventType{EventInsert, EventInsert, EventDelete}
	wantPKs := []string{"1", "2", "2"}
	for i, ev := range dml {
		if ev.EventType != wantTypes[i] {
			t.Errorf("event[%d]: EventType = %d, want %d", i, ev.EventType, wantTypes[i])
		}
		if ev.PKValues != wantPKs[i] {
			t.Errorf("event[%d]: PKValues = %q, want %q", i, ev.PKValues, wantPKs[i])
		}
		// Positions must come from the OUTER payload event (5000-900..5000) —
		// the inner headers have no usable file position.
		if ev.StartPos != 4100 || ev.EndPos != 5000 {
			t.Errorf("event[%d]: positions = [%d, %d], want outer [4100, 5000]", i, ev.StartPos, ev.EndPos)
		}
		// Connection id must be extracted from the BEGIN *inside* the payload.
		if ev.ConnectionID != 42 {
			t.Errorf("event[%d]: ConnectionID = %d, want 42 (from inner BEGIN)", i, ev.ConnectionID)
		}
		// GTID set by the uncompressed outer event must carry into inner rows.
		if ev.GTID == "" {
			t.Errorf("event[%d]: expected non-empty GTID from outer GTID event", i)
		}
		if ev.Timestamp.Unix() != 1770000000 {
			t.Errorf("event[%d]: Timestamp = %v, want inner commit time 1770000000", i, ev.Timestamp.Unix())
		}
	}
	if dml[2].RowBefore == nil {
		t.Error("DELETE event: expected non-nil RowBefore from inner before-image")
	}
}

// TestStreamParser_emptyPayloadNoEffect: a payload with no inner events, and
// one whose only inner event bintrail ignores (XID), both emit nothing and do
// not error.
func TestStreamParser_emptyPayloadNoEffect(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)
	innerXID := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: 0},
		Event:  &replication.XIDEvent{XID: 1},
	}
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makePayloadEvent(3000, 200),           // no inner events at all
		makePayloadEvent(4000, 200, innerXID), // only an ignored inner event
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events from empty/ignored payloads, got %d", len(out))
	}
}
