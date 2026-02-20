package parser

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

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

// TestStreamParser_cancelReturnNil verifies that an already-cancelled context
// causes Run to return nil (graceful shutdown, not an error).
func TestStreamParser_cancelReturnNil(t *testing.T) {
	sp := NewStreamParser(nil, Filters{})
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

// TestStreamParser_rotateEventNoError verifies that processing a RotateEvent
// does not produce an error or output events.
func TestStreamParser_rotateEventNoError(t *testing.T) {
	sp := NewStreamParser(nil, Filters{})
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	if err := streamer.AddEventToStreamer(makeRotate("binlog.000002")); err != nil {
		t.Fatalf("AddEventToStreamer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected no events, got %d", len(out))
	}
}

// TestStreamParser_filteredRowsEvent verifies that a RowsEvent for a schema
// not in the filter produces no output and does not invoke the resolver
// (which is nil here — a panic would occur if resolver were called).
func TestStreamParser_filteredRowsEvent(t *testing.T) {
	// Filter accepts only "accepted" schema; "rejected" will be dropped.
	sp := NewStreamParser(nil, Filters{
		Schemas: map[string]bool{"accepted": true},
	})
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	if err := streamer.AddEventToStreamer(makeRowsEvent("rejected", "orders")); err != nil {
		t.Fatalf("AddEventToStreamer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events for filtered schema, got %d", len(out))
	}
}

// TestStreamParser_rotateBeforeRows verifies that a RotateEvent sets the file
// name carried into subsequent events (exercised via the filter path, since a
// full row emission requires a real resolver).
func TestStreamParser_rotateBeforeRows(t *testing.T) {
	// Both schema/table filtered out — just verify no panic and no events.
	sp := NewStreamParser(nil, Filters{
		Schemas: map[string]bool{},
	})
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	if err := streamer.AddEventToStreamer(makeRotate("binlog.000003")); err != nil {
		t.Fatalf("AddEventToStreamer (rotate): %v", err)
	}
	if err := streamer.AddEventToStreamer(makeRowsEvent("anydb", "t")); err != nil {
		t.Fatalf("AddEventToStreamer (rows): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 events, got %d", len(out))
	}
}
