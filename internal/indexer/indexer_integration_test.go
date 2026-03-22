//go:build integration

package indexer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/testutil"
)

func TestRun_singleBatch(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := New(db, 1000)
	events := make(chan parser.Event, 10)

	ts := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)
	events <- parser.Event{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: ts, Schema: "mydb", Table: "orders",
		EventType: parser.EventInsert, PKValues: "1",
		RowAfter: map[string]any{"id": float64(1), "name": "Alice"},
	}
	events <- parser.Event{
		BinlogFile: "binlog.000001", StartPos: 200, EndPos: 300,
		Timestamp: ts, Schema: "mydb", Table: "orders",
		EventType: parser.EventUpdate, PKValues: "1",
		RowBefore: map[string]any{"id": float64(1), "name": "Alice"},
		RowAfter:  map[string]any{"id": float64(1), "name": "Bob"},
	}
	close(events)

	count, err := idx.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows inserted, got %d", count)
	}

	// Verify rows in the database.
	var n int
	db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&n)
	if n != 2 {
		t.Errorf("expected 2 rows in binlog_events, got %d", n)
	}
}

func TestRun_multiBatch(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	batchSize := 100
	idx := New(db, batchSize)
	events := make(chan parser.Event, 250)

	ts := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)
	for i := range 250 {
		events <- parser.Event{
			BinlogFile: "binlog.000001", StartPos: uint64(i * 100), EndPos: uint64((i + 1) * 100),
			Timestamp: ts, Schema: "mydb", Table: "orders",
			EventType: parser.EventInsert, PKValues: string(rune('0' + i%10)),
			RowAfter: map[string]any{"id": float64(i)},
		}
	}
	close(events)

	count, err := idx.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if count != 250 {
		t.Errorf("expected 250 rows, got %d", count)
	}
}

func TestRun_emptyChannel(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := New(db, 1000)
	events := make(chan parser.Event)
	close(events)

	count, err := idx.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows for empty channel, got %d", count)
	}
}

func TestRun_jsonNotBase64(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := New(db, 1000)
	events := make(chan parser.Event, 1)

	ts := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)
	events <- parser.Event{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: ts, Schema: "mydb", Table: "orders",
		EventType: parser.EventInsert, PKValues: "1",
		RowAfter: map[string]any{
			"id":   float64(1),
			"data": []byte(`{"nested":"value"}`), // valid JSON []byte
		},
	}
	close(events)

	count, err := idx.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row inserted, got %d", count)
	}

	// Read back and verify the JSON was not base64-encoded.
	var rowAfterBytes []byte
	err = db.QueryRow("SELECT row_after FROM binlog_events WHERE pk_values = '1'").Scan(&rowAfterBytes)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	var rowAfter map[string]json.RawMessage
	if err := json.Unmarshal(rowAfterBytes, &rowAfter); err != nil {
		t.Fatalf("row_after is not valid JSON: %v", err)
	}

	// The "data" field should be an embedded JSON object, not a base64 string.
	var nested map[string]string
	if err := json.Unmarshal(rowAfter["data"], &nested); err != nil {
		t.Errorf("data field should be embedded JSON, not base64: %v (raw: %s)", err, rowAfter["data"])
	}
}

func TestRun_contextCancellation(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := New(db, 1000)
	events := make(chan parser.Event, 10)

	ts := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)
	// Send a few events.
	for i := range 5 {
		events <- parser.Event{
			BinlogFile: "binlog.000001", StartPos: uint64(i * 100), EndPos: uint64((i + 1) * 100),
			Timestamp: ts, Schema: "mydb", Table: "orders",
			EventType: parser.EventInsert, PKValues: "1",
			RowAfter: map[string]any{"id": float64(i)},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	_, err := idx.Run(ctx, events)
	if err == nil {
		t.Error("expected context cancellation error, got nil")
	}
}
