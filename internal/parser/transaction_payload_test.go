package parser

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// TestParseFile_compressedFixture is the FILE-mode unit guard for compressed
// transactions, built on a real captured binlog so it runs without Docker.
//
// testdata/compressed_mysql8046.binlog was captured from MySQL 8.0.46 with
// `binlog_transaction_compression=ON` between two FLUSH BINARY LOGS:
//
//	CREATE TABLE payloadtest.orders (id INT PRIMARY KEY, name VARCHAR(255));
//	BEGIN;
//	INSERT INTO payloadtest.orders VALUES (1, REPEAT('a',200)), (2, REPEAT('b',200));
//	COMMIT;
//	UPDATE payloadtest.orders SET name = REPEAT('c', 200) WHERE id = 1;
//	DELETE FROM payloadtest.orders WHERE id = 2;
//
// It contains exactly three ZSTD Transaction_payload events (one per
// transaction), each wrapping BEGIN + TABLE_MAP + rows + XID, with every
// inner header carrying LogPos=0. The outer payload coordinates are:
// INSERT 236..432, UPDATE 511..711, DELETE 790..975 (frozen with the file).
func TestParseFile_compressedFixture(t *testing.T) {
	tm := &metadata.TableMeta{
		Schema: "payloadtest",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "name", OrdinalPosition: 2, DataType: "varchar"},
		},
		PKColumns: []string{"id"},
	}
	resolver := metadata.NewResolverFromTables(3, map[string]*metadata.TableMeta{"payloadtest.orders": tm})

	p := New("testdata", resolver, Filters{Schemas: map[string]bool{"payloadtest": true}}, nil)

	events := make(chan Event, 20)
	errCh := make(chan error, 1)
	go func() {
		defer close(events)
		errCh <- p.ParseFile(context.Background(), "compressed_mysql8046.binlog", events)
	}()
	var got []Event
	for ev := range events {
		got = append(got, ev)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("ParseFile: %v", err)
	}

	// 2 INSERT + 1 UPDATE + 1 DELETE; zero is exactly the dropped-payload bug.
	if len(got) != 4 {
		t.Fatalf("expected 4 events from the compressed fixture, got %d", len(got))
	}

	want := []struct {
		evType   EventType
		pk       string
		startPos uint64
		endPos   uint64
	}{
		{EventInsert, "1", 236, 432},
		{EventInsert, "2", 236, 432},
		{EventUpdate, "1", 511, 711},
		{EventDelete, "2", 790, 975},
	}
	for i, w := range want {
		ev := got[i]
		if ev.EventType != w.evType {
			t.Errorf("event[%d]: EventType = %d, want %d", i, ev.EventType, w.evType)
		}
		if ev.PKValues != w.pk {
			t.Errorf("event[%d]: PKValues = %q, want %q", i, ev.PKValues, w.pk)
		}
		// Outer payload coordinates — inner headers carry LogPos=0, so an
		// unrewritten event would show StartPos ≈ 2^64.
		if ev.StartPos != w.startPos || ev.EndPos != w.endPos {
			t.Errorf("event[%d]: positions = [%d, %d], want outer [%d, %d]",
				i, ev.StartPos, ev.EndPos, w.startPos, w.endPos)
		}
		if ev.ConnectionID == 0 {
			t.Errorf("event[%d]: ConnectionID = 0, want pseudo_thread_id from inner BEGIN", i)
		}
		// Inner commit-time timestamps survive dispatch (frozen with the file).
		if ev.Timestamp.Before(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("event[%d]: Timestamp = %v, want real 2026 commit time", i, ev.Timestamp)
		}
	}
	if got[2].RowBefore == nil || got[2].RowAfter == nil {
		t.Error("UPDATE event: expected both before and after images")
	}
	if got[3].RowBefore == nil || got[3].RowAfter != nil {
		t.Error("DELETE event: expected only the before image")
	}
}
