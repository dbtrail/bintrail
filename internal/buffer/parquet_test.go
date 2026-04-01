package buffer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

func TestWriteParquet_empty(t *testing.T) {
	n, err := WriteParquet(nil, "/dev/null", "none")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("count = %d, want 0", n)
	}
}

func TestWriteParquet_roundTrip(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "buffer.parquet")

	// Build rows from buffer.
	buf := New(6*time.Hour, nil)
	base := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	buf.Insert(makeEvents(3, "mydb", "users", base))
	buf.Insert([]parser.Event{makeUpdate("mydb", "orders", "42", base)})

	rows := buf.Snapshot()
	n, err := WriteParquet(rows, outPath, "none")
	if err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}
	if n != 4 {
		t.Errorf("count = %d, want 4", n)
	}

	// Read back and verify.
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rf.Close()
	info, _ := rf.Stat()
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	if pf.NumRows() != 4 {
		t.Errorf("NumRows = %d, want 4", pf.NumRows())
	}

	// Verify metadata.
	if _, ok := pf.Lookup("bintrail.buffer.version"); !ok {
		t.Error("expected bintrail.buffer.version metadata key")
	}
}

func TestWriteParquet_nullableFields(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "nulls.parquet")

	// INSERT event: row_before is nil, GTID empty.
	rows := []query.ResultRow{{
		EventID:        idOffset + 1,
		BinlogFile:     "binlog.000001",
		StartPos:       100,
		EndPos:         200,
		EventTimestamp: time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
		SchemaName:     "db",
		TableName:      "t",
		EventType:      parser.EventInsert,
		PKValues:       "1",
		RowAfter:       map[string]any{"id": 1},
		// GTID nil, RowBefore nil, ChangedColumns nil
	}}

	n, err := WriteParquet(rows, outPath, "none")
	if err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}
	if n != 1 {
		t.Errorf("count = %d, want 1", n)
	}

	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rf.Close()
	info, _ := rf.Stat()
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if pf.NumRows() != 1 {
		t.Errorf("NumRows = %d, want 1", pf.NumRows())
	}
}
