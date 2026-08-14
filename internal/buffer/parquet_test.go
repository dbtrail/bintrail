package buffer

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver — exercises the real read path
	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
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
	buf := New(Config{MaxAge: 6 * time.Hour})
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

// TestWriteParquet_connectionIDUnsignedDuckDBScan pins the fail-loud regression
// fix on the buffer write path: ResultRow.ConnectionID is *uint32 and is written
// via FormatUint, so a value above int32's 2147483647 (a real CONNECTION_ID())
// must round-trip through the unsigned-widened connection_id column rather than
// abort the BYOS flush. Read back via DuckDB parquet_scan — the real consumer.
func TestWriteParquet_connectionIDUnsignedDuckDBScan(t *testing.T) {
	for _, connID := range []uint32{4294967295, 3000000000} {
		t.Run(strconv.FormatUint(uint64(connID), 10), func(t *testing.T) {
			dir := t.TempDir()
			outPath := filepath.Join(dir, "conn.parquet")

			cid := connID
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
				ConnectionID:   &cid,
				RowAfter:       map[string]any{"id": 1},
			}}

			n, err := WriteParquet(rows, outPath, "none")
			if err != nil {
				t.Fatalf("WriteParquet(connection_id=%d): got error, want round-trip (fail-loud regression): %v", connID, err)
			}
			if n != 1 {
				t.Fatalf("count = %d, want 1", n)
			}

			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			defer db.Close()

			safePath := strings.ReplaceAll(outPath, "'", "''")
			var conn any
			if err := db.QueryRowContext(context.Background(),
				"SELECT connection_id FROM parquet_scan('"+safePath+"')").Scan(&conn); err != nil {
				t.Fatalf("scan connection_id: %v", err)
			}
			got, ok := conn.(int64) // INT UNSIGNED widened to signed Int64 → DuckDB BIGINT
			if !ok {
				t.Fatalf("connection_id scanned as %T, want int64", conn)
			}
			if got != int64(connID) {
				t.Errorf("connection_id = %d, want %d", got, connID)
			}
		})
	}
}

// TestWriteParquet_queryTextRoundTrip pins #699 on the buffer write path:
// a captured statement round-trips through the query_text column, and a row
// without one reads back NULL (not empty string). Read back via DuckDB
// parquet_scan — the real consumer.
func TestWriteParquet_queryTextRoundTrip(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "qtext.parquet")

	stmt := "INSERT INTO db.t (id) VALUES (1)"
	withText := query.ResultRow{
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
		QueryText:      &stmt,
	}
	withoutText := withText
	withoutText.EventID = idOffset + 2
	withoutText.PKValues = "2"
	withoutText.QueryText = nil

	n, err := WriteParquet([]query.ResultRow{withText, withoutText}, outPath, "none")
	if err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}
	if n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	safePath := strings.ReplaceAll(outPath, "'", "''")
	rows, err := db.QueryContext(context.Background(),
		"SELECT query_text FROM parquet_scan('"+safePath+"') ORDER BY event_id")
	if err != nil {
		t.Fatalf("scan query_text: %v", err)
	}
	defer rows.Close()

	var got []sql.NullString
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	if len(got) != 2 {
		t.Fatalf("rows = %d, want 2", len(got))
	}
	if !got[0].Valid || got[0].String != stmt {
		t.Errorf("row 1 query_text = %+v, want %q", got[0], stmt)
	}
	if got[1].Valid {
		t.Errorf("row 2 query_text = %q, want NULL (statement not captured)", got[1].String)
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

// TestWriteParquet_positionsAbove2to63 pins the buffer's write half of #1218:
// rowToParquet always rendered StartPos/EndPos via FormatUint, so before the
// archive schema widened those columns to unsigned, a buffered event carrying
// the #986/#1117 underflow shape (>2^63) aborted WriteParquet at conversion.
// Now it must round-trip exactly through the real consumer, parquetquery.Fetch.
func TestWriteParquet_positionsAbove2to63(t *testing.T) {
	const (
		bigStart = uint64(18446744073709551516) // 2^64 - 100
		bigEnd   = uint64(18446744073709551615) // max BIGINT UNSIGNED
	)
	dir := t.TempDir()
	outPath := filepath.Join(dir, "buffer.parquet")

	rows := []query.ResultRow{{
		EventID:        1,
		BinlogFile:     "mariadb-bin.000001",
		StartPos:       bigStart,
		EndPos:         bigEnd,
		EventTimestamp: time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
		SchemaName:     "mydb",
		TableName:      "orders",
		EventType:      event.EventInsert,
		PKValues:       "1",
	}}
	n, err := WriteParquet(rows, outPath, "none")
	if err != nil {
		t.Fatalf("WriteParquet(positions>2^63): %v", err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}

	got, err := parquetquery.Fetch(context.Background(), query.Options{Limit: 10}, dir)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("rows = %d, want 1", len(got))
	}
	if got[0].StartPos != bigStart || got[0].EndPos != bigEnd {
		t.Errorf("positions = [%d, %d], want [%d, %d] (exact, no wrap)",
			got[0].StartPos, got[0].EndPos, bigStart, bigEnd)
	}
}
