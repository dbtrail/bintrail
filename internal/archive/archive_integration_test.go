//go:build integration

package archive

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestArchivePartition_nullBinlogFile is the rotate-path counterpart to
// query.TestFetch_nullBinlogFile (dbtrail/bintrail#318). Before the fix,
// ArchivePartition crashed at Scan when any row in the partition had a NULL
// binlog_file — same root cause as the query crash, but the consequence is
// worse: rotate failures block partition pruning and grow the index
// unbounded.
func TestArchivePartition_nullBinlogFile(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Relax the NOT NULL on binlog_file to simulate a drifted customer schema.
	if _, err := db.Exec(`ALTER TABLE binlog_events MODIFY binlog_file VARCHAR(255) NULL`); err != nil {
		t.Fatalf("ALTER TABLE failed: %v", err)
	}

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1",
		nil, nil, []byte(`{"id":1}`))
	if _, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, gtid,
		 schema_name, table_name, event_type, pk_values,
		 changed_columns, row_before, row_after)
		VALUES (NULL, 0, 0, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?)`,
		ts, "mydb", "orders", 1, "2", []byte(`{"id":2}`),
	); err != nil {
		t.Fatalf("insert NULL-binlog_file row failed: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "p_future.parquet")
	n, err := ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none")
	if err != nil {
		t.Fatalf("ArchivePartition failed: %v", err)
	}
	if n != 2 {
		t.Errorf("rowCount = %d, want 2", n)
	}

	// Verify NULL semantics at the Parquet layer: row 0 (the non-NULL row,
	// inserted first → lower event_id → sorted first by ORDER BY event_id)
	// must carry "binlog.000001"; row 1 must be a real Parquet NULL, not
	// an empty string. The nulls[1]=!binlogFile.Valid line in archive.go
	// is what makes this true — testing it specifically guards against a
	// regression that smuggles "" into the column.
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open archived parquet: %v", err)
	}
	defer rf.Close()
	info, _ := rf.Stat()
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	binlogFileIdx := parquetColumnIndex(t, pf, "binlog_file")
	reader := parquet.NewReader(pf)
	defer reader.Close()
	parquetRows := make([]parquet.Row, 2)
	if rn, err := reader.ReadRows(parquetRows); err != nil || rn != 2 {
		t.Fatalf("ReadRows returned (%d, %v), want (2, nil)", rn, err)
	}
	if parquetRows[0][binlogFileIdx].IsNull() {
		t.Errorf("row 0 binlog_file: got NULL, want \"binlog.000001\"")
	} else if got := parquetRows[0][binlogFileIdx].String(); got != "binlog.000001" {
		t.Errorf("row 0 binlog_file: got %q, want \"binlog.000001\"", got)
	}
	if !parquetRows[1][binlogFileIdx].IsNull() {
		t.Errorf("row 1 binlog_file: got %q, want NULL", parquetRows[1][binlogFileIdx].String())
	}
}
