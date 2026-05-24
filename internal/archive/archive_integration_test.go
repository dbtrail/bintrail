//go:build integration

package archive

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dbtrail/bintrail/internal/testutil"
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
}
