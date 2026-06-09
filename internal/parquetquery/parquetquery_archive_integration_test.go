//go:build integration

package parquetquery_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestFetch_archivedNullBinlogFile closes the dbtrail/bintrail#318 loop
// end-to-end: drift in the MySQL index → bintrail rotate writes Parquet
// containing a real NULL at column 1 → parquetquery.Fetch reads the
// archive back via DuckDB and returns the rows without crashing. Before
// the scanRows fix in this file, the consumer-side bug was latent
// (archive.ArchivePartition crashed before producing a NULL-bearing
// Parquet); the archive.go fix in this same PR turned it into a live
// regression by correctly preserving the NULL through to the writer.
func TestFetch_archivedNullBinlogFile(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

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
	if _, err := archive.ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none"); err != nil {
		t.Fatalf("ArchivePartition failed: %v", err)
	}

	rows, err := parquetquery.Fetch(context.Background(),
		query.Options{Schema: "mydb", Table: "orders", Limit: 100},
		outPath)
	if err != nil {
		t.Fatalf("parquetquery.Fetch failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Rows from parquetquery come back ordered by event_timestamp, event_id
	// (matches the MySQL-side Fetch ordering). The non-NULL row was inserted
	// first → lower event_id → first in results.
	if rows[0].BinlogFile != "binlog.000001" {
		t.Errorf("row 0: expected BinlogFile=binlog.000001, got %q", rows[0].BinlogFile)
	}
	if rows[1].BinlogFile != "" {
		t.Errorf("row 1: expected BinlogFile=\"\" for NULL column, got %q", rows[1].BinlogFile)
	}
}
