//go:build integration

package parquetquery

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestQueryFileList_globalTopNAcrossFiles proves the ultrafast S3-direct path's
// core correctness claim: queryFileList (the multi-file scan that fetchS3Direct
// runs after its httpfs/region setup) applies a GLOBAL ORDER BY + LIMIT (top-N
// across all files), not a per-file limit. Two disjoint LOCAL parquet files
// stand in for S3 objects — the query logic is identical; only the httpfs
// transport + region pinning differ, and those need a live S3/minio (a
// documented gap; SQL construction is covered by the unit tests). The test
// exercises queryFileList directly so it does not trigger INSTALL httpfs (which
// can stall on a runner that can't reach the extension registry).
func TestQueryFileList_globalTopNAcrossFiles(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	dir := t.TempDir()

	// File 1: events at 14:00 (pk 1) and 16:00 (pk 3).
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, "2026-02-19 14:00:00", nil, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, "2026-02-19 16:00:00", nil, "mydb", "orders", 3, "3", nil, nil, []byte(`{"id":3}`))
	file1 := filepath.Join(dir, "f1.parquet")
	if _, err := archive.ArchivePartition(context.Background(), db, dbName, "p_future", file1, "none"); err != nil {
		t.Fatalf("archive file1: %v", err)
	}

	// Reset, then File 2: a single event at 15:00 (pk 2) — chronologically
	// BETWEEN file1's two events, so a correct global top-2 must pick it over
	// file1's 16:00 row.
	if _, err := db.Exec("DELETE FROM binlog_events"); err != nil {
		t.Fatalf("reset events: %v", err)
	}
	testutil.InsertEvent(t, db, "binlog.000002", 100, 200, "2026-02-19 15:00:00", nil, "mydb", "orders", 2, "2", nil, nil, []byte(`{"id":2}`))
	file2 := filepath.Join(dir, "f2.parquet")
	if _, err := archive.ArchivePartition(context.Background(), db, dbName, "p_future", file2, "none"); err != nil {
		t.Fatalf("archive file2: %v", err)
	}

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	// Apply the ultrafast tuning (threads/memory unset → DuckDB parallelizes the
	// multi-file scan), the realistic condition for this path.
	duckdbutil.Ultrafast().Apply(context.Background(), ddb)

	// LIMIT 2 ascending must return the two GLOBALLY earliest events — pk 1
	// (14:00, file1) and pk 2 (15:00, file2) — NOT file1's own first two
	// (pk 1, pk 3), which is what a per-file limit would have produced.
	rows, err := queryFileList(context.Background(), ddb, []string{file1, file2},
		query.Options{Schema: "mydb", Table: "orders", Limit: 2})
	if err != nil {
		t.Fatalf("queryFileList: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows under LIMIT 2, got %d", len(rows))
	}
	if rows[0].PKValues != "1" || rows[1].PKValues != "2" {
		t.Errorf("global top-2 ASC pk = [%s %s], want [1 2] (14:00 file1, 15:00 file2) — a per-file limit would wrongly give [1 3]",
			rows[0].PKValues, rows[1].PKValues)
	}
}
