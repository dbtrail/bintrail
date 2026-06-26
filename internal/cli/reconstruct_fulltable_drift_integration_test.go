//go:build integration

package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunReconstruct_fullTable_columnAddedAfterBaseline_failsLoud is the
// end-to-end regression for #602. It drives the REAL read path
// (query.FetchMerged → UnmarshalRowImage), not a hand-built change map, so it
// locks the assumption the fix rests on: a column ADDED to the source after
// the baseline shows up as a verbatim key in the delta event's row_after. If a
// future refactor ever inserts a schema-projection step before the change map
// is built, postBaselineColumns would silently become dead code and re-open
// the data-loss bug — this test would catch that.
//
// Setup mirrors TestRunReconstruct_fullTableRoundTrip but minimal (no archive):
// a baseline with columns (id, status); then a `note` column is ADDed and an
// existing row UPDATEd so its row_after carries id, status AND note. The run
// must fail loud naming `note` and leave no chunk file on disk.
func TestRunReconstruct_fullTable_columnAddedAfterBaseline_failsLoud(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// schema_snapshots so the resolver finds the table columns (id, status) and
	// its PK (id). Note the `note` column is intentionally NOT in the snapshot
	// or the baseline —
	// it exists only in the captured delta event, exactly like a column added
	// after the baseline that the resolver/baseline never saw.
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'testdb', 'orders', 'id', 1, 'PRI', 'int', 'NO', 0)`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'testdb', 'orders', 'status', 2, '', 'varchar', 'NO', 0)`)

	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"

	baselineDir := t.TempDir()
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	snapshotTS := h1
	snapshotTSDir := strings.ReplaceAll(snapshotTS.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, snapshotTSDir, "testdb")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	baselinePath := filepath.Join(parquetDir, "orders.parquet")

	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	bw, err := baseline.NewWriter(baselinePath, cols, baseline.WriterConfig{
		Compression:  "zstd",
		RowGroupSize: 100,
		Metadata:     map[string]string{baseline.MetaKeyCreateTableSQL: createSQL},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, row := range [][]string{{"1", "start-1"}, {"2", "start-2"}} {
		if err := bw.WriteRow(row, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	// id=2 UPDATEd at h2 after `note` was ADDed: row_after carries the new
	// column. note ∉ baseline columns (id, status).
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts2, nil,
		"testdb", "orders", 2 /* UPDATE */, "2", nil,
		[]byte(`{"id":2,"status":"start-2"}`),
		[]byte(`{"id":2,"status":"shipped","note":"gift-wrap"}`))

	// ── Drive runReconstructFullTable via flag vars ──────────────────────
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	savedOutputFormat := recOutputFormat
	savedOutputDir := recOutputDir
	savedTables := recTables
	savedChunkSize := recChunkSize
	savedParallelism := recParallelism
	t.Cleanup(func() {
		recOutputFormat = savedOutputFormat
		recOutputDir = savedOutputDir
		recTables = savedTables
		recChunkSize = savedChunkSize
		recParallelism = savedParallelism
	})

	outputDir := t.TempDir()
	recIndexDSN = testutil.SnapshotDSN(dbName)
	recBaselineDir = baselineDir
	recBaselineS3 = ""
	recAllowGaps = false
	recNoArchive = false // full-table ignores this flag; gap check stays active
	recOutputFormat = "mydumper"
	recOutputDir = outputDir
	recTables = "testdb.orders"
	recChunkSize = "256MB"
	recParallelism = 1
	recAt = h2.Add(45 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	err = runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatalf("expected a fail-loud error for the post-baseline 'note' column, got nil")
	}
	if !strings.Contains(err.Error(), "note") {
		t.Errorf("error should name the dropped column %q, got: %v", "note", err)
	}

	// No partial output: the guard fires before the writer opens.
	entries, derr := os.ReadDir(outputDir)
	if derr != nil {
		t.Fatalf("read output dir: %v", derr)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			t.Errorf("partial output left on disk after failed run: %s", e.Name())
		}
	}
}
