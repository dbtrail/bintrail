//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/archive"
	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/testutil"
)

// TestRunReconstruct_fullTableRoundTrip is the end-to-end regression test for
// #187: build a baseline + live events + an archived-and-dropped partition,
// run `bintrail reconstruct --output-format mydumper`, then apply the
// generated CREATE TABLE + INSERT files against a fresh table and verify the
// restored row set matches the expected merged state.
//
// This is the functional guarantee the feature promises: "the dump directory
// is restorable with a plain mysql client and produces the correct point-in-
// time state."
func TestRunReconstruct_fullTableRoundTrip(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// ── 1. Populate schema_snapshots so the resolver can find PK columns ──
	// parser.BuildPKValues needs ColumnMeta entries for the test table so
	// it encodes baseline rows with the same key format as the indexer.
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'testdb', 'orders', 'id', 1, 'PRI', 'int', 'NO', 0)`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'testdb', 'orders', 'status', 2, '', 'varchar', 'NO', 0)`)

	// ── 2. Write a minimal baseline Parquet with embedded CREATE TABLE ───
	// The baseline captures rows {1,'start-1'}, {2,'start-2'}, {3,'start-3'}.
	// The CreateTableSQL has no schema prefix so we can run it against the
	// target DB with USE.
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"

	baselineDir := t.TempDir()
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	snapshotTS := h1 // anchor at h1 so the fetch range is tight
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
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: createSQL,
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, row := range [][]string{{"1", "start-1"}, {"2", "start-2"}, {"3", "start-3"}} {
		if err := bw.WriteRow(row, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	// ── 3. Set up partitions and insert events ──────────────────────────
	// Event matrix:
	//   id=2 UPDATE (h1) start-2 → paid   (will be in archive)
	//   id=3 DELETE (h1)         start-3  (will be in archive)
	//   id=4 INSERT (h2) new-4            (live; not in baseline)
	//   id=2 UPDATE (h2) paid    → shipped (live; latest event wins for id=2)
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testdb", "orders", 2 /* UPDATE */, "2", nil,
		[]byte(`{"id":2,"status":"start-2"}`),
		[]byte(`{"id":2,"status":"paid"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts1, nil,
		"testdb", "orders", 3 /* DELETE */, "3", nil,
		[]byte(`{"id":3,"status":"start-3"}`),
		nil)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts2, nil,
		"testdb", "orders", 1 /* INSERT */, "4", nil,
		nil,
		[]byte(`{"id":4,"status":"new-4"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, ts2, nil,
		"testdb", "orders", 2 /* UPDATE */, "2", nil,
		[]byte(`{"id":2,"status":"paid"}`),
		[]byte(`{"id":2,"status":"shipped"}`))

	// ── 4. Archive h1 and drop it from live MySQL ────────────────────────
	archiveDir := t.TempDir()
	bintrailID := "test-187-roundtrip"
	outPath, err := hiveArchivePath(archiveDir, bintrailID, partitionName(h1))
	if err != nil {
		t.Fatalf("hiveArchivePath: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}
	if _, err := archive.ArchivePartition(context.Background(), db, dbName, partitionName(h1), outPath, "zstd"); err != nil {
		t.Fatalf("ArchivePartition: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES (?, ?, ?, 2, NULL, NULL, NULL)`,
		partitionName(h1), bintrailID, outPath)
	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` DROP PARTITION `%s`",
		dbName, partitionName(h1),
	))

	// ── 5. Drive runReconstructFullTable via flag vars ───────────────────
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	// #187 uses a separate set of flag variables, all of which we must reset
	// after the test. Save them explicitly.
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
	recNoArchive = false
	recOutputFormat = "mydumper"
	recOutputDir = outputDir
	recTables = "testdb.orders"
	recChunkSize = "256MB"
	recParallelism = 1
	// --at lands strictly inside h2 so the planner classifies hours {h1, h2}
	// without spilling into h2+1h (which would be a gap).
	recAt = h2.Add(30 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	if err := runReconstruct(reconstructCmd, nil); err != nil {
		t.Fatalf("runReconstruct: %v", err)
	}

	// ── 6. Inspect the output directory ──────────────────────────────────
	schemaFile := filepath.Join(outputDir, "testdb.orders-schema.sql")
	if _, err := os.Stat(schemaFile); err != nil {
		t.Fatalf("expected schema file at %s: %v", schemaFile, err)
	}
	chunkFile := filepath.Join(outputDir, "testdb.orders.00000.sql")
	if _, err := os.Stat(chunkFile); err != nil {
		t.Fatalf("expected chunk file at %s: %v", chunkFile, err)
	}
	metadataFile := filepath.Join(outputDir, "metadata")
	if _, err := os.Stat(metadataFile); err != nil {
		t.Fatalf("expected metadata file at %s: %v", metadataFile, err)
	}

	// ── 7. Apply the dump to a fresh destination within the same DB ──────
	// Create a second database for restore so we don't clash with the
	// bintrail index tables. The SQL chunk references `testdb.orders`, so
	// we use testdb as the restore DB name.
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `testdb`")
	testutil.MustExec(t, db, "CREATE DATABASE `testdb`")
	t.Cleanup(func() {
		testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `testdb`")
	})

	schemaSQL, err := os.ReadFile(schemaFile)
	if err != nil {
		t.Fatalf("read schema file: %v", err)
	}
	// The schema SQL references the unqualified `orders` table, so we must
	// USE testdb before executing it.
	testutil.MustExec(t, db, "USE `testdb`")
	testutil.MustExec(t, db, string(schemaSQL))

	chunkSQL, err := os.ReadFile(chunkFile)
	if err != nil {
		t.Fatalf("read chunk file: %v", err)
	}
	// Execute the INSERT chunk. The INSERTs are schema-qualified
	// (`testdb`.`orders`) so they work regardless of the current database.
	testutil.MustExec(t, db, string(chunkSQL))

	// ── 8. Read the restored rows and assert the merged state ───────────
	rows, err := db.Query("SELECT id, status FROM `testdb`.`orders` ORDER BY id")
	if err != nil {
		t.Fatalf("select restored: %v", err)
	}
	defer rows.Close()

	type restoredRow struct {
		ID     int
		Status string
	}
	var got []restoredRow
	for rows.Next() {
		var r restoredRow
		if err := rows.Scan(&r.ID, &r.Status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// Expected final state:
	//   id=1: passthrough from baseline, "start-1"
	//   id=2: baseline + UPDATE h1 (→paid) + UPDATE h2 (→shipped), last wins
	//   id=3: deleted (absent)
	//   id=4: new INSERT (live), "new-4"
	want := []restoredRow{
		{1, "start-1"},
		{2, "shipped"},
		{4, "new-4"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d; got=%+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("row %d: got %+v, want %+v", i, got[i], w)
		}
	}
}
