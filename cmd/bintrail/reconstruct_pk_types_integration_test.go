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

// TestRunReconstruct_fullTableRoundTrip_datetimePK is the #212 regression
// test: full-table reconstruct against a table whose primary key is a
// DATETIME column. Before the PK canonicalizer (#212), DuckDB parquet_scan
// returned time.Time for the DATETIME column while the indexer stored
// pk_values as the go-mysql-formatted string "2026-04-11 14:30:45" — the
// keys diverged and every event silently missed the baseline, producing
// a dump with DUPLICATE rows (baseline row + event row for the same PK)
// or missing deletions.
//
// After the fix, canonicalizePKValue normalises the DuckDB-side time.Time
// to the same string format as the indexer, so the merge produces the
// expected final state.
func TestRunReconstruct_fullTableRoundTrip_datetimePK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// ── 1. Populate schema_snapshots for the DATETIME-PK table ────────────
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkdatetime', 'events', 'created_at', 1, 'PRI', 'datetime', 'NO', 0)`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkdatetime', 'events', 'payload', 2, '', 'varchar', 'NO', 0)`)

	// ── 2. Write a baseline Parquet with DATETIME pk values ──────────────
	createSQL := "CREATE TABLE `events` (\n" +
		"  `created_at` DATETIME NOT NULL,\n" +
		"  `payload` VARCHAR(128) NOT NULL,\n" +
		"  PRIMARY KEY (`created_at`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"

	pkT1 := "2026-04-10 10:00:00"
	pkT2 := "2026-04-10 10:01:00"
	pkT3 := "2026-04-10 10:02:00"
	pkT4 := "2026-04-10 10:03:00" // new insert via event

	baselineDir := t.TempDir()
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	snapshotTSDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, snapshotTSDir, "pkdatetime")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	baselinePath := filepath.Join(parquetDir, "events.parquet")

	cols := []baseline.Column{
		{Name: "created_at", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "payload", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
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
	for _, row := range [][]string{
		{pkT1, "payload-t1"},
		{pkT2, "payload-t2"},
		{pkT3, "payload-t3"},
	} {
		if err := bw.WriteRow(row, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	// ── 3. Set up partitions and insert events ──────────────────────────
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")

	// UPDATE t2 payload (in archived h1)
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"pkdatetime", "events", 2 /* UPDATE */, pkT2, nil,
		[]byte(`{"created_at":"2026-04-10 10:01:00","payload":"payload-t2"}`),
		[]byte(`{"created_at":"2026-04-10 10:01:00","payload":"payload-t2-updated"}`))
	// DELETE t3 (in archived h1)
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts1, nil,
		"pkdatetime", "events", 3 /* DELETE */, pkT3, nil,
		[]byte(`{"created_at":"2026-04-10 10:02:00","payload":"payload-t3"}`),
		nil)
	// INSERT t4 (in live h2)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts2, nil,
		"pkdatetime", "events", 1 /* INSERT */, pkT4, nil,
		nil,
		[]byte(`{"created_at":"2026-04-10 10:03:00","payload":"payload-t4-new"}`))

	// ── 4. Archive h1 and drop it from live MySQL ───────────────────────
	archiveDir := t.TempDir()
	bintrailID := "test-212-datetime-pk"
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

	// ── 5. Run full-table reconstruct ───────────────────────────────────
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
	recNoArchive = false
	recOutputFormat = "mydumper"
	recOutputDir = outputDir
	recTables = "pkdatetime.events"
	recChunkSize = "256MB"
	recParallelism = 1
	recAt = h2.Add(30 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	if err := runReconstruct(reconstructCmd, nil); err != nil {
		t.Fatalf("runReconstruct: %v", err)
	}

	// ── 6. Apply the dump and verify the merged state ───────────────────
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `pkdatetime`")
	testutil.MustExec(t, db, "CREATE DATABASE `pkdatetime`")
	t.Cleanup(func() {
		testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `pkdatetime`")
	})

	schemaSQL, err := os.ReadFile(filepath.Join(outputDir, "pkdatetime.events-schema.sql"))
	if err != nil {
		t.Fatalf("read schema file: %v", err)
	}
	testutil.MustExec(t, db, "USE `pkdatetime`")
	testutil.MustExec(t, db, string(schemaSQL))

	chunkSQL, err := os.ReadFile(filepath.Join(outputDir, "pkdatetime.events.00000.sql"))
	if err != nil {
		t.Fatalf("read chunk file: %v", err)
	}
	testutil.MustExec(t, db, string(chunkSQL))

	rows, err := db.Query("SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), payload FROM `pkdatetime`.`events` ORDER BY created_at")
	if err != nil {
		t.Fatalf("select restored: %v", err)
	}
	defer rows.Close()

	type restoredRow struct {
		CreatedAt string
		Payload   string
	}
	var got []restoredRow
	for rows.Next() {
		var r restoredRow
		if err := rows.Scan(&r.CreatedAt, &r.Payload); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// Expected final state:
	//   t1: passthrough from baseline
	//   t2: baseline → UPDATE (archived event) — payload-t2-updated
	//   t3: DELETED (not present)
	//   t4: INSERTED by live event — payload-t4-new
	want := []restoredRow{
		{pkT1, "payload-t1"},
		{pkT2, "payload-t2-updated"},
		{pkT4, "payload-t4-new"},
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

// TestRunReconstruct_fullTableRoundTrip_varcharPK covers VARCHAR primary
// keys end-to-end. VARCHAR PKs already worked in v1 (#187) because both
// sides return Go string, but #187's integration test only covered INT
// PKs — this test closes the obvious gap and pins the contract.
func TestRunReconstruct_fullTableRoundTrip_varcharPK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkvarchar', 'tenants', 'slug', 1, 'PRI', 'varchar', 'NO', 0)`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkvarchar', 'tenants', 'name', 2, '', 'varchar', 'NO', 0)`)

	createSQL := "CREATE TABLE `tenants` (\n" +
		"  `slug` VARCHAR(64) NOT NULL,\n" +
		"  `name` VARCHAR(128) NOT NULL,\n" +
		"  PRIMARY KEY (`slug`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"

	baselineDir := t.TempDir()
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	snapshotTSDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, snapshotTSDir, "pkvarchar")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	baselinePath := filepath.Join(parquetDir, "tenants.parquet")

	cols := []baseline.Column{
		{Name: "slug", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
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
	for _, row := range [][]string{{"acme", "Acme Inc"}, {"foobar", "FooBar Ltd"}} {
		if err := bw.WriteRow(row, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")

	// UPDATE foobar → "FooBar Renamed", INSERT newco.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"pkvarchar", "tenants", 2 /* UPDATE */, "foobar", nil,
		[]byte(`{"slug":"foobar","name":"FooBar Ltd"}`),
		[]byte(`{"slug":"foobar","name":"FooBar Renamed"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil,
		"pkvarchar", "tenants", 1 /* INSERT */, "newco", nil,
		nil,
		[]byte(`{"slug":"newco","name":"NewCo"}`))

	archiveDir := t.TempDir()
	bintrailID := "test-212-varchar-pk"
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
		VALUES (?, ?, ?, 1, NULL, NULL, NULL)`,
		partitionName(h1), bintrailID, outPath)
	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` DROP PARTITION `%s`",
		dbName, partitionName(h1),
	))

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
	recNoArchive = false
	recOutputFormat = "mydumper"
	recOutputDir = outputDir
	recTables = "pkvarchar.tenants"
	recChunkSize = "256MB"
	recParallelism = 1
	recAt = h2.Add(30 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	if err := runReconstruct(reconstructCmd, nil); err != nil {
		t.Fatalf("runReconstruct: %v", err)
	}

	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `pkvarchar`")
	testutil.MustExec(t, db, "CREATE DATABASE `pkvarchar`")
	t.Cleanup(func() {
		testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `pkvarchar`")
	})

	schemaSQL, err := os.ReadFile(filepath.Join(outputDir, "pkvarchar.tenants-schema.sql"))
	if err != nil {
		t.Fatalf("read schema file: %v", err)
	}
	testutil.MustExec(t, db, "USE `pkvarchar`")
	testutil.MustExec(t, db, string(schemaSQL))

	chunkSQL, err := os.ReadFile(filepath.Join(outputDir, "pkvarchar.tenants.00000.sql"))
	if err != nil {
		t.Fatalf("read chunk file: %v", err)
	}
	testutil.MustExec(t, db, string(chunkSQL))

	rows, err := db.Query("SELECT slug, name FROM `pkvarchar`.`tenants` ORDER BY slug")
	if err != nil {
		t.Fatalf("select restored: %v", err)
	}
	defer rows.Close()

	type tenant struct {
		Slug string
		Name string
	}
	var got []tenant
	for rows.Next() {
		var r tenant
		if err := rows.Scan(&r.Slug, &r.Name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []tenant{
		{"acme", "Acme Inc"},          // passthrough
		{"foobar", "FooBar Renamed"},  // updated
		{"newco", "NewCo"},            // new insert
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
