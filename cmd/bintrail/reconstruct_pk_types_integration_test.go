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
	// column_type is "datetime" (precision 0) — the canonicalizer parses
	// that to dec=0 and emits "YYYY-MM-DD HH:MM:SS" without fraction.
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkdatetime', 'events', 'created_at', 1, 'PRI', 'datetime', 'datetime', 'NO', 0)`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkdatetime', 'events', 'payload', 2, '', 'varchar', 'varchar(128)', 'NO', 0)`)

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
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkvarchar', 'tenants', 'slug', 1, 'PRI', 'varchar', 'varchar(64)', 'NO', 0)`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkvarchar', 'tenants', 'name', 2, '', 'varchar', 'varchar(128)', 'NO', 0)`)

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

// TestRunReconstruct_rejectsDecimalPK is the call-site test for the
// unsupported-PK-type hard-error path in ReconstructTable. The canonicalizer
// unit tests cover supportedPKType as a pure predicate, but this test pins
// that the guard actually fires at the start of the full-table reconstruct
// flow — a future refactor that reorders the validation block can't regress
// to the pre-#212 silent-wrong-output behaviour without tripping this test.
//
// We build a schema_snapshots row with data_type=decimal and run the CLI
// dispatcher. No baseline is needed because the reconstruct must fail
// before any baseline work happens.
func TestRunReconstruct_rejectsDecimalPK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// schema_snapshots row with a DECIMAL PK.
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkdecimal', 'prices', 'amount', 1, 'PRI', 'decimal', 'decimal(10,2)', 'NO', 0)`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkdecimal', 'prices', 'label', 2, '', 'varchar', 'varchar(64)', 'NO', 0)`)

	// We need a baseline directory to pass the CLI validation, but the
	// reconstruct will fail before it ever reads the baseline because
	// the PK type check fires at ReconstructTable entry.
	baselineDir := t.TempDir()
	h1 := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Hour)
	snapshotTSDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, snapshotTSDir, "pkdecimal")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	// Write a minimal (even invalid) baseline file so FindBaseline succeeds.
	// Since the PK type check should abort BEFORE baseline read, the
	// contents don't matter.
	cols := []baseline.Column{
		{Name: "amount", MySQLType: "decimal", ParquetType: baseline.MysqlToParquetNode("decimal")},
		{Name: "label", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "prices.parquet"), cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 10,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: "CREATE TABLE `prices` (...)",
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

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
	recTables = "pkdecimal.prices"
	recChunkSize = "256MB"
	recParallelism = 1

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	err = runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for DECIMAL PK, got nil")
	}
	if !strings.Contains(err.Error(), "decimal") {
		t.Errorf("expected error to mention 'decimal', got: %v", err)
	}
	if !strings.Contains(err.Error(), "not in the supported") {
		t.Errorf("expected error to mention unsupported set, got: %v", err)
	}

	// No output files should be written — the reconstruct bailed out
	// before any mydumper writer was opened.
	entries, readErr := os.ReadDir(outputDir)
	if readErr != nil {
		t.Fatalf("read output dir: %v", readErr)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("output dir should be empty on PK-type rejection, got: %v", names)
	}

	// Use the dbName variable so the compiler doesn't complain.
	_ = dbName
}

// TestRunReconstruct_fullTableRoundTrip_datetime6PK is the post-review
// regression test for the precision-aware fix. A DATETIME(6) PK with
// whole-second values was silently broken by the v1 canonicalizer's
// Nanosecond()==0 heuristic: the indexer stored "14:30:45.000000" but the
// canonicalizer emitted "14:30:45" without the microsecond tail, causing
// every baseline row to mismatch the change map.
//
// After the fix, ColumnType "datetime(6)" tells the canonicalizer to
// always format with the full 6-digit fractional tail, whether the value
// has a fractional part or not. The test intentionally mixes whole-second
// (t1, t3) and microsecond-precision (t2, t4) values so both branches
// of the precision-aware formatter are exercised.
func TestRunReconstruct_fullTableRoundTrip_datetime6PK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// data_type = "datetime" (base type), column_type = "datetime(6)".
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkdt6', 'events', 'created_at', 1, 'PRI', 'datetime', 'datetime(6)', 'NO', 0)`)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), 'pkdt6', 'events', 'payload', 2, '', 'varchar', 'varchar(128)', 'NO', 0)`)

	createSQL := "CREATE TABLE `events` (\n" +
		"  `created_at` DATETIME(6) NOT NULL,\n" +
		"  `payload` VARCHAR(128) NOT NULL,\n" +
		"  PRIMARY KEY (`created_at`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"

	pkT1 := "2026-04-10 10:00:00.000000" // whole-second — the v1 bug case
	pkT2 := "2026-04-10 10:01:00.123456"
	pkT3 := "2026-04-10 10:02:00.000000"
	pkT4 := "2026-04-10 10:03:00.654321" // new insert via event

	baselineDir := t.TempDir()
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	snapshotTSDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, snapshotTSDir, "pkdt6")
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

	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")

	// UPDATE t2 (archived), DELETE t3 (archived), INSERT t4 (live).
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"pkdt6", "events", 2 /* UPDATE */, pkT2, nil,
		[]byte(`{"created_at":"2026-04-10 10:01:00.123456","payload":"payload-t2"}`),
		[]byte(`{"created_at":"2026-04-10 10:01:00.123456","payload":"payload-t2-updated"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts1, nil,
		"pkdt6", "events", 3 /* DELETE */, pkT3, nil,
		[]byte(`{"created_at":"2026-04-10 10:02:00.000000","payload":"payload-t3"}`),
		nil)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts2, nil,
		"pkdt6", "events", 1 /* INSERT */, pkT4, nil,
		nil,
		[]byte(`{"created_at":"2026-04-10 10:03:00.654321","payload":"payload-t4-new"}`))

	archiveDir := t.TempDir()
	bintrailID := "test-212-datetime6-pk"
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
	recTables = "pkdt6.events"
	recChunkSize = "256MB"
	recParallelism = 1
	recAt = h2.Add(30 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	if err := runReconstruct(reconstructCmd, nil); err != nil {
		t.Fatalf("runReconstruct: %v", err)
	}

	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `pkdt6`")
	testutil.MustExec(t, db, "CREATE DATABASE `pkdt6`")
	t.Cleanup(func() {
		testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `pkdt6`")
	})

	schemaSQL, err := os.ReadFile(filepath.Join(outputDir, "pkdt6.events-schema.sql"))
	if err != nil {
		t.Fatalf("read schema file: %v", err)
	}
	testutil.MustExec(t, db, "USE `pkdt6`")
	testutil.MustExec(t, db, string(schemaSQL))

	chunkSQL, err := os.ReadFile(filepath.Join(outputDir, "pkdt6.events.00000.sql"))
	if err != nil {
		t.Fatalf("read chunk file: %v", err)
	}
	testutil.MustExec(t, db, string(chunkSQL))

	rows, err := db.Query("SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'), payload FROM `pkdt6`.`events` ORDER BY created_at")
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
