//go:build integration

package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/parquet-go/parquet-go"

	"github.com/bintrail/bintrail/internal/archive"
	"github.com/bintrail/bintrail/internal/serverid"
	"github.com/bintrail/bintrail/internal/status"
	"github.com/bintrail/bintrail/internal/testutil"
)

// ─── getFileStatus ─────────────────────────────────────────────────────────────────

func TestGetFileStatus_existing(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.MustExec(t, db, `INSERT INTO index_state
		(binlog_file, file_size, last_position, events_indexed, status, started_at)
		VALUES ('binlog.000042', 1024, 512, 100, 'completed', UTC_TIMESTAMP())`)

	status, err := getFileStatus(db, "binlog.000042")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "completed" {
		t.Errorf("expected 'completed', got %q", status)
	}
}

func TestGetFileStatus_missing(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	status, err := getFileStatus(db, "nonexistent.000001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "" {
		t.Errorf("expected empty string for missing file, got %q", status)
	}
}

// ─── upsertFileState ─────────────────────────────────────────────────────────────────

func TestUpsertFileState_inProgress(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	if err := upsertFileState(db, "binlog.000001", "in_progress", 2048, 0, 0, "", ""); err != nil {
		t.Fatalf("upsert in_progress failed: %v", err)
	}

	status, err := getFileStatus(db, "binlog.000001")
	if err != nil {
		t.Fatalf("getFileStatus failed: %v", err)
	}
	if status != "in_progress" {
		t.Errorf("expected 'in_progress', got %q", status)
	}
}

func TestUpsertFileState_completed(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// First mark in_progress.
	if err := upsertFileState(db, "binlog.000001", "in_progress", 2048, 0, 0, "", ""); err != nil {
		t.Fatalf("upsert in_progress failed: %v", err)
	}
	// Then mark completed.
	if err := upsertFileState(db, "binlog.000001", "completed", 2048, 2048, 500, "", ""); err != nil {
		t.Fatalf("upsert completed failed: %v", err)
	}

	status, err := getFileStatus(db, "binlog.000001")
	if err != nil {
		t.Fatalf("getFileStatus failed: %v", err)
	}
	if status != "completed" {
		t.Errorf("expected 'completed', got %q", status)
	}

	// Verify completed_at is set.
	var completedAt sql.NullTime
	if err := db.QueryRow("SELECT completed_at FROM index_state WHERE binlog_file = 'binlog.000001'").Scan(&completedAt); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if !completedAt.Valid {
		t.Error("expected completed_at to be set")
	}
}

func TestUpsertFileState_failed(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	if err := upsertFileState(db, "binlog.000001", "in_progress", 2048, 0, 0, "", ""); err != nil {
		t.Fatalf("upsert in_progress failed: %v", err)
	}
	if err := upsertFileState(db, "binlog.000001", "failed", 2048, 512, 42, "connection lost", ""); err != nil {
		t.Fatalf("upsert failed status: %v", err)
	}

	status, err := getFileStatus(db, "binlog.000001")
	if err != nil {
		t.Fatalf("getFileStatus failed: %v", err)
	}
	if status != "failed" {
		t.Errorf("expected 'failed', got %q", status)
	}

	// Verify error_message is stored.
	var errMsg sql.NullString
	if err := db.QueryRow("SELECT error_message FROM index_state WHERE binlog_file = 'binlog.000001'").Scan(&errMsg); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if !errMsg.Valid || errMsg.String != "connection lost" {
		t.Errorf("expected error_message='connection lost', got %v", errMsg)
	}
}

// ─── upsertFileState: bintrail_id round-trip ──────────────────────────────────────────

func TestUpsertFileState_bintrailID(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	const id = "aabbccdd-0000-0000-0000-000000000001"
	if err := upsertFileState(db, "binlog.000001", "in_progress", 1024, 0, 0, "", id); err != nil {
		t.Fatalf("upsert with bintrailID failed: %v", err)
	}
	var got sql.NullString
	if err := db.QueryRow("SELECT bintrail_id FROM index_state WHERE binlog_file = 'binlog.000001'").Scan(&got); err != nil {
		t.Fatalf("query bintrail_id: %v", err)
	}
	if !got.Valid {
		t.Error("expected bintrail_id to be non-NULL")
	}
	if got.String != id {
		t.Errorf("expected bintrail_id=%q, got %q", id, got.String)
	}
}

func TestUpsertFileState_emptyBintrailIDStoresNULL(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := upsertFileState(db, "binlog.000001", "in_progress", 1024, 0, 0, "", ""); err != nil {
		t.Fatalf("upsert with empty bintrailID failed: %v", err)
	}
	var got sql.NullString
	if err := db.QueryRow("SELECT bintrail_id FROM index_state WHERE binlog_file = 'binlog.000001'").Scan(&got); err != nil {
		t.Fatalf("query bintrail_id: %v", err)
	}
	if got.Valid {
		t.Errorf("expected bintrail_id to be NULL, got %q", got.String)
	}
}

// ─── validateBinlogFormat ────────────────────────────────────────────────────────────

func TestValidateBinlogFormat_row(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	// Docker test container should have binlog_format=ROW.
	dsn := testutil.IntegrationDSN("")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	if err := validateBinlogFormat(db); err != nil {
		t.Fatalf("expected nil error for ROW binlog_format, got: %v", err)
	}
}

// ─── validateBinlogRowImage ────────────────────────────────────────────────────────────

func TestValidateBinlogRowImage_full(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	// Docker test container should have binlog_row_image=FULL (default).
	dsn := testutil.IntegrationDSN("")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	if err := validateBinlogRowImage(db); err != nil {
		t.Fatalf("expected nil error for FULL binlog_row_image, got: %v", err)
	}
}

// ─── validateNoFKCascades ────────────────────────────────────────────────────────────

func TestValidateNoFKCascades_none(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		total DECIMAL(10,2) NOT NULL
	)`)

	if err := validateNoFKCascades(db, []string{dbName}); err != nil {
		t.Fatalf("expected nil error for schema with no cascades, got: %v", err)
	}
}

func TestValidateNoFKCascades_cascade(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		total DECIMAL(10,2) NOT NULL
	)`)
	testutil.MustExec(t, db, `CREATE TABLE order_items (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		order_id INT NOT NULL,
		CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
	)`)

	if err := validateNoFKCascades(db, []string{dbName}); err == nil {
		t.Fatal("expected error for schema with FK cascade, got nil")
	}
}

func TestValidateNoFKCascades_updateCascade(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	testutil.MustExec(t, db, `CREATE TABLE categories (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)
	testutil.MustExec(t, db, `CREATE TABLE products (
		id          INT PRIMARY KEY AUTO_INCREMENT,
		category_id INT NOT NULL,
		CONSTRAINT fk_cat FOREIGN KEY (category_id) REFERENCES categories(id) ON UPDATE CASCADE
	)`)

	if err := validateNoFKCascades(db, []string{dbName}); err == nil {
		t.Fatal("expected error for schema with UPDATE CASCADE, got nil")
	}
}

func TestValidateNoFKCascades_otherSchemaIgnored(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	otherDB, otherName := testutil.CreateTestDB(t)

	// Create a cascade in otherDB.
	testutil.MustExec(t, otherDB, `CREATE TABLE parents (id INT PRIMARY KEY)`)
	testutil.MustExec(t, otherDB, `CREATE TABLE children (
		id INT PRIMARY KEY,
		parent_id INT NOT NULL,
		CONSTRAINT fk_p FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE
	)`)

	// dbName has no cascades — checking only dbName should pass.
	_ = dbName
	if err := validateNoFKCascades(db, []string{dbName}); err != nil {
		t.Fatalf("expected nil when cascade is only in %q (not targeted), got: %v", otherName, err)
	}
}

// ─── ensureResolver ──────────────────────────────────────────────────────────────────

func TestEnsureResolver_autoSnapshot(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Create a table on the source.
	testutil.MustExec(t, sourceDB, `CREATE TABLE products (
		id   INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100) NOT NULL
	)`)

	resolver, err := ensureResolver(indexDB, sourceDB, []string{sourceName})
	if err != nil {
		t.Fatalf("ensureResolver failed: %v", err)
	}

	if resolver.SnapshotID() == 0 {
		t.Error("expected non-zero snapshot ID")
	}
	if resolver.TableCount() != 1 {
		t.Errorf("expected 1 table, got %d", resolver.TableCount())
	}
}

func TestEnsureResolver_noSnapshotNoSource(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	_, err := ensureResolver(indexDB, nil, nil)
	if err == nil {
		t.Fatal("expected error when no snapshot and no sourceDB")
	}
}

func TestEnsureResolver_existingSnapshot(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(100)
	)`)

	// Take snapshot manually first.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		sourceName, "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		sourceName, "orders", "name", 2, "", "varchar", "YES")

	// Should load existing snapshot without needing sourceDB.
	resolver, err := ensureResolver(indexDB, nil, nil)
	if err != nil {
		t.Fatalf("ensureResolver failed: %v", err)
	}
	if resolver.SnapshotID() != 1 {
		t.Errorf("expected snapshot ID 1, got %d", resolver.SnapshotID())
	}
}

// ─── loadIndexState ────────────────────────────────────────────────────────────────

func TestLoadIndexState(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.MustExec(t, db, `INSERT INTO index_state
		(binlog_file, file_size, last_position, events_indexed, status, started_at)
		VALUES ('binlog.000001', 1024, 1024, 100, 'completed', '2026-01-01 00:00:00')`)
	testutil.MustExec(t, db, `INSERT INTO index_state
		(binlog_file, file_size, last_position, events_indexed, status, started_at, error_message)
		VALUES ('binlog.000002', 2048, 512, 50, 'failed', '2026-01-01 01:00:00', 'timeout')`)

	rows, err := status.LoadIndexState(context.Background(), db)
	if err != nil {
		t.Fatalf("loadIndexState failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	if rows[0].BinlogFile != "binlog.000001" || rows[0].Status != "completed" {
		t.Errorf("row 0: expected binlog.000001/completed, got %s/%s", rows[0].BinlogFile, rows[0].Status)
	}
	if rows[0].EventsIndexed != 100 {
		t.Errorf("row 0: expected 100 events, got %d", rows[0].EventsIndexed)
	}

	if rows[1].BinlogFile != "binlog.000002" || rows[1].Status != "failed" {
		t.Errorf("row 1: expected binlog.000002/failed, got %s/%s", rows[1].BinlogFile, rows[1].Status)
	}
	if !rows[1].ErrorMessage.Valid || rows[1].ErrorMessage.String != "timeout" {
		t.Errorf("row 1: expected error_message='timeout', got %v", rows[1].ErrorMessage)
	}
}

// ─── loadPartitionStats ────────────────────────────────────────────────────────────

func TestLoadPartitionStats(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	// Create table with 3 daily partitions + p_future.
	if err := createBinlogEventsTable(db, 3, false); err != nil {
		t.Fatalf("createBinlogEventsTable failed: %v", err)
	}

	stats, err := status.LoadPartitionStats(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("loadPartitionStats failed: %v", err)
	}

	// 3 daily + p_future = 4.
	if len(stats) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(stats))
	}

	// Last partition should be p_future.
	last := stats[len(stats)-1]
	if last.Name != "p_future" {
		t.Errorf("expected last partition to be p_future, got %s", last.Name)
	}
	if last.Description != "MAXVALUE" {
		t.Errorf("expected MAXVALUE description, got %s", last.Description)
	}
}

// ─── ensureDatabase ────────────────────────────────────────────────────────────────

func TestEnsureDatabase(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	dbName := "bt_ensure_db_test"
	dsn := testutil.IntegrationDSN(dbName)
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN failed: %v", err)
	}

	// Clean up before and after.
	rootDB, _ := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	defer rootDB.Close()
	rootDB.Exec("DROP DATABASE IF EXISTS `" + dbName + "`")
	t.Cleanup(func() {
		rootDB.Exec("DROP DATABASE IF EXISTS `" + dbName + "`")
	})

	if err := ensureDatabase(cfg, dbName); err != nil {
		t.Fatalf("ensureDatabase failed: %v", err)
	}

	// Verify database exists.
	var name string
	if err := rootDB.QueryRow("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", dbName).Scan(&name); err != nil {
		t.Fatalf("database %q was not created: %v", dbName, err)
	}

	// Calling again should succeed (idempotent).
	if err := ensureDatabase(cfg, dbName); err != nil {
		t.Fatalf("second ensureDatabase call failed: %v", err)
	}
}

// ─── createBinlogEventsTable ─────────────────────────────────────────────────────────────

func TestCreateBinlogEventsTable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := createBinlogEventsTable(db, 3, false); err != nil {
		t.Fatalf("createBinlogEventsTable failed: %v", err)
	}

	// Verify the table has 4 partitions (3 hourly + p_future).
	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'`,
		dbName).Scan(&count); err != nil {
		t.Fatalf("query partitions failed: %v", err)
	}
	if count != 4 {
		t.Errorf("expected 4 partitions, got %d", count)
	}
}

// ─── listPartitions ──────────────────────────────────────────────────────────────────

func TestListPartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := createBinlogEventsTable(db, 3, false); err != nil {
		t.Fatalf("createBinlogEventsTable failed: %v", err)
	}

	parts, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions failed: %v", err)
	}

	if len(parts) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(parts))
	}

	// Verify ordinals are sequential.
	for i, p := range parts {
		if p.Ordinal != i+1 {
			t.Errorf("partition %d: expected ordinal %d, got %d", i, i+1, p.Ordinal)
		}
	}

	// Verify the last is p_future.
	if parts[3].Name != "p_future" {
		t.Errorf("expected last partition p_future, got %s", parts[3].Name)
	}
}

// ─── dropPartitions ──────────────────────────────────────────────────────────────────

func TestDropPartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := createBinlogEventsTable(db, 5, false); err != nil {
		t.Fatalf("createBinlogEventsTable failed: %v", err)
	}

	// List the first partition to drop.
	parts, _ := listPartitions(context.Background(), db, dbName)
	if len(parts) < 2 {
		t.Fatal("need at least 2 partitions to test drop")
	}

	toDrop := parts[0].Name // drop the first daily partition
	if err := dropPartitions(context.Background(), db, dbName, []string{toDrop}); err != nil {
		t.Fatalf("dropPartitions failed: %v", err)
	}

	// Verify count decreased.
	partsAfter, _ := listPartitions(context.Background(), db, dbName)
	if len(partsAfter) != len(parts)-1 {
		t.Errorf("expected %d partitions after drop, got %d", len(parts)-1, len(partsAfter))
	}

	// Verify the dropped partition is gone.
	for _, p := range partsAfter {
		if p.Name == toDrop {
			t.Errorf("partition %s should have been dropped", toDrop)
		}
	}
}

// ─── partitionHasData ────────────────────────────────────────────────────────────────

func TestPartitionHasData_empty(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db) // creates binlog_events with only p_future

	has, err := partitionHasData(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("partitionHasData failed: %v", err)
	}
	if has {
		t.Error("expected false for empty p_future partition")
	}
}

func TestPartitionHasData_withData(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Insert a row into binlog_events. It will land in p_future since
	// InitIndexTables creates only p_future.
	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200,
		time.Now().UTC().Format("2006-01-02 15:04:05"),
		nil, "testdb", "orders", 1, "1",
		nil, nil, []byte(`{"id": 1}`),
	)

	has, err := partitionHasData(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("partitionHasData failed: %v", err)
	}
	if !has {
		t.Error("expected true when p_future has data")
	}
}

// ─── addFuturePartitions ─────────────────────────────────────────────────────────────

func TestAddFuturePartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db) // only p_future

	startDate := time.Now().UTC().Truncate(time.Hour)

	if err := addFuturePartitions(context.Background(), db, dbName, startDate, 3); err != nil {
		t.Fatalf("addFuturePartitions failed: %v", err)
	}

	parts, _ := listPartitions(context.Background(), db, dbName)
	// Should be 3 hourly + p_future = 4.
	if len(parts) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(parts))
	}

	// The last one must still be p_future.
	if parts[len(parts)-1].Name != "p_future" {
		t.Errorf("expected last partition p_future, got %s", parts[len(parts)-1].Name)
	}

	// First 3 should be hourly partitions.
	for i := range 3 {
		expected := partitionName(startDate.Add(time.Duration(i) * time.Hour))
		if parts[i].Name != expected {
			t.Errorf("partition %d: expected %s, got %s", i, expected, parts[i].Name)
		}
	}
}

// ─── ArchivePartition ───────────────────────────────────────────────────────────────────

func TestArchivePartition(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db) // creates binlog_events with only p_future

	// Insert two events; they land in p_future since InitIndexTables creates
	// only a p_future catch-all partition.
	now := time.Now().UTC()
	ts := now.Format("2006-01-02 15:04:05")
	gtid := "abc:1"
	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, ts, &gtid,
		"mydb", "orders", 1, "42",
		nil, nil, []byte(`{"id":42}`),
	)
	testutil.InsertEvent(t, db,
		"binlog.000001", 200, 300, ts, nil,
		"mydb", "orders", 3, "43",
		nil, []byte(`{"id":43}`), nil,
	)

	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "p_future.parquet")

	n, err := archive.ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none")
	if err != nil {
		t.Fatalf("ArchivePartition: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 rows archived, got %d", n)
	}

	// Verify the file exists.
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("parquet file not created: %v", err)
	}

	// Read back and verify row count and metadata.
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open parquet file: %v", err)
	}
	defer rf.Close()
	info, _ := rf.Stat()
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	if pf.NumRows() != 2 {
		t.Errorf("NumRows = %d, want 2", pf.NumRows())
	}

	got, ok := pf.Lookup("bintrail.archive.partition")
	if !ok {
		t.Error("expected bintrail.archive.partition metadata key")
	} else if got != "p_future" {
		t.Errorf("archive.partition = %q, want p_future", got)
	}
}

func TestArchivePartition_empty(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "p_future.parquet")

	n, err := archive.ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none")
	if err != nil {
		t.Fatalf("ArchivePartition on empty partition: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows for empty partition, got %d", n)
	}

	// File should still be created (valid empty Parquet).
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("parquet file not created for empty partition: %v", err)
	}
}

// ─── Server identity integration tests ─────────────────────────────────────────────

// initServerTables creates bintrail_servers and bintrail_server_changes in db.
func initServerTables(t *testing.T, db *sql.DB) {
	t.Helper()
	testutil.MustExec(t, db, ddlBintrailServers)
	testutil.MustExec(t, db, ddlBintrailServerChanges)
}

// countChanges returns the number of rows in bintrail_server_changes for the given bintrail_id.
func countChanges(t *testing.T, db *sql.DB, bintrailID string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM bintrail_server_changes WHERE bintrail_id = ?`, bintrailID).Scan(&n); err != nil {
		t.Fatalf("countChanges: %v", err)
	}
	return n
}

// TestServerID_UUIDRegeneration verifies that when server_uuid changes but
// host+port+username stay the same (Rule 3 — UUID regeneration), the same
// bintrail_id is returned and the change is recorded in bintrail_server_changes.
func TestServerID_UUIDRegeneration(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	initServerTables(t, db)
	ctx := context.Background()

	// First registration — brand new server.
	id1, err := serverid.ResolveServer(ctx, db, "uuid-original", "db01", 3306, "bintrail")
	if err != nil {
		t.Fatalf("initial registration failed: %v", err)
	}
	if id1 == "" {
		t.Fatal("expected non-empty bintrail_id")
	}

	// Simulate UUID regeneration: same host+port+user, new server_uuid.
	id2, err := serverid.ResolveServer(ctx, db, "uuid-regenerated", "db01", 3306, "bintrail")
	if err != nil {
		t.Fatalf("uuid regen resolution failed: %v", err)
	}
	if id2 != id1 {
		t.Errorf("bintrail_id changed after UUID regen: got %q, want %q", id2, id1)
	}

	// Verify the change was logged.
	if n := countChanges(t, db, id1); n != 1 {
		t.Errorf("expected 1 change row for uuid regen, got %d", n)
	}

	// Verify the stored server_uuid was updated.
	var storedUUID string
	if err := db.QueryRow(`SELECT server_uuid FROM bintrail_servers WHERE bintrail_id = ?`, id1).Scan(&storedUUID); err != nil {
		t.Fatalf("query stored uuid: %v", err)
	}
	if storedUUID != "uuid-regenerated" {
		t.Errorf("stored uuid not updated: got %q", storedUUID)
	}
}

// TestServerID_HostMigration verifies that when server_uuid stays the same but
// host changes (Rule 2 — migration), the same bintrail_id is returned and the
// change is recorded.
func TestServerID_HostMigration(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	initServerTables(t, db)
	ctx := context.Background()

	id1, err := serverid.ResolveServer(ctx, db, "uuid-stable", "db-old", 3306, "bintrail")
	if err != nil {
		t.Fatalf("initial registration: %v", err)
	}

	// Server migrated to a new host; UUID is unchanged.
	id2, err := serverid.ResolveServer(ctx, db, "uuid-stable", "db-new", 3306, "bintrail")
	if err != nil {
		t.Fatalf("host migration resolution: %v", err)
	}
	if id2 != id1 {
		t.Errorf("bintrail_id changed after host migration: got %q, want %q", id2, id1)
	}
	if n := countChanges(t, db, id1); n != 1 {
		t.Errorf("expected 1 change row for host migration, got %d", n)
	}

	var storedHost string
	if err := db.QueryRow(`SELECT host FROM bintrail_servers WHERE bintrail_id = ?`, id1).Scan(&storedHost); err != nil {
		t.Fatalf("query stored host: %v", err)
	}
	if storedHost != "db-new" {
		t.Errorf("stored host not updated: got %q", storedHost)
	}
}

// TestServerID_CloneConflict verifies that when two servers share a server_uuid
// (clone scenario) and host+port+user match a different record (Rule 5), an
// ErrConflict is returned and no auto-resolution occurs.
func TestServerID_CloneConflict(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	initServerTables(t, db)
	ctx := context.Background()

	// Register two distinct servers.
	_, err := serverid.ResolveServer(ctx, db, "uuid-source", "db-source", 3306, "bintrail")
	if err != nil {
		t.Fatalf("register source server: %v", err)
	}
	_, err = serverid.ResolveServer(ctx, db, "uuid-clone", "db-clone", 3306, "bintrail")
	if err != nil {
		t.Fatalf("register clone server: %v", err)
	}

	// Now the clone presents the source's server_uuid (datadir was copied with auto.cnf).
	// uuid-source matches db-source's record; db-clone matches the second record → conflict.
	_, err = serverid.ResolveServer(ctx, db, "uuid-source", "db-clone", 3306, "bintrail")
	if !errors.Is(err, serverid.ErrConflict) {
		t.Errorf("expected ErrConflict for clone scenario, got: %v", err)
	}
}

// TestServerID_Decommission verifies that a decommissioned server is excluded
// from identity resolution and a new server on the same host+port gets a fresh
// bintrail_id (Rule 4).
func TestServerID_Decommission(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	initServerTables(t, db)
	ctx := context.Background()

	// Register and then decommission a server.
	id1, err := serverid.ResolveServer(ctx, db, "uuid-old", "db01", 3306, "bintrail")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := serverid.DecommissionServer(ctx, db, id1); err != nil {
		t.Fatalf("decommission: %v", err)
	}

	// A new server initialised on the same host+port+user should get a new identity.
	id2, err := serverid.ResolveServer(ctx, db, "uuid-new", "db01", 3306, "bintrail")
	if err != nil {
		t.Fatalf("register new server after decommission: %v", err)
	}
	if id2 == id1 {
		t.Error("new server after decommission should have a different bintrail_id")
	}
	if id2 == "" {
		t.Error("expected non-empty bintrail_id for new server")
	}

	// Verify the old record is still present with decommissioned_at set.
	var decomAt sql.NullTime
	if err := db.QueryRowContext(ctx,
		`SELECT decommissioned_at FROM bintrail_servers WHERE bintrail_id = ?`, id1,
	).Scan(&decomAt); err != nil {
		t.Fatalf("query decommissioned_at: %v", err)
	}
	if !decomAt.Valid {
		t.Error("expected decommissioned_at to be set for decommissioned server")
	}

	// Verify the new server record was persisted.
	var stored string
	if err := db.QueryRowContext(ctx,
		`SELECT bintrail_id FROM bintrail_servers WHERE bintrail_id = ?`, id2,
	).Scan(&stored); err != nil {
		t.Fatalf("query new server record: %v", err)
	}
	if stored != id2 {
		t.Errorf("expected bintrail_id %q stored, got %q", id2, stored)
	}
}
