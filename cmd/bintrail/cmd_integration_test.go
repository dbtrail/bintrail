//go:build integration

package main

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/bintrail/bintrail/internal/testutil"
)

// ─── getFileStatus ───────────────────────────────────────────────────────────

func TestGetFileStatus_existing(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.MustExec(t, db, `INSERT INTO index_state
		(binlog_file, file_size, last_position, events_indexed, status, started_at)
		VALUES ('binlog.000042', 1024, 512, 100, 'completed', NOW())`)

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

// ─── upsertFileState ─────────────────────────────────────────────────────────

func TestUpsertFileState_inProgress(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	if err := upsertFileState(db, "binlog.000001", "in_progress", 2048, 0, 0, ""); err != nil {
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
	if err := upsertFileState(db, "binlog.000001", "in_progress", 2048, 0, 0, ""); err != nil {
		t.Fatalf("upsert in_progress failed: %v", err)
	}
	// Then mark completed.
	if err := upsertFileState(db, "binlog.000001", "completed", 2048, 2048, 500, ""); err != nil {
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

	if err := upsertFileState(db, "binlog.000001", "in_progress", 2048, 0, 0, ""); err != nil {
		t.Fatalf("upsert in_progress failed: %v", err)
	}
	if err := upsertFileState(db, "binlog.000001", "failed", 2048, 512, 42, "connection lost"); err != nil {
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

// ─── validateBinlogRowImage ──────────────────────────────────────────────────

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

// ─── ensureResolver ──────────────────────────────────────────────────────────

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

// ─── loadIndexState ──────────────────────────────────────────────────────────

func TestLoadIndexState(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.MustExec(t, db, `INSERT INTO index_state
		(binlog_file, file_size, last_position, events_indexed, status, started_at)
		VALUES ('binlog.000001', 1024, 1024, 100, 'completed', '2026-01-01 00:00:00')`)
	testutil.MustExec(t, db, `INSERT INTO index_state
		(binlog_file, file_size, last_position, events_indexed, status, started_at, error_message)
		VALUES ('binlog.000002', 2048, 512, 50, 'failed', '2026-01-01 01:00:00', 'timeout')`)

	rows, err := loadIndexState(context.Background(), db)
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

// ─── loadPartitionStats ──────────────────────────────────────────────────────

func TestLoadPartitionStats(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	// Create table with 3 daily partitions + p_future.
	if err := createBinlogEventsTable(db, 3); err != nil {
		t.Fatalf("createBinlogEventsTable failed: %v", err)
	}

	stats, err := loadPartitionStats(context.Background(), db, dbName)
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

// ─── ensureDatabase ──────────────────────────────────────────────────────────

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

// ─── createBinlogEventsTable ─────────────────────────────────────────────────

func TestCreateBinlogEventsTable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := createBinlogEventsTable(db, 3); err != nil {
		t.Fatalf("createBinlogEventsTable failed: %v", err)
	}

	// Verify the table has 4 partitions (3 daily + p_future).
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

// ─── listPartitions ──────────────────────────────────────────────────────────

func TestListPartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := createBinlogEventsTable(db, 3); err != nil {
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

// ─── dropPartitions ──────────────────────────────────────────────────────────

func TestDropPartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := createBinlogEventsTable(db, 5); err != nil {
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

// ─── partitionHasData ────────────────────────────────────────────────────────

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

// ─── addFuturePartitions ─────────────────────────────────────────────────────

func TestAddFuturePartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db) // only p_future

	startDate := time.Now().UTC().Truncate(24 * time.Hour)

	if err := addFuturePartitions(context.Background(), db, dbName, startDate, 3); err != nil {
		t.Fatalf("addFuturePartitions failed: %v", err)
	}

	parts, _ := listPartitions(context.Background(), db, dbName)
	// Should be 3 daily + p_future = 4.
	if len(parts) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(parts))
	}

	// The last one must still be p_future.
	if parts[len(parts)-1].Name != "p_future" {
		t.Errorf("expected last partition p_future, got %s", parts[len(parts)-1].Name)
	}

	// First 3 should be daily partitions.
	for i := range 3 {
		expected := partitionName(startDate.AddDate(0, 0, i))
		if parts[i].Name != expected {
			t.Errorf("partition %d: expected %s, got %s", i, expected, parts[i].Name)
		}
	}
}

