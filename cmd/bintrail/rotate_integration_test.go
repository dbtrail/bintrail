//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/testutil"
)

// ─── hasPendingS3Upload ──────────────────────────────────────────────────────

func TestHasPendingS3Upload_noRow(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "test-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pending {
		t.Error("expected false when no archive_state row exists")
	}
}

func TestHasPendingS3Upload_localOnly(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Row exists but s3_bucket is NULL — no S3 intent recorded.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count)
		VALUES ('p_2026030100', 'test-uuid', '/data/test.parquet', 42)`)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "test-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pending {
		t.Error("expected false when s3_bucket is NULL (no S3 intent)")
	}
}

func TestHasPendingS3Upload_pendingUpload(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Row with s3_bucket set but s3_uploaded_at NULL — pending upload.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES ('p_2026030100', 'test-uuid', '/data/test.parquet', 42, 'my-bucket', 'archives/test.parquet')`)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "test-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pending {
		t.Error("expected true when s3_bucket is set but s3_uploaded_at is NULL")
	}
}

func TestHasPendingS3Upload_completed(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Row with s3_uploaded_at set — upload complete.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES ('p_2026030100', 'test-uuid', '/data/test.parquet', 42, 'my-bucket', 'archives/test.parquet', UTC_TIMESTAMP())`)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "test-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pending {
		t.Error("expected false when s3_uploaded_at is set")
	}
}

func TestHasPendingS3Upload_emptyBintrailID(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Row with a specific bintrail_id but query uses empty string — should still detect.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES ('p_2026030100', 'some-uuid', '/data/test.parquet', 42, 'my-bucket', 'archives/test.parquet')`)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pending {
		t.Error("expected true when bintrailID is empty and any row has pending S3")
	}
}

// ─── performRotation S3 safety ───────────────────────────────────────────────

// setupPartitionedTable creates partitions that cover the given hours so we can
// insert test data and then rotate them away.
func setupPartitionedTable(t *testing.T, db *sql.DB, dbName string, hours []time.Time) {
	t.Helper()
	// Drop existing partitioning and recreate with specific partitions.
	// First, drop all non-future partitions by reorganizing.
	parts := ""
	for i, h := range hours {
		nextHour := h.Add(time.Hour)
		if i > 0 {
			parts += ",\n"
		}
		parts += fmt.Sprintf(
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s'))",
			partitionName(h),
			nextHour.UTC().Format("2006-01-02 15:04:05"),
		)
	}
	parts += ",\nPARTITION p_future VALUES LESS THAN MAXVALUE"

	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` REORGANIZE PARTITION p_future INTO (\n%s\n)",
		dbName, parts,
	))
}

// TestPerformRotation_PendingS3BlocksDrop verifies that when a previous
// rotation run recorded S3 upload intent (s3_bucket set) but the upload
// did not complete (s3_uploaded_at NULL), a subsequent rotation run — even
// without --archive-s3 — refuses to drop that partition.
//
// Note: the S3-upload-failure continue path (uploadFileFunc returning an error)
// is not directly integration-tested here because performRotation creates its
// own S3 client internally from --archive-s3 flags. The safety check tested
// here is the defense-in-depth layer that catches any scenario where the
// partition reaches the drop step with a pending upload.
func TestPerformRotation_PendingS3BlocksDrop(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Create two old partitions.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	// Insert a row into each partition so archiving has data.
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	// Pre-archive: create local Parquet files and archive_state rows
	// simulating a previous run where S3 upload failed for h1 but
	// succeeded for h2.
	archiveDir := t.TempDir()
	savedVars := saveRotateVars()
	t.Cleanup(func() { restoreRotateVars(savedVars) })

	rotArchiveDir = archiveDir
	rotBintrailID = "test-uuid-167"
	rotArchiveCompression = "none"
	rotFormat = "text"
	rotNoReplace = true
	rotAddFuture = 0

	outPath1, _ := hiveArchivePath(archiveDir, rotBintrailID, partitionName(h1))
	outPath2, _ := hiveArchivePath(archiveDir, rotBintrailID, partitionName(h2))
	os.MkdirAll(filepath.Dir(outPath1), 0o755)
	os.MkdirAll(filepath.Dir(outPath2), 0o755)
	os.WriteFile(outPath1, []byte("parquet-data"), 0o644)
	os.WriteFile(outPath2, []byte("parquet-data"), 0o644)

	// Insert archive_state: first partition has pending S3, second is complete.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES (?, ?, ?, 1, 'my-bucket', 'archives/p1.parquet')`,
		partitionName(h1), rotBintrailID, outPath1)
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES (?, ?, ?, 1, 'my-bucket', 'archives/p2.parquet', UTC_TIMESTAMP())`,
		partitionName(h2), rotBintrailID, outPath2)

	// Run rotation WITHOUT --archive-s3, WITH --retry (so it skips re-archiving).
	rotArchiveS3 = ""
	rotRetry = true

	res, err := performRotation(context.Background(), db, dbName, 24*time.Hour)
	if err != nil {
		t.Fatalf("performRotation failed: %v", err)
	}

	// First partition should NOT be dropped (pending S3 upload).
	// Second partition should be dropped (S3 upload complete).
	if res.dropped != 1 {
		t.Errorf("expected 1 partition dropped, got %d", res.dropped)
	}

	// Verify partition h1 still exists.
	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	p1Name := partitionName(h1)
	p2Name := partitionName(h2)
	var foundP1, foundP2 bool
	for _, p := range partitions {
		if p.Name == p1Name {
			foundP1 = true
		}
		if p.Name == p2Name {
			foundP2 = true
		}
	}
	if !foundP1 {
		t.Errorf("partition %s should NOT have been dropped (pending S3 upload)", p1Name)
	}
	if foundP2 {
		t.Errorf("partition %s should have been dropped (S3 upload complete)", p2Name)
	}
}

func TestPerformRotation_NoPendingS3DropsAll(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Create two old partitions.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	savedVars := saveRotateVars()
	t.Cleanup(func() { restoreRotateVars(savedVars) })

	rotArchiveDir = ""
	rotBintrailID = ""
	rotArchiveS3 = ""
	rotFormat = "text"
	rotRetry = false
	rotNoReplace = true
	rotAddFuture = 0

	// No archive_state rows at all — partitions should be dropped freely.
	res, err := performRotation(context.Background(), db, dbName, 24*time.Hour)
	if err != nil {
		t.Fatalf("performRotation failed: %v", err)
	}
	if res.dropped != 2 {
		t.Errorf("expected 2 partitions dropped, got %d", res.dropped)
	}
}

func TestPerformRotation_BulkDropSkipsPendingS3(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Create two old partitions.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	// Insert archive_state with pending S3 for h1 only.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES (?, 'prev-uuid', '/data/test.parquet', 1, 'my-bucket', 'archives/p1.parquet')`,
		partitionName(h1))

	savedVars := saveRotateVars()
	t.Cleanup(func() { restoreRotateVars(savedVars) })

	// No --archive-dir on this run (bulk-drop path).
	rotArchiveDir = ""
	rotBintrailID = ""
	rotArchiveS3 = ""
	rotFormat = "text"
	rotRetry = false
	rotNoReplace = true
	rotAddFuture = 0

	res, err := performRotation(context.Background(), db, dbName, 24*time.Hour)
	if err != nil {
		t.Fatalf("performRotation failed: %v", err)
	}

	// h1 should be skipped (pending S3), h2 dropped.
	if res.dropped != 1 {
		t.Errorf("expected 1 partition dropped (h2 only), got %d", res.dropped)
	}

	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	p1Name := partitionName(h1)
	var foundP1 bool
	for _, p := range partitions {
		if p.Name == p1Name {
			foundP1 = true
		}
	}
	if !foundP1 {
		t.Errorf("partition %s should NOT have been dropped (pending S3 from previous run)", p1Name)
	}
}

// ─── protect-unarchived guard (built-in `up` rotation, #420) ─────────────────

// TestPerformRotation_ProtectUnarchivedDefers verifies the full guard matrix
// in one rotation cycle when rotProtectUnarchived is set (the built-in `up`
// rotation) and the index has archiving history:
//   - h1: past retention, NOT archived          → deferred (guard)
//   - h2: archived but S3 upload still pending  → skipped (pending-S3 filter)
//   - h3: archived and uploaded                 → dropped
func TestPerformRotation_ProtectUnarchivedDefers(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	h3 := h2.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h3})

	for i, h := range []time.Time{h1, h2, h3} {
		ts := h.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
		testutil.InsertEvent(t, db, "binlog.000001", uint64(100*(i+1)), uint64(100*(i+2)), ts, nil,
			"testdb", "users", 1, fmt.Sprintf("%d", i+1), nil, nil, []byte(`{"id":1}`))
	}

	savedVars := saveRotateVars()
	t.Cleanup(func() { restoreRotateVars(savedVars) })

	// Built-in rotation profile: no archive flags, protection on.
	rotArchiveDir = ""
	rotArchiveS3 = ""
	rotBintrailID = ""
	rotFormat = "text"
	rotRetry = false
	rotNoReplace = true
	rotAddFuture = 0
	rotProtectUnarchived = true

	// Archiving history exists: h2 archived with a pending S3 upload
	// (s3_bucket set, s3_uploaded_at NULL); h3 archived and uploaded;
	// h1 not archived at all.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES (?, 'cron-uuid', '/archives/p2.parquet', 1, 'my-bucket', 'archives/p2.parquet')`,
		partitionName(h2))
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES (?, 'cron-uuid', '/archives/p3.parquet', 1, 'my-bucket', 'archives/p3.parquet', UTC_TIMESTAMP())`,
		partitionName(h3))

	res, err := performRotation(context.Background(), db, dbName, 24*time.Hour)
	if err != nil {
		t.Fatalf("performRotation failed: %v", err)
	}
	if res.dropped != 1 {
		t.Errorf("expected 1 partition dropped (archived+uploaded h3 only), got %d", res.dropped)
	}
	if res.deferred != 1 {
		t.Errorf("expected 1 partition deferred (unarchived h1; pending-S3 h2 is a skip, not a guard deferral), got %d", res.deferred)
	}

	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	remaining := map[string]bool{}
	for _, p := range partitions {
		remaining[p.Name] = true
	}
	if !remaining[partitionName(h1)] {
		t.Errorf("partition %s should NOT have been dropped (past retention but unarchived)", partitionName(h1))
	}
	if !remaining[partitionName(h2)] {
		t.Errorf("partition %s should NOT have been dropped (archived but S3 upload pending)", partitionName(h2))
	}
	if remaining[partitionName(h3)] {
		t.Errorf("partition %s should have been dropped (archived and uploaded)", partitionName(h3))
	}
}

// TestPerformRotation_ProtectUnarchivedNoHistoryDropsAll verifies the guard's
// other half: an index with NO archiving history at all (empty archive_state —
// the quickstart world) rotates freely even under rotProtectUnarchived, which
// is what keeps an unattended bundled volume bounded.
func TestPerformRotation_ProtectUnarchivedNoHistoryDropsAll(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	savedVars := saveRotateVars()
	t.Cleanup(func() { restoreRotateVars(savedVars) })

	rotArchiveDir = ""
	rotArchiveS3 = ""
	rotBintrailID = ""
	rotFormat = "text"
	rotRetry = false
	rotNoReplace = true
	rotAddFuture = 0
	rotProtectUnarchived = true

	res, err := performRotation(context.Background(), db, dbName, 24*time.Hour)
	if err != nil {
		t.Fatalf("performRotation failed: %v", err)
	}
	if res.dropped != 2 {
		t.Errorf("expected 2 partitions dropped (no archiving history), got %d", res.dropped)
	}
}

// ─── test helpers ────────────────────────────────────────────────────────────

type rotateVarSnapshot struct {
	indexDSN, retain, archiveDir, archiveCompression string
	bintrailID, archiveS3, archiveS3Region           string
	format, interval                                 string
	addFuture                                        int
	noReplace, daemon, retry, protectUnarchived      bool
}

func saveRotateVars() rotateVarSnapshot {
	return rotateVarSnapshot{
		indexDSN:           rotIndexDSN,
		retain:             rotRetain,
		archiveDir:         rotArchiveDir,
		archiveCompression: rotArchiveCompression,
		bintrailID:         rotBintrailID,
		archiveS3:          rotArchiveS3,
		archiveS3Region:    rotArchiveS3Region,
		format:             rotFormat,
		interval:           rotInterval,
		addFuture:          rotAddFuture,
		noReplace:          rotNoReplace,
		daemon:             rotDaemon,
		retry:              rotRetry,
		protectUnarchived:  rotProtectUnarchived,
	}
}

func restoreRotateVars(s rotateVarSnapshot) {
	rotIndexDSN = s.indexDSN
	rotRetain = s.retain
	rotArchiveDir = s.archiveDir
	rotArchiveCompression = s.archiveCompression
	rotBintrailID = s.bintrailID
	rotArchiveS3 = s.archiveS3
	rotArchiveS3Region = s.archiveS3Region
	rotFormat = s.format
	rotInterval = s.interval
	rotAddFuture = s.addFuture
	rotNoReplace = s.noReplace
	rotDaemon = s.daemon
	rotRetry = s.retry
	rotProtectUnarchived = s.protectUnarchived
}
