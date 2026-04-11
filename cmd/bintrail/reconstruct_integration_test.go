//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/archive"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/query"
	"github.com/dbtrail/bintrail/internal/testutil"
)

// TestFetchMerged_archiveAware is the regression test for #209: prove that
// FetchMerged returns events from both live MySQL partitions AND Parquet
// archives, and that --no-archive limits it to live-only.
//
// Setup:
//  1. Create two hourly partitions for two hours ago (h1, h2).
//  2. Insert event E1 into h1 and event E2 into h2.
//  3. Archive h1 to a temp Parquet file and register it in archive_state.
//  4. Drop h1 so its event is no longer in live MySQL.
//
// Expectations:
//   - NoArchive=true  → returns {E2}        (live only)
//   - NoArchive=false → returns {E1, E2}    (live + archive, merged)
//
// Before #209 the single-row reconstruct path called engine.Fetch directly
// with no archive awareness, so it would return {E2} even without NoArchive,
// silently missing E1.
func TestFetchMerged_archiveAware(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Build two adjacent hourly partitions, placed two days in the past to
	// avoid collisions with the default p_future catch-all.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")

	// Insert one event into each partition for the same schema.table.pk so
	// we can query them as one logical row history.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testdb", "orders", 2 /* UPDATE */, "42",
		nil, []byte(`{"id":42,"status":"new"}`), []byte(`{"id":42,"status":"paid"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil,
		"testdb", "orders", 2 /* UPDATE */, "42",
		nil, []byte(`{"id":42,"status":"paid"}`), []byte(`{"id":42,"status":"shipped"}`))

	// Archive h1 to a Hive-partitioned path. The bintrail_id= marker in the
	// directory layout is required for query.ResolveArchiveSources to find it.
	archiveDir := t.TempDir()
	bintrailID := "test-209-reconstruct"
	outPath, err := hiveArchivePath(archiveDir, bintrailID, partitionName(h1))
	if err != nil {
		t.Fatalf("hiveArchivePath: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	archivedRows, err := archive.ArchivePartition(context.Background(), db, dbName, partitionName(h1), outPath, "zstd")
	if err != nil {
		t.Fatalf("ArchivePartition: %v", err)
	}
	if archivedRows != 1 {
		t.Fatalf("expected 1 archived row, got %d", archivedRows)
	}

	// Register the archive in archive_state so ResolveArchiveSources can find it.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES (?, ?, ?, 1, NULL, NULL, NULL)`,
		partitionName(h1), bintrailID, outPath)

	// Drop h1 so its event is gone from live MySQL. FetchMerged now has to
	// read it from the Parquet archive or miss it entirely.
	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` DROP PARTITION `%s`",
		dbName, partitionName(h1),
	))

	engine := query.New(db)
	opts := query.Options{
		Schema:   "testdb",
		Table:    "orders",
		PKValues: "42",
		Since:    ptrTime(h1),
		Until:    ptrTime(h2.Add(time.Hour)),
	}

	// ── Case 1: NoArchive=true → must return only the live event ───────────
	liveOnly, _, err := query.FetchMerged(context.Background(), db, engine, query.FetchMergedOptions{
		Opts:      opts,
		DBName:    dbName,
		NoArchive: true,
		AllowGaps: true, // gap detection is also disabled under NoArchive, but set explicitly
	})
	if err != nil {
		t.Fatalf("FetchMerged(NoArchive=true): %v", err)
	}
	if len(liveOnly) != 1 {
		t.Fatalf("NoArchive=true: expected 1 event (live only), got %d", len(liveOnly))
	}
	if liveOnly[0].StartPos != 200 {
		t.Errorf("NoArchive=true: expected live event with start_pos=200, got %d", liveOnly[0].StartPos)
	}

	// ── Case 2: NoArchive=false → must return BOTH events, merged ──────────
	//
	// This is the core #209 regression check: before the fix, reconstruct
	// never consulted archive_state and would return only the live event.
	all, _, err := query.FetchMerged(context.Background(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      false,
		AllowGaps:      true, // h1 is archived, so technically no gap; keep true defensively
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err != nil {
		t.Fatalf("FetchMerged(NoArchive=false): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("NoArchive=false: expected 2 events (live + archive), got %d", len(all))
	}

	// MergeResults sorts by (event_timestamp, event_id) ascending, so the
	// archived h1 event must appear first.
	if all[0].StartPos != 100 {
		t.Errorf("merged[0]: expected archived event (start_pos=100), got %d", all[0].StartPos)
	}
	if all[1].StartPos != 200 {
		t.Errorf("merged[1]: expected live event (start_pos=200), got %d", all[1].StartPos)
	}
}

// TestFetchMerged_gapAbort verifies the gap-detection strict-abort path:
// when a time range covers hours that are neither live in MySQL nor archived,
// FetchMerged returns an error unless AllowGaps=true.
func TestFetchMerged_gapAbort(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Build ONE partition at h1, then query a range [h1, h3) — h2 is unmapped
	// (rotated out, not archived) and will be reported as a gap hour by the
	// planner.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	h3 := h2.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testdb", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	engine := query.New(db)
	opts := query.Options{
		Schema:   "testdb",
		Table:    "orders",
		PKValues: "42",
		Since:    ptrTime(h1),
		Until:    ptrTime(h3),
	}

	// Seed archive_state with a bogus archived partition at a far-away hour
	// so ResolveArchiveSources finds at least one source (the planner then
	// activates and classifies hours). The partition name must be a valid
	// p_YYYYMMDDHH literal (VARCHAR(12) column).
	far := time.Now().UTC().Add(-365 * 24 * time.Hour).Truncate(time.Hour)
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count)
		VALUES (?, 'dummy', '/tmp/dummy/bintrail_id=dummy/events.parquet', 0)`,
		partitionName(far))

	_, _, err := query.FetchMerged(context.Background(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      false,
		AllowGaps:      false,
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err == nil {
		t.Fatal("AllowGaps=false: expected gap error, got nil")
	}

	// ── Permissive mode (AllowGaps=true) → warn + proceed ──────────────────
	rows, _, err := query.FetchMerged(context.Background(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      false,
		AllowGaps:      true,
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err != nil {
		t.Fatalf("AllowGaps=true: expected success, got error: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("AllowGaps=true: expected 1 row (live h1 only), got %d", len(rows))
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
