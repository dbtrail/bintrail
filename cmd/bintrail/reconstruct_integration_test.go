//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/archive"
	"github.com/dbtrail/bintrail/internal/baseline"
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
//  1. Create two adjacent hourly partitions 48h in the past (h1, h2).
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
	// Verify the error is typed so callers can unwrap it via errors.As.
	var gapErr *query.GapError
	if !errors.As(err, &gapErr) {
		t.Errorf("expected error to be a *query.GapError, got %T: %v", err, err)
	} else if len(gapErr.GapHours) == 0 {
		t.Error("expected at least one gap hour in GapError")
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

// TestFetchMerged_noArchiveKeepsPlannerGapDetection is the regression test
// for the recover --profile gap-warning regression flagged in PR #210 review
// (and doubles as the strict-mode gap detection check for reconstruct
// --no-archive). Before the fix, NoArchive=true silently disabled the query
// planner, so a caller using strict mode under --no-archive could silently
// miss gap hours. After the fix, the planner runs regardless of NoArchive
// because it only reads information_schema.PARTITIONS and archive_state.
func TestFetchMerged_noArchiveKeepsPlannerGapDetection(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// One live partition, then query a range that also covers an adjacent
	// unmapped hour (h2) — the planner must classify h2 as a gap even
	// though NoArchive=true prevents any archive discovery.
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

	// NoArchive=true + AllowGaps=false + h2 is a gap → must abort with GapError.
	// This is the new behavior introduced by the PR #210 review fix: the
	// planner runs independently of NoArchive, so gap detection is preserved.
	// ArchiveFetcher intentionally nil — NoArchive=true skips validation.
	_, _, err := query.FetchMerged(context.Background(), db, engine, query.FetchMergedOptions{
		Opts:      opts,
		DBName:    dbName,
		NoArchive: true,
		AllowGaps: false,
	})
	if err == nil {
		t.Fatal("NoArchive=true + AllowGaps=false: expected gap error, got nil")
	}
	var gapErr *query.GapError
	if !errors.As(err, &gapErr) {
		t.Fatalf("expected *query.GapError under NoArchive=true, got %T: %v", err, err)
	}
}

// TestFetchMerged_allArchiveSourcesFailStrict covers the I4 review finding:
// when every archive source returns an error, strict mode must abort rather
// than silently returning MySQL-only results. Under AllowGaps=true the
// failures are logged as warnings and the live results are returned.
func TestFetchMerged_allArchiveSourcesFailStrict(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testdb", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	// Register an archive_state row pointing at a real directory so
	// ResolveArchiveSources returns it (os.Stat must succeed on the base).
	archiveDir := t.TempDir()
	baseDir := filepath.Join(archiveDir, "bintrail_id=stub-failer")
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	fakeParquet := filepath.Join(baseDir, "event_date=2026-04-09", "event_hour=12", "events.parquet")
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count)
		VALUES (?, 'stub-failer', ?, 1)`,
		partitionName(h1), fakeParquet)

	stubErr := errors.New("stub archive failure (intentional)")
	stubFetcher := func(_ context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
		return nil, stubErr
	}

	engine := query.New(db)
	// Keep the query range inside a single mapped hour so the planner does
	// not report an adjacent-hour gap — we want this test to exercise the
	// archive-failure path, not the gap path. The planner's rangeEnd
	// rounds up to the next hour, so Until must land strictly within h1.
	opts := query.Options{
		Schema:   "testdb",
		Table:    "orders",
		PKValues: "42",
		Since:    ptrTime(h1),
		Until:    ptrTime(h1.Add(45 * time.Minute)),
	}

	// Strict mode: must abort because every archive source failed.
	_, _, err := query.FetchMerged(context.Background(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      false,
		AllowGaps:      false,
		ArchiveFetcher: stubFetcher,
	})
	if err == nil {
		t.Fatal("strict mode: expected archive failure error, got nil")
	}
	if !strings.Contains(err.Error(), "archive source") {
		t.Errorf("expected error message to mention archive sources, got: %v", err)
	}
	if !errors.Is(err, stubErr) {
		t.Errorf("expected wrapped stub error, got: %v", err)
	}

	// Permissive mode: must succeed with live rows despite archive failure.
	rows, _, err := query.FetchMerged(context.Background(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      false,
		AllowGaps:      true,
		ArchiveFetcher: stubFetcher,
	})
	if err != nil {
		t.Fatalf("permissive mode: expected success despite archive failure, got: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("permissive mode: expected 1 row from live MySQL, got %d", len(rows))
	}
}

// TestFetchMerged_noArchiveDoesNotCallFetcher is the positive replacement
// for the deleted panic-recovery test: prove that ArchiveFetcher is never
// invoked when NoArchive=true, even when the fetcher is non-nil.
func TestFetchMerged_noArchiveDoesNotCallFetcher(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	h1 := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts1 := h1.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testdb", "orders", 1, "7", nil, nil, []byte(`{"id":7}`))

	called := false
	stubFetcher := func(_ context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
		called = true
		return nil, nil
	}

	engine := query.New(db)
	// Query range stays strictly inside h1 so the planner doesn't flag the
	// adjacent hour as a gap and abort under AllowGaps=false. This test is
	// specifically about "archive fetcher is not invoked", not gap handling.
	rows, _, err := query.FetchMerged(context.Background(), db, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:   "testdb",
			Table:    "orders",
			PKValues: "7",
			Since:    ptrTime(h1),
			Until:    ptrTime(h1.Add(45 * time.Minute)),
		},
		DBName:         dbName,
		NoArchive:      true,
		AllowGaps:      false,
		ArchiveFetcher: stubFetcher,
	})
	if err != nil {
		t.Fatalf("FetchMerged: %v", err)
	}
	if called {
		t.Error("ArchiveFetcher was called with NoArchive=true; archive discovery must be disabled")
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 live row, got %d", len(rows))
	}
}

// TestRunReconstruct_archiveAwareE2E is the end-to-end regression test for
// #209: it drives the full runReconstruct CLI path (not just FetchMerged)
// with a real baseline Parquet, live + archived events, and a dropped live
// partition. Before #209 the output silently missed the archived event's
// effects; after the fix the final row state must reflect both the
// archived and live updates.
func TestRunReconstruct_archiveAwareE2E(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// ── 1. Write a minimal baseline Parquet ────────────────────────────────
	// Layout: <baselineDir>/<RFC3339 ts with : → ->/<schema>/<table>.parquet
	// Must match reconstruct.FindBaseline's directory conventions.
	//
	// The baseline snapshot timestamp is the lower bound of reconstruct's
	// event-fetch range (since=snapshotTime). To keep the planner from
	// classifying hours between the baseline and h1 as gaps, anchor the
	// snapshot at the start of h1 — the same partition where the first
	// event lives. Built below, after h1 is known.
	baselineDir := t.TempDir()
	// parquetDir and baselinePath are assigned after h1 is known so the
	// snapshot timestamp can anchor at h1.
	var baselinePath string

	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	// ── 2. Set up two live partitions and insert UPDATE events ─────────────
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	// Now anchor the baseline snapshot at h1 so reconstruct's query range
	// becomes [h1, h2 + 30min], which the planner classifies as hours
	// {h1, h2} — both mapped (h1 archived, h2 live). No gap.
	snapshotTSDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, snapshotTSDir, "testdb")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	baselinePath = filepath.Join(parquetDir, "orders.parquet")

	w, err := baseline.NewWriter(baselinePath, cols, baseline.WriterConfig{
		Compression:  "zstd",
		RowGroupSize: 100,
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"42", "baseline"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	// Event E1 (in h1): baseline → archived-state
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testdb", "orders", 2 /* UPDATE */, "42", nil,
		[]byte(`{"id":42,"status":"baseline"}`),
		[]byte(`{"id":42,"status":"archived-state"}`))
	// Event E2 (in h2): archived-state → live-state
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil,
		"testdb", "orders", 2 /* UPDATE */, "42", nil,
		[]byte(`{"id":42,"status":"archived-state"}`),
		[]byte(`{"id":42,"status":"live-state"}`))

	// ── 3. Archive h1 and drop it from live MySQL ──────────────────────────
	archiveDir := t.TempDir()
	bintrailID := "test-209-e2e"
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

	// ── 4. Drive runReconstruct via package-level flag vars ────────────────
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recIndexDSN = testutil.SnapshotDSN(dbName)
	recSchema = "testdb"
	recTable = "orders"
	recPK = "42"
	recPKColumns = "id"
	recBaselineDir = baselineDir
	recBaselineS3 = ""
	recBaselineOnly = false
	recHistory = false
	recSQL = ""
	recFormat = "json"
	recNoArchive = false
	recAllowGaps = false
	// --at must land strictly inside h2 so the planner's range [h1, h2+30min)
	// covers only hours {h1, h2}. h2.Add(time.Hour) would pull the planner
	// into h2+1h which is unmapped.
	recAt = h2.Add(30 * time.Minute).Format(time.RFC3339)

	// cobra v1.10 Command.Context() returns c.ctx with no Background fallback,
	// so a test that drives runReconstruct directly (without rootCmd.Execute)
	// must explicitly seed the context or the downstream DuckDB call panics
	// on a nil ctx.
	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	// ── 5. Capture stdout and run ──────────────────────────────────────────
	oldStdout := os.Stdout
	r, wPipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = wPipe

	runErr := runReconstruct(reconstructCmd, nil)

	wPipe.Close()
	os.Stdout = oldStdout
	outputBytes, _ := io.ReadAll(r)
	output := string(outputBytes)

	if runErr != nil {
		t.Fatalf("runReconstruct failed: %v\noutput: %s", runErr, output)
	}

	// ── 6. Assert output reflects BOTH events ──────────────────────────────
	// The final state must be "live-state" (applied from E2). Crucially,
	// E2's row_before is "archived-state" — so for ApplyAt to reach
	// "live-state", it had to first observe E1 (the archived event), which
	// proves the archive was fetched. Before #209 the archived h1 partition
	// was dropped from MySQL, so engine.Fetch alone would have returned only
	// E2 applied against a baseline still reading "baseline" — the final
	// state would still be "live-state" but the reconstruction would not
	// have been semantically consistent. We assert on the final state and
	// on the intermediate step via --history in a companion test below.
	if !strings.Contains(output, `"live-state"`) {
		t.Errorf("expected final state to contain live-state, got: %s", output)
	}
	if strings.Contains(output, `"baseline"`) {
		t.Errorf("final state should reflect events, not raw baseline, got: %s", output)
	}

	// ── 7. Run again with --history to prove BOTH events were applied ──────
	// history mode emits one entry per event. With E1 dropped from MySQL,
	// history will only include E2 unless the archive path works — so this
	// is the sharpest assertion of the #209 fix.
	recHistory = true
	recFormat = "json"

	r2, wPipe2, _ := os.Pipe()
	os.Stdout = wPipe2
	historyErr := runReconstruct(reconstructCmd, nil)
	wPipe2.Close()
	os.Stdout = oldStdout
	historyBytes, _ := io.ReadAll(r2)
	history := string(historyBytes)

	if historyErr != nil {
		t.Fatalf("runReconstruct --history failed: %v\noutput: %s", historyErr, history)
	}
	// Expect: baseline entry + E1 (archived-state) + E2 (live-state) = three entries.
	// If the archive is silently missed, we would see only baseline + E2.
	if !strings.Contains(history, `"archived-state"`) {
		t.Errorf("expected history to include the archived event's state transition, got: %s", history)
	}
	if !strings.Contains(history, `"live-state"`) {
		t.Errorf("expected history to include the live event's state transition, got: %s", history)
	}
}
