//go:build integration

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunReconstruct_fullTableRoundTrip is the end-to-end acceptance test
// for #187 (full-table reconstruct is a brand-new feature, not a bug fix,
// so this is an acceptance test rather than a regression test). It builds
// a baseline + live events + an archived-and-dropped partition, runs
// `bintrail reconstruct --output-format mydumper`, then applies the
// generated CREATE TABLE + INSERT files against a fresh table and verifies
// the restored row set matches the expected merged state.
//
// Passing this test means: the dump directory is restorable with a plain
// mysql client, the merge semantics are correct, and the archive fetch
// path actually provides events that were dropped from live MySQL.
//
// #1129: the reconstruct is run three times, at --fetch-batch-size 0
// (the 100000 default = one page over this fixture), 1, and 2, and the
// emitted dumps must be byte-identical across all three. Batch size 1 puts
// a page boundary after every event — including between the two same-second
// event pairs — so keyset tie-handling, cross-page last-write-wins, the
// decoder state reused across pages, and the #843 image-column intersection
// accumulating across pages are all exercised on every event. Any paging
// off-by-one (skipped or duplicated event) produces a dump diff.
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
	// Event matrix (4 events; the pairs at ts1 and ts2 share a second, so
	// keyset paging at batch size 1 must break the tie on event_id):
	//   id=2 UPDATE (h1) start-2 → paid   (will be in archive)
	//   id=3 DELETE (h1)         start-3  (will be in archive)
	//   id=4 INSERT (h2) new-4            (live; not in baseline)
	//   id=2 UPDATE (h2) paid    → shipped (live; latest event wins for id=2)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
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
	const wantEventsApplied = 4

	// ── 4. Archive h1 and drop it from live MySQL ────────────────────────
	archiveDir := t.TempDir()
	bintrailID := "test-187-roundtrip"
	outPath, err := rotation.HiveArchivePath(archiveDir, bintrailID, indexer.PartitionName(h1))
	if err != nil {
		t.Fatalf("hiveArchivePath: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}
	if _, err := archive.ArchivePartition(context.Background(), db, dbName, indexer.PartitionName(h1), outPath, "zstd"); err != nil {
		t.Fatalf("ArchivePartition: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES (?, ?, ?, 2, NULL, NULL, NULL)`,
		indexer.PartitionName(h1), bintrailID, outPath)
	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` DROP PARTITION `%s`",
		dbName, indexer.PartitionName(h1),
	))

	// ── 5. Drive runReconstructFullTable via flag vars ───────────────────
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	// #187/#1097 use a separate set of flag variables, all of which we must
	// reset after the test. Save them explicitly.
	savedOutputFormat := recOutputFormat
	savedOutputDir := recOutputDir
	savedTables := recTables
	savedChunkSize := recChunkSize
	savedParallelism := recParallelism
	savedFetchBatch := recFetchBatch
	t.Cleanup(func() {
		recOutputFormat = savedOutputFormat
		recOutputDir = savedOutputDir
		recTables = savedTables
		recChunkSize = savedChunkSize
		recParallelism = savedParallelism
		recFetchBatch = savedFetchBatch
	})

	recIndexDSN = testutil.SnapshotDSN(dbName)
	recBaselineDir = baselineDir
	recBaselineS3 = ""
	recAllowGaps = false
	recNoArchive = false
	recOutputFormat = "mydumper"
	recTables = "testdb.orders"
	recChunkSize = "256MB"
	recParallelism = 1
	// --at lands strictly inside h2 so the planner classifies hours {h1, h2}
	// without spilling into h2+1h (which would be a gap).
	recAt = h2.Add(30 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	// #1129: run the identical reconstruct at three page sizes into three
	// output directories. Batch size 0 is the 100000 default (one page over
	// this fixture — the pre-#1129 coverage); 1 and 2 force multi-page runs
	// over both the archive and live sources.
	batchSizes := []int{0, 1, 2}
	outDirs := make([]string, len(batchSizes))
	eventsApplied := make([]int64, len(batchSizes))
	for i, bs := range batchSizes {
		outDirs[i] = t.TempDir()
		recOutputDir = outDirs[i]
		recFetchBatch = bs

		// runReconstruct returns only an error; the per-table report reaches
		// the CLI layer as the "table dump complete" slog summary, so capture
		// logs to assert EventsApplied. slog.SetDefault mutates process-global
		// state — do not t.Parallel() this test.
		var logBuf bytes.Buffer
		prevLogger := slog.Default()
		t.Cleanup(func() { slog.SetDefault(prevLogger) })
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
		err := runReconstruct(reconstructCmd, nil)
		slog.SetDefault(prevLogger)
		if err != nil {
			t.Fatalf("runReconstruct (fetch-batch-size=%d): %v", bs, err)
		}
		eventsApplied[i] = eventsAppliedFromLogs(t, logBuf.Bytes(), bs)
	}

	// Every page size must observe exactly the four indexed events — a paging
	// bug that skips or duplicates an event shows up here even if the dump
	// happens to come out right (e.g. a duplicated no-op re-apply).
	for i, bs := range batchSizes {
		if eventsApplied[i] != wantEventsApplied {
			t.Errorf("fetch-batch-size=%d: events_applied = %d, want %d",
				bs, eventsApplied[i], wantEventsApplied)
		}
	}

	// ── 6. The emitted dumps must be byte-identical across page sizes ────
	// The only nondeterministic content in the output directory is the
	// "# Started dump at:" wall-clock line in the metadata file
	// (WriteMetadataFile); readDumpDir strips exactly that line and the rest
	// is compared byte-for-byte.
	refDump := readDumpDir(t, outDirs[0])
	if len(refDump) == 0 {
		t.Fatal("reference run emitted no output files; byte-identity below would compare nothing")
	}
	for i := 1; i < len(batchSizes); i++ {
		gotDump := readDumpDir(t, outDirs[i])
		for name := range refDump {
			if _, ok := gotDump[name]; !ok {
				t.Errorf("fetch-batch-size=%d: missing output file %s", batchSizes[i], name)
			}
		}
		for name, got := range gotDump {
			want, ok := refDump[name]
			if !ok {
				t.Errorf("fetch-batch-size=%d: unexpected output file %s", batchSizes[i], name)
				continue
			}
			if got != want {
				t.Errorf("fetch-batch-size=%d: %s differs from fetch-batch-size=%d\n--- batch %d ---\n%s\n--- batch %d ---\n%s",
					batchSizes[i], name, batchSizes[0], batchSizes[0], want, batchSizes[i], got)
			}
		}
	}

	// ── 7. Inspect one output directory ──────────────────────────────────
	// Byte-identity above makes the choice of directory immaterial; use the
	// default-batch run for the restore.
	outputDir := outDirs[0]
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

	// ── 8. Apply the dump to a fresh destination within the same DB ──────
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

	// ── 9. Read the restored rows and assert the merged state ───────────
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

// eventsAppliedFromLogs extracts events_applied from the "table dump
// complete" summary line runReconstructFullTable emits per table. The
// fixture reconstructs exactly one table, so exactly one such line must
// exist per run.
func eventsAppliedFromLogs(t *testing.T, logs []byte, batchSize int) int64 {
	t.Helper()
	var found bool
	var events int64
	for line := range strings.SplitSeq(string(logs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("fetch-batch-size=%d: unparseable log line %q: %v", batchSize, line, err)
		}
		if rec["msg"] != "table dump complete" {
			continue
		}
		if found {
			t.Fatalf("fetch-batch-size=%d: more than one 'table dump complete' log line", batchSize)
		}
		found = true
		v, ok := rec["events_applied"].(float64)
		if !ok {
			t.Fatalf("fetch-batch-size=%d: 'table dump complete' line lacks numeric events_applied: %q", batchSize, line)
		}
		events = int64(v)
	}
	if !found {
		t.Fatalf("fetch-batch-size=%d: no 'table dump complete' log line found in:\n%s", batchSize, logs)
	}
	return events
}

// readDumpDir reads every file in a reconstruct output directory into a
// name → content map. The metadata file's "# Started dump at:" line is the
// one wall-clock-dependent byte sequence in the whole output (see
// WriteMetadataFile), so it is stripped before comparison; everything else
// is compared verbatim.
func readDumpDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dump dir %s: %v", dir, err)
	}
	out := make(map[string]string, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			t.Fatalf("unexpected subdirectory %s in dump dir %s", e.Name(), dir)
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read dump file %s: %v", e.Name(), err)
		}
		content := string(data)
		if e.Name() == "metadata" {
			var kept []string
			for line := range strings.SplitSeq(content, "\n") {
				if strings.HasPrefix(line, "# Started dump at:") {
					continue
				}
				kept = append(kept, line)
			}
			content = strings.Join(kept, "\n")
		}
		out[e.Name()] = content
	}
	return out
}
