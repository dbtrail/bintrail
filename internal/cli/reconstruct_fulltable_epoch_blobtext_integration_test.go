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

// TestRunReconstruct_fullTable_blobTextEpochDrift is the end-to-end regression
// for #668: the full-table reconstruct writer path must type each delta
// event's BLOB/TEXT decode columns from the schema snapshot in effect AT THE
// EVENT'S TIMESTAMP, not from the latest snapshot. Before the fix,
// ReconstructTable resolved the decode-column set once from the latest
// snapshot and applied it to every event regardless of epoch.
//
// Repro: `body` starts as VARCHAR (epoch 1, delivered as a plain Go string by
// go-mysql — never base64-encoded at storage time) and is later widened to
// TEXT (epoch 2, delivered as []byte — base64-encoded at storage time, #668's
// own "VARCHAR→TEXT" example). An old event captured under epoch 1 stores its
// plain string "test" — which happens to be valid base64 — verbatim. A new
// event captured under epoch 2 stores its TEXT value base64-encoded. Typing
// both from the latest (epoch 2 / TEXT) snapshot makes the old plain "test"
// look like a BLOB/TEXT column needing decode, corrupting it to garbage bytes;
// typing each event by its own epoch (the fix) leaves it untouched and still
// correctly decodes the new event.
func TestRunReconstruct_fullTable_blobTextEpochDrift(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)

	epoch1TS := h1.Format("2006-01-02 15:04:05")
	epoch2TS := h2.Format("2006-01-02 15:04:05")

	// Epoch 1 (snapshot_id=1, at h1): body is VARCHAR.
	testutil.InsertSnapshot(t, db, 1, epoch1TS, "testdb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, epoch1TS, "testdb", "orders", "body", 2, "", "varchar", "NO")
	// Epoch 2 (snapshot_id=2, at h2): body widened to TEXT.
	testutil.InsertSnapshot(t, db, 2, epoch2TS, "testdb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 2, epoch2TS, "testdb", "orders", "body", 2, "", "text", "NO")

	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `body` TEXT NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"

	baselineDir := t.TempDir()
	snapshotTSDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, snapshotTSDir, "testdb")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	baselinePath := filepath.Join(parquetDir, "orders.parquet")

	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "body", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	}
	bw, err := baseline.NewWriter(baselinePath, cols, baseline.WriterConfig{
		Compression:  "zstd",
		RowGroupSize: 100,
		Metadata:     map[string]string{baseline.MetaKeyCreateTableSQL: createSQL},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := bw.WriteRow([]string{"1", "base-1"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	// id=2 INSERT under epoch 1 (h1+10m): body="test" stored PLAIN — VARCHAR
	// delivers as a Go string, never base64-encoded — and "test" happens to be
	// valid base64.
	ts1 := h1.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testdb", "orders", 1 /* INSERT */, "2", nil,
		nil, []byte(`{"id":2,"body":"test"}`))

	// id=3 INSERT under epoch 2 (h2+10m): body stored base64("hello text") —
	// TEXT delivers as []byte, base64-encoded by marshalRow at storage time.
	ts2 := h2.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil,
		"testdb", "orders", 1 /* INSERT */, "3", nil,
		nil, []byte(`{"id":3,"body":"aGVsbG8gdGV4dA=="}`))

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
	recTables = "testdb.orders"
	recChunkSize = "256MB"
	recParallelism = 1
	recAt = h2.Add(30 * time.Minute).Format(time.RFC3339)

	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	if err := runReconstruct(reconstructCmd, nil); err != nil {
		t.Fatalf("runReconstruct: %v", err)
	}

	// ── Apply the dump to a fresh destination and read back the rows ──────
	testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `testdb`")
	testutil.MustExec(t, db, "CREATE DATABASE `testdb`")
	t.Cleanup(func() {
		testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `testdb`")
	})

	schemaSQL, err := os.ReadFile(filepath.Join(outputDir, "testdb.orders-schema.sql"))
	if err != nil {
		t.Fatalf("read schema file: %v", err)
	}
	testutil.MustExec(t, db, "USE `testdb`")
	testutil.MustExec(t, db, string(schemaSQL))

	chunkSQL, err := os.ReadFile(filepath.Join(outputDir, "testdb.orders.00000.sql"))
	if err != nil {
		t.Fatalf("read chunk file: %v", err)
	}
	testutil.MustExec(t, db, string(chunkSQL))

	rows, err := db.Query("SELECT id, body FROM `testdb`.`orders` ORDER BY id")
	if err != nil {
		t.Fatalf("select restored: %v", err)
	}
	defer rows.Close()

	got := map[int]string{}
	for rows.Next() {
		var id int
		var body string
		if err := rows.Scan(&id, &body); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = body
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := map[int]string{
		1: "base-1",     // baseline pass-through, untouched
		2: "test",       // epoch-1 VARCHAR plain value: must survive untouched, NOT base64-decoded
		3: "hello text", // epoch-2 TEXT base64 value: must be decoded
	}
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Errorf("id=%d: missing from restored rows (got %v)", id, got)
			continue
		}
		if g != w {
			t.Errorf("id=%d: body = %q, want %q", id, g, w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("unexpected row count: got %v, want %v", got, want)
	}
}
