//go:build integration

package archive

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestArchivePartition_nullBinlogFile is the rotate-path counterpart to
// query.TestFetch_nullBinlogFile (dbtrail/bintrail#318). Before the fix,
// ArchivePartition crashed at Scan when any row in the partition had a NULL
// binlog_file — same root cause as the query crash, but the consequence is
// worse: rotate failures block partition pruning and grow the index
// unbounded.
func TestArchivePartition_nullBinlogFile(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Relax the NOT NULL on binlog_file to simulate a drifted customer schema.
	if _, err := db.Exec(`ALTER TABLE binlog_events MODIFY binlog_file VARCHAR(255) NULL`); err != nil {
		t.Fatalf("ALTER TABLE failed: %v", err)
	}

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1",
		nil, nil, []byte(`{"id":1}`))
	if _, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, gtid,
		 schema_name, table_name, event_type, pk_values,
		 changed_columns, row_before, row_after)
		VALUES (NULL, 0, 0, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?)`,
		ts, "mydb", "orders", 1, "2", []byte(`{"id":2}`),
	); err != nil {
		t.Fatalf("insert NULL-binlog_file row failed: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "p_future.parquet")
	n, err := ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none")
	if err != nil {
		t.Fatalf("ArchivePartition failed: %v", err)
	}
	if n.Rows != 2 {
		t.Errorf("rowCount = %d, want 2", n.Rows)
	}

	// Verify NULL semantics at the Parquet layer: row 0 (the non-NULL row,
	// inserted first → lower event_id → sorted first by ORDER BY event_id)
	// must carry "binlog.000001"; row 1 must be a real Parquet NULL, not
	// an empty string. The nulls[1]=!binlogFile.Valid line in archive.go
	// is what makes this true — testing it specifically guards against a
	// regression that smuggles "" into the column.
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open archived parquet: %v", err)
	}
	defer rf.Close()
	info, _ := rf.Stat()
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	binlogFileIdx := parquetColumnIndex(t, pf, "binlog_file")
	reader := parquet.NewReader(pf)
	defer reader.Close()
	parquetRows := make([]parquet.Row, 2)
	if rn, err := reader.ReadRows(parquetRows); err != nil || rn != 2 {
		t.Fatalf("ReadRows returned (%d, %v), want (2, nil)", rn, err)
	}
	if parquetRows[0][binlogFileIdx].IsNull() {
		t.Errorf("row 0 binlog_file: got NULL, want \"binlog.000001\"")
	} else if got := parquetRows[0][binlogFileIdx].String(); got != "binlog.000001" {
		t.Errorf("row 0 binlog_file: got %q, want \"binlog.000001\"", got)
	}
	if !parquetRows[1][binlogFileIdx].IsNull() {
		t.Errorf("row 1 binlog_file: got %q, want NULL", parquetRows[1][binlogFileIdx].String())
	}
}

// TestArchivePartition_NoStrayTmpFileOnSuccess pins the issue #802 atomic-write
// invariant on the success path: ArchivePartition must write to
// outputPath+".tmp" and rename it into place, leaving no ".tmp" leftover once
// it returns without error.
func TestArchivePartition_NoStrayTmpFileOnSuccess(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1",
		nil, nil, []byte(`{"id":1}`))

	outPath := filepath.Join(t.TempDir(), "events.parquet")
	if _, err := ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none"); err != nil {
		t.Fatalf("ArchivePartition failed: %v", err)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("final output file must exist: %v", err)
	}
	if _, err := os.Stat(outPath + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("stray .tmp file left behind after a successful archive, stat err = %v", err)
	}
}

// TestArchivePartition_QueryErrorLeavesNoFinalFile reproduces the failure
// window issue #802 targets: an error partway through ArchivePartition (here,
// querying a partition that doesn't exist) must never leave anything at the
// final outputPath — only the (cleaned-up) .tmp file may have existed. Before
// the tmp+rename fix, ArchivePartition wrote directly to outputPath, so a
// crash after the writer was created but before Close() could leave a
// truncated file exactly there; a process kill (which skips Go's defer-based
// cleanup entirely) could not be caught by this test, but the underlying
// write target can: the fixed code physically cannot reach outputPath except
// via the final os.Rename.
func TestArchivePartition_QueryErrorLeavesNoFinalFile(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	outPath := filepath.Join(t.TempDir(), "events.parquet")
	_, err := ArchivePartition(context.Background(), db, dbName, "p_does_not_exist", outPath, "none")
	if err == nil {
		t.Fatal("expected an error archiving a nonexistent partition")
	}
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("final output path must not exist after a failed archive, stat err = %v", statErr)
	}
	if _, statErr := os.Stat(outPath + ".tmp"); !os.IsNotExist(statErr) {
		t.Errorf(".tmp file must be cleaned up after a failed archive, stat err = %v", statErr)
	}
}

// TestArchivePartition_queryTextRoundTrip pins the #699 wiring through the
// rotate path: the SELECT list, Scan targets, and the positional values/nulls
// arrays must stay aligned — a transposition of queryText/queryHash would
// archive "cleanly" with swapped fields at rest and nothing else would
// notice. Assert the exact values (and a real NULL) at the Parquet layer.
func TestArchivePartition_queryTextRoundTrip(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	stmt := "UPDATE mydb.orders SET amount = 5 WHERE id = 1"
	hash := "aa11bb22cc33"
	if _, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp,
		 schema_name, table_name, event_type, pk_values, row_after,
		 query_text, query_hash)
		VALUES (?, 100, 200, ?, 'mydb', 'orders', 2, '1', '{"id":1}', ?, ?)`,
		"binlog.000001", ts, stmt, hash,
	); err != nil {
		t.Fatalf("insert row with query_text failed: %v", err)
	}
	// Second row: statement not captured (both NULL).
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts, nil, "mydb", "orders", 1, "2",
		nil, nil, []byte(`{"id":2}`))

	outPath := filepath.Join(t.TempDir(), "p_future.parquet")
	n, err := ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none")
	if err != nil {
		t.Fatalf("ArchivePartition failed: %v", err)
	}
	if n.Rows != 2 {
		t.Fatalf("rowCount = %d, want 2", n.Rows)
	}

	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open archived parquet: %v", err)
	}
	defer rf.Close()
	info, _ := rf.Stat()
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	textIdx := parquetColumnIndex(t, pf, "query_text")
	hashIdx := parquetColumnIndex(t, pf, "query_hash")
	reader := parquet.NewReader(pf)
	defer reader.Close()

	// Row 0 (lower event_id, ORDER BY event_id) carries the exact values;
	// row 1 must be real Parquet NULLs.
	row := make([]parquet.Row, 1)
	if _, err := reader.ReadRows(row); err != nil {
		t.Fatalf("ReadRows row 0: %v", err)
	}
	if got := row[0][textIdx].String(); got != stmt {
		t.Errorf("row 0 query_text = %q, want %q (values/nulls transposition?)", got, stmt)
	}
	if got := row[0][hashIdx].String(); got != hash {
		t.Errorf("row 0 query_hash = %q, want %q", got, hash)
	}
	if _, err := reader.ReadRows(row); err != nil {
		t.Fatalf("ReadRows row 1: %v", err)
	}
	if !row[0][textIdx].IsNull() || !row[0][hashIdx].IsNull() {
		t.Errorf("row 1 query fields must be real Parquet NULLs, got text=%v hash=%v",
			row[0][textIdx], row[0][hashIdx])
	}
}

// TestArchivePartition_commitTsRoundTrip pins the #18 write half against a real
// index: the microsecond stamp reaches the Parquet file EXACTLY, and an event
// without one is written as a real Parquet NULL rather than a zero (which would
// read back as the epoch). The scan/values/nulls triple in ArchivePartition is
// positional — this is what catches a column added to the SELECT and forgotten
// in one of the other two.
func TestArchivePartition_commitTsRoundTrip(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const stamped = uint64(1771509600123456)
	ts := "2026-02-19 14:00:00"
	if _, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp,
		 schema_name, table_name, event_type, pk_values, row_after, commit_ts_us)
		VALUES (?, 100, 200, ?, 'mydb', 'orders', 2, '1', '{"id":1}', ?)`,
		"binlog.000001", ts, stamped,
	); err != nil {
		t.Fatalf("insert row with commit_ts_us failed: %v", err)
	}
	// Second row: no commit timestamp (MariaDB, or pre-8.0.1 MySQL).
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts, nil, "mydb", "orders", 1, "2",
		nil, nil, []byte(`{"id":2}`))

	outPath := filepath.Join(t.TempDir(), "p_future.parquet")
	n, err := ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none")
	if err != nil {
		t.Fatalf("ArchivePartition failed: %v", err)
	}
	if n.Rows != 2 {
		t.Fatalf("rowCount = %d, want 2", n.Rows)
	}

	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open archived parquet: %v", err)
	}
	defer rf.Close()
	info, _ := rf.Stat()
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	idx := parquetColumnIndex(t, pf, "commit_ts_us")
	reader := parquet.NewReader(pf)
	defer reader.Close()

	row := make([]parquet.Row, 1)
	if _, err := reader.ReadRows(row); err != nil {
		t.Fatalf("ReadRows row 0: %v", err)
	}
	if got := row[0][idx].Int64(); uint64(got) != stamped {
		t.Errorf("row 0 commit_ts_us = %d, want %d", got, stamped)
	}
	if _, err := reader.ReadRows(row); err != nil {
		t.Fatalf("ReadRows row 1: %v", err)
	}
	if !row[0][idx].IsNull() {
		t.Errorf("row 1 commit_ts_us must be a real Parquet NULL, got %v", row[0][idx])
	}
}

// ─── #1218: start_pos/end_pos above 2^63 through the rotate + restore paths ──

// oldSignedPositionColumnsTest reproduces the pre-#1218 archive schema:
// identical to BinlogEventColumns except start_pos/end_pos are the SIGNED
// Int(64) nodes the old MysqlToParquetNode("bigint") mapping produced. Every
// archive written before the fix carries this schema on disk forever.
func oldSignedPositionColumnsTest() []baseline.Column {
	cols := make([]baseline.Column, len(BinlogEventColumns))
	copy(cols, BinlogEventColumns)
	for i, c := range cols {
		if c.Name == "start_pos" || c.Name == "end_pos" {
			c.Unsigned = false
			c.ParquetType = baseline.MysqlToParquetNode("bigint")
			cols[i] = c
		}
	}
	return cols
}

// writePositionArchive writes one archive parquet file with the given schema
// and one INSERT row per (eventID, startPos, endPos, pk) tuple.
func writePositionArchive(t *testing.T, path string, cols []baseline.Column, rows [][4]string) {
	t.Helper()
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none"})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, r := range rows {
		values := []string{r[0], "mariadb-bin.000001", r[1], r[2], "2026-02-19 14:00:00", "", "", "mydb", "orders", "1", r[3], "", "", `{"id":` + r[3] + `}`, "0", "", ""}
		nulls := []bool{false, false, false, false, false, true, true, false, false, false, false, true, true, false, false, true, true}
		if err := w.WriteRow(values, nulls); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestArchivePartition_positionsAbove2to63 is the #1218 rotate-path pin: a
// partition holding the #986/#1117 MariaDB underflow shape (start_pos =
// 2^64 - EventSize, stored by pre-#1180 builds and still present in real
// indexes) used to fail ArchivePartition's int64 Scan — "scan row: ... value
// out of range" — so the partition never archived and rotation could never
// drop it. The full loop must now hold: MySQL → ArchivePartition → Parquet →
// parquetquery.Fetch, with the positions EXACT at the far end.
func TestArchivePartition_positionsAbove2to63(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const (
		bigStart = uint64(18446744073709551516) // 2^64 - 100
		bigEnd   = uint64(18446744073709551615) // max BIGINT UNSIGNED
	)
	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "mariadb-bin.000001", bigStart, bigEnd, ts, nil,
		"mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "mariadb-bin.000001", 100, 200, ts, nil,
		"mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`))

	dir := t.TempDir()
	outPath := filepath.Join(dir, "events.parquet")
	stats, err := ArchivePartition(context.Background(), db, dbName, "p_future", outPath, "none")
	if err != nil {
		t.Fatalf("ArchivePartition over >2^63 positions failed (the #1218 bug): %v", err)
	}
	if stats.Rows != 2 {
		t.Fatalf("rowCount = %d, want 2", stats.Rows)
	}

	rows, err := parquetquery.Fetch(context.Background(),
		query.Options{Schema: "mydb", Table: "orders", Limit: 10}, dir)
	if err != nil {
		t.Fatalf("Fetch over the archived file: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	byPK := map[string]query.ResultRow{}
	for _, r := range rows {
		byPK[r.PKValues] = r
	}
	if r := byPK["1"]; r.StartPos != bigStart || r.EndPos != bigEnd {
		t.Errorf("pk 1 positions = [%d, %d], want [%d, %d] (exact, no wrap)",
			r.StartPos, r.EndPos, bigStart, bigEnd)
	}
	if r := byPK["2"]; r.StartPos != 100 || r.EndPos != 200 {
		t.Errorf("pk 2 positions = [%d, %d], want [100, 200]", r.StartPos, r.EndPos)
	}
}

// TestRestorePartition_mixedSignedUnsignedPositionArchives pins restore-index's
// read side across both archive generations: an OLD file (signed Int64
// positions, pre-#1218) and a NEW file (unsigned Uint64, one position above
// 2^63) must both load back into binlog_events with exact values —
// RestorePartition scans per file, so each schema is exercised on its own.
func TestRestorePartition_mixedSignedUnsignedPositionArchives(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const (
		bigStart = uint64(18446744073709551516)
		bigEnd   = uint64(18446744073709551615)
	)
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old-signed.parquet")
	newPath := filepath.Join(dir, "new-unsigned.parquet")
	writePositionArchive(t, oldPath, oldSignedPositionColumnsTest(), [][4]string{
		{"1", "100", "200", "1"},
	})
	writePositionArchive(t, newPath, BinlogEventColumns, [][4]string{
		{"2", "18446744073709551516", "18446744073709551615", "2"},
	})

	for _, p := range []string{oldPath, newPath} {
		n, err := RestorePartition(context.Background(), db, p, 0)
		if err != nil {
			t.Fatalf("RestorePartition(%s): %v", filepath.Base(p), err)
		}
		if n != 1 {
			t.Fatalf("RestorePartition(%s) loaded %d rows, want 1", filepath.Base(p), n)
		}
	}

	check := func(pk string, wantStart, wantEnd uint64) {
		t.Helper()
		var start, end uint64
		if err := db.QueryRow(
			`SELECT start_pos, end_pos FROM binlog_events WHERE pk_values = ?`, pk).
			Scan(&start, &end); err != nil {
			t.Fatalf("read back pk %s: %v", pk, err)
		}
		if start != wantStart || end != wantEnd {
			t.Errorf("pk %s positions = [%d, %d], want [%d, %d]", pk, start, end, wantStart, wantEnd)
		}
	}
	check("1", 100, 200)
	check("2", bigStart, bigEnd)
}
