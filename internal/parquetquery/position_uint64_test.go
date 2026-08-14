package parquetquery

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── #1218 start_pos/end_pos above 2^63 through Fetch ────────────────────────
//
// start_pos/end_pos are BIGINT UNSIGNED in MySQL. Pre-#1180 builds wrote the
// MariaDB underflow shape (StartPos = 2^64 - EventSize) into real indexes, so
// a legitimate stored position can exceed 2^63. The archive schema stored them
// as SIGNED Int(64) parquet columns until #1218; the fixed schema is Uint(64).
// Both generations of file exist on disk simultaneously, and one multi-file
// DuckDB scan must read them together. These tests are the empirical proof of
// the mixed-schema design: an old signed file and a new unsigned file carrying
// a >2^63 position are scanned in ONE parquet_scan(union_by_name=true) and
// every position comes back EXACT — no float promotion, no wrap, no scan error.

const (
	// The #986/#1117 MariaDB underflow shape: 2^64 - EventSize.
	bigStartPos = uint64(18446744073709551516) // 2^64 - 100
	bigEndPos   = uint64(18446744073709551615) // 2^64 - 1 (max BIGINT UNSIGNED)
)

// oldSignedPositionColumns reproduces the pre-#1218 archive schema exactly:
// identical to archive.BinlogEventColumns except start_pos/end_pos are the
// SIGNED Int(64) nodes MysqlToParquetNode("bigint") used to produce. Every
// archive written before the fix carries this schema on disk forever, so the
// fixture writer is pinned here rather than deleted with the production code.
func oldSignedPositionColumns() []baseline.Column {
	cols := make([]baseline.Column, len(archive.BinlogEventColumns))
	copy(cols, archive.BinlogEventColumns)
	for i, c := range cols {
		if c.Name == "start_pos" || c.Name == "end_pos" {
			c.Unsigned = false
			c.ParquetType = baseline.MysqlToParquetNode("bigint")
			cols[i] = c
		}
	}
	return cols
}

// writeArchiveFileInto writes one parquet file with the given column schema at
// dir/name (writeArchiveFixture's sibling for multi-file directories).
func writeArchiveFileInto(t *testing.T, dir, name string, cols []baseline.Column, rows [][2][]string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none"})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, r := range rows {
		values, nullFlags := r[0], r[1]
		nulls := make([]bool, len(nullFlags))
		for i, f := range nullFlags {
			nulls[i] = f == "1"
		}
		if err := w.WriteRow(values, nulls); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

// positionRow builds one 17-value fixture row (commit_ts_us omitted → NULL)
// with the given event_id, timestamp and positions.
func positionRow(id, ts, startPos, endPos, pk string) [2][]string {
	return [2][]string{
		{id, "mariadb-bin.000001", startPos, endPos, ts, "", "", "mydb", "orders", "1", pk, "", "", `{"id":` + pk + `}`, "0", "", ""},
		{"0", "0", "0", "0", "0", "1", "1", "0", "0", "0", "0", "1", "1", "0", "0", "1", "1"},
	}
}

// TestFetch_mixedSignedUnsignedPositionArchives is the design-deciding test for
// #1218's read side: a directory holding an OLD archive (signed Int64
// positions, written pre-fix) next to a NEW archive (unsigned Uint64 positions,
// one of them above 2^63) is read through Fetch's real local-glob path — one
// parquet_scan over both files with union_by_name=true. Both files' positions
// must come back exactly.
func TestFetch_mixedSignedUnsignedPositionArchives(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	dir := t.TempDir()
	writeArchiveFileInto(t, dir, "old-signed.parquet", oldSignedPositionColumns(), [][2][]string{
		positionRow("1", "2026-02-19 14:00:00", "100", "200", "1"),
	})
	writeArchiveFileInto(t, dir, "new-unsigned.parquet", archive.BinlogEventColumns, [][2][]string{
		positionRow("2", "2026-02-19 15:00:00", "18446744073709551516", "18446744073709551615", "2"),
	})

	rows, err := Fetch(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 10}, dir)
	if err != nil {
		t.Fatalf("Fetch over mixed signed/unsigned position archives: %v", err)
	}
	assertMixedPositionRows(t, rows)
}

// TestQueryFileList_mixedSignedUnsignedPositionArchives drives the same mixed
// pair through queryFileList — the multi-file scan behind the S3-direct
// (--ultrafast) path, whose SQL comes from buildQueryFromFiles rather than
// buildQueryForFile. Local paths, per queryFileList's contract: only the
// transport differs from s3://.
func TestQueryFileList_mixedSignedUnsignedPositionArchives(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	dir := t.TempDir()
	oldFile := writeArchiveFileInto(t, dir, "old-signed.parquet", oldSignedPositionColumns(), [][2][]string{
		positionRow("1", "2026-02-19 14:00:00", "100", "200", "1"),
	})
	newFile := writeArchiveFileInto(t, dir, "new-unsigned.parquet", archive.BinlogEventColumns, [][2][]string{
		positionRow("2", "2026-02-19 15:00:00", "18446744073709551516", "18446744073709551615", "2"),
	})

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	rows, err := queryFileList(context.Background(), db, []string{oldFile, newFile},
		query.Options{Schema: "mydb", Table: "orders", Limit: 10})
	if err != nil {
		t.Fatalf("queryFileList over mixed signed/unsigned position archives: %v", err)
	}
	assertMixedPositionRows(t, rows)
}

func assertMixedPositionRows(t *testing.T, rows []query.ResultRow) {
	t.Helper()
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	byPK := map[string]query.ResultRow{}
	for _, r := range rows {
		byPK[r.PKValues] = r
	}
	oldRow, ok := byPK["1"]
	if !ok {
		t.Fatalf("old-signed file's row missing; got %v", byPK)
	}
	if oldRow.StartPos != 100 || oldRow.EndPos != 200 {
		t.Errorf("old file positions = [%d, %d], want [100, 200]", oldRow.StartPos, oldRow.EndPos)
	}
	newRow, ok := byPK["2"]
	if !ok {
		t.Fatalf("new-unsigned file's row missing; got %v", byPK)
	}
	if newRow.StartPos != bigStartPos || newRow.EndPos != bigEndPos {
		t.Errorf("new file positions = [%d, %d], want [%d, %d] (exact, no wrap/float promotion)",
			newRow.StartPos, newRow.EndPos, bigStartPos, bigEndPos)
	}
}

// TestFetch_oldSignedPositionArchiveAlone pins the single-file read of a
// pre-#1218 archive: no union promotion is in play (the whole scan is signed
// BIGINT), and the widened scan targets must still accept it.
func TestFetch_oldSignedPositionArchiveAlone(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	dir := t.TempDir()
	writeArchiveFileInto(t, dir, "old-signed.parquet", oldSignedPositionColumns(), [][2][]string{
		positionRow("1", "2026-02-19 14:00:00", "100", "200", "1"),
	})

	rows, err := Fetch(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 10}, dir)
	if err != nil {
		t.Fatalf("Fetch over an old signed-position archive: %v", err)
	}
	if len(rows) != 1 || rows[0].StartPos != 100 || rows[0].EndPos != 200 {
		t.Fatalf("rows = %+v, want one row with positions [100, 200]", rows)
	}
}

// TestFetch_untilPosAbove2to63 pins the position PREDICATE over a >2^63 anchor:
// buildFilters binds UntilPos.Pos as a uint64 and compares it against the
// (possibly type-promoted) end_pos column, so the cut must land exactly even
// when both sides exceed int64.
func TestFetch_untilPosAbove2to63(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	dir := t.TempDir()
	writeArchiveFileInto(t, dir, "new-unsigned.parquet", archive.BinlogEventColumns, [][2][]string{
		positionRow("1", "2026-02-19 14:00:00", "100", "200", "1"),                                   // ≤ anchor (included)
		positionRow("2", "2026-02-19 15:00:00", "18446744073709551516", "18446744073709551615", "2"), // > anchor (excluded)
		positionRow("3", "2026-02-19 15:30:00", "18446744073709551000", "18446744073709551516", "3"), // end == anchor (included)
	})

	rows, err := Fetch(context.Background(), query.Options{
		Schema: "mydb", Table: "orders",
		UntilPos: &query.BinlogPos{File: "mariadb-bin.000001", Pos: bigStartPos},
		Limit:    10,
	}, dir)
	if err != nil {
		t.Fatalf("Fetch with a >2^63 UntilPos anchor: %v", err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.PKValues] = true
	}
	if len(rows) != 2 || !got["1"] || !got["3"] {
		t.Fatalf("expected exactly pks {1,3} at-or-before the >2^63 anchor, got %v", got)
	}
}
