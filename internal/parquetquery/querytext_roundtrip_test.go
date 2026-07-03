package parquetquery

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── #699 real-file round-trips through Fetch ─────────────────────────────────
//
// TestBuildQueryQueryTextSubstitution pins the generated SQL; these two pin
// the actual DuckDB behavior: a current 17-column archive file round-trips
// query_text/query_hash through Fetch, and a pre-#699 15-column file — the
// backward-compat case the column-probe NULL substitution exists for — reads
// back nil fields with no error.

// writeArchiveFixture writes one parquet file with the given column schema
// under dir and returns the directory (Fetch takes the dir as source).
func writeArchiveFixture(t *testing.T, cols []baseline.Column, rows [][2][]string) string {
	t.Helper()
	dir := t.TempDir()
	w, err := baseline.NewWriter(filepath.Join(dir, "events.parquet"), cols, baseline.WriterConfig{Compression: "none"})
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
	return dir
}

func TestFetch_queryTextRoundTripCurrentSchema(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	// Row layout follows archive.BinlogEventColumns (17 columns): the first
	// row carries a statement + hash, the second has both NULL.
	stmt := "UPDATE mydb.orders SET amount = 5 WHERE id = 1"
	hash := "aa11bb22"
	mkRow := func(id, pk, text, textNull, hashVal, hashNull string) [2][]string {
		return [2][]string{
			{id, "binlog.000001", "100", "200", "2026-02-19 14:00:00", "", "", "mydb", "orders", "2", pk, "", `{"id":` + pk + `}`, `{"id":` + pk + `,"amount":5}`, "0", text, hashVal},
			{"0", "0", "0", "0", "0", "1", "1", "0", "0", "0", "0", "1", "0", "0", "0", textNull, hashNull},
		}
	}
	dir := writeArchiveFixture(t, archive.BinlogEventColumns, [][2][]string{
		mkRow("1", "1", stmt, "0", hash, "0"),
		mkRow("2", "2", "", "1", "", "1"),
	})

	rows, err := Fetch(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 10}, dir)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].QueryText == nil || *rows[0].QueryText != stmt {
		t.Errorf("row 1 QueryText = %v, want %q", rows[0].QueryText, stmt)
	}
	if rows[0].QueryHash == nil || *rows[0].QueryHash != hash {
		t.Errorf("row 1 QueryHash = %v, want %q", rows[0].QueryHash, hash)
	}
	if rows[1].QueryText != nil || rows[1].QueryHash != nil {
		t.Errorf("row 2 must read back nil query fields, got %v / %v", rows[1].QueryText, rows[1].QueryHash)
	}
}

func TestFetch_oldFifteenColumnArchiveReadsNullQueryText(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	// A REAL pre-#699 archive file: the first 15 columns of the current
	// schema, exactly what every v<0.27 rotate wrote. The probe must detect
	// the missing columns and substitute typed NULLs — an unguarded SELECT
	// would Binder-error, the pre-v0.4.4 connection_id incident class.
	oldCols := archive.BinlogEventColumns[:15]
	if oldCols[len(oldCols)-1].Name != "schema_version" {
		t.Fatalf("fixture assumption broken: 15th column is %q, want schema_version", oldCols[len(oldCols)-1].Name)
	}
	row := [2][]string{
		{"1", "binlog.000001", "100", "200", "2026-02-19 14:00:00", "", "", "mydb", "orders", "1", "1", "", "", `{"id":1}`, "0"},
		{"0", "0", "0", "0", "0", "1", "1", "0", "0", "0", "0", "1", "1", "0", "0"},
	}
	dir := writeArchiveFixture(t, oldCols, [][2][]string{row})

	rows, err := Fetch(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 10}, dir)
	if err != nil {
		t.Fatalf("Fetch over a pre-#699 archive must not error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].QueryText != nil || rows[0].QueryHash != nil {
		t.Errorf("pre-#699 file must read back nil query fields, got %v / %v", rows[0].QueryText, rows[0].QueryHash)
	}
	if rows[0].PKValues != "1" {
		t.Errorf("PKValues = %q, want 1 (scan alignment)", rows[0].PKValues)
	}
}
