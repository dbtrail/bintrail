package parquetquery

import (
	"context"
	"os"
	"testing"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── #18 real-file round-trips through Fetch ─────────────────────────────────
//
// Adding a column to the archive schema has one failure mode that matters more
// than the new column working: every archive already on disk lacks it. These
// two tests cover both directions — a current file round-trips the microsecond
// stamp, and a pre-#18 file reads back nil instead of Binder-erroring, which is
// what the column probe's typed-NULL substitution exists for (the same
// incident class as pre-v0.4.4 connection_id and pre-#699 query_text).

func TestFetch_commitTsRoundTripCurrentSchema(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	// Sub-second digits are the point of the column: a value ending in
	// 000000 would survive a truncation bug unnoticed.
	const stamped = "1767225600123456"
	mkRow := func(id, pk, commitTs, commitNull string) [2][]string {
		return [2][]string{
			{id, "binlog.000001", "100", "200", "2026-02-19 14:00:00", "", "", "mydb", "orders", "2", pk, "", `{"id":` + pk + `}`, `{"id":` + pk + `,"amount":5}`, "0", "", "", commitTs},
			{"0", "0", "0", "0", "0", "1", "1", "0", "0", "0", "0", "1", "0", "0", "0", "1", "1", commitNull},
		}
	}
	dir := writeArchiveFixture(t, archive.BinlogEventColumns, [][2][]string{
		mkRow("1", "1", stamped, "0"),
		mkRow("2", "2", "", "1"),
	})

	rows, err := Fetch(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 10}, dir)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].CommitTsUS == nil {
		t.Fatalf("row 1 CommitTsUS = nil, want %s", stamped)
	}
	if got := *rows[0].CommitTsUS; got != 1767225600123456 {
		t.Errorf("row 1 CommitTsUS = %d, want 1767225600123456 (exact microseconds)", got)
	}
	if rows[1].CommitTsUS != nil {
		t.Errorf("row 2 must read back a nil CommitTsUS, got %d", *rows[1].CommitTsUS)
	}
}

func TestFetch_preCommitTsArchiveReadsNull(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	// A REAL pre-#18 archive: the first 17 columns of the current schema —
	// exactly what every rotate wrote between #699 and this change. An
	// unguarded SELECT of commit_ts_us over it would Binder-error and take
	// out archive reads for every existing installation.
	oldCols := archive.BinlogEventColumns[:17]
	if last := oldCols[len(oldCols)-1].Name; last != "query_hash" {
		t.Fatalf("fixture assumption broken: 17th column is %q, want query_hash", last)
	}
	row := [2][]string{
		{"1", "binlog.000001", "100", "200", "2026-02-19 14:00:00", "", "", "mydb", "orders", "1", "1", "", "", `{"id":1}`, "0", "", ""},
		{"0", "0", "0", "0", "0", "1", "1", "0", "0", "0", "0", "1", "1", "0", "0", "1", "1"},
	}
	dir := writeArchiveFixture(t, oldCols, [][2][]string{row})

	rows, err := Fetch(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 10}, dir)
	if err != nil {
		t.Fatalf("Fetch over a pre-#18 archive must not error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].CommitTsUS != nil {
		t.Errorf("a pre-#18 file must read back a nil CommitTsUS, got %d", *rows[0].CommitTsUS)
	}
	// Scan alignment: a mis-positioned substitution shifts every field.
	if rows[0].PKValues != "1" {
		t.Errorf("PKValues = %q, want 1 (scan alignment)", rows[0].PKValues)
	}
}
