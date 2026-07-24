package parquetquery

import (
	"context"
	"os"
	"testing"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestFetch_untilPosRollover runs the DuckDB mirror of the #840 fix against a
// real parquet file: after mysql-bin.999999 the server continues with
// mysql-bin.1000000, and plain lexicographic binlog_file comparison inverts
// ('1000000' < '999999'). An anchor in the post-rollover file must keep every
// pre-rollover event and still cut exactly inside the anchor file — the same
// semantics the live-MySQL path is pinned to in internal/query's
// TestFetch_untilPosRollover, so the two mirrors stay in lockstep.
func TestFetch_untilPosRollover(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	// Row layout follows archive.BinlogEventColumns (17 columns): INSERT
	// events with NULL gtid/connection_id/changed_columns/row_before/query_*.
	mkRow := func(id, file, startPos, endPos, pk string) [2][]string {
		return [2][]string{
			{id, file, startPos, endPos, "2026-02-19 14:00:00", "", "", "mydb", "orders", "1", pk, "", "", `{"id":` + pk + `}`, "0", "", ""},
			{"0", "0", "0", "0", "0", "1", "1", "0", "0", "0", "0", "1", "1", "0", "0", "1", "1"},
		}
	}
	dir := writeArchiveFixture(t, archive.BinlogEventColumns, [][2][]string{
		mkRow("1", "mysql-bin.999999", "100", "200", "1"),  // pre-rollover (included; excluded pre-fix)
		mkRow("2", "mysql-bin.1000000", "100", "200", "2"), // anchor file, ≤ pos (included)
		mkRow("3", "mysql-bin.1000000", "200", "400", "3"), // anchor file, > pos (excluded)
		mkRow("4", "mysql-bin.1000001", "100", "200", "4"), // later file (excluded)
	})

	rows, err := Fetch(context.Background(), query.Options{
		Schema: "mydb", Table: "orders",
		UntilPos: &query.BinlogPos{File: "mysql-bin.1000000", Pos: 300},
		Limit:    100,
	}, dir)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.PKValues] = true
	}
	if len(rows) != 2 || !got["1"] || !got["2"] {
		t.Fatalf("expected exactly pks {1,2} at-or-before the rollover anchor, got %v", got)
	}
}
