package views

import (
	"database/sql"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeNarrowFixtureArchive writes an archived partition the way a build that
// predates a column would have: the REAL writer, over a PREFIX of the real
// column set.
//
// The narrow shape is what makes union_by_name load-bearing on this layout and
// is therefore the only fixture that can tell a correct grouping from a wrong
// one. A fixture where every file has every column passes under any scheme,
// including the two DuckDB behaviours #1535 measured and rejected (silently
// dropping a column with the narrow file first, failing the read with the wide
// file first).
func writeNarrowFixtureArchive(t *testing.T, root, id, date, hour string, drop int) []string {
	t.Helper()
	cols := slices.Clone(archive.BinlogEventColumns)
	cols = cols[:len(cols)-drop]
	path := filepath.Join(root, "bintrail_id="+id, "event_date="+date, "event_hour="+hour, "events.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("archive writer: %v", err)
	}
	values := []string{
		"7", "binlog.000001", "100", "200", "2026-05-01 " + hour + ":00:00", "",
		"42", "shop", "orders", "1", "7",
		`["status"]`, `{"id":7}`, `{"id":7,"status":"paid"}`,
		"1", "UPDATE orders SET status='paid'", "abc123", "1777000000000000",
	}
	nulls := make([]bool, len(cols))
	nulls[5] = true // gtid
	if err := w.WriteRow(values[:len(cols)], nulls); err != nil {
		t.Fatalf("write archive row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close archive writer: %v", err)
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// TestGroupedEvents_executeInDuckDB is the discriminating test for #1535: two
// groups whose column sets DIFFER, executed by the real engine, queried for the
// column only one of them has.
//
// A generator test cannot catch this. The two failure modes the issue measured
// are both SILENT or engine-side: reading both groups' files in one
// union_by_name = false scan either drops query_text from the result with no
// error at all (narrow file first) or fails at read time (wide file first).
// Only running the SQL distinguishes them, and only a fixture where one file
// genuinely lacks the column makes either reachable.
func TestGroupedEvents_executeInDuckDB(t *testing.T) {
	root := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	// Two generations of the same source: an old partition written before
	// query_text/query_hash/commit_ts_us existed, and a current one.
	oldCols := writeNarrowFixtureArchive(t, root, id, "2026-04-01", "01", 3)
	newCols := writeNarrowFixtureArchive(t, root, id, "2026-05-01", "03", 0)
	if len(oldCols) == len(newCols) {
		t.Fatal("the fixture's two groups have the same column set, so it cannot discriminate")
	}

	base := filepath.Join(root, "bintrail_id="+id)
	in := Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: []string{base},
		ArchiveGroups: []ArchiveGroup{
			{Columns: lower(oldCols), Files: []string{filepath.Join(base, "event_date=2026-04-01", "event_hour=01", "events.parquet")}},
			{Columns: lower(newCols), Files: []string{filepath.Join(base, "event_date=2026-05-01", "event_hour=03", "events.parquet")}},
		},
	}
	sqlText := Generate(in)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("DuckDB rejected the grouped views:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}

	// Both groups' rows are in the view, each attributed to its own partition.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 2 {
		t.Fatalf("events has %d row(s), want 2 — one group is missing from the union", n)
	}

	// The column only the NEW group has: present on its row, NULL on the old
	// one. Both halves matter. A wrong grouping that drops the column entirely
	// still returns two rows and still has query_text = NULL on both.
	rows, err := db.Query(`SELECT CAST("event_date" AS VARCHAR), "query_text" FROM events ORDER BY 1`)
	if err != nil {
		t.Fatalf("query query_text: %v", err)
	}
	defer rows.Close()
	got := map[string]sql.NullString{}
	for rows.Next() {
		var d string
		var q sql.NullString
		if err := rows.Scan(&d, &q); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[d] = q
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if q := got["2026-04-01"]; q.Valid {
		t.Errorf("the old partition reports query_text = %q; its file does not have the column, so it must read NULL", q.String)
	}
	if q := got["2026-05-01"]; !q.Valid || q.String != "UPDATE orders SET status='paid'" {
		t.Errorf("the new partition's query_text = %+v, want the statement it stored — "+
			"a group that reads its files without its own column set loses the column silently", q)
	}

	// commit_time is derived from commit_ts_us, which the old group lacks. The
	// derived column has to be padded too, or the two legs project a different
	// number of columns and the UNION lines the wrong pairs up.
	var ct sql.NullTime
	if err := db.QueryRow(`SELECT "commit_time" FROM events WHERE CAST("event_date" AS VARCHAR) = '2026-04-01'`).Scan(&ct); err != nil {
		t.Fatalf("query commit_time: %v", err)
	}
	if ct.Valid {
		t.Errorf("the old partition reports commit_time = %s from a file with no commit_ts_us", ct.Time)
	}

	// Not cosmetic: union_by_name = true here is the 114s bind the issue
	// measured, and it would still pass every assertion above.
	if strings.Contains(sqlText, "union_by_name = true") {
		t.Errorf("a grouped events view still asks for union_by_name; the bind stays O(archived files):\n%s", sqlText)
	}
}

func lower(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = strings.ToLower(n)
	}
	return out
}
