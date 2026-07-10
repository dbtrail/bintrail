package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
)

// TestFetchSnapshot_limitDeterministic pins the #839 fix: a LIMIT pushed into
// the baseline parquet_scan must ride on a deterministic ORDER BY. Without it,
// DuckDB's top-N picks an arbitrary subset (dependent on row-group layout and
// scan parallelism) — and even on a single-threaded scan it returns FILE order.
// The fixture writes ids in descending physical order, so the pre-fix code
// returned {9, 8} for Limit=2; ORDER BY ALL must return {1, 2}, with the
// synthetic EventIDs (MergeAndTrim's tie-break key) assigned in that order on
// every run.
func TestFetchSnapshot_limitDeterministic(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`COPY (SELECT * FROM (VALUES (9),(8),(7),(3),(2),(1)) t(id))` +
		` TO '` + p + `' (FORMAT PARQUET, KV_METADATA {'bintrail.snapshot_timestamp': '2026-07-01T00:00:00Z'})`); err != nil {
		t.Fatalf("write baseline fixture: %v", err)
	}

	et := event.EventSnapshot
	for run := range 2 {
		rows, err := FetchSnapshot(context.Background(), p,
			Options{Schema: "shop", Table: "orders", EventType: &et, Limit: 2})
		if err != nil {
			t.Fatalf("run %d: FetchSnapshot: %v", run, err)
		}
		if len(rows) != 2 {
			t.Fatalf("run %d: expected 2 rows, got %d", run, len(rows))
		}
		for i, want := range []string{"1", "2"} {
			if got := fmt.Sprint(rows[i].RowAfter["id"]); got != want {
				t.Errorf("run %d row %d: id = %s, want %s (LIMIT must keep the ORDER BY ALL-smallest rows)", run, i, got, want)
			}
			if rows[i].EventID != snapshotEventIDBase|uint64(i) {
				t.Errorf("run %d row %d: EventID = %d, want %d (synthetic IDs must follow the deterministic order)", run, i, rows[i].EventID, snapshotEventIDBase|uint64(i))
			}
		}
	}
}
