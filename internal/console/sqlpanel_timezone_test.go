package console

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// ambientDuckDBZone opens a bare DuckDB the way openSandboxedSession does and
// reports the timezone it comes up with, i.e. the host's.
//
// The test below is only meaningful when that zone is NOT UTC: on a UTC host
// every assertion here passes with or without the production line, and the
// guard would be green for an unrelated reason. So the premise is measured
// rather than assumed.
func ambientDuckDBZone(t *testing.T) string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	var tz string
	if err := db.QueryRow("SELECT current_setting('TimeZone')").Scan(&tz); err != nil {
		t.Fatalf("read the ambient TimeZone: %v", err)
	}
	return tz
}

// TestSQLPanel_readsAndRendersInUTC pins that a panel session is already in
// UTC, so an operator never has to (and, with the SELECT-only gate, never
// could) type `SET TimeZone = 'UTC'` before their query means what they think.
//
// The archive column reads back as TIMESTAMP WITH TIME ZONE, so both the
// PRINTED form and the truncated VALUE move with the session's zone. The
// second matters more than the first: `date_trunc('day', event_timestamp)`
// under a host zone buckets events by a local midnight, and the daily counts
// that come out are wrong with nothing to see.
func TestSQLPanel_readsAndRendersInUTC(t *testing.T) {
	// UTC-5 all year, so there is no DST branch to reason about.
	t.Setenv("TZ", "America/Bogota")
	if tz := ambientDuckDBZone(t); tz == "UTC" {
		t.Skipf("the host resolves TimeZone to %q, so this test cannot tell the fix from its absence", tz)
	}

	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id) // one event at 2026-05-01 03:00:00 UTC
	in := panelInput([]string{filepath.Join(archiveRoot, "bintrail_id="+id)}, "", "")

	res, err := runSandboxedSQL(context.Background(), in,
		`SELECT strftime(event_timestamp, '%Y-%m-%d %H:%M:%S') AS printed,
		        date_trunc('day', event_timestamp) AS day
		   FROM events`, time.Now())
	if err != nil {
		t.Fatalf("query events view: %v", err)
	}
	if res.RowCount != 1 {
		t.Fatalf("rows = %d, want 1", res.RowCount)
	}
	// Under America/Bogota this prints 2026-04-30 22:00:00.
	if got := fmt.Sprint(res.Rows[0][0]); got != "2026-05-01 03:00:00" {
		t.Errorf("event_timestamp printed as %q, want the stored UTC instant 2026-05-01 03:00:00", got)
	}
	// Under America/Bogota the day starts at 05:00 UTC, so this comes back
	// 2026-05-01T05:00:00Z and the event lands in the wrong daily bucket.
	if got := fmt.Sprint(res.Rows[0][1]); got != "2026-05-01T00:00:00Z" {
		t.Errorf("date_trunc('day', event_timestamp) = %q, want 2026-05-01T00:00:00Z", got)
	}
}
