//go:build integration

package query_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1324: Plan swallowed EVERY loadArchiveCoverage failure with one slog.Debug
// and planned as if archive_state were empty. The two causes are not the same
// fact — "this index has no archive tier" makes the gaps true, "I could not
// read the archive tier" makes them unverified — and collapsing them let the
// console render "complete" over a window nobody checked, while the CLI told
// the operator their data was never archived when the real failure sat at
// Debug. Same conflation #816 retired in status.LoadCoverage, one layer over.
//
// This goes to a real database because the discrimination is on a driver
// error code; a fixture would assert the mapping we wrote rather than the one
// MySQL produces.
func TestPlanDistinguishesUnreadableArchivesFromNone(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	// An hour that is NOT a live partition (rotated out), recorded — when the
	// table is healthy — in archive_state.
	hour := time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)
	since, until := hour, hour.Add(30*time.Minute)
	plan := func(t *testing.T) *query.QueryPlan {
		t.Helper()
		p, err := query.Plan(ctx, db, dbName, &since, &until, false, query.AllArchives())
		if err != nil {
			t.Fatalf("Plan must stay non-fatal on an archive_state failure: %v", err)
		}
		if p == nil {
			t.Fatal("Plan returned nil for a bounded range")
		}
		return p
	}

	// Healthy: the archived hour counts as covered and nothing is flagged.
	exec(`INSERT INTO archive_state (partition_name, bintrail_id, local_path)
	      VALUES ('p_2020010203', 'src', '/archives/bintrail_id=src/x.parquet')`)
	p := plan(t)
	if len(p.GapHours) != 0 || p.ArchiveCoverageUnavailable {
		t.Errorf("healthy archive_state: gaps=%d unavailable=%v, want 0/false",
			len(p.GapHours), p.ArchiveCoverageUnavailable)
	}

	// No archive tier at all (ER_NO_SUCH_TABLE): the gap IS the truth, so no
	// flag. This is the case the old blanket Debug was written for, and it
	// must not regress into a warning on every pre-archive index.
	exec(`DROP TABLE archive_state`)
	p = plan(t)
	if len(p.GapHours) != 1 {
		t.Errorf("no archive tier: %d gap hour(s), want 1", len(p.GapHours))
	}
	if p.ArchiveCoverageUnavailable {
		t.Error("an index with no archive_state was flagged unavailable; 'no archive tier' is a fact, not a failure")
	}

	// Present but unreadable — a shape missing partition_name entirely, the
	// "table exists, query fails" class (both the #1037 query and its 1054
	// legacy fallback fail on it). THE regression: before #1324 this planned
	// exactly like the case above.
	exec(`CREATE TABLE archive_state (bintrail_id VARCHAR(64) NOT NULL PRIMARY KEY)`)
	p = plan(t)
	if !p.ArchiveCoverageUnavailable {
		t.Fatal("an unreadable archive_state planned as 'no archives'; the console renders 'complete' off exactly this")
	}
	if len(p.GapHours) != 1 {
		t.Errorf("fail-closed must hold: the unverifiable hour classified as %d gap(s), want 1", len(p.GapHours))
	}

	// A legacy pre-#1037 shape (partition_name, no min/max_event_ts) reads
	// through the 1054 fallback: coverage works and nothing is flagged — that
	// fallback is a supported path, not a failure.
	exec(`DROP TABLE archive_state`)
	exec(`CREATE TABLE archive_state (
	        partition_name VARCHAR(64) NOT NULL,
	        bintrail_id    VARCHAR(64) NOT NULL,
	        PRIMARY KEY (partition_name, bintrail_id))`)
	exec(`INSERT INTO archive_state (partition_name, bintrail_id) VALUES ('p_2020010203', 'src')`)
	p = plan(t)
	if len(p.GapHours) != 0 || p.ArchiveCoverageUnavailable {
		t.Errorf("legacy archive_state shape: gaps=%d unavailable=%v, want 0/false",
			len(p.GapHours), p.ArchiveCoverageUnavailable)
	}
}
