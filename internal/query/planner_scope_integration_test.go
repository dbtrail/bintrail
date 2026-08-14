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

// The bug (#1232): rotation is per-process, so on an index two sources rotate
// into two destinations. Source A archives hour H into A's destination and
// drops the partition; B never archived it. A read that opens only B's
// archives fetches nothing for H — but the planner read archive_state
// unscoped, saw A's row, and called H covered. A strict AllowGaps=false
// reconstruct then proceeded over data it would never fetch.
//
// The fixture is deliberately the SAME archive_state either way; only the
// scope moves. That is what makes this a test of the scoping rule rather than
// of the fixture.
func TestPlanScopesCoverageToTheArchivesTheReadOpens(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}

	// An hour that is NOT a live partition (rotated out) but is recorded in
	// archive_state under source A only.
	hour := time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO archive_state (partition_name, bintrail_id, local_path)
		 VALUES (?, 'source-a', '/archives/bintrail_id=source-a/x.parquet')`,
		indexer.PartitionName(hour)); err != nil {
		t.Fatalf("seed archive_state: %v", err)
	}

	since, until := hour, hour.Add(30*time.Minute)
	gapHours := func(t *testing.T, scope query.ArchiveScope) int {
		t.Helper()
		p, err := query.Plan(ctx, db, dbName, &since, &until, false, scope)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if p == nil {
			t.Fatal("Plan returned nil for a bounded range")
		}
		return len(p.GapHours)
	}

	// A read that opens A's archives sees the hour as covered — it really
	// will fetch that file.
	if n := gapHours(t, query.OnlyArchives("source-a")); n != 0 {
		t.Errorf("a read scoped to the source that archived the hour reported %d gap hour(s); it will fetch that archive", n)
	}

	// THE REGRESSION: a read that opens only B's archives must NOT inherit
	// A's coverage. Before #1232 this returned 0 and a strict reconstruct
	// proceeded over an hour it could not read.
	if n := gapHours(t, query.OnlyArchives("source-b")); n != 1 {
		t.Errorf("a read scoped to a source that never archived the hour reported %d gap hour(s), want 1 — A's archive is being counted as B's coverage", n)
	}

	// A read that opens NO archives is the same statement in its strongest
	// form. If this reports 0, OnlyArchives() has collapsed into
	// AllArchives() — the exact conflation #1327's type exists to prevent.
	if n := gapHours(t, query.OnlyArchives()); n != 1 {
		t.Errorf("a read that opens no archives reported %d gap hour(s), want 1", n)
	}

	// AllArchives is "every archive in the index", the honest answer for a
	// caller that reads them all — and the one that keeps single-source
	// indexes and the index-wide gauges byte-identical to before.
	if n := gapHours(t, query.AllArchives()); n != 0 {
		t.Errorf("an unscoped plan reported %d gap hour(s), want 0 — this is the pre-#1232 behaviour every index-wide caller still relies on", n)
	}

	// Scoping must be a filter, not a rename: a source that archived the hour
	// is still covered when named alongside one that did not.
	if n := gapHours(t, query.OnlyArchives("source-b", "source-a")); n != 0 {
		t.Errorf("a read opening both destinations reported %d gap hour(s), want 0", n)
	}
}
