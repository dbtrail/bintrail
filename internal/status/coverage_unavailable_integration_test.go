//go:build integration

package status_test

import (
	"context"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #816: LoadCoverage degraded EVERY archive_state failure to live-only
// coverage with nothing but a slog.Warn. The two causes are not the same
// fact — "this index has no archive tier" is true and describable, "I could
// not read the archive tier" is not — and collapsing them makes `status`
// print a restore window SHORTER than reality. An operator reads it and
// concludes an old incident is unrecoverable while the Parquet covering it is
// sitting in the bucket.
//
// This goes to a real database because the discrimination is on a driver
// error code; a fixture would assert the mapping we wrote rather than the one
// MySQL produces.
func TestLoadCoverage_DistinguishesUnreadableArchivesFromNone(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	// Healthy: a real archive row extends the window and nothing is flagged.
	exec(`INSERT INTO archive_state (partition_name, bintrail_id, row_count)
	      VALUES ('p_2020010203', 'src', 41)`)
	c, err := status.LoadCoverage(ctx, db)
	if err != nil {
		t.Fatalf("LoadCoverage: %v", err)
	}
	if c.ArchiveUnavailable {
		t.Error("a readable archive_state was reported as unavailable")
	}
	if c.ArchiveTotalRows != 41 || !c.ArchiveEarliestHour.Valid {
		t.Errorf("archive coverage not read: rows=%d earliest=%v", c.ArchiveTotalRows, c.ArchiveEarliestHour)
	}

	// No archive tier at all: the zeros ARE the truth, so no flag. This is
	// the case the old blanket warn was written for, and it must not regress
	// into a scary banner on every pre-archive index.
	exec(`DROP TABLE archive_state`)
	c, err = status.LoadCoverage(ctx, db)
	if err != nil {
		t.Fatalf("LoadCoverage (no table): %v", err)
	}
	if c.ArchiveUnavailable {
		t.Error("an index with no archive_state was flagged unavailable; 'no archive tier' is a fact, not a failure")
	}
	if c.ArchiveTotalRows != 0 || c.ArchiveEarliestHour.Valid {
		t.Errorf("archive fields should be zero with no table: rows=%d earliest=%v", c.ArchiveTotalRows, c.ArchiveEarliestHour)
	}

	// Present but unreadable — here a legacy shape missing row_count (1054),
	// which is exactly the "table exists, query fails" class. THE regression:
	// before #816 this was indistinguishable from the case above.
	exec(`CREATE TABLE archive_state (
	        partition_name VARCHAR(64) NOT NULL,
	        bintrail_id    VARCHAR(64) NOT NULL,
	        PRIMARY KEY (partition_name, bintrail_id))`)
	exec(`INSERT INTO archive_state (partition_name, bintrail_id) VALUES ('p_2019010203', 'src')`)
	c, err = status.LoadCoverage(ctx, db)
	if err != nil {
		t.Fatalf("LoadCoverage must stay non-fatal — coverage is a report, not a gate: %v", err)
	}
	if !c.ArchiveUnavailable {
		t.Fatal("an unreadable archive_state was reported as 'no archives'; the restore window silently understates reality")
	}
	if c.ArchiveError == "" {
		t.Error("no reason recorded — an operator cannot act on 'unavailable' alone")
	}

	// The RENDERING assertions live in coverage_render_test.go, as a plain
	// unit test: they are pure functions over *CoverageInfo, and behind
	// //go:build integration they are skipped in silence on any machine
	// without MySQL. Only the discrimination below needs a real driver error.

	// A partition name that will not parse is the same class, reached one
	// step later: the row was read, and the archives still cannot be placed
	// in time.
	exec(`DROP TABLE archive_state`)
	exec(`CREATE TABLE archive_state (
	        partition_name VARCHAR(64) NOT NULL,
	        bintrail_id    VARCHAR(64) NOT NULL,
	        row_count      BIGINT DEFAULT 0,
	        PRIMARY KEY (partition_name, bintrail_id))`)
	exec(`INSERT INTO archive_state (partition_name, bintrail_id, row_count)
	      VALUES ('not-a-partition', 'src', 5)`)
	c, err = status.LoadCoverage(ctx, db)
	if err != nil {
		t.Fatalf("LoadCoverage (unparseable name): %v", err)
	}
	if !c.ArchiveUnavailable {
		t.Error("an unplaceable archive floor was reported as no archives; the restore window silently understates reality")
	}

}
