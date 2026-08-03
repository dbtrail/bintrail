//go:build integration

package metadata

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestNewResolver_loadsSnapshotExclusions pins the #1199 diagnosis end to end
// through the REAL production path: TakeSnapshotExcludingInvalid records the
// exclusion, NewResolver loads it back from snapshot_exclusions, and Resolve
// reports a validation-excluded table with a truthful, converging remediation
// (fix the table, THEN re-snapshot) instead of the stale-snapshot message
// whose "re-run `bintrail snapshot`" advice can never converge for it.
func TestNewResolver_loadsSnapshotExclusions(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE ok_tbl (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk_tbl (v INT) ENGINE=InnoDB`)

	stats, err := TakeSnapshotExcludingInvalid(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}

	r, err := NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	reason, excluded := r.ExclusionReason(sourceName, "nopk_tbl")
	if !excluded || reason != "no primary key" {
		t.Fatalf("ExclusionReason(%s.nopk_tbl) = (%q, %v), want (\"no primary key\", true)", sourceName, reason, excluded)
	}
	if _, ok := r.ExclusionReason(sourceName, "ok_tbl"); ok {
		t.Fatalf("ExclusionReason must be false for a captured table")
	}

	// Resolve on the excluded table: truthful diagnosis, no stale-snapshot text.
	_, err = r.Resolve(sourceName, "nopk_tbl")
	if err == nil {
		t.Fatal("Resolve on an excluded table must error")
	}
	if !strings.Contains(err.Error(), "excluded from snapshot") ||
		!strings.Contains(err.Error(), "not capturable as-is") {
		t.Errorf("excluded-table Resolve error must carry the exclusion diagnosis, got: %v", err)
	}
	if strings.Contains(err.Error(), "not found in snapshot") {
		t.Errorf("excluded-table Resolve error must not read as a stale snapshot, got: %v", err)
	}

	// Resolve on a genuinely unknown table: the stale diagnosis is intact.
	_, err = r.Resolve(sourceName, "ghost_tbl")
	if err == nil || !strings.Contains(err.Error(), "not found in snapshot") {
		t.Errorf("absent (non-excluded) table must keep the stale-snapshot diagnosis, got: %v", err)
	}
}

// TestNewResolver_toleratesMissingExclusionsTable: an index created before
// #1051 that only ever took strict snapshots has no snapshot_exclusions table.
// NewResolver must load fine with zero exclusions — not error, not warn-fail.
func TestNewResolver_toleratesMissingExclusionsTable(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	testutil.InsertSnapshot(t, indexDB, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.MustExec(t, indexDB, `DROP TABLE IF EXISTS snapshot_exclusions`)

	r, err := NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver on a pre-#1051 index: %v", err)
	}
	if _, ok := r.ExclusionReason("testdb", "orders"); ok {
		t.Fatal("no exclusions table must mean no exclusions")
	}
	if _, err := r.Resolve("testdb", "orders"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
}
