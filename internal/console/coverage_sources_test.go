package console

import (
	"testing"
	"time"
)

// Restore Coverage answers "how far back can this be restored", and it used to
// derive that from bundle.baselineSrc alone (#1571). On a server with a local
// directory AND an S3 destination that is a verdict about a subset: a table
// whose only surviving anchor is in the bucket was not graded, not named
// broken, and not counted -- it was simply absent, and the panel reported a
// clean verdict over an inventory missing a table.
//
// This is the third instance of the shape; #1542 fixed the listing and #1541
// the restore. The helpers those added are what the fix reuses.
func TestCoverageAPI_gradesEveryBackupLocation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215") // the floor
	tsDir := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }

	primary, fallback := t.TempDir(), t.TempDir()
	// Healthy and well inside the floor, so the verdict is "ok" either way and
	// the test cannot pass merely because everything went unknown.
	writeBaselineFixture(t, primary, tsDir(time.Hour), "shop", "orders.parquet")
	// This table exists ONLY in the second location, and its newest anchor
	// predates the floor. Ungraded it vanishes; graded it is broken.
	writeBaselineFixture(t, fallback, tsDir(150*time.Hour), "shop", "archived.parquet")

	srv := newBaselineServerWithFallback(t, primary, fallback)
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if got.FullTableStatus != "ok" {
		t.Fatalf("status = %q, want ok: both locations answered", got.FullTableStatus)
	}
	found := false
	for _, b := range got.BrokenTables {
		if b == "shop.archived" {
			found = true
		}
	}
	if !found {
		t.Errorf("broken_tables = %v, want shop.archived named. Its only anchor lives in the "+
			"second backup location, so reading one location drops the table from the verdict "+
			"entirely: not covered, not broken, just absent from a panel that claims to say "+
			"what is restorable", got.BrokenTables)
	}
}

// A location that does not answer makes the verdict UNKNOWN, even when the
// other one did. A partial listing can only understate coverage, and an
// understated window names healthy tables broken -- the cry-wolf failure
// status.DeltaFloor already refuses when archives cannot be attributed.
// Neither "ok" over a subset nor "broken" from a subset: unknown.
func TestCoverageAPI_partialListingIsUnknownNotAShorterWindow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	tsDir := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }

	primary := t.TempDir()
	writeBaselineFixture(t, primary, tsDir(time.Hour), "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, primary, "/definitely/not/a/directory/1571")
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if got.FullTableStatus != "unknown" {
		t.Errorf("status = %q with one location unreadable, want unknown. The readable half is a "+
			"SUBSET, so grading it states a window narrower than the one that exists and can name "+
			"healthy tables broken", got.FullTableStatus)
	}
	if got.FullTableFrom != "" {
		t.Errorf("full_table_from = %q, want empty: an unknown verdict must claim no anchor", got.FullTableFrom)
	}
	if len(got.BrokenTables) != 0 {
		t.Errorf("broken_tables = %v, want none: a partial view must not accuse", got.BrokenTables)
	}
	// The delta half is computed from the index, not from the backup
	// locations, so it stays a real answer.
	if got.DeltaFrom == "" {
		t.Error("the live floor is independent of the backup listing and must still be reported")
	}
}
