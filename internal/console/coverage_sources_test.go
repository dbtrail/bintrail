package console

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
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

// The listing behind this card reads every backup location (#1571); the
// Restore button beside it folds from the local directory alone (#1541). A
// table whose only usable anchor is in the bucket must therefore NOT advance
// full_table_from -- the card would print a start the button then refuses with
// "no backup exists at or before <t>" -- and must NOT join broken_tables,
// which drives an alarm a backup that exists off site does not deserve.
//
// Driven through gradeFullTable rather than the endpoint because an s3://
// location cannot be listed from a unit test, so a handler-level test can only
// ever produce local anchors: exactly the shape this split is not about.
func TestGradeFullTable_anOffsiteAnchorDoesNotWidenTheRestoreWindow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	below, above := now.Add(-150*time.Hour), now.Add(-2*time.Hour)
	// OLDER than the S3-only anchor on purpose: the window is the LATEST of the
	// per-table starts, so counting the offsite anchor would move `from`
	// forward to it. With the local table newer, both readings coincide and
	// the assertion below could not tell them apart.
	localOrders := now.Add(-9 * time.Hour)

	got := gradeFullTable([]reconstruct.BaselineFile{
		// A healthy local table, which is what full_table_from may name.
		{Schema: "shop", Table: "orders", SnapshotTime: localOrders, Path: "/backups/2026/shop/orders.parquet"},
		// Local copy aged out below the floor; the usable one is in S3 only.
		{Schema: "shop", Table: "carts", SnapshotTime: below, Path: "/backups/old/shop/carts.parquet"},
		{Schema: "shop", Table: "carts", SnapshotTime: above, Path: "s3://bucket/prefix/2026/shop/carts.parquet"},
	}, floor, now)

	if got.from.Equal(above) {
		t.Errorf("full_table_from advanced to %s, the S3-only anchor. The console's Restore "+
			"folds from BaselineDir and never opens the bucket (#1541), so this start is one "+
			"the button refuses -- a green panel over a restore that cannot run", above)
	}
	if !got.from.Equal(localOrders) {
		t.Errorf("from = %s, want %s: the window is the latest LOCAL earliest-usable anchor", got.from, localOrders)
	}
	if len(got.broken) != 0 {
		t.Errorf("broken = %v, want none: shop.carts has a current backup, it is just off site. "+
			"broken_tables drives an alarm and says 'take a fresh backup', which is wrong advice here", got.broken)
	}
	if len(got.offsite) != 1 || got.offsite[0] != "shop.carts" {
		t.Errorf("offsite = %v, want [shop.carts]: silence here is the failure -- the operator sees "+
			"a clean card and never learns the table is unrestorable from this console", got.offsite)
	}
	if got.unevaluable {
		t.Error("unevaluable: an offsite anchor is a known answer, not an unreadable one")
	}
}

// The complement, so the classifier is not simply "s3 means offsite": a table
// whose usable anchor IS local keeps defining the window even when a newer
// copy also sits in the bucket. Without this, returning offsite for every
// table with any S3 file would pass the test above.
func TestGradeFullTable_aLocalAnchorStillCountsWhenS3AlsoHasIt(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	local := now.Add(-3 * time.Hour)

	got := gradeFullTable([]reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: local, Path: "/backups/2026/shop/orders.parquet"},
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "s3://bucket/p/2026/shop/orders.parquet"},
	}, floor, now)

	if !got.from.Equal(local) {
		t.Errorf("from = %s, want %s: the earliest usable LOCAL anchor is what Restore can reach", got.from, local)
	}
	if len(got.offsite) != 0 {
		t.Errorf("offsite = %v, want none: this table is restorable from the local directory", got.offsite)
	}
}
