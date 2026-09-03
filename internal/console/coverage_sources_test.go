package console

import (
	"slices"
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
	// OLDER than the S3-only anchor on purpose: the window is the LATEST of the
	// per-table starts, so counting the offsite anchor would move `from`
	// forward to it. With the local table newer, both readings coincide and
	// the assertion below could not tell them apart.
	localOrders := now.Add(-9 * time.Hour)

	got := gradeFullTable([]reconstruct.BaselineFile{
		// A healthy local table, which is what full_table_from may name.
		{Schema: "shop", Table: "orders", SnapshotTime: localOrders, Path: "/backups/2026/shop/orders.parquet"},
		// The SAME table, also in the bucket and OLDER than its local copy --
		// the #616 local-retention shape, and the common configuration. It is
		// what makes `from` discriminate: reading earliestUsable instead of
		// earliestUsableLocal would start the window here, at an instant with
		// no local snapshot at or before it, which is exactly the refusal
		// quoted below.
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-40 * time.Hour), Path: "s3://bucket/prefix/2026/shop/orders.parquet"},
		// NO local copy at all, so findBaseline gets ErrNoBaseline from the
		// local root and the #766 fallback really does serve this from S3.
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://bucket/prefix/2026/shop/carts.parquet"},
		// A second offsite-only table, so the sort order of offsite_tables is
		// asserted rather than hidden behind a single element.
		{Schema: "shop", Table: "audit", SnapshotTime: now.Add(-3 * time.Hour), Path: "s3://bucket/prefix/2026/shop/audit.parquet"},
	}, floor, now, true)

	if got.from.Equal(now.Add(-2 * time.Hour)) {
		t.Errorf("full_table_from advanced to the S3-only anchor. The console's Restore folds " +
			"from BaselineDir and never opens the bucket (#1541), so this start is one the " +
			"button refuses -- a green panel over a restore that cannot run")
	}
	if !got.from.Equal(localOrders) {
		t.Errorf("from = %s, want %s: the window is the latest LOCAL earliest-usable anchor", got.from, localOrders)
	}
	if len(got.broken) != 0 {
		t.Errorf("broken = %v, want none: shop.carts has a current backup, it is just off site. "+
			"broken_tables drives an alarm and says 'take a fresh backup', which is wrong advice here", got.broken)
	}
	if !slices.Equal(got.offsite, []string{"shop.audit", "shop.carts"}) {
		t.Errorf("offsite = %v, want [shop.audit shop.carts] in that order: silence here is the "+
			"failure -- the operator sees a clean card and never learns the tables are "+
			"unrestorable from this console -- and an unsorted list reorders between requests", got.offsite)
	}
	if len(got.unevaluable) != 0 {
		t.Errorf("unevaluable = %v: an offsite anchor is a known answer, not an unreadable one", got.unevaluable)
	}
}

// A STALE LOCAL COPY SHADOWS THE FRESH OFFSITE ONE, and that is broken, not
// offsite. bundle.findBaseline falls back to the bucket only on ErrNoBaseline
// (#766); a table with any local snapshot at-or-before the instant gets a nil
// error, so the fallback never fires and time travel resolves the stale copy.
// No console surface reaches the fresh S3 sibling.
//
// Calling this offsite would trade the red "take a fresh backup" this card
// gave before #1571 for a warning that PROMISES a working time travel -- the
// operator would read the promise and not take the backup.
func TestGradeFullTable_aStaleLocalCopyShadowsTheOffsiteOneAndStaysBroken(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}

	got := gradeFullTable([]reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "/backups/new/shop/orders.parquet"},
		// Below the floor, and it is what findBaseline will resolve.
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-150 * time.Hour), Path: "/backups/old/shop/carts.parquet"},
		// Fresh, in the bucket, and unreachable because of the line above.
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://bucket/prefix/2026/shop/carts.parquet"},
	}, floor, now, true)

	if len(got.offsite) != 0 {
		t.Errorf("offsite = %v, want none: the stale LOCAL copy is what findBaseline resolves, so "+
			"the fresh S3 one is unreachable from every console surface and the card would be "+
			"promising a time travel that silently serves the stale data", got.offsite)
	}
	if len(got.broken) != 1 || got.broken[0] != "shop.carts" {
		t.Errorf("broken = %v, want [shop.carts]: this is the verdict the card gave before it read "+
			"both locations, and reading the second one must not silence it", got.broken)
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
	}, floor, now, true)

	if !got.from.Equal(local) {
		t.Errorf("from = %s, want %s: the earliest usable LOCAL anchor is what Restore can reach", got.from, local)
	}
	if len(got.offsite) != 0 {
		t.Errorf("offsite = %v, want none: this table is restorable from the local directory", got.offsite)
	}
}

// An S3-only server has no local anchor by construction, so every table would
// be enumerated as offsite and the card would lose the number it exists to
// print. That is a configuration fact, stated once. A warn line naming the
// whole schema is the kind of line operators learn to skip.
func TestCoverageAPI_anS3OnlyServerStatesItOnceInsteadOfNamingEveryTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")

	// The listing of this bucket will fail; that is the point. The branch must
	// be decided from the CONFIGURATION, before any listing, or an unreachable
	// bucket and an S3-only server look the same.
	//
	// IMDS off so the credential chain fails immediately instead of spending
	// seconds probing an instance-metadata endpoint that is not there. The
	// listing has to fail either way, so this only removes the wait.
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	srv := newBaselineServerWithFallback(t, "s3://bucket/prefix", "")
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if len(got.OffsiteTables) != 0 {
		t.Errorf("offsite_tables = %v, want none: with no local directory at all, every table is "+
			"offsite and the enumeration says nothing a single sentence does not", got.OffsiteTables)
	}
	if !got.RestoreNeedsLocal {
		t.Error("restore_needs_local is false: the operator gets no explanation for a card with " +
			"no restore window, on a server where Restore refuses outright for want of a local folder")
	}
}

// The ambiguity demotion (#1219) turns "broken" into "unknown" on an index
// whose archives cannot be attributed to one source. In the shadowed branch
// every local anchor is below the floor by construction, so that demotion is
// not an edge case there: it is the ROUTINE verdict, and a bare unevaluable
// flag dropped the table name on every one of those indexes.
//
// The whole console package had ZERO coverage of BelowIsUnknown, which is how
// two review passes read this branch without seeing it.
func TestGradeFullTable_anUnattributableFloorStillNamesTheTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	files := []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "/backups/new/shop/orders.parquet"},
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-150 * time.Hour), Path: "/backups/old/shop/carts.parquet"},
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://bucket/prefix/2026/shop/carts.parquet"},
	}

	// Attributable floor: the same fixture is a plain broken verdict.
	attributable := gradeFullTable(files, status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}, now, true)
	if !slices.Equal(attributable.broken, []string{"shop.carts"}) {
		t.Fatalf("broken = %v, want [shop.carts] on an attributable floor", attributable.broken)
	}

	// Unattributable: below the floor may still be covered by that source's own
	// archives, so the verdict softens. The NAME must not soften with it.
	got := gradeFullTable(files, status.DeltaFloor{Hour: now.Add(-100 * time.Hour), BelowIsUnknown: true}, now, true)
	if len(got.broken) != 0 {
		t.Errorf("broken = %v, want none: an unattributable floor must not accuse (#1219)", got.broken)
	}
	if !slices.Equal(got.unevaluable, []string{"shop.carts"}) {
		t.Errorf("unevaluable = %v, want [shop.carts]. The card says 'could not be checked' and the "+
			"operator gets no table name, on the verdict that is ROUTINE for a multi-source index -- "+
			"the shadowing this branch exists to catch would be invisible there", got.unevaluable)
	}
	if len(got.offsite) != 0 {
		t.Errorf("offsite = %v, want none: the stale local copy still shadows the bucket, whatever "+
			"the floor can attribute", got.offsite)
	}
}

// With no local location configured, every usable table is offsite by
// construction, so the list would name the whole schema and say nothing the
// single restore_needs_local sentence does not. A warn line enumerating every
// table is the kind operators learn to skip, and the next one they skip is a
// real one.
//
// Stated as a property of the fold rather than blanked in the handler: the
// handler calls reconstruct.ListBaselines directly with no seam, so an s3://
// source there never lists and the assertion could not be reached at all.
func TestGradeFullTable_withNoLocalLocationTheOffsiteListIsSuppressed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	files := []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "s3://b/p/2026/shop/orders.parquet"},
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://b/p/2026/shop/carts.parquet"},
	}

	if got := gradeFullTable(files, floor, now, false); len(got.offsite) != 0 {
		t.Errorf("offsite = %v, want none: with no local location every table is offsite and the "+
			"enumeration is the whole schema", got.offsite)
	}
	// The complement, so the suppression is not simply "offsite is never
	// populated": the same files with a local location configured DO enumerate.
	if got := gradeFullTable(files, floor, now, true); len(got.offsite) != 2 {
		t.Errorf("offsite = %v, want both tables: a server WITH a local directory needs to know "+
			"which tables its Restore cannot fold", got.offsite)
	}
}

// A dir-backed server must report restore_needs_local FALSE. Nothing asserted
// the negative, and omitempty makes a spurious true invisible: hardcoding the
// field survived the entire console suite, which would have told every
// operator on the product that their backups go to S3 only.
func TestCoverageAPI_aDirBackedServerDoesNotAskForALocalFolder(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	tsDir := now.Add(-time.Hour).Format("2006-01-02T15-04-05Z")

	primary := t.TempDir()
	writeBaselineFixture(t, primary, tsDir, "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, primary, "")
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if got.RestoreNeedsLocal {
		t.Error("restore_needs_local is true on a server whose backups go to a local directory. " +
			"The card would tell the operator their backups are S3-only and drop the offsite list")
	}
	if got.FullTableFrom == "" {
		t.Error("a dir-backed server with a healthy backup must still report its restore window")
	}
}
