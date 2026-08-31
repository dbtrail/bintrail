package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// mkSnapshot creates <root>/<name>/<db>/<table>.parquet and returns the
// snapshot directory. It does NOT write a completeness marker: tests that care
// about markers write them through WriteSuccessMarker so the wiring is what is
// under test, not a hand-placed file.
func mkSnapshot(t *testing.T, root, name string) string {
	t.Helper()
	snap := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snap, "shop", "orders.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return snap
}

func readPointer(t *testing.T, root string) string {
	t.Helper()
	target, err := os.Readlink(filepath.Join(root, CurrentLinkName))
	if err != nil {
		t.Fatalf("readlink %s: %v", CurrentLinkName, err)
	}
	return target
}

// TestPublishCurrentPointer_targetIsTheBareSnapshotName pins the link target as
// a RELATIVE name. An absolute path would break the moment the baselines root
// is bind-mounted at another path in a container, copied to another host, or
// simply moved — which is the whole situation the pointer exists to survive.
func TestPublishCurrentPointer_targetIsTheBareSnapshotName(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
	if err := PublishCurrentPointer(snap); err != nil {
		t.Fatalf("PublishCurrentPointer: %v", err)
	}
	if got := readPointer(t, root); got != "2026-08-31T03-00-00Z" {
		t.Fatalf("pointer target = %q, want the bare snapshot name", got)
	}
	// And it must actually resolve to the snapshot's data.
	if _, err := os.Stat(filepath.Join(root, CurrentLinkName, "shop", "orders.parquet")); err != nil {
		t.Fatalf("reading through the pointer: %v", err)
	}
}

// TestPublishCurrentPointer_movesForwardOnly guards the rule that keeps
// `reconstruct --output-format parquet --at <a past instant>` from dragging
// every generated views file back to yesterday's data. Completing an OLDER
// snapshot is a legitimate operation; it just must not win the pointer.
func TestPublishCurrentPointer_movesForwardOnly(t *testing.T) {
	root := t.TempDir()
	newer := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
	older := mkSnapshot(t, root, "2026-08-30T03-00-00Z")

	if err := PublishCurrentPointer(newer); err != nil {
		t.Fatalf("publish newer: %v", err)
	}
	if err := PublishCurrentPointer(older); err != nil {
		t.Fatalf("publish older: %v", err)
	}
	if got := readPointer(t, root); got != "2026-08-31T03-00-00Z" {
		t.Fatalf("pointer moved backwards to %q", got)
	}

	// Forward still works after the declined move.
	newest := mkSnapshot(t, root, "2026-09-01T03-00-00Z")
	if err := PublishCurrentPointer(newest); err != nil {
		t.Fatalf("publish newest: %v", err)
	}
	if got := readPointer(t, root); got != "2026-09-01T03-00-00Z" {
		t.Fatalf("pointer = %q, want the newest snapshot", got)
	}
}

// TestPublishCurrentPointer_ignoresANonSnapshotDirectory covers the
// `reconstruct --output-format mydumper` shape: _SUCCESS is written into an
// operator-chosen dump directory that is not a timestamped snapshot under a
// baselines root. Publishing a pointer next to it would litter an unrelated
// directory and name a target no baseline reader understands.
func TestPublishCurrentPointer_ignoresANonSnapshotDirectory(t *testing.T) {
	root := t.TempDir()
	dump := filepath.Join(root, "nightly-dump")
	if err := os.MkdirAll(dump, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := PublishCurrentPointer(dump); err != nil {
		t.Fatalf("PublishCurrentPointer: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, CurrentLinkName)); !os.IsNotExist(err) {
		t.Fatalf("a pointer was published beside a non-snapshot directory (Lstat err = %v)", err)
	}
}

// TestPublishCurrentPointer_refusesToReplaceRealData pins that an existing
// non-symlink `current` is never removed. An operator may have a real directory
// by that name; silently deleting it to make room for a convenience pointer
// would be data loss in a recovery product.
func TestPublishCurrentPointer_refusesToReplaceRealData(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, CurrentLinkName)
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatal(err)
	}
	canary := filepath.Join(real, "keep-me")
	if err := os.WriteFile(canary, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	snap := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
	err := PublishCurrentPointer(snap)
	if err == nil {
		t.Fatal("PublishCurrentPointer replaced a real directory without complaining")
	}
	if !strings.Contains(err.Error(), "not a symlink") {
		t.Fatalf("error does not say what is wrong: %v", err)
	}
	if _, serr := os.Stat(canary); serr != nil {
		t.Fatalf("the existing directory's contents were destroyed: %v", serr)
	}
}

// TestWriteSuccessMarker_publishesThePointer tests the WIRING, not the
// function: the pointer's whole correctness argument is that it moves at the
// one place a snapshot becomes complete. A PublishCurrentPointer that works
// perfectly but is never called is the failure this guards.
func TestWriteSuccessMarker_publishesThePointer(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
	if err := WriteSuccessMarker(snap); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	if got := readPointer(t, root); got != "2026-08-31T03-00-00Z" {
		t.Fatalf("pointer = %q after WriteSuccessMarker", got)
	}
}

// TestWriteSuccessMarker_onADumpDirectoryPublishesNoPointer is the same wiring
// from the other side: `reconstruct --output-format mydumper` marks a plain
// dump directory complete, and that must not create a pointer in whatever
// directory happens to be its parent.
func TestWriteSuccessMarker_onADumpDirectoryPublishesNoPointer(t *testing.T) {
	root := t.TempDir()
	dump := filepath.Join(root, "nightly-dump")
	if err := os.MkdirAll(dump, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteSuccessMarker(dump); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, CurrentLinkName)); !os.IsNotExist(err) {
		t.Fatalf("a pointer was published for a dump directory (Lstat err = %v)", err)
	}
}

// TestUploadWithOps_skipsTheCurrentPointer is a regression guard with a real
// failure behind it. filepath.WalkDir does not follow symlinks, so `current`
// arrives at the walk callback with IsDir() false; before the regular-file
// check it was handed to the file uploader, which opens the path — following
// the link to a DIRECTORY — and fails with "is a directory", taking every
// baseline S3 upload down with it.
//
// The mock therefore READS the file the way storage.UploadFile does. An
// uploadFile that only records its key would pass this test while production
// failed, which is the trap that made the bug invisible in the first place.
func TestUploadWithOps_skipsTheCurrentPointer(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
	if err := WriteSuccessMarker(snap); err != nil { // also publishes the pointer
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, CurrentLinkName)); err != nil {
		t.Fatalf("test premise: the pointer was not published: %v", err)
	}

	var keys []string
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, k string) error {
			if _, err := os.ReadFile(path); err != nil {
				return err // exactly what the real uploader does with this path
			}
			keys = append(keys, k)
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}

	n, err := uploadWithOps(context.Background(), root, "p", false, ops)
	if err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if n != 2 { // orders.parquet + _SUCCESS
		t.Fatalf("uploaded %d objects, want 2 (the pointer must not be one)", n)
	}
	for _, k := range keys {
		if strings.Contains(k, CurrentLinkName) {
			t.Fatalf("the pointer was uploaded as %q; it is a local convenience and means nothing in S3", k)
		}
	}
}

// TestCurrentPointerIsInvisibleToPruneAndDiscovery pins the claim the pointer's
// safety rests on: it cannot be selected as a baseline and it cannot be pruned.
// Both fall out of enumerating with ReadDir+IsDir (false for a symlink) AND
// requiring the name to parse as a timestamp, so this asserts the behaviour
// rather than the two mechanisms.
func TestCurrentPointerIsInvisibleToPruneAndDiscovery(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
	if err := WriteSuccessMarker(snap); err != nil {
		t.Fatal(err)
	}

	snaps, err := enumerateLocalSnapshots(root)
	if err != nil {
		t.Fatalf("enumerateLocalSnapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("enumerated %d snapshots, want 1 (the pointer must not be one)", len(snaps))
	}
	if snaps[0].name != "2026-08-31T03-00-00Z" {
		t.Fatalf("enumerated %q", snaps[0].name)
	}

	// And the snapshot the pointer names is a keeper, so prune can never leave
	// the pointer dangling: it holds the newest copy of every table it carries.
	keepers := computeKeepers(snaps, snaps[0].ts.Add(1))
	if !keepers["2026-08-31T03-00-00Z"] {
		t.Fatal("the pointed-at snapshot is not a keeper; prune could dangle the pointer")
	}
}

// TestResolveCurrentPointer_reportsUnusableShapes covers what the views
// generator asks before deciding to follow: a root with no pointer, and a
// pointer whose target is not a snapshot name, must both report unusable so the
// caller names a snapshot directly instead of emitting a path that resolves to
// nothing.
func TestResolveCurrentPointer_reportsUnusableShapes(t *testing.T) {
	t.Run("no pointer", func(t *testing.T) {
		if name, ok := ResolveCurrentPointer(t.TempDir()); ok {
			t.Fatalf("reported a usable pointer %q in a root that has none", name)
		}
	})

	t.Run("not a symlink", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, CurrentLinkName), 0o755); err != nil {
			t.Fatal(err)
		}
		if name, ok := ResolveCurrentPointer(root); ok {
			t.Fatalf("reported a real directory as a usable pointer (%q)", name)
		}
	})

	t.Run("target is not a snapshot name", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Symlink("somewhere-else", filepath.Join(root, CurrentLinkName)); err != nil {
			t.Fatal(err)
		}
		if name, ok := ResolveCurrentPointer(root); ok {
			t.Fatalf("reported %q as a usable pointer", name)
		}
	})

	t.Run("usable", func(t *testing.T) {
		root := t.TempDir()
		snap := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
		if err := PublishCurrentPointer(snap); err != nil {
			t.Fatal(err)
		}
		name, ok := ResolveCurrentPointer(root)
		if !ok || name != "2026-08-31T03-00-00Z" {
			t.Fatalf("ResolveCurrentPointer = %q, %v", name, ok)
		}
	})
}

// TestRewriteToPointer_refusesAURLRoot pins the behaviour the console relies on
// for an S3 baseline destination. The console hands its configured root through
// verbatim, so this function is the only thing standing between an s3:// URL
// and a rewritten path that resolves nowhere. Asserted here rather than with a
// prefix check at the call site: a check that cannot change the outcome reads
// as a guard without being one.
func TestRewriteToPointer_refusesAURLRoot(t *testing.T) {
	paths := []string{"s3://bucket/baselines/2026-08-31T03-00-00Z/shop/orders.parquet"}
	if got, ok := RewriteToPointer("s3://bucket/baselines/", paths); ok {
		t.Fatalf("rewrote an S3 root to %v", got)
	}
}

// TestRewriteToPointer_refusesAPathOutsideTheRoot covers the other way a caller
// can hand in something that is not a table of this snapshot. Rewriting it
// would silently move a view to a file nobody asked for.
func TestRewriteToPointer_refusesAPathOutsideTheRoot(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
	if err := PublishCurrentPointer(snap); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		filepath.Join(snap, "shop", "orders.parquet"),
		filepath.Join(t.TempDir(), "2026-08-31T03-00-00Z", "shop", "elsewhere.parquet"),
	}
	if got, ok := RewriteToPointer(root, paths); ok {
		t.Fatalf("rewrote paths spanning two roots to %v", got)
	}
}

// TestSplitSnapshotPath_contract pins the helper's refusals directly. They are
// not reachable through RewriteToPointer's callers today, because a path that
// escapes the root also fails the pointer-name comparison a line later. That
// makes them exactly the kind of check that rots: it reads as a guard, nothing
// exercises it, and the day a caller passes a different shape it turns out to
// have been decorative. Specifying it here is what keeps it real.
func TestSplitSnapshotPath_contract(t *testing.T) {
	root := "/data/baselines"
	cases := []struct {
		name           string
		path           string
		snapshot, rest string
	}{
		{"a table of a snapshot", root + "/2026-08-31T03-00-00Z/shop/orders.parquet",
			"2026-08-31T03-00-00Z", "shop/orders.parquet"},
		{"outside the root", "/elsewhere/2026-08-31T03-00-00Z/shop/orders.parquet", "", ""},
		{"the snapshot directory itself", root + "/2026-08-31T03-00-00Z", "", ""},
		{"the snapshot directory with a trailing separator", root + "/2026-08-31T03-00-00Z/", "", ""},
		{"the root itself", root, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snapshot, rest := splitSnapshotPath(root, c.path)
			if snapshot != c.snapshot || rest != c.rest {
				t.Fatalf("splitSnapshotPath(%q, %q) = %q, %q; want %q, %q",
					root, c.path, snapshot, rest, c.snapshot, c.rest)
			}
		})
	}
}

// TestPublishCurrentPointer_advancesToAFewerTableSnapshot pins a DECISION, not
// a convenience. Publishing a snapshot that carries fewer tables moves the
// pointer, so a views file following it loses the view for a table the new
// snapshot does not hold -- loudly, by an unresolvable path.
//
// The alternative, refusing to advance unless the new snapshot is a superset,
// is the change someone will reach for on reading that. It must not be made
// without also reading why: a dropped table would freeze the pointer forever,
// since no later snapshot could satisfy the rule again, and every generated
// file would quietly serve older and older rows. This test is the tripwire.
func TestPublishCurrentPointer_advancesToAFewerTableSnapshot(t *testing.T) {
	root := t.TempDir()
	wide := mkSnapshot(t, root, "2026-08-30T03-00-00Z")
	if err := os.WriteFile(filepath.Join(wide, "shop", "customers.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteSuccessMarker(wide); err != nil {
		t.Fatal(err)
	}

	// The newer snapshot holds only one of the two tables.
	narrow := mkSnapshot(t, root, "2026-08-31T03-00-00Z")
	if err := WriteSuccessMarker(narrow); err != nil {
		t.Fatal(err)
	}

	if got := readPointer(t, root); got != "2026-08-31T03-00-00Z" {
		t.Fatalf("pointer = %q; it must advance to the newest complete snapshot "+
			"even when that snapshot carries fewer tables (read this test's comment before changing it)", got)
	}
	// The consequence, stated so it cannot be introduced by accident later.
	if _, err := os.Stat(filepath.Join(root, CurrentLinkName, "shop", "customers.parquet")); err == nil {
		t.Fatal("test premise: the narrower snapshot was expected not to hold customers")
	}
}
