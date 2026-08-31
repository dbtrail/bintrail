package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// baselineDirWithTwoSnapshots builds a root holding an older and a newer
// snapshot of the same table, and returns the root plus both directory names
// (older first). No pointer: each test publishes the one it wants.
func baselineDirWithTwoSnapshots(t *testing.T) (root, older, newer string) {
	t.Helper()
	root = t.TempDir()
	older, newer = "2026-06-09T12-00-00Z", "2026-06-10T12-00-00Z"
	for _, s := range []string{older, newer} {
		p := filepath.Join(root, s, "shop", "orders.parquet")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root, older, newer
}

// runViewsOverBaselines runs the command with only a baseline root configured
// and returns the generated SQL.
func runViewsOverBaselines(t *testing.T, root string, pin bool) string {
	t.Helper()
	saveViewsFlags(t)
	vIndexDSN, vArchiveDir, vArchiveS3, vBaselineS3 = "", t.TempDir(), "", ""
	vBintrailID, vBaselineDir, vOut = "aaaa", root, "-"
	vNoBaselines, vIncludeLive, vIncludeEvents, vPinSnapshot = false, false, false, pin
	out, err := runViewsToString(t)
	if err != nil {
		t.Fatalf("runViews: %v", err)
	}
	return out
}

func statePath(t *testing.T, sql string) string {
	t.Helper()
	for _, line := range strings.Split(sql, "\n") {
		if i := strings.Index(line, "read_parquet('"); i >= 0 {
			rest := line[i+len("read_parquet('"):]
			return rest[:strings.Index(rest, "'")]
		}
	}
	t.Fatalf("no state view in:\n%s", sql)
	return ""
}

// TestRunViews_stateViewsFollowTheCurrentPointer is THE assertion for #1484:
// by default a generated file names the pointer, so a snapshot published after
// it was written reaches it. Driven through runViews rather than the
// generator, because what can silently regress is the command layer never
// rewriting the paths at all.
func TestRunViews_stateViewsFollowTheCurrentPointer(t *testing.T) {
	root, _, newer := baselineDirWithTwoSnapshots(t)
	if err := baseline.PublishCurrentPointer(filepath.Join(root, newer)); err != nil {
		t.Fatal(err)
	}

	sql := runViewsOverBaselines(t, root, false)
	got := statePath(t, sql)
	want := filepath.Join(root, baseline.CurrentLinkName, "shop", "orders.parquet")
	if got != want {
		t.Fatalf("state view reads %q, want it through the pointer (%q)", got, want)
	}
	if !strings.Contains(sql, "views follow the `"+baseline.CurrentLinkName+"` pointer") {
		t.Fatal("the file does not say it follows the pointer")
	}
	// And it must not keep the old promise, which is now false.
	if strings.Contains(sql, "stays bound to the snapshot it was generated against") {
		t.Fatal("the file still warns that its state views stop changing")
	}
}

// TestRunViews_pinSnapshotBindsToTheSnapshot covers the opt-out: an operator
// who wants a fixed point in time gets a path that cannot move under them.
func TestRunViews_pinSnapshotBindsToTheSnapshot(t *testing.T) {
	root, _, newer := baselineDirWithTwoSnapshots(t)
	if err := baseline.PublishCurrentPointer(filepath.Join(root, newer)); err != nil {
		t.Fatal(err)
	}

	sql := runViewsOverBaselines(t, root, true)
	got := statePath(t, sql)
	if !strings.Contains(got, newer) {
		t.Fatalf("--pin-snapshot produced %q, want the snapshot directory %q", got, newer)
	}
	if strings.Contains(got, baseline.CurrentLinkName) {
		t.Fatalf("--pin-snapshot still went through the pointer: %q", got)
	}
	if strings.Contains(sql, "follow the `"+baseline.CurrentLinkName+"` pointer") {
		t.Fatal("a pinned file claims to follow the pointer")
	}
}

// TestRunViews_doesNotFollowAPointerNamingAnotherSnapshot is the honesty
// guard. A pointer left behind at an older snapshot (a root written by a build
// that did not maintain it, say) must not be followed: the header describes the
// newest snapshot's tables and column types, so reading rows from a different
// one would make the file lie about its own contents.
func TestRunViews_doesNotFollowAPointerNamingAnotherSnapshot(t *testing.T) {
	root, older, newer := baselineDirWithTwoSnapshots(t)
	if err := os.Symlink(older, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
		t.Fatal(err)
	}

	sql := runViewsOverBaselines(t, root, false)
	got := statePath(t, sql)
	if !strings.Contains(got, newer) {
		t.Fatalf("state view reads %q, want the newest snapshot %q", got, newer)
	}
	if strings.Contains(sql, "follow the `"+baseline.CurrentLinkName+"` pointer") {
		t.Fatal("followed a pointer that names a different snapshot")
	}
}

// TestRunViews_doesNotFollowWhenTheRootHasNoPointer covers every root that
// exists today: none carries a pointer until its next snapshot completes. The
// file must be exactly what it was before this feature, with no dangling path
// and no claim it cannot keep.
func TestRunViews_doesNotFollowWhenTheRootHasNoPointer(t *testing.T) {
	root, _, newer := baselineDirWithTwoSnapshots(t)

	sql := runViewsOverBaselines(t, root, false)
	got := statePath(t, sql)
	if !strings.Contains(got, newer) {
		t.Fatalf("state view reads %q, want the newest snapshot %q", got, newer)
	}
	if strings.Contains(sql, baseline.CurrentLinkName+"/shop") {
		t.Fatalf("a path through a nonexistent pointer was emitted: %q", got)
	}
	if !strings.Contains(sql, "stays bound to the snapshot it was generated against") {
		t.Fatal("a pinned file dropped the warning that its rows stop changing")
	}
}

// TestFollowCurrentPointer_declinesForS3 pins that an S3 baseline root is never
// followed. There is no pointer object to follow there: publishing one would
// mean copying every table to a second prefix, which is not atomic across
// tables, so a query could read half of one snapshot and half of another.
func TestFollowCurrentPointer_declinesForS3(t *testing.T) {
	in := &views.Input{
		BaselineSource: "s3://bucket/baselines/",
		Baselines: []views.BaselineTable{
			{Schema: "shop", Table: "orders", Path: "s3://bucket/baselines/2026-06-10T12-00-00Z/shop/orders.parquet"},
		},
	}
	followCurrentPointer(in, "", false) // --baseline-s3 leaves the local root empty
	if in.FollowsSnapshot {
		t.Fatal("an S3 baseline root was marked as following a pointer")
	}
	if !strings.Contains(in.Baselines[0].Path, "2026-06-10T12-00-00Z") {
		t.Fatalf("S3 path was rewritten: %q", in.Baselines[0].Path)
	}
}
