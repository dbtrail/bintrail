package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// stageSnapshot writes one snapshot directory holding a single table's Parquet
// plus whichever markers the case needs, and returns its path.
func stageSnapshot(t *testing.T, root, name string, markers ...string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(dir, "shop"), 0o755); err != nil {
		t.Fatalf("stage snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop", "orders.parquet"), []byte("rows"), 0o644); err != nil {
		t.Fatalf("stage table file: %v", err)
	}
	for _, m := range markers {
		if err := os.WriteFile(filepath.Join(dir, m), nil, 0o644); err != nil {
			t.Fatalf("stage marker %s: %v", m, err)
		}
	}
	return dir
}

func TestDiscardUnpublishedSnapshot_reclaimsAnIncompleteSnapshot(t *testing.T) {
	root := t.TempDir()
	dir := stageSnapshot(t, root, "2026-08-28T10-00-00Z", baseline.IncompleteMarker)

	discarded, err := DiscardUnpublishedSnapshot(dir)
	if err != nil {
		t.Fatalf("DiscardUnpublishedSnapshot = %v, want nil", err)
	}
	if !discarded {
		t.Fatal("an incomplete snapshot was not discarded, so the disk it holds is never reclaimed")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the snapshot directory is still on disk: stat = %v", err)
	}
	// Nothing staged is left behind either: a discard that renames and never
	// deletes reclaims no disk at all, which is the whole point of the change.
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read baseline root: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("the baseline root still holds %d entries after a discard: %v", len(entries), entries)
	}
}

// A PUBLISHED snapshot must survive, and the marker is the only thing that
// separates it from a partial one. This is the case where getting it wrong
// deletes a customer's backup.
func TestDiscardUnpublishedSnapshot_refusesAPublishedSnapshot(t *testing.T) {
	root := t.TempDir()
	// Both markers on purpose: baseline.SnapshotComplete reads _SUCCESS FIRST,
	// so a snapshot that was published after an earlier failed attempt carries
	// both, and it is complete.
	dir := stageSnapshot(t, root, "2026-08-28T10-00-00Z", baseline.SuccessMarker, baseline.IncompleteMarker)

	discarded, err := DiscardUnpublishedSnapshot(dir)
	if discarded {
		t.Fatal("a published snapshot was deleted")
	}
	if err == nil {
		t.Fatal("refusing to discard a published snapshot said nothing, so an operator cannot tell it was kept")
	}
	if _, err := os.Stat(filepath.Join(dir, "shop", "orders.parquet")); err != nil {
		t.Errorf("the published snapshot's table file is gone: %v", err)
	}
}

// A snapshot written before the markers existed carries NEITHER, and
// baseline.SnapshotComplete reads that as complete by default. It must be
// refused for the same reason a _SUCCESS one is.
func TestDiscardUnpublishedSnapshot_refusesAMarkerlessSnapshot(t *testing.T) {
	root := t.TempDir()
	dir := stageSnapshot(t, root, "2026-08-28T10-00-00Z")

	discarded, err := DiscardUnpublishedSnapshot(dir)
	if discarded {
		t.Fatal("a legacy markerless snapshot was deleted; those are complete by default")
	}
	if err == nil {
		t.Fatal("refusing a markerless snapshot said nothing")
	}
	if _, err := os.Stat(filepath.Join(dir, "shop", "orders.parquet")); err != nil {
		t.Errorf("the legacy snapshot's table file is gone: %v", err)
	}
}

// A run that failed before it created the directory has nothing to reclaim, and
// that is not an error: the caller uses the nil to decide whether to name a
// path at all.
func TestDiscardUnpublishedSnapshot_absentDirectoryIsNotAnError(t *testing.T) {
	discarded, err := DiscardUnpublishedSnapshot(filepath.Join(t.TempDir(), "2026-08-28T10-00-00Z"))
	if discarded {
		t.Error("an absent directory was reported as discarded")
	}
	if err != nil {
		t.Errorf("an absent directory was reported as an error: %v", err)
	}
}

// THE ordering guard. os.RemoveAll deletes children in directory order with no
// promise the markers go last, and '_' sorts ahead of a lowercase schema
// directory. A delete killed part way through can therefore leave Parquet files
// with NO marker, and a markerless snapshot directory is complete by default:
// an undiscoverable partial snapshot would become a discoverable one, caused by
// the cleanup meant to help.
//
// The rename closes that, so this drives a delete that gets the marker and then
// dies, and asks DISCOVERY what it sees. Asserting on the staging name instead
// would only restate the string the code just built.
func TestDiscardUnpublishedSnapshot_staysUndiscoverableWhenTheDeleteDiesPartWay(t *testing.T) {
	root := t.TempDir()
	dir := stageSnapshot(t, root, "2026-08-28T10-00-00Z", baseline.IncompleteMarker)

	prev := removeAllDir
	t.Cleanup(func() { removeAllDir = prev })
	removeAllDir = func(path string) error {
		// The marker goes first, exactly as a real interrupted RemoveAll can
		// leave it, and then the delete dies with the table file still there.
		if err := os.Remove(filepath.Join(path, baseline.IncompleteMarker)); err != nil {
			t.Errorf("the fixture could not remove the marker at %s: %v", path, err)
		}
		return errors.New("disk went away")
	}

	discarded, err := DiscardUnpublishedSnapshot(dir)
	if !discarded {
		t.Fatal("a delete that died part way reported the snapshot as still in place")
	}
	if err == nil {
		t.Fatal("a delete that failed reported success, so the bytes it did not reclaim go unreported")
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatalf("the snapshot is still at its published-shaped path %s, so only the marker protects it now", dir)
	}

	files, listErr := ListBaselines(context.Background(), root)
	if listErr != nil {
		t.Fatalf("ListBaselines: %v", listErr)
	}
	if len(files) != 0 {
		t.Errorf("discovery found %d baseline file(s) in what is left of a discarded partial snapshot: %v; "+
			"the marker is gone, so only the directory NAME keeps it out of discovery", len(files), files)
	}
}

func TestSnapshotDirVacant(t *testing.T) {
	root := t.TempDir()

	absent := filepath.Join(root, "2026-08-28T10-00-00Z")
	if vacant, err := SnapshotDirVacant(absent); err != nil || !vacant {
		t.Errorf("SnapshotDirVacant(absent) = %v, %v; want true, nil", vacant, err)
	}

	// Nothing but the marker a previous failed run left: a fold writes into
	// this, so a caller may claim it.
	markerOnly := stageSnapshot(t, root, "2026-08-28T11-00-00Z", baseline.IncompleteMarker)
	if err := os.RemoveAll(filepath.Join(markerOnly, "shop")); err != nil {
		t.Fatalf("strip the table file: %v", err)
	}
	if vacant, err := SnapshotDirVacant(markerOnly); err != nil || !vacant {
		t.Errorf("SnapshotDirVacant(marker only) = %v, %v; want true, nil", vacant, err)
	}

	withFiles := stageSnapshot(t, root, "2026-08-28T12-00-00Z", baseline.IncompleteMarker)
	if vacant, err := SnapshotDirVacant(withFiles); err != nil || vacant {
		t.Errorf("SnapshotDirVacant(holding a table file) = %v, %v; want false, nil", vacant, err)
	}
}

// A directory holding nothing but the marker is what a run that failed before
// its first table folded leaves: an unreachable index, a missing schema
// snapshot, archive discovery refusing. There is no data in it to protect.
func TestDiscardUnpublishedSnapshot_reclaimsAMarkerOnlySnapshot(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "2026-08-28T10-00-00Z")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatalf("stage marker: %v", err)
	}

	discarded, err := DiscardUnpublishedSnapshot(dir)
	if err != nil || !discarded {
		t.Fatalf("DiscardUnpublishedSnapshot(marker only) = %v, %v; want true, nil", discarded, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the marker-only directory is still on disk: %v", err)
	}
}

// A caller has to be able to tell the guard declining from the filesystem
// refusing: the first is nothing to act on, the second means the reclaim cannot
// run for this directory and its disk leaks. Both are (false, err).
func TestDiscardUnpublishedSnapshot_refusalIsDistinguishableFromAFailure(t *testing.T) {
	root := t.TempDir()
	published := stageSnapshot(t, root, "2026-08-28T10-00-00Z", baseline.SuccessMarker)
	if _, err := DiscardUnpublishedSnapshot(published); !errors.Is(err, ErrSnapshotNotDiscardable) {
		t.Errorf("refusing a published snapshot = %v, want it to wrap ErrSnapshotNotDiscardable so the caller "+
			"does not report the guard working as a filesystem failure", err)
	}

	// A plain file where a snapshot directory would be: also a refusal, not a
	// failure, and it must never be followed.
	notADir := filepath.Join(root, "2026-08-28T11-00-00Z")
	if err := os.WriteFile(notADir, []byte("not a snapshot"), 0o644); err != nil {
		t.Fatalf("stage: %v", err)
	}
	discarded, err := DiscardUnpublishedSnapshot(notADir)
	if discarded {
		t.Error("a plain file at the snapshot path was removed")
	}
	if !errors.Is(err, ErrSnapshotNotDiscardable) {
		t.Errorf("a plain file at the snapshot path = %v, want a refusal", err)
	}
	if _, err := os.Stat(notADir); err != nil {
		t.Errorf("the file was deleted: %v", err)
	}
}

// The other half of the convention discardingSuffix borrows. Without the sweep,
// a daemon killed during the delete leaves a staging directory that NO listing,
// status or console panel will ever mention, because every discovery path skips
// it on the name. The leak this change fixes would come back in a shape nothing
// can see.
func TestSweepDiscardedSnapshots(t *testing.T) {
	root := t.TempDir()
	leftover := filepath.Join(root, ".2026-08-28T09-00-00Z.discarding")
	if err := os.MkdirAll(filepath.Join(leftover, "shop"), 0o755); err != nil {
		t.Fatalf("stage leftover: %v", err)
	}
	if err := os.WriteFile(filepath.Join(leftover, "shop", "orders.parquet"), []byte("rows"), 0o644); err != nil {
		t.Fatalf("stage leftover file: %v", err)
	}
	// A real published snapshot and an unrelated dot-directory both have to
	// survive: the sweep must match on BOTH halves of the staging name.
	keep := stageSnapshot(t, root, "2026-08-28T10-00-00Z", baseline.SuccessMarker)
	// Both halves of the name are load-bearing, so both get a fixture: a
	// dot-directory that is not ours, and a directory ending in the suffix that
	// an operator named themselves.
	unrelated := filepath.Join(root, ".config")
	if err := os.MkdirAll(unrelated, 0o755); err != nil {
		t.Fatalf("stage unrelated: %v", err)
	}
	operatorNamed := filepath.Join(root, "old-backups.discarding")
	if err := os.MkdirAll(operatorNamed, 0o755); err != nil {
		t.Fatalf("stage operator-named: %v", err)
	}

	removed, err := SweepDiscardedSnapshots(root)
	if err != nil {
		t.Fatalf("SweepDiscardedSnapshots = %v", err)
	}
	if removed != 1 {
		t.Errorf("swept %d directories, want 1", removed)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Errorf("the leftover staging directory survived the sweep: %v", err)
	}
	if _, err := os.Stat(filepath.Join(keep, "shop", "orders.parquet")); err != nil {
		t.Errorf("the sweep deleted a published snapshot: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Errorf("the sweep deleted an unrelated dot-directory: %v", err)
	}
	if _, err := os.Stat(operatorNamed); err != nil {
		t.Errorf("the sweep deleted a directory an operator named, which this package never wrote: %v", err)
	}
}

// The negative direction of the sentinel, which is the half that matters: a
// filesystem failure must NOT be classed as the guard declining, or the caller
// reports a leaking directory as "working as designed" and says nothing about
// it.
func TestDiscardUnpublishedSnapshot_aFailureIsNotARefusal(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so a failing rename cannot be staged")
	}
	root := t.TempDir()
	dir := stageSnapshot(t, root, "2026-08-28T10-00-00Z", baseline.IncompleteMarker)
	// A read-only PARENT fails the rename while the directory itself still
	// passes every guard above it.
	if err := os.Chmod(root, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })

	discarded, err := DiscardUnpublishedSnapshot(dir)
	if discarded {
		t.Fatal("a discard whose rename failed reported the snapshot as gone")
	}
	if err == nil {
		t.Fatal("a rename that failed was reported as success")
	}
	if errors.Is(err, ErrSnapshotNotDiscardable) {
		t.Errorf("a filesystem failure was classed as the guard declining, so the caller reports a directory it "+
			"cannot reclaim as working as designed: %v", err)
	}
}

// An absent or unreadable baseline root is not an error: the sweep is
// housekeeping beside real work, and the real work fails on the same root and
// says so first.
func TestSweepDiscardedSnapshots_absentRootIsQuiet(t *testing.T) {
	removed, err := SweepDiscardedSnapshots(filepath.Join(t.TempDir(), "nope"))
	if removed != 0 || err != nil {
		t.Errorf("SweepDiscardedSnapshots(absent) = %d, %v; want 0, nil", removed, err)
	}
}
