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
