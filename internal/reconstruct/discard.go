package reconstruct

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// discardingSuffix names a snapshot directory that is on its way out.
//
// The prefix dot and the suffix are BOTH there so the name cannot parse as a
// snapshot timestamp: every discovery path (FindBaseline, ListBaselines,
// baseline.enumerateLocalSnapshots) skips a directory whose name
// snapshotdir.ParseTime rejects. `bintrail baseline`'s prune already stages the
// same way with ".<ts>.pruning", so this is that convention and not a second
// one.
const discardingSuffix = ".discarding"

// discardingName renders the staging name for one snapshot directory.
func discardingName(base string) string {
	return "." + base + discardingSuffix
}

// removeAllDir is os.RemoveAll, indirected so a test can drive a delete that
// fails PART WAY, which is the failure the rename below exists for and the one
// no fixture can stage on a healthy filesystem. Written by tests only;
// production never reassigns it.
var removeAllDir = os.RemoveAll

// SnapshotDirVacant reports whether dir is free for a new fold to write into:
// absent, or holding nothing but the _INCOMPLETE marker a previous failed run
// left behind.
//
// It is the same test ReconstructTables applies before it creates the snapshot
// directory, exported for one purpose: a caller that intends to RECLAIM what
// its own fold wrote can establish, BEFORE the fold runs, that everything in
// the directory afterwards is its own. Asking afterwards cannot answer that
// question, because by then the caller's own files are in there too.
//
// A directory that cannot be read is reported as NOT vacant, with the error, so
// a caller that cannot see what is in there never concludes it may delete it.
func SnapshotDirVacant(dir string) (bool, error) {
	leftover, err := snapshotDirLeftovers(dir)
	if err != nil {
		return false, err
	}
	return len(leftover) == 0, nil
}

// DiscardUnpublishedSnapshot removes a snapshot directory that a FAILED fold
// wrote, reclaiming the disk it holds. discarded reports whether the directory
// is gone from dir.
//
// It refuses unless the directory is positively marked incomplete: _INCOMPLETE
// present and _SUCCESS absent, which is exactly baseline.SnapshotComplete
// returning false. That is not a formality. A published snapshot carries
// _SUCCESS, and a legacy snapshot carries NEITHER marker and is complete by
// default, so both are refused by the same check. It also makes a path mistake
// fail safe: a directory this fold never wrote has no _INCOMPLETE marker, so a
// caller that computes the wrong path deletes nothing.
//
// The removal is a rename FIRST and a delete second, and the order is the whole
// point. os.RemoveAll deletes children in directory order with no promise that
// _INCOMPLETE goes last, and '_' sorts ahead of a lowercase schema directory,
// so a process killed mid-delete could leave Parquet files behind with no
// marker at all. baseline.SnapshotComplete reads a markerless directory as
// COMPLETE, which would turn an undiscoverable partial snapshot into a
// discoverable one: the exact failure #467 exists to prevent, caused by the
// cleanup meant to help. A rename inside one directory is atomic, and once it
// lands nothing can discover the snapshot however the delete then goes.
//
// A delete that fails after the rename returns discarded=true WITH the error:
// the snapshot is already beyond every reader, and only the bytes are still
// there. Callers report both.
func DiscardUnpublishedSnapshot(dir string) (discarded bool, err error) {
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		// The run failed before it created the directory. Nothing to reclaim,
		// and nothing to report.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect snapshot directory %s: %w", dir, err)
	}
	// Lstat, so a symlink is not followed: a fold creates a real directory, so
	// anything else here is not ours.
	if !info.IsDir() {
		return false, fmt.Errorf("%s is not a directory, so it is not a snapshot this run wrote", dir)
	}
	if baseline.SnapshotComplete(dir) {
		return false, fmt.Errorf("%s does not carry the %s marker, so it is not an unpublished snapshot",
			dir, baseline.IncompleteMarker)
	}
	staging := filepath.Join(filepath.Dir(dir), discardingName(filepath.Base(dir)))
	if err := os.Rename(dir, staging); err != nil {
		return false, fmt.Errorf("move %s aside before deleting it: %w", dir, err)
	}
	if err := removeAllDir(staging); err != nil {
		return true, fmt.Errorf("%s is no longer discoverable, but the files left at %s could not be deleted: %w",
			dir, staging, err)
	}
	return true, nil
}
