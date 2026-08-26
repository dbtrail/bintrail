package reconstruct

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
)

// carryForward publishes a table into a new Parquet snapshot by taking the
// previous snapshot's file as-is, for the case where the delta window held no
// events for that table at all.
//
// # Why this is correct rather than a shortcut
//
// The fold that would otherwise run has an empty change map, so its output is
// the baseline's rows in the baseline's own encoding: the same bytes, minus
// whatever a re-encode would perturb. What makes carrying the file forward
// safe, though, is not that the rows match but that the ANCHOR travels with
// them. Each table's Parquet footer holds its own
// bintrail.baseline_binlog_file / _position, so a table with no deltas has an
// anchor that still points exactly at where its deltas resume. The next fold
// picks up from there and finds the same nothing.
//
// Snapshot DISCOVERY does not read that footer: findBaselineLocal derives a
// snapshot's time from its directory name. So a file carried into a newer
// directory is discovered at the newer time, which is what makes this work
// without rewriting the footer — and rewriting it is not cheap, since a
// Parquet footer is not practically editable in place and re-emitting the file
// costs about what the fold costs.
//
// # What must stay ahead of this
//
// "No row events" is NOT the same as "nothing happened": a TRUNCATE, DROP or
// RENAME emits no row events at all. CheckDestructiveDDL covers the window and
// REFUSES, and it runs at step 3b, before the change map exists. This must
// stay behind it. The same ordering gives the stale-schema and capture-gap
// refusals for free.
//
// The chain stays continuous across cycles: this run checked from the source
// snapshot's time and publishes at the new one, so the next run checking from
// the new time skips no window.
//
// # Integrity
//
// The source is validated against its own snapshot's _MANIFEST before it is
// copied. Skipping that would let a corrupt file propagate into a fresh
// snapshot and be re-certified under the new manifest, which is worse than
// reading it: the merge path validates on read (materializeBaselineLocal), so
// carrying forward without validating would be the one route into a snapshot
// that bypasses the check.
//
// # Hard link, then copy
//
// A link is tried first because the whole point is to stop paying for bytes
// that did not change. Old and new snapshot directories live under the same
// baseline root and therefore the same filesystem, so it normally succeeds.
// Snapshot files are written once and never modified in place, so sharing an
// inode between two snapshots is safe. The copy fallback covers a filesystem
// that has no links and the cross-device case.
//
// One consequence to know: a prune that removes the older snapshot will not
// reclaim a linked file's bytes while the newer one still references it. That
// is correct (the data is still in use) but it means reclaimed-space figures
// count the directory entry, not the blocks.
func carryForward(ctx context.Context, srcPath, snapshotDir, schema, table string) (linked bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := baselineintegrity.ValidateLocalFile(srcPath); err != nil {
		return false, fmt.Errorf("validate the snapshot being carried forward: %w", err)
	}
	dst := filepath.Join(snapshotDir, schema, table+".parquet")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return false, err
	}
	// A previous attempt's leftover would make os.Link fail with EEXIST and
	// would be silently kept by a copy, so clear it either way.
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if err := os.Link(srcPath, dst); err == nil {
		return true, nil
	}
	return false, copyFile(srcPath, dst)
}

// carryForwardEligible reports whether a table can be published by carrying its
// previous file forward instead of folding.
//
// S3 sources are excluded deliberately rather than incidentally. A refresh
// reads and writes snapshot files on disk (an S3-only baseline destination
// cannot be refreshed in place), so the loop this exists for is always
// local-to-local; an S3 source would have to be downloaded, which buys the
// re-encode back and reintroduces the cost this avoids. Those runs take the
// ordinary merge path, which is correct, just not free.
func carryForwardEligible(format, srcPath string, changes int) bool {
	// Mydumper output is a SQL dump for a human to load, not a snapshot to be
	// discovered, so there is no previous file to carry: the rows still have
	// to be emitted.
	return format == OutputFormatParquet && changes == 0 && !strings.HasPrefix(srcPath, "s3://")
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	// Close before returning success: a deferred close would drop a write
	// error on a file this snapshot is about to certify in its manifest.
	return out.Close()
}
