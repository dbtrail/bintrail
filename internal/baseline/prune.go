package baseline

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// baselinePruneMinAge is a safety floor on how young a snapshot may be and still
// be pruned, independent of the retention window. A just-completed snapshot is
// the most likely to be the target of an in-flight reconstruct read; the floor
// keeps it on disk for at least an hour regardless of how aggressive Retain is.
// In practice Retain (parsed via cliutil.ParseRetain, whose smallest unit is 1h)
// already subsumes this, so it is belt-and-suspenders against a future caller
// that passes a sub-hour Retain programmatically.
const baselinePruneMinAge = time.Hour

// pruningSuffix marks a snapshot directory that has been renamed aside for
// deletion. A ".<timestamp>.pruning" name does NOT parse via
// parseBaselineDirTimestamp, so it is invisible to every reader/discovery walk
// the instant it is renamed — a crash mid-delete therefore self-excludes the
// half-removed tree rather than leaving a _SUCCESS-marked directory with missing
// table files (which a reader would treat as complete and then get ErrNoBaseline).
const pruningSuffix = ".pruning"

// PruneOptions configures a local baseline-snapshot prune (#616).
//
// Pruning is a deliberate no-op (logged loudly) unless S3URL is set: a local
// snapshot with no durable S3 counterpart is the ONLY copy, and the prune never
// deletes the only copy. This mirrors rotation's archive invariant exactly —
// `PruneLocalAfterUpload && ArchiveS3 != ""` (internal/rotation/rotation.go) — a
// local-only baseline IS the durable copy and is never reclaimed.
type PruneOptions struct {
	// LocalDir is the baseline output root pruned, laid out as
	// <LocalDir>/<timestamp>/<schema>/<table>.parquet. Required.
	LocalDir string
	// S3URL is the durable S3 prefix the snapshots were uploaded to
	// (s3://bucket/prefix/). Empty disables pruning entirely.
	S3URL string
	// S3Region is the optional AWS region for the durability probe; empty lets
	// the SDK resolve it from the ambient chain (AWS_REGION / ~/.aws / IAM role).
	S3Region string
	// Retain prunes COMPLETE snapshots older than this. Required (> 0).
	Retain time.Duration
	// Now is an injectable clock for tests; the zero value means time.Now().UTC().
	Now time.Time
	// DryRun logs what would be pruned without deleting anything.
	DryRun bool
}

// PruneResult reports what a prune did, for the caller to log/surface.
type PruneResult struct {
	// Pruned holds the snapshot directory names actually removed (or, under
	// DryRun, that would be removed).
	Pruned []string
	// ReclaimedBytes is the on-disk size of the pruned snapshots.
	ReclaimedBytes int64
	// KeptKeeper counts snapshots retained because they are the newest snapshot
	// containing some table — reconstruct.FindBaseline's per-table target.
	KeptKeeper int
	// KeptNotDurable counts snapshots retained because no durable S3 copy was
	// confirmed (never delete the only copy).
	KeptNotDurable int
	// KeptRecent counts snapshots retained because they are within the retention
	// window or younger than the safety floor.
	KeptRecent int
	// KeptIncomplete counts snapshots skipped because they carry an _INCOMPLETE
	// marker (an in-progress or --retry-resumable run — never our business).
	KeptIncomplete int
}

// durableProbe reports whether a snapshot directory has a confirmed durable copy
// in S3. It is the testing seam: PruneLocal builds an S3 HeadObject probe;
// pruneWithProbe takes it so the invariant logic is exercised without a live
// client (mirrors upload.go's s3UploadOps seam).
type durableProbe func(ctx context.Context, snapshotName string) (bool, error)

// PruneLocal removes redundant local baseline snapshots under opts.LocalDir,
// honoring three invariants (#616):
//
//   - Never delete the only copy: a snapshot is pruned only when its _SUCCESS
//     marker is confirmed present in S3 at the exact same timestamp prefix.
//   - Never delete the newest usable snapshot: the newest COMPLETE snapshot
//     containing each table is kept, matching reconstruct.FindBaseline's
//     per-table selection — pruning it would break Time-travel for that table.
//   - Respect markers: _INCOMPLETE snapshots are never touched (they may be an
//     in-progress write or a --retry-resumable partial), and a complete snapshot
//     is renamed aside before deletion so a reader never sees a half-removed one.
//
// Pruning only ever narrows how far back local Time-travel reaches; the present
// (an `at=now` reconstruct) always resolves to a kept keeper, never to a pruned
// snapshot — findBaselineLocal filters candidates to at-or-before `at`.
func PruneLocal(ctx context.Context, opts PruneOptions) (PruneResult, error) {
	if opts.LocalDir == "" {
		return PruneResult{}, fmt.Errorf("baseline prune: LocalDir is required")
	}
	if opts.Retain <= 0 {
		return PruneResult{}, fmt.Errorf("baseline prune: Retain must be positive")
	}
	if opts.S3URL == "" {
		// No durable destination → every local snapshot is the only copy. Refuse,
		// loudly, so a retention setting on a local-only deployment is not a
		// silent no-op that looks like a bug when the disk keeps filling.
		slog.Warn("baseline prune: no S3 destination configured; refusing to prune local snapshots (they are the only copy). Upload baselines to S3 to enable retention.",
			"dir", opts.LocalDir)
		return PruneResult{}, nil
	}

	bucket, prefix, err := storage.ParseS3URL(opts.S3URL)
	if err != nil {
		return PruneResult{}, fmt.Errorf("baseline prune: invalid S3 URL %q: %w", opts.S3URL, err)
	}
	client, err := storage.NewS3Client(ctx, opts.S3Region)
	if err != nil {
		return PruneResult{}, fmt.Errorf("baseline prune: %w", err)
	}

	probe := func(ctx context.Context, snapshotName string) (bool, error) {
		// The exact key Upload published the _SUCCESS marker at:
		// <prefix>/<timestamp>/_SUCCESS. An exact HeadObject (not a glob/prefix
		// match) is the strongest "this snapshot is durable" signal — a loose
		// match could false-positive and prune the only copy.
		key, err := storage.BuildS3Key(opts.LocalDir, filepath.Join(opts.LocalDir, snapshotName, SuccessMarker), prefix)
		if err != nil {
			return false, err
		}
		return storage.S3ObjectExists(ctx, client, bucket, key)
	}
	return pruneWithProbe(ctx, opts, probe)
}

// pruneWithProbe is PruneLocal's IO body with the durability check injected, so
// the keeper/marker/age invariants are testable without S3.
func pruneWithProbe(ctx context.Context, opts PruneOptions, probe durableProbe) (PruneResult, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	// Sweep any ".<ts>.pruning" leftovers from a previous crashed cycle first —
	// they are already invisible to discovery, but they still occupy disk.
	sweepPruningLeftovers(opts.LocalDir)

	snaps, err := enumerateLocalSnapshots(opts.LocalDir)
	if err != nil {
		return PruneResult{}, fmt.Errorf("baseline prune: %w", err)
	}
	keepers := computeKeepers(snaps)

	// Confirm durability only for snapshots that could actually be pruned
	// (complete, not a keeper) — keepers are never deleted, so probing them would
	// waste HeadObject calls. A probe error is fail-safe: treat as NOT durable
	// (keep), never as durable (which could delete the only copy).
	durable := make(map[string]bool)
	for _, s := range snaps {
		if !s.complete || keepers[s.name] {
			continue
		}
		ok, perr := probe(ctx, s.name)
		if perr != nil {
			slog.Warn("baseline prune: could not confirm S3 durability; keeping snapshot",
				"snapshot", s.name, "error", perr)
			continue // absent from `durable` → planPrune keeps it
		}
		durable[s.name] = ok
	}

	pruneNames, res := planPrune(snaps, keepers, durable, opts.Retain, baselinePruneMinAge, now)

	for _, name := range pruneNames {
		size := dirSize(filepath.Join(opts.LocalDir, name))
		if opts.DryRun {
			slog.Info("baseline prune (dry-run): would remove redundant local snapshot",
				"snapshot", name, "bytes", size)
			res.Pruned = append(res.Pruned, name)
			res.ReclaimedBytes += size
			continue
		}
		if err := removeSnapshot(opts.LocalDir, name); err != nil {
			slog.Warn("baseline prune: could not remove snapshot; it will be retried next cycle",
				"snapshot", name, "error", err)
			continue
		}
		slog.Info("baseline prune: removed redundant local snapshot (durable copy in S3)",
			"snapshot", name, "bytes", size)
		res.Pruned = append(res.Pruned, name)
		res.ReclaimedBytes += size
	}
	return res, nil
}

// planPrune is the pure prune decision: given the enumerated snapshots, the
// keeper set, and the confirmed-durable set, it returns the names to prune and a
// per-reason tally of what was kept. No IO — every invariant lives here so a unit
// test pins it without a filesystem or S3.
func planPrune(snaps []localSnapshot, keepers, durable map[string]bool, retain, minAge time.Duration, now time.Time) ([]string, PruneResult) {
	var prune []string
	var res PruneResult
	for _, s := range snaps {
		switch {
		case !s.complete:
			// _INCOMPLETE: an in-progress write or a --retry-resumable partial.
			res.KeptIncomplete++
		case keepers[s.name]:
			// Newest snapshot for some table — FindBaseline's target.
			res.KeptKeeper++
		case now.Sub(s.ts) < retain || now.Sub(s.ts) < minAge:
			res.KeptRecent++
		case !durable[s.name]:
			// No confirmed durable S3 copy → never delete the only copy.
			res.KeptNotDurable++
		default:
			prune = append(prune, s.name)
		}
	}
	return prune, res
}

// localSnapshot is one snapshot directory under a baseline root, classified for
// pruning. tables is populated only for complete snapshots ("schema/table").
type localSnapshot struct {
	name     string
	ts       time.Time
	complete bool
	tables   []string
}

// enumerateLocalSnapshots lists the snapshot directories under dir. Entries whose
// name does not parse as a baseline timestamp are skipped (this also excludes any
// ".<ts>.pruning" staging dirs). A missing dir is "nothing to prune", not an
// error — the daemon may run before any baseline has been created.
func enumerateLocalSnapshots(dir string) ([]localSnapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read baseline directory %q: %w", dir, err)
	}
	var out []localSnapshot
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		ts, ok := parseBaselineDirTimestamp(e.Name())
		if !ok {
			continue
		}
		snapDir := filepath.Join(dir, e.Name())
		s := localSnapshot{name: e.Name(), ts: ts, complete: SnapshotComplete(snapDir)}
		if s.complete {
			s.tables = listSnapshotTables(snapDir)
		}
		out = append(out, s)
	}
	return out, nil
}

// listSnapshotTables returns the "schema/table" pairs present in a snapshot
// directory, mirroring the <timestamp>/<schema>/<table>.parquet layout that
// DiscoverBaselines and reconstruct.ListBaselines both walk.
func listSnapshotTables(snapDir string) []string {
	dbDirs, err := os.ReadDir(snapDir)
	if err != nil {
		return nil
	}
	var tables []string
	for _, dbDir := range dbDirs {
		if !dbDir.IsDir() {
			continue
		}
		schemaDir := filepath.Join(snapDir, dbDir.Name())
		files, err := os.ReadDir(schemaDir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
				continue
			}
			tables = append(tables, dbDir.Name()+"/"+strings.TrimSuffix(f.Name(), ".parquet"))
		}
	}
	return tables
}

// computeKeepers returns the set of snapshot directory names that must never be
// pruned to keep Time-travel intact: for every table, the newest COMPLETE
// snapshot that contains it. This is exactly reconstruct.FindBaseline's per-table
// selection (newest complete snapshot containing the table) — deleting any member
// would make at least one table's `at=now` reconstruct return ErrNoBaseline.
func computeKeepers(snaps []localSnapshot) map[string]bool {
	newestName := make(map[string]string)  // table → snapshot dir name
	newestTS := make(map[string]time.Time) // table → snapshot timestamp
	for _, s := range snaps {
		if !s.complete {
			continue
		}
		for _, tbl := range s.tables {
			if cur, ok := newestTS[tbl]; !ok || s.ts.After(cur) {
				newestTS[tbl] = s.ts
				newestName[tbl] = s.name
			}
		}
	}
	keepers := make(map[string]bool, len(newestName))
	for _, name := range newestName {
		keepers[name] = true
	}
	return keepers
}

// removeSnapshot deletes a snapshot directory atomically-then-lazily: it renames
// <dir>/<name> to <dir>/.<name>.pruning (an atomic same-filesystem rename that
// instantly hides the tree from discovery), then RemoveAll's the staged path. If
// the rename succeeds but RemoveAll partially fails, the leftover ".pruning" dir
// is harmless (invisible to readers) and swept next cycle.
func removeSnapshot(dir, name string) error {
	src := filepath.Join(dir, name)
	staged := filepath.Join(dir, "."+name+pruningSuffix)
	if err := os.Rename(src, staged); err != nil {
		return fmt.Errorf("stage snapshot %q for removal: %w", name, err)
	}
	if err := os.RemoveAll(staged); err != nil {
		return fmt.Errorf("remove staged snapshot %q: %w", staged, err)
	}
	return nil
}

// sweepPruningLeftovers removes ".<ts>.pruning" staging directories left by a
// crashed prune. Best-effort; failures are logged, not fatal.
func sweepPruningLeftovers(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasPrefix(n, ".") && strings.HasSuffix(n, pruningSuffix) {
			p := filepath.Join(dir, n)
			if err := os.RemoveAll(p); err != nil {
				slog.Warn("baseline prune: could not sweep leftover staging dir", "path", p, "error", err)
			}
		}
	}
}

// dirSize sums the sizes of all regular files under dir (best-effort, for the
// reclaimed-bytes report).
func dirSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, ierr := d.Info(); ierr == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
