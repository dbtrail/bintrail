package baseline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CurrentLinkName is the name of the pointer a baseline root carries alongside
// its timestamped snapshot directories:
//
//	<root>/2026-08-30T03-00-00Z/   <- a snapshot
//	<root>/2026-08-31T03-00-00Z/   <- a newer snapshot
//	<root>/current -> 2026-08-31T03-00-00Z
//
// It exists so a generated artifact can name a path that stays correct after
// the next snapshot lands. `bintrail views` points its state_<schema>_<table>
// views at <root>/current/... by default, so a periodically refreshed baseline
// reaches an already-generated views file without regenerating it (#1484).
//
// The pointer is a SYMLINK, not a copy or a rewritten directory, for one
// reason: replacing it is a single rename(2), so every table moves to the new
// snapshot in the same instant. A reader mid-query holds an already-open file
// and finishes against the old snapshot; a reader that starts after the swap
// sees the new one whole. There is no window in which one table is new and
// another is old.
//
// It is deliberately NOT part of discovery. FindBaseline, ListBaselines,
// DiscoverBaselines and PruneLocal all enumerate with os.ReadDir + IsDir(),
// which reports false for a symlink, and every one of them additionally
// requires the name to parse as a timestamp. So "current" is invisible to
// recovery: it can never be selected as a baseline, and it can never be
// pruned. Recovery reads snapshots by their own names, as it always has.
const CurrentLinkName = "current"

// currentLinkTmp is the staging name PublishCurrentPointer renames from. It is
// dot-prefixed and does not parse as a timestamp, so a leftover from a crash
// between the symlink and the rename is invisible to discovery too.
const currentLinkTmp = "." + CurrentLinkName + ".tmp"

// PublishCurrentPointer repoints <root>/current at snapshotDir, where root is
// snapshotDir's parent. It is called from WriteSuccessMarker, so the pointer
// moves exactly when a snapshot becomes complete — never before, so the
// pointer can never name a snapshot that is still being written.
//
// Three rules, each of which only ever declines to move the pointer:
//
//   - Snapshot directories only. snapshotDir's base name must parse as a
//     baseline timestamp, the same predicate discovery uses. `reconstruct
//     --output-format mydumper` writes its _SUCCESS into an operator-chosen
//     dump directory that is not a snapshot under a baselines root; that call
//     lands here and does nothing.
//   - Forward only. A snapshot can be produced for a past instant (`reconstruct
//     --output-format parquet --at <yesterday>` does exactly that), and
//     completing it must not walk the pointer backwards to it.
//   - Never clobber real data. If <root>/current exists and is not a symlink,
//     an operator has a directory or file by that name; refuse rather than
//     remove it.
//
// What it deliberately does NOT check is that the new snapshot carries every
// table the current one does. The pointer names the newest COMPLETE snapshot,
// whatever tables that snapshot holds, and a followed views file can therefore
// lose a view when the table set shrinks: the view keeps naming
// <root>/current/<schema>/<table>.parquet, which stops resolving, and DuckDB
// says so by path.
//
// That is the chosen failure, not an oversight. A superset rule would freeze
// the pointer FOREVER the first time a table is legitimately dropped, since no
// later snapshot could ever satisfy it again, and every already-generated file
// would go on returning older and older rows with nothing to indicate it. This
// product's whole reason for existing argues the other way: a loud failure that
// names the missing table beats silent staleness. The generated file's header
// says to regenerate after a table is added or dropped, which is exactly this
// case.
//
// The unattended producers do not shrink the table set anyway: the periodic
// refresh folds exactly the tables of the snapshot it read (newestSnapshotTables),
// and the console's dump filters by configured SCHEMA, not by table. Reaching
// this needs an operator action -- `--tables` on a hand-run baseline, or
// narrowing a server's schemas -- which is the same act the header already
// tells them to regenerate after.
//
// The link target is the bare directory name, not an absolute path, so a
// baselines root stays valid after being moved, bind-mounted at a different
// path in a container, or copied to another host.
func PublishCurrentPointer(snapshotDir string) error {
	name := filepath.Base(snapshotDir)
	ts, ok := parseBaselineDirTimestamp(name)
	if !ok {
		return nil // not a snapshot directory under a baselines root
	}
	root := filepath.Dir(snapshotDir)
	link := filepath.Join(root, CurrentLinkName)

	fi, err := os.Lstat(link)
	switch {
	case err == nil && fi.Mode()&os.ModeSymlink == 0:
		return fmt.Errorf("%s exists and is not a symlink; refusing to replace it "+
			"(move it aside to let baseline snapshots publish the %s pointer)", link, CurrentLinkName)
	case err == nil:
		// Forward only. An unreadable or unparseable target is treated as "no
		// usable pointer" and replaced: a dangling or hand-edited link is worse
		// than a correct one, and anything this function wrote parses.
		if target, rerr := os.Readlink(link); rerr == nil {
			if cur, ok := parseBaselineDirTimestamp(filepath.Base(target)); ok && !ts.After(cur) {
				return nil
			}
		}
	case !os.IsNotExist(err):
		return fmt.Errorf("stat %s: %w", link, err)
	}

	tmp := filepath.Join(root, currentLinkTmp)
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale %s: %w", tmp, err)
	}
	if err := os.Symlink(name, tmp); err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	// rename(2) over an existing symlink replaces it atomically, which is the
	// whole point: readers see either the old target or the new one.
	if err := os.Rename(tmp, link); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("publish %s: %w", link, err)
	}
	return nil
}

// ResolveCurrentPointer returns the snapshot directory name <root>/current
// points at, and whether the root carries a usable pointer at all. A missing
// pointer, a non-symlink, or a target that does not parse as a baseline
// timestamp all report false: callers fall back to naming a snapshot directly.
//
// It deliberately does NOT check that the target exists. The pointer's value is
// that it is read at QUERY time by whatever opens the generated file, which may
// be long after and on another host; reporting "usable" from the state of this
// filesystem right now would be the wrong question.
func ResolveCurrentPointer(root string) (string, bool) {
	link := filepath.Join(root, CurrentLinkName)
	fi, err := os.Lstat(link)
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		return "", false
	}
	target, err := os.Readlink(link)
	if err != nil {
		return "", false
	}
	name := filepath.Base(target)
	if _, ok := parseBaselineDirTimestamp(name); !ok {
		return "", false
	}
	return name, true
}

// RewriteToPointer rewrites baseline table paths under root so they read
// through <root>/current instead of naming a snapshot directory, and reports
// whether it did. It is the shared half of "follow the newest snapshot": both
// producers of a views file (the `bintrail views` command and the console
// download) apply it, so neither can drift into a different rule about when
// following is safe.
//
// It rewrites nothing and reports false unless EVERY path is a table of the one
// snapshot the pointer currently names. A pointer naming some other snapshot is
// not a degraded version of following — it would serve rows from a snapshot the
// generated file does not describe, which is worse than being one refresh
// behind. Same for a root with no usable pointer (every root written before
// this feature, until its next snapshot completes) and for an S3 root, which
// has no pointer to follow: callers pass "" for root there.
func RewriteToPointer(root string, paths []string) ([]string, bool) {
	// An empty root is the --baseline-s3 shape on the CLI side. Refused rather
	// than passed through: ResolveCurrentPointer would probe a bare "current"
	// against the PROCESS working directory, which is the same class of mistake
	// as reading a server-side filename against the wrong base.
	if root == "" || len(paths) == 0 {
		return nil, false
	}
	pointer, ok := ResolveCurrentPointer(root)
	if !ok {
		return nil, false
	}
	current := filepath.Join(root, CurrentLinkName)
	out := make([]string, len(paths))
	for i, path := range paths {
		snapshot, rest := splitSnapshotPath(root, path)
		if snapshot != pointer {
			return nil, false
		}
		out[i] = filepath.Join(current, rest)
	}
	return out, true
}

// splitSnapshotPath splits a baseline table path into its snapshot directory
// name and the part below it (<schema>/<table>.parquet). Both are "" when path
// is not a table under root.
func splitSnapshotPath(root, path string) (snapshot, rest string) {
	rel, err := filepath.Rel(root, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", ""
	}
	// filepath.Rel cleans its result, so a path naming the snapshot directory
	// itself has no separator left to cut, with or without a trailing one. That
	// is why "there is something below the snapshot directory" needs no check of
	// its own: !found already covers it.
	snapshot, rest, found := strings.Cut(rel, string(filepath.Separator))
	if !found {
		return "", ""
	}
	return snapshot, rest
}
