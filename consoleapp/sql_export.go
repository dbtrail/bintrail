package consoleapp

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Custom .sql backup (the Backups page's "build a backup for a moment"):
// fold the newest snapshot at-or-before the chosen instant forward through
// the index's deltas and write the result as a mydumper-format dump —
// schema files, INSERT chunks and the coordinates `metadata` file — that
// `myloader` (or the mysql client, applying mydumper's session assumptions)
// loads directly, no bintrail needed on the restore side.
//
// Same engine as `reconstruct --output-format mydumper --at T`, same
// posture as the other supervisor folds: all-or-nothing (the #842
// completeness marker gates the download), fail-closed on capture gaps and
// schema changes, conservative DuckDB budget, and the per-server
// single-flight shared with dump/refresh/restore — every one of these
// writes or reads the same baseline store, and all but the dump the same
// index.
//
// Deliberately NOT recorded in the baseline run history: an export
// publishes no snapshot, so FindBySnapshot (the history's one consumer)
// could never match its record, while each one would still consume the
// per-server cap and evict a dump/refresh/restore record the Backups
// detail view needs.
//
// LIFETIME (#1448): the build is a full plaintext copy of every row parked
// on the index host, so it lives exactly as long as it is useful. A
// finished build is removed the moment a download completes, or when
// sqlExportTTL passes without one, or when a new build for the same server
// starts; a failed build is removed at once; and every build a previous
// process left behind is swept at boot (sweepSQLExportStaging). Every one
// of those removals goes through removeStagedBuild, which refuses any path
// outside the sql-export staging base and any path that crosses a symbolic
// link, so no state this supervisor can reach makes it delete something it
// did not write.

// sqlExportTTL is how long a finished build stays downloadable. Long enough
// to survive a coffee break and a slow browser; short enough that a forgotten
// build does not park gigabytes on the index host until someone finds it
// with du. The Backups page tells the operator the deadline.
const sqlExportTTL = 4 * time.Hour

// sqlExportReapEvery is how often the background reaper looks for builds
// past their TTL. Status reads expire lazily too, so this only bounds how
// long an UNWATCHED build outlives its deadline.
const sqlExportReapEvery = time.Minute

// sqlExportBase is the one directory every build lives under. It is the
// boundary removeStagedBuild enforces.
func (s *baselineSupervisor) sqlExportBase() string {
	return filepath.Join(s.stagingDir, "sql-export")
}

// sqlExportRoot holds a server's builds. Each build writes into its OWN
// subdirectory: a shared path reused across builds would let a rebuild
// interleave two instants into one archive whose per-file guards all pass
// (fresh Stat, matching sizes) — the silent half of the wipe race.
func (s *baselineSupervisor) sqlExportRoot(serverID string) string {
	return filepath.Join(s.sqlExportBase(), serverID)
}

// clock is the export lifecycle's time source. Injectable so the TTL is
// testable without waiting for it; nil means the wall clock.
func (s *baselineSupervisor) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// TriggerSQLExport starts a build. console.ErrBaselineRunning when another
// baseline job for the server is in flight.
func (s *baselineSupervisor) TriggerSQLExport(req console.SQLExportRequest) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	now := s.clock().UTC()
	dir := filepath.Join(s.sqlExportRoot(req.ServerID), strconv.FormatInt(now.UnixNano(), 10))
	s.exports[req.ServerID] = &console.BaselineStatus{State: "running", Since: now.Format(time.RFC3339),
		At: req.At.UTC().Format(time.RFC3339)}
	s.exportDirs[req.ServerID] = dir
	s.mu.Unlock()

	slog.Info("sql export: starting", "server", req.ServerName, "id", req.ServerID,
		"at", req.At.UTC().Format(time.RFC3339))
	go s.runSQLExport(req, dir)
	return nil
}

// SQLExportStatus reports the last build for a server (idle if none ran here).
// It expires first, so a build past its deadline, or one whose files were
// removed from under it, reads "expired" on the very poll that finds it
// rather than "succeeded" over a download that would 409.
func (s *baselineSupervisor) SQLExportStatus(serverID string) console.BaselineStatus {
	s.expireSQLExports()
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.exports[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

// SQLExportDir returns the finished dump's directory plus the status it
// belongs to, from ONE locked read — two separate reads let a trigger land
// between them and label the old build's bytes with the new build's
// instant. ok is false until the build has SUCCEEDED and its directory
// affirmatively carries the #842 _SUCCESS marker without an _INCOMPLETE
// one. Affirmative on purpose: baseline.SnapshotComplete is
// complete-by-default when NO marker exists (a legacy rule for pre-marker
// snapshots), and the marker-less shapes here are never legacy — they are a
// torn or externally-removed staging directory (the default staging root
// lives under the system temp dir, which some hosts reap), and must read
// not-ready, not stream a 500.
func (s *baselineSupervisor) SQLExportDir(serverID string) (string, console.BaselineStatus, bool) {
	s.expireSQLExports()
	s.mu.Lock()
	st := console.BaselineStatus{State: "idle"}
	var dir string
	if p, ok := s.exports[serverID]; ok {
		st = *p
		dir = s.exportDirs[serverID]
	}
	s.mu.Unlock()
	if st.State != "succeeded" || dir == "" {
		return "", st, false
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		return "", st, false
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); err == nil {
		return "", st, false
	}
	return dir, st, true
}

// SQLExportDelivered is the download handler's signal that the whole
// archive reached the client: the build has done its job, so its rows leave
// the disk now instead of waiting for the TTL. dir pins WHICH build was
// delivered — a trigger that landed mid-stream owns a different directory,
// and that one must not be removed on the strength of the old one's
// download.
func (s *baselineSupervisor) SQLExportDelivered(serverID, dir string) {
	s.mu.Lock()
	st := s.exports[serverID]
	if st == nil || st.State != "succeeded" || dir == "" || s.exportDirs[serverID] != dir {
		s.mu.Unlock()
		return
	}
	st.State = "downloaded"
	st.DownloadedAt = s.clock().UTC().Format(time.RFC3339)
	s.mu.Unlock()
	s.removeSQLExportBuild(serverID, dir, "downloaded")
}

// SQLExportStaged reports what the sql-export staging holds right now, for
// the Storage page's disk picture: every build that is running or waiting
// for its download, with its LIVE size (a build in progress grows; a
// finished one is what st.Bytes already says). Expires first, so a build
// past its deadline never counts.
func (s *baselineSupervisor) SQLExportStaged() console.SQLExportStagingInfo {
	s.expireSQLExports()
	info := console.SQLExportStagingInfo{Dir: s.sqlExportBase(), TTL: sqlExportTTL}
	s.mu.Lock()
	type held struct {
		id  string
		st  console.BaselineStatus
		dir string
	}
	var builds []held
	for id, st := range s.exports {
		if st.State != "running" && st.State != "succeeded" {
			continue
		}
		builds = append(builds, held{id: id, st: *st, dir: s.exportDirs[id]})
	}
	s.mu.Unlock()
	sort.Slice(builds, func(i, j int) bool { return builds[i].id < builds[j].id })
	for _, b := range builds {
		bytes := b.st.Bytes
		if b.dir != "" {
			if n, err := dirBytes(b.dir); err == nil {
				bytes = n
			}
		}
		info.Builds = append(info.Builds, console.SQLExportStagedBuild{
			ServerID: b.id, State: b.st.State, At: b.st.At, ExpiresAt: b.st.ExpiresAt, Bytes: bytes,
		})
	}
	return info
}

// expireSQLExports moves every finished build that is past its TTL, or
// whose files are gone from under it, to "expired" and removes whatever is
// left of it. Called by the background reaper and lazily by every read, so
// the state the API reports and the bytes on disk can never disagree for
// longer than one poll: a build whose directory an operator removed by hand
// used to stay "succeeded" until the next build, pointing a download button
// at nothing.
func (s *baselineSupervisor) expireSQLExports() {
	now := s.clock()
	type doomed struct {
		id, dir, why string
	}
	var remove []doomed
	s.mu.Lock()
	for id, st := range s.exports {
		if st.State != "succeeded" {
			continue
		}
		dir := s.exportDirs[id]
		why := ""
		switch {
		case dir == "" || !fileExists(filepath.Join(dir, baseline.SuccessMarker)):
			why = "files removed from the staging directory"
		case sqlExportExpired(st.ExpiresAt, now):
			why = "download deadline passed"
		}
		if why == "" {
			continue
		}
		st.State = "expired"
		remove = append(remove, doomed{id: id, dir: dir, why: why})
	}
	s.mu.Unlock()
	for _, d := range remove {
		s.removeSQLExportBuild(d.id, d.dir, d.why)
	}
}

// sqlExportExpired reports whether a build stamped with expiresAt is past
// its deadline at now. A stamp this build cannot parse counts as expired:
// keeping an unbounded build is the failure this exists to prevent.
func sqlExportExpired(expiresAt string, now time.Time) bool {
	exp, err := time.Parse(time.RFC3339, expiresAt)
	if err != nil {
		return true
	}
	return !now.Before(exp)
}

// runSQLExportReaper expires builds on a timer until the daemon stops. It
// exists for the build nobody is watching: the Backups page polls only
// while it is open, and a build left behind after the tab closed would
// otherwise sit on disk until the next visit.
func (s *baselineSupervisor) runSQLExportReaper() {
	every := s.exportReapEvery
	if every <= 0 {
		every = sqlExportReapEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.expireSQLExports()
		}
	}
}

// removeSQLExportBuild removes one build directory through the path guard
// and logs what it did. A refusal is logged at Warn and the bytes stay: a
// guard that refused has found a path this supervisor never wrote, and the
// right answer to that is a human looking, not a broader delete.
func (s *baselineSupervisor) removeSQLExportBuild(serverID, dir, why string) {
	if dir == "" {
		return
	}
	freed, err := removeStagedBuild(s.sqlExportBase(), dir)
	if err != nil {
		slog.Warn("sql export: could not remove a staged build", "id", serverID, "dir", dir, "why", why, "error", err)
		return
	}
	slog.Info("sql export: removed staged build", "id", serverID, "dir", dir, "why", why, "bytes", freed)
}

// removeStagedBuild deletes dir, which must be a build directory strictly
// below base (the sql-export staging base), and reports the bytes it held.
// Two refusals, both loud, both before anything is touched:
//
//   - dir outside base (an absolute elsewhere, a ".." escape, base itself):
//     nothing this supervisor writes lives there.
//   - base or any component below it is a symbolic link: os.RemoveAll does
//     not follow links, but a link swapped in for a directory would still
//     redirect a path this code trusts onto a tree it must not delete. The
//     components ABOVE base are not checked: an operator may legitimately
//     point BINTRAIL_CONSOLE_BASELINE_STAGING at a link to a bigger disk.
//
// A dir that is already gone is a success: the goal is "not there".
func removeStagedBuild(base, dir string) (int64, error) {
	base, dir = filepath.Clean(base), filepath.Clean(dir)
	rel, err := filepath.Rel(base, dir)
	if err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) ||
		strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return 0, fmt.Errorf("refusing to remove %q: not inside the sql-export staging directory %q", dir, base)
	}
	if fi, err := os.Lstat(base); err == nil && fi.Mode()&fs.ModeSymlink != 0 {
		return 0, fmt.Errorf("refusing to remove %q: the staging directory %q is a symbolic link", dir, base)
	}
	cur := base
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return 0, fmt.Errorf("refusing to remove %q: %q is a symbolic link", dir, cur)
		}
	}
	freed, _ := dirBytes(dir)
	if err := os.RemoveAll(dir); err != nil {
		return 0, err
	}
	return freed, nil
}

// dirBytes sums the regular files under dir without following symbolic
// links (WalkDir reports a link as itself, never as its target). A missing
// dir is zero bytes, not an error.
func dirBytes(dir string) (int64, error) {
	var n int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		n += info.Size()
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	return n, err
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// sweepSQLExportStaging removes every build a previous process left under
// stagingDir/sql-export. A restart empties the in-memory exports map, so
// any dump a dead process built (finished, interrupted mid-fold, or
// abandoned when the operator closed the tab) is unreachable from the API
// and would otherwise sit on disk indefinitely. Called from watch startup
// UNCONDITIONALLY as well as from the supervisor constructor, because a
// restart that turned the baseline features off would otherwise keep the
// old artifact forever (no supervisor would ever sweep it).
//
// One build at a time through removeStagedBuild, never one RemoveAll over
// the base: the guard is what makes a boot-time delete safe to run on a
// path an operator may have rearranged while the daemon was down.
func sweepSQLExportStaging(stagingDir string) {
	base := filepath.Join(stagingDir, "sql-export")
	fi, err := os.Lstat(base)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		slog.Warn("sql export: could not sweep the staging directory", "dir", base, "error", err)
		return
	}
	if fi.Mode()&fs.ModeSymlink != 0 {
		slog.Warn("sql export: the staging directory is a symbolic link; not sweeping it. Remove the link and restart to let old builds be cleared here", "dir", base)
		return
	}
	servers, err := os.ReadDir(base)
	if err != nil {
		slog.Warn("sql export: could not sweep the staging directory", "dir", base, "error", err)
		return
	}
	var builds int
	var freed int64
	for _, srv := range servers {
		srvDir := filepath.Join(base, srv.Name())
		if !srv.IsDir() {
			// A stray file or a link where a server directory should be:
			// nothing this code wrote, so nothing this code removes.
			slog.Warn("sql export: skipping an entry this daemon did not write", "path", srvDir)
			continue
		}
		runs, err := os.ReadDir(srvDir)
		if err != nil {
			slog.Warn("sql export: could not sweep a server's staged builds", "dir", srvDir, "error", err)
			continue
		}
		for _, run := range runs {
			n, err := removeStagedBuild(base, filepath.Join(srvDir, run.Name()))
			if err != nil {
				slog.Warn("sql export: could not remove a staged build", "error", err)
				continue
			}
			builds++
			freed += n
		}
		// Only an empty directory goes; anything skipped above keeps it.
		_ = os.Remove(srvDir)
	}
	_ = os.Remove(base)
	if builds > 0 {
		slog.Info("sql export: removed staged builds left by a previous run", "builds", builds, "bytes", freed)
	}
}

func (s *baselineSupervisor) runSQLExport(req console.SQLExportRequest, dir string) {
	defer s.recoverBaselineJob(baselineJobExport, req.ServerID, req.ServerName)
	tables, rows, bytes, err := s.executeSQLExport(req, dir)
	s.finishSQLExport(req, dir, tables, rows, bytes, err)
}

// finishSQLExport is the status tail of a build: it publishes the verdict
// and, on success, stamps the download deadline; on failure it removes the
// partial build at once (a refused fold can still have written gigabytes
// under _INCOMPLETE, and nothing will ever download them).
func (s *baselineSupervisor) finishSQLExport(req console.SQLExportRequest, dir string, tables int, rows, bytes int64, err error) {
	now := s.clock().UTC()
	s.mu.Lock()
	st := s.exports[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.exports[req.ServerID] = st
	}
	st.FinishedAt = now.Format(time.RFC3339)
	st.Tables = tables
	st.Rows = rows
	st.Bytes = bytes
	if err != nil {
		// Attempt-scoped partials read as progress to a status-API consumer
		// ({state:"failed", rows:12000} looks half-done); a failed build
		// published nothing, so report nothing.
		st.Tables, st.Rows, st.Bytes = 0, 0, 0
		st.State = "failed"
		st.LastError = err.Error()
		s.mu.Unlock()
		// Warn, never Error: a refusal (gap, schema change) is the fail-closed
		// contract working; the operator picks another moment.
		slog.Warn("sql export: failed, nothing downloadable", "server", req.ServerName,
			"id", req.ServerID, "error", err)
		s.removeSQLExportBuild(req.ServerID, dir, "build failed")
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	st.ExpiresAt = now.Add(sqlExportTTL).Format(time.RFC3339)
	s.mu.Unlock()
	slog.Info("sql export: ready", "server", req.ServerName, "id", req.ServerID,
		"tables", tables, "rows", rows, "at", st.At, "expires_at", st.ExpiresAt)
}

func (s *baselineSupervisor) executeSQLExport(req console.SQLExportRequest, dir string) (tables int, rows, bytes int64, err error) {
	tableList, err := reconstruct.SnapshotTablesAt(s.ctx, req.BaselineSrc, req.At)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list the backup to build from: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, 0, fmt.Errorf("no backup exists at or before %s; the build folds an existing backup forward, so pick a moment after your oldest backup", req.At.UTC().Format("2006-01-02 15:04:05"))
	}
	// Tear down previous builds as this one starts; the new build writes only
	// into its own directory, so an in-flight download of an OLD build either
	// completes byte-exact (files already open survive the unlink) or dies
	// loudly on its abort guard — never silently mixed.
	root := filepath.Dir(dir)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return 0, 0, 0, fmt.Errorf("create staging directory: %w", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("scan staging directory: %w", err)
	}
	for _, ent := range entries {
		if ent.Name() == filepath.Base(dir) {
			continue
		}
		if _, err := removeStagedBuild(s.sqlExportBase(), filepath.Join(root, ent.Name())); err != nil {
			return 0, 0, 0, fmt.Errorf("clear previous build: %w", err)
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, 0, 0, fmt.Errorf("create build directory: %w", err)
	}

	reports, _, runErr := foldTables(s.ctx, sqlExportFoldConfig(req, dir, tableList))
	for _, rep := range reports {
		rows += rep.RowsWritten
	}
	// The artifact's weight, for the UI's blob-download confirm. Best-effort:
	// a stat failure here must not fail a build the fold already finished.
	if built, err := os.ReadDir(dir); err == nil {
		for _, ent := range built {
			if info, err := ent.Info(); err == nil && !ent.IsDir() {
				bytes += info.Size()
			}
		}
	}
	return len(tableList), rows, bytes, sqlExportRunError(reports, runErr)
}

// sqlExportFoldConfig is the configuration one SQL export build folds with.
//
// Split out of executeSQLExport for the same reason refreshFoldConfig is split
// out of foldSnapshot: the budgets it carries are then checkable without
// running a fold or touching a filesystem.
//
// RESOURCE POSTURE: this build folds inside the process that is also capturing
// and serving the console, so it takes the shared in-daemon posture. The DuckDB
// budget stays at its zero value, which resolves to the container-safe
// DefaultTuning. Parallelism and WarnEventThreshold do NOT, so both are named:
// zero would mean runtime.NumCPU() and a warning that never fires. They are the
// same constants the refresh and restore folds use, because a host that cannot
// afford one of these folds cannot afford another.
func sqlExportFoldConfig(req console.SQLExportRequest, dir string, tableList []string) reconstruct.FullTableConfig {
	return reconstruct.FullTableConfig{
		IndexDSN:           req.IndexDSN,
		BaselineSrc:        req.BaselineSrc,
		Tables:             tableList,
		At:                 req.At.UTC(),
		OutputDir:          dir,
		OutputFormat:       reconstruct.OutputFormatMydumper,
		Parallelism:        daemonFoldParallelism,
		WarnEventThreshold: daemonFoldWarnEventThreshold,
		RemediationHint:    daemonFoldRemediation,
		// AllowGaps stays FALSE: a dump the operator will load somewhere is
		// the last artifact that may be knowingly incomplete.
	}
}

// sqlExportRunError folds the engine's per-table reports into the run
// verdict. The engine's binlog-only fallback (baseline vanished mid-build)
// warns and keeps going in mydumper mode; here it must fail the run: such a
// table holds only the rows the window touched, and drill's doctrine
// applies — a dump without its baseline is never a PASS.
func sqlExportRunError(reports []*reconstruct.TableReport, runErr error) error {
	for _, rep := range reports {
		if rep.BinlogOnly {
			runErr = errors.Join(runErr, fmt.Errorf(
				"table %s.%s lost its backup mid-build and was rebuilt from recorded changes only; that dump would silently miss every row those changes never touched", rep.Schema, rep.Table))
		}
	}
	return runErr
}
