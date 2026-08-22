package consoleapp

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
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

// sqlExportRoot holds a server's builds. Each build writes into its OWN
// subdirectory: a shared path reused across builds would let a rebuild
// interleave two instants into one archive whose per-file guards all pass
// (fresh Stat, matching sizes) — the silent half of the wipe race.
func (s *baselineSupervisor) sqlExportRoot(serverID string) string {
	return filepath.Join(s.stagingDir, "sql-export", serverID)
}

// TriggerSQLExport starts a build. console.ErrBaselineRunning when another
// baseline job for the server is in flight.
func (s *baselineSupervisor) TriggerSQLExport(req console.SQLExportRequest) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	dir := filepath.Join(s.sqlExportRoot(req.ServerID), strconv.FormatInt(time.Now().UnixNano(), 10))
	s.exports[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp(),
		At: req.At.UTC().Format(time.RFC3339)}
	s.exportDirs[req.ServerID] = dir
	s.mu.Unlock()

	slog.Info("sql export: starting", "server", req.ServerName, "id", req.ServerID,
		"at", req.At.UTC().Format(time.RFC3339))
	go s.runSQLExport(req, dir)
	return nil
}

// SQLExportStatus reports the last build for a server (idle if none ran here).
func (s *baselineSupervisor) SQLExportStatus(serverID string) console.BaselineStatus {
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

func (s *baselineSupervisor) runSQLExport(req console.SQLExportRequest, dir string) {
	tables, rows, bytes, err := s.executeSQLExport(req, dir)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.exports[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.exports[req.ServerID] = st
	}
	st.FinishedAt = nowStamp()
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
		// Warn, never Error: a refusal (gap, schema change) is the fail-closed
		// contract working; the operator picks another moment. Partial files
		// may remain in the build dir under _INCOMPLETE — never downloadable,
		// removed by the next build or the boot sweep.
		slog.Warn("sql export: failed, nothing downloadable", "server", req.ServerName,
			"id", req.ServerID, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	slog.Info("sql export: ready", "server", req.ServerName, "id", req.ServerID,
		"tables", tables, "rows", rows, "at", st.At)
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
		if err := os.RemoveAll(filepath.Join(root, ent.Name())); err != nil {
			return 0, 0, 0, fmt.Errorf("clear previous build: %w", err)
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, 0, 0, fmt.Errorf("create build directory: %w", err)
	}

	// RESOURCE POSTURE: the DuckDB budget stays conservative like every
	// supervisor fold (zero value = DefaultTuning) — this daemon is also
	// capturing and serving the console — plus a bound this fold adds that
	// the refresh does not: table parallelism 2 instead of the engine's
	// NumCPU default.
	reports, _, runErr := reconstruct.ReconstructTablesDetailed(s.ctx, reconstruct.FullTableConfig{
		IndexDSN:     req.IndexDSN,
		BaselineSrc:  req.BaselineSrc,
		Tables:       tableList,
		At:           req.At.UTC(),
		OutputDir:    dir,
		OutputFormat: reconstruct.OutputFormatMydumper,
		Parallelism:  2,
		// AllowGaps stays FALSE: a dump the operator will load somewhere is
		// the last artifact that may be knowingly incomplete.
	})
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
