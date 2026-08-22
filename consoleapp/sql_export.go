package consoleapp

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Custom .sql backup (the Backups page's "build a backup for a moment"):
// fold the newest snapshot at-or-before the chosen instant forward through
// the index's deltas and write the result as a mydumper-format dump —
// schema files, INSERT chunks and the coordinates `metadata` file — that
// `myloader` (or the mysql client with mydumper's session assumptions)
// loads directly, no bintrail needed on the restore side.
//
// Same engine as `reconstruct --output-format mydumper --at T`, same
// posture as the other supervisor folds: all-or-nothing (the #842
// completeness marker gates the download), fail-closed on capture gaps and
// schema changes, conservative DuckDB budget, and the per-server
// single-flight shared with dump/refresh/restore — every one of these
// writes or reads the same store and index.

// sqlExportDir is where a server's last built dump lives. One per server,
// wiped at the start of each build: the artifact is a download, not a
// store — keeping N of them would grow the staging disk silently.
func (s *baselineSupervisor) sqlExportDir(serverID string) string {
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
	s.exports[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp(),
		At: req.At.UTC().Format(time.RFC3339)}
	s.mu.Unlock()

	slog.Info("sql export: starting", "server", req.ServerName, "id", req.ServerID,
		"at", req.At.UTC().Format(time.RFC3339))
	go s.runSQLExport(req)
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

// SQLExportDir returns the finished dump's directory. ok is false until a
// build has SUCCEEDED and its completeness marker agrees — a status that
// says succeeded while the marker says otherwise (a crash between the two)
// must not hand out a partial dump.
func (s *baselineSupervisor) SQLExportDir(serverID string) (string, bool) {
	s.mu.Lock()
	st, ok := s.exports[serverID]
	s.mu.Unlock()
	if !ok || st.State != "succeeded" {
		return "", false
	}
	dir := s.sqlExportDir(serverID)
	if !baseline.SnapshotComplete(dir) {
		return "", false
	}
	return dir, true
}

func (s *baselineSupervisor) runSQLExport(req console.SQLExportRequest) {
	started := time.Now().UTC()
	tables, refused, rows, err := s.executeSQLExport(req)
	s.recordRun(req.ServerID, req.ServerName, console.BaselineRunRecord{
		Kind: console.BaselineRunSQLExport, StartedAt: started.Format(time.RFC3339),
		Tables: tables, Refused: refused, Rows: rows,
	}, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.exports[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.exports[req.ServerID] = st
	}
	st.FinishedAt = nowStamp()
	st.Tables = tables
	st.Refused = refused
	st.Rows = rows
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		// Warn, never Error: a refusal (gap, schema change) is the fail-closed
		// contract working; the operator picks another moment.
		slog.Warn("sql export: built nothing", "server", req.ServerName, "id", req.ServerID,
			"refused", refused, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	slog.Info("sql export: ready", "server", req.ServerName, "id", req.ServerID,
		"tables", tables, "rows", rows, "at", st.At)
}

func (s *baselineSupervisor) executeSQLExport(req console.SQLExportRequest) (tables, refused int, rows int64, err error) {
	tableList, err := reconstruct.SnapshotTablesAt(s.ctx, req.BaselineSrc, req.At)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list the backup to build from: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, 0, fmt.Errorf("no backup exists at or before %s; the build folds an existing backup forward, so pick a moment after your oldest backup", req.At.UTC().Format("2006-01-02 15:04:05"))
	}
	dir := s.sqlExportDir(req.ServerID)
	// Wipe the previous build: scoped to this server's own export dir, which
	// holds nothing but the artifact this function writes.
	if err := os.RemoveAll(dir); err != nil {
		return 0, 0, 0, fmt.Errorf("clear previous build: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, 0, 0, fmt.Errorf("create build directory: %w", err)
	}

	// RESOURCE POSTURE: same reasoning as executeRefresh — this daemon is
	// also capturing and serving the console, so the fold keeps the
	// conservative DuckDB budget (zero value = DefaultTuning) and a bounded
	// table parallelism instead of NumCPU.
	reports, failures, runErr := reconstruct.ReconstructTablesDetailed(s.ctx, reconstruct.FullTableConfig{
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
	if runErr != nil {
		return len(tableList), len(failures), rows, runErr
	}
	return len(tableList), 0, rows, nil
}
