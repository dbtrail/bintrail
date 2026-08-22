package consoleapp

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Point-in-time restore (the backups page's PITR action): fold the snapshot
// at-or-before the chosen instant forward through the index's deltas and
// publish the result as a NEW discoverable snapshot named by that instant.
// It shares foldSnapshot with the periodic refresh — same all-or-nothing
// publication, same sentinel-classified refusals (capture gap, destructive
// DDL), same conservative DuckDB budget (this is a daemon that is also
// capturing; see executeRefresh's resource-posture comment).

// TriggerRestore starts a restore, sharing the supervisor's per-server
// single-flight with dumps and refreshes: all three write the same store.
func (s *baselineSupervisor) TriggerRestore(req console.BaselineRestoreRequest) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	s.restores[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp(),
		At: req.At.UTC().Format(time.RFC3339)}
	s.mu.Unlock()

	slog.Info("baseline restore: starting", "server", req.ServerName, "id", req.ServerID,
		"at", req.At.UTC().Format(time.RFC3339))
	go s.runRestore(req)
	return nil
}

// RestoreStatus reports the last restore for a server (idle if none ran here).
func (s *baselineSupervisor) RestoreStatus(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.restores[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

func (s *baselineSupervisor) runRestore(req console.BaselineRestoreRequest) {
	started := time.Now().UTC()
	tables, refused, err := s.executeRestore(req)
	s.recordRun(req.ServerID, req.ServerName, console.BaselineRunRecord{
		Kind: console.BaselineRunRestore, StartedAt: started.Format(time.RFC3339),
		SnapshotTime: publishedSnapshotTime(req.At, err), Tables: tables, Refused: refused,
	}, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.restores[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.restores[req.ServerID] = st
	}
	st.FinishedAt = nowStamp()
	st.Tables = tables
	st.Refused = refused
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		// Warn, never Error: a refusal (gap, schema change) is the fail-closed
		// contract working, and the operator picks another moment.
		slog.Warn("baseline restore: published nothing", "server", req.ServerName, "id", req.ServerID,
			"refused", refused, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	slog.Info("baseline restore: published", "server", req.ServerName, "id", req.ServerID,
		"tables", tables, "at", req.At.UTC().Format(time.RFC3339))
}

// executeRestore folds toward the chosen instant. The table list comes from
// the snapshot FindBaseline will anchor on (newest at-or-before At), NOT the
// newest snapshot overall: restoring to a moment before the newest snapshot
// must fold the older snapshot's tables.
func (s *baselineSupervisor) executeRestore(req console.BaselineRestoreRequest) (tables, refused int, err error) {
	tableList, err := reconstruct.SnapshotTablesAt(s.ctx, req.BaselineDir, req.At)
	if err != nil {
		return 0, 0, fmt.Errorf("list the snapshot to restore from: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, fmt.Errorf("no backup exists at or before %s; a restore folds an existing backup forward, so pick a moment after your oldest backup", req.At.UTC().Format("2006-01-02 15:04:05"))
	}
	return s.foldSnapshot(refreshRequest{
		ServerID: req.ServerID, ServerName: req.ServerName,
		IndexDSN: req.IndexDSN, BaselineDir: req.BaselineDir,
	}, req.At.UTC(), tableList)
}

// wireBaselineExtras attaches the run history, the restore capability and
// the sql-export capability to a freshly built supervisor, and exposes them
// on the console Config. Called by
// both watch entry paths; a no-op without a supervisor (serve-only consoles
// keep their 403s).
//
// Restore rides EITHER opt-in deliberately, which looks like the derivation
// the BaselineRestorer comment warns about — it is not. #1171's hazard was a
// derived flag turning on a feature of a DIFFERENT class (a dump locks and
// reads the source; mydumper may not exist). A restore is the same fold the
// refresh already performs, into the same store, with no source contact; the
// only delta is who picks the instant. A daemon that opted into either
// baseline producer has already accepted exactly this work. The sql export
// rides the same reasoning: the same fold, into staging instead of the
// store, handed out only through the query:execute download. An unreadable history file runs without history rather
// than refusing the daemon — durations degrade to file-timestamp spans, and
// not opening a store means nothing overwrites the file it might describe.
func wireBaselineExtras(cfg *console.Config, sup *baselineSupervisor, serversPath string) {
	if sup == nil {
		return
	}
	history, err := console.OpenBaselineHistory(console.DefaultBaselineHistoryPath(serversPath))
	if err != nil {
		slog.Error("baseline history unavailable; run durations will NOT be recorded (file timestamps still approximate them); fix or move the file and restart", "error", err)
		history = nil
	}
	sup.history = history
	cfg.BaselineHistory = history
	cfg.BaselineRestore = sup
	cfg.SQLExport = sup
}
