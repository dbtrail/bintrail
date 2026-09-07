package consoleapp

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

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

func (s *baselineSupervisor) RestoreStatus(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.restores[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

func (s *baselineSupervisor) runRestore(req console.BaselineRestoreRequest) {
	defer s.recoverBaselineJob(baselineJobRestore, req.ServerID, req.ServerName)
	started := time.Now().UTC()
	tables, refused, reuse, err := s.executeRestore(req)
	// Same rule as runRefresh, for the same reason: on a server whose backups
	// go to S3, a snapshot that exists on one box is not where the backups
	// live, and a restore that stopped there would be the one Parquet
	// publisher retention could never reclaim (PruneLocal confirms the S3 copy
	// before it deletes a local one). Gated on the fold's success so an
	// incomplete snapshot never reaches the destination.
	var uploaded int
	if err == nil && req.BaselineS3 != "" {
		uploaded, err = uploadRefreshedSnapshot(s.ctx, restoreFoldRequest(req), req.At.UTC())
	}
	s.recordRun(req.ServerID, req.ServerName, foldRunCounts(console.BaselineRunRecord{
		Kind: console.BaselineRunRestore, StartedAt: started.Format(time.RFC3339),
		SnapshotTime: publishedSnapshotTime(req.At, err),
		Uploaded:     uploaded,
	}, tables, refused, reuse), err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.restores[req.ServerID]
	if st == nil {
		st = &console.BaselineStatus{}
		s.restores[req.ServerID] = st
	}
	applyFoldStatus(st, tables, refused, reuse, err)
	if err != nil {
		if foldPublished(err) {
			// The fold did its work; only the sending failed. Said as such,
			// because "published nothing" below would send an operator looking
			// for a backup that is sitting complete in the local directory.
			slog.Warn("baseline restore: published locally, not uploaded", "server", req.ServerName, "id", req.ServerID,
				"tables", tables, "error", err)
			return
		}
		slog.Warn("baseline restore: published nothing", "server", req.ServerName, "id", req.ServerID,
			"refused", refused, "error", err)
		return
	}
	slog.Info("baseline restore: published", "server", req.ServerName, "id", req.ServerID,
		"tables", tables, "reused", reuse.reused, "reused_copied", reuse.copied, "uploaded", uploaded,
		"at", req.At.UTC().Format(time.RFC3339))
}

// snapshotTablesAt is reconstruct.SnapshotTablesAt behind a seam, for the
// reason newestSnapshotTables is: on an S3-backed server the listing reads the
// bucket, and a unit test that reached one would be neither hermetic nor
// offline-safe. Written by tests only; the same restore-after-terminal rule
// as foldTables applies.
var snapshotTablesAt = reconstruct.SnapshotTablesAt

func (s *baselineSupervisor) executeRestore(req console.BaselineRestoreRequest) (tables, refused int, reuse reuseTally, err error) {
	// The backup to fold from is looked for where the scheduled update looks
	// (#1541): the bucket on an S3-backed server, the local directory
	// otherwise. Listing req.BaselineDir here was the bug: on a server whose
	// backups are uploaded and pruned, or made by another host, the local
	// directory holds only what this daemon folded since it started, and the
	// refusal below fired while the bucket held dozens of backups.
	tableList, err := snapshotTablesAt(s.ctx, req.FoldSource(), req.At)
	if err != nil {
		return 0, 0, reuseTally{}, fmt.Errorf("list the snapshot to restore from: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, reuseTally{}, fmt.Errorf("no backup exists at or before %s; a restore folds an existing backup forward, so pick a moment after your oldest backup", req.At.UTC().Format("2006-01-02 15:04:05"))
	}
	return s.foldSnapshot(restoreFoldRequest(req), req.At.UTC(), tableList)
}

// restoreFoldRequest translates a restore request into the fold request the
// refresh path uses, which is the whole claim wireBaselineExtras makes below:
// same fold, same store, same read source and same destination; the delta is
// who picks the instant.
//
// Split out so that claim is checkable without an index and a baseline. Left
// inline, dropping CarryForwardUnchanged compiled, passed every test, and
// silently made the restore the one Parquet publisher that ignores the
// operator's setting. Dropping BaselineS3 would compile too, and would make it
// the one fold that reads and writes the local directory alone on a server
// whose backups live in a bucket (#1541).
func restoreFoldRequest(req console.BaselineRestoreRequest) refreshRequest {
	return refreshRequest{
		ServerID: req.ServerID, ServerName: req.ServerName,
		IndexDSN: req.IndexDSN, BaselineDir: req.BaselineDir,
		// Read source AND upload destination, exactly as the scheduled update
		// carries it: refreshFoldConfig reads the previous snapshot from it and
		// runRestore uploads to it.
		BaselineS3: req.BaselineS3,
		// The console resolves the effective value at request time; this only
		// executes it. Resolving again here would let a toggle saved while a
		// restore sits in the queue change what the operator asked for.
		CarryForwardUnchanged: req.CarryForwardUnchanged,
	}
}

// wireBaselineExtras attaches the run history, the restore capability and
// the sql-export capability to a freshly built supervisor, and exposes them
// on the console Config. Called by
// both watch entry paths; a no-op without a supervisor (serve-only consoles
// keep their 403s).
//
// Restore rides EITHER opt-in deliberately, which looks like the derivation
// the BaselineRestorer comment warns about — it is not.
// derived flag turning on a feature of a DIFFERENT class (a dump locks and
// reads the source; mydumper may not exist). A restore is the same fold the
// refresh already performs, into the same store, with no source contact,
// reading its previous snapshot from the same place and sending the result
// to the same destination; the only delta is who picks the instant. A daemon
// that opted into either baseline producer has already accepted exactly this
// work. The sql export rides the same reasoning: the same fold, into staging
// instead of the store, handed out only through the query:execute download.
// An unreadable history file runs without history rather than refusing the
// daemon — durations degrade to file-timestamp spans, and not opening a store
// means nothing overwrites the file it might describe.
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
