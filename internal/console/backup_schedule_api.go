package console

import (
	"encoding/json"
	"net/http"
	"time"
)

// backupScheduleDTO is a server's backup schedule and what it last did, on
// the wire (GET /api/baselines → schedule, and the PUT/DELETE responses).
type backupScheduleDTO struct {
	Every  string `json:"every"`
	At     string `json:"at"`
	Method string `json:"method"`
	// NextRun is the next slot on the grid (RFC3339 UTC). Present even when
	// the schedule is not runnable, so the page can say "would run at X but
	// cannot, because Y".
	NextRun string `json:"next_run,omitempty"`
	// Runnable reports whether THIS daemon, as configured right now, will run
	// this schedule; Reason says why not.
	Runnable bool   `json:"runnable"`
	Reason   string `json:"reason,omitempty"`
	// Running: a job this schedule started is in flight.
	Running bool `json:"running,omitempty"`
	// LastRun is the newest scheduled run that started (succeeded or
	// failed), LastSkipped the newest slot that could not start. From the
	// persisted history, so both survive a restart.
	LastRun     *backupScheduleRunDTO  `json:"last_run,omitempty"`
	LastSkipped *backupScheduleSkipDTO `json:"last_skipped,omitempty"`
}

type backupScheduleRunDTO struct {
	Method     string `json:"method"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at"`
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	// SnapshotTime names the backup the run published, when it did.
	SnapshotTime string `json:"snapshot_time,omitempty"`
	Tables       int    `json:"tables,omitempty"`
	Rows         int64  `json:"rows,omitempty"`
	Uploaded     int    `json:"uploaded,omitempty"`
	Carried      int    `json:"carried,omitempty"`
	Refused      int    `json:"refused,omitempty"`
}

type backupScheduleSkipDTO struct {
	At     string `json:"at"`
	Reason string `json:"reason"`
}

// backupScheduleRequest is the PUT body.
type backupScheduleRequest struct {
	Every  string `json:"every"`
	At     string `json:"at"`
	Method string `json:"method"`
}

// scheduleGates resolves what this process can do, for CheckBackupSchedule.
func (s *Server) scheduleGates() BackupScheduleGates {
	return BackupScheduleGates{
		LoopRunning:     s.backupSchedules != nil,
		FullBackups:     s.backupSchedules != nil && s.backupSchedules.FullBackups(),
		ReadOnlyConsole: s.monitorCtrl == nil,
	}
}

// backupScheduleDTO renders e's schedule. Requires e.BackupSchedule != nil.
func (s *Server) backupScheduleDTO(e ServerEntry, now time.Time) *backupScheduleDTO {
	sched := *e.BackupSchedule
	dto := &backupScheduleDTO{Every: sched.Every, At: sched.At, Method: sched.Method}
	if dto.At == "" {
		dto.At = "00:00"
	}
	if dto.Method == "" {
		dto.Method = BackupMethodFull
	}
	if p, err := sched.Parse(); err == nil {
		dto.NextRun = p.NextRun(now).Format(time.RFC3339)
	}
	if err := CheckBackupSchedule(e, sched, s.scheduleGates()); err != nil {
		dto.Reason = RefusalReason(err)
	} else {
		dto.Runnable = true
	}
	if s.backupSchedules != nil {
		dto.Running = s.backupSchedules.ScheduleState(e.ID).Running
	}
	if s.baselineHistory != nil {
		run, skip := s.baselineHistory.LastScheduled(e.ID)
		if run != nil {
			dto.LastRun = &backupScheduleRunDTO{
				Method:       runMethod(run.Kind),
				StartedAt:    run.StartedAt,
				FinishedAt:   run.FinishedAt,
				OK:           run.Error == "",
				Error:        run.Error,
				SnapshotTime: run.SnapshotTime,
				Tables:       run.Tables,
				Rows:         run.Rows,
				Uploaded:     run.Uploaded,
				Carried:      run.Carried,
				Refused:      run.Refused,
			}
		}
		if skip != nil {
			dto.LastSkipped = &backupScheduleSkipDTO{At: skip.FinishedAt, Reason: skip.SkipReason}
		}
	}
	return dto
}

// runMethod maps a history record's Kind back to the schedule vocabulary.
func runMethod(kind string) string {
	if kind == BaselineRunRefresh {
		return BackupMethodRefresh
	}
	return BackupMethodFull
}

// handleBackupScheduleUpdate serves PUT /api/servers/{id}/backup-schedule:
// validate, check the schedule can run on this daemon, persist. A schedule
// that could never run is refused with the reason rather than saved: saving
// it would put a timer on the page that nothing honours.
func (s *Server) handleBackupScheduleUpdate(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireScheduleEntry(w, r)
	if !ok {
		return
	}
	var req backupScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	sched, err := BackupSchedule{Every: req.Every, At: req.At, Method: req.Method}.Normalized()
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := CheckBackupSchedule(e, sched, s.scheduleGates()); err != nil {
		writeJSONError(w, http.StatusBadRequest, RefusalReason(err))
		return
	}
	if e.BackupSchedule != nil {
		sched.Extra = e.BackupSchedule.Extra
	}
	e.BackupSchedule = &sched
	if err := s.cm.reg.Update(e); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": s.backupScheduleDTO(e, time.Now().UTC())})
}

// handleBackupScheduleDelete serves DELETE /api/servers/{id}/backup-schedule.
// Removing a schedule that is not there is not an error.
func (s *Server) handleBackupScheduleDelete(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireScheduleEntry(w, r)
	if !ok {
		return
	}
	if e.BackupSchedule != nil {
		e.BackupSchedule = nil
		if err := s.cm.reg.Update(e); err != nil {
			writeJSONError(w, registryErrStatus(err), err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": nil})
}

// requireScheduleEntry is requireMonitorEntry plus the loop gate: on a
// process that runs no schedule loop the write is refused up front, with the
// same words the listing would report a saved schedule with.
func (s *Server) requireScheduleEntry(w http.ResponseWriter, r *http.Request) (ServerEntry, bool) {
	if s.backupSchedules == nil {
		if s.monitorCtrl == nil {
			writeJSONError(w, http.StatusForbidden,
				"scheduled backups run in the watch daemon (bintrail-console watch), not the read-only console")
		} else {
			writeJSONError(w, http.StatusForbidden,
				"backup features are turned off on this daemon (BINTRAIL_CONSOLE_BASELINE_TRIGGER=0 and no --baseline-refresh-interval), so nothing can run a schedule")
		}
		return ServerEntry{}, false
	}
	return s.requireMonitorEntry(w, r.PathValue("id"))
}
