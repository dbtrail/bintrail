package consoleapp

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/dbtrail/dbtrail/internal/console"
)

// baselineJobKind names one of the four jobs baselineSupervisor runs in a
// goroutine, and with it the status slot that job publishes to. The string
// value is the log prefix each of those jobs already uses.
type baselineJobKind string

const (
	baselineJobDump    baselineJobKind = "baseline"
	baselineJobRefresh baselineJobKind = "baseline refresh"
	baselineJobRestore baselineJobKind = "baseline restore"
	baselineJobExport  baselineJobKind = "sql export"
)

// statusSlotLocked returns the status map a job kind publishes to. Callers
// must hold s.mu. An unknown kind returns nil, which the one caller handles;
// a nil map also reads as "no entry", so nothing here can panic on it.
func (s *baselineSupervisor) statusSlotLocked(kind baselineJobKind) map[string]*console.BaselineStatus {
	switch kind {
	case baselineJobDump:
		return s.jobs
	case baselineJobRefresh:
		return s.refreshes
	case baselineJobRestore:
		return s.restores
	case baselineJobExport:
		return s.exports
	}
	return nil
}

// recoverBaselineJob is the panic guard every baselineSupervisor job goroutine
// defers as its first statement. Deferred as a method value
// (`defer s.recoverBaselineJob(...)`), so the recover() below is called
// directly by the deferred function and does catch the panic.
//
// Under `bintrail-console watch` this process is ALSO the capture plane, so an
// unrecovered panic in a background baseline job stops replication capture. A
// baseline that stopped refreshing is a degradation, a daemon that stopped
// capturing is an outage, and the first must never cause the second
// (docs/dump-and-baseline.md states that as a guarantee). The reachable
// surface is not small: these jobs fold through reconstruct and DuckDB over
// Parquet whose schema came from a customer's CREATE TABLE. Mirrors
// verifySupervisor's guard, which is the same hazard on the neighbouring
// supervisor.
//
// Swallowing the panic quietly would trade a loud outage for a silent
// degradation, which is worse. So two things happen here, and BOTH are
// load-bearing:
//
//   - The panic is logged at error level WITH the stack trace, which is the
//     only place the panic site is ever recorded now that the process no
//     longer dies printing it.
//   - The job's own status slot is moved to "failed", the same terminal state
//     its ordinary error path writes. The four slots share ONE per-server
//     single-flight (busyLocked reads State == "running" across all of them),
//     so a guard that logged and left the slot "running" would permanently
//     refuse this server's refresh, dump, restore AND sql export. That cure
//     would be worse than the disease.
//
// It rewrites the slot ONLY while it still reads "running". Every one of the
// four jobs publishes its success inside the same locked region it then logs
// from, so a panic in that tail (a nil deref in a log argument, say) would
// otherwise have this overwrite a genuinely succeeded run with "failed" and
// zero its table counts while the snapshot it published sits on disk. That is
// a false report about durable data. Wedge-safety is untouched by the
// condition: a terminal state already frees the single-flight.
//
// It deliberately does NOT append a run to the history (the Backups page's
// durations ledger). recordRun runs BEFORE the status write in the dump,
// refresh and restore jobs, so the guard cannot tell whether the panicking run
// already has a row, and recording unconditionally would double-count it;
// beyond that, history.Append is itself inside the guarded region, so
// re-entering it from here could panic a second time, which the guard could
// not catch. The consequence to know: a panicked run leaves no row on the
// Backups page's run list. The server's status card shows failed with the
// panic value, and the daemon log carries the stack.
func (s *baselineSupervisor) recoverBaselineJob(kind baselineJobKind, serverID, serverName string) {
	r := recover()
	if r == nil {
		return
	}
	// debug.Stack() here still walks the panicking frames: the deferred
	// function runs on top of them, so the stack names where the panic came
	// from and not just this guard.
	slog.Error(string(kind)+": the job hit an internal error and stopped. Capture and the console keep "+
		"running, and this server's other backup jobs stay available. Please report this with the "+
		"stack recorded here.",
		"server", serverName, "id", serverID, "panic", r, "stack", string(debug.Stack()))

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.statusSlotLocked(kind)[serverID]
	if st == nil || st.State != "running" {
		return
	}
	st.State = "failed"
	st.LastError = fmt.Sprintf("internal error: %v", r)
	st.FinishedAt = nowStamp()
	// Nothing was published, so report nothing: a partial count reads as
	// progress to the status API ({state:"failed", rows:12000} looks
	// half-done). Same reasoning the sql export's ordinary failure path
	// spells out. Since and At are kept — they identify the run.
	st.Tables, st.Refused, st.Carried, st.Uploaded = 0, 0, 0, 0
	st.Rows, st.Bytes = 0, 0
}
