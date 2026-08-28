package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// refreshRequest is one server's periodic baseline refresh.
type refreshRequest struct {
	ServerID    string
	ServerName  string
	IndexDSN    string
	BaselineDir string
	// CarryForwardUnchanged is the EFFECTIVE setting for this cycle: a console
	// override if one is saved, else the daemon's own flag. Resolved per cycle
	// rather than at boot so a change made in the settings panel takes effect
	// on the next tick, the same way a rotation override does.
	CarryForwardUnchanged bool
}

// TriggerRefresh starts a periodic baseline refresh for a server, sharing the
// supervisor's single-flight with the manual dump path.
//
// The shared lock is the point, not an implementation detail: a refresh folds
// the newest snapshot forward while a dump writes a new one, and letting them
// overlap on the same server would have the refresh anchored on a snapshot that
// is being written underneath it. ErrBaselineRunning here means "something else
// is already producing this server's baseline" — the loop skips this tick and
// tries again at the next one, which is exactly right for a periodic job.
func (s *baselineSupervisor) TriggerRefresh(req refreshRequest, interval time.Duration) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	at := time.Now().UTC()
	s.refreshes[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp(),
		At: at.Format(time.RFC3339)}
	s.mu.Unlock()

	slog.Info("baseline refresh: starting", "server", req.ServerName, "id", req.ServerID)
	go s.runRefresh(req, at, interval)
	return nil
}

// RefreshStatus reports the last periodic refresh for a server.
func (s *baselineSupervisor) RefreshStatus(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.refreshes[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

// busyLocked reports whether any of the four baseline job kinds (dump,
// refresh, restore, sql export) is in flight for a server. Callers must
// hold s.mu.
func (s *baselineSupervisor) busyLocked(serverID string) bool {
	if st, ok := s.jobs[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.refreshes[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.restores[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.exports[serverID]; ok && st.State == "running" {
		return true
	}
	return false
}

func (s *baselineSupervisor) runRefresh(req refreshRequest, at time.Time, interval time.Duration) {
	// The cycle's own recover in runBaselineRefreshCycle cannot reach here: it
	// sits on the near side of the `go` in TriggerRefresh, so it guards the
	// dispatch and not the fold. See recoverBaselineJob.
	defer s.recoverBaselineJob(baselineJobRefresh, req.ServerID, req.ServerName)
	started := time.Now().UTC()
	// Separate capture for the ELAPSED time, and the duplication is not
	// redundant: t.UTC() strips the monotonic reading, so time.Since(started)
	// would subtract wall clocks. A daemon whose folds run for minutes to
	// hours is exactly where an NTP step lands mid-measurement, and it would
	// move the number this change exists to get right, in either direction.
	// started stays for the RFC3339 stamp, which wants the wall clock.
	elapsed := time.Now()
	tables, refused, carried, err := s.executeRefresh(req, at)
	// Measured HERE, on the far side of the `go` in TriggerRefresh, because
	// this is where the fold actually happens. Timing the dispatch loop
	// instead measures how long it takes to spawn a goroutine, which is
	// microseconds no matter what the refresh costs.
	took := time.Since(elapsed)
	s.recordRun(req.ServerID, req.ServerName, console.BaselineRunRecord{
		Kind: console.BaselineRunRefresh, StartedAt: started.Format(time.RFC3339),
		SnapshotTime: publishedSnapshotTime(at, err), Tables: tables, Refused: refused, Carried: carried,
	}, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.refreshes[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.refreshes[req.ServerID] = st
	}
	applyFoldStatus(st, tables, refused, carried, err)
	if err != nil {
		// Deliberately NO duration report here. reportRefreshDuration's advice
		// is "raise the interval, or refresh fewer tables", which is the wrong
		// remediation for a run that published nothing: a capture gap, a
		// schema change or a shutdown mid-fold are not fixed by scheduling.
		// The fan-out runs every table to completion before it reports a
		// refusal, so a refusal costs about what a success costs and WOULD
		// trip the overrun threshold, printing tuning advice above the actual
		// cause.
		// Warn, never Error: a refusal is the fail-closed contract working, and
		// the next tick retries. Nothing about the daemon is unhealthy.
		slog.Warn("baseline refresh: published nothing", "server", req.ServerName, "id", req.ServerID,
			"refused", refused, "error", err)
		return
	}
	slog.Info("baseline refresh: published", "server", req.ServerName, "id", req.ServerID,
		"tables", tables, "reused", carried)
	reportRefreshDuration(req.ServerName, interval, took)
}

// applyFoldStatus writes a finished fold's outcome onto the status the console
// polls. Shared by the refresh and the restore, which had byte-identical copies
// of it.
//
// Split out for the reason the rest of this file keeps splitting things out:
// both callers sit behind a `go` and a live fold, so nothing at the unit tier
// could reach them, and dropping the reused count from either copy compiled and
// passed the whole suite. It is also the deduplication: two copies of a
// four-field assignment is exactly how one of them silently loses a field.
func applyFoldStatus(st *console.BaselineStatus, tables, refused, carried int, err error) {
	st.FinishedAt = nowStamp()
	st.Tables = tables
	st.Refused = refused
	st.Carried = carried
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		return
	}
	st.State = "succeeded"
	st.LastError = ""
}

// executeRefresh folds the newest snapshot forward. Returns the table count and
// the number of tables that refused.
//
// RESOURCE POSTURE — read before changing. Every DuckDB budget here is left at
// its zero value on purpose, which resolves to duckdbutil.DefaultTuning (2
// threads / 4 GB) and the container-safe archive fetcher. This is a long-lived
// daemon that is also streaming replication and serving a console; --ultrafast
// exists for offline commands that own the machine (#510), and letting a
// background refresh self-tune to ~80% of host RAM would starve the capture path
// it depends on. If this ever needs to go faster, it needs its own bounded knob,
// not the offline one.
//
// The zero value is NOT uniformly the safe choice on that struct, which is the
// trap this note used to leave open. It is safe for the DuckDB budgets and the
// archive fetcher, where zero resolves to the container-safe default. It is the
// OPPOSITE for Parallelism (zero means runtime.NumCPU()) and for
// WarnEventThreshold (zero means the volume warning never fires). Those two are
// therefore set explicitly in refreshFoldConfig; see the constants above it.
func (s *baselineSupervisor) executeRefresh(req refreshRequest, at time.Time) (tables, refused, carried int, err error) {
	tableList, err := reconstruct.NewestSnapshotTables(s.ctx, req.BaselineDir)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list the snapshot to refresh: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, 0, fmt.Errorf("no baseline snapshot to refresh under %s", req.BaselineDir)
	}
	return s.foldSnapshot(req, at, tableList)
}

// The bounded knobs EVERY in-daemon fold shares: the periodic refresh, the
// point-in-time restore, and the SQL export build. All three fold inside the
// process that is also capturing, so they get one posture rather than three
// opinions. Both are spelled out rather than left at zero because, unlike
// every other budget on FullTableConfig, their zero values mean the opposite
// of conservative.
const (
	// daemonFoldWarnEventThreshold is the same RAW value every CLI path ships
	// (internal/cli/reconstruct.go, cliapp/baseline_refresh.go, the hardcoded
	// one in internal/cli/drill.go, and the config init template in
	// cliapp/config.go). Zero DISABLES the warning outright: shouldWarnEvents is
	// `threshold > 0 && n > threshold`. Silence is backwards here, because the
	// operator who typed a command is watching its output and this job has
	// nobody reading it.
	//
	// Do NOT read "same raw value" as "warns at the same point per table". The
	// threshold reported is scaledEventThreshold(raw, effectiveParallelism),
	// so the per-table trigger here is 5M/2 = 2.5M against an attended run's
	// 5M/NumCPU. What the shared raw value actually equalizes is the TOTAL
	// concurrent event volume at which either warns, which is the quantity #842
	// scaling exists to hold steady and the one that tracks RAM. Note also that
	// effectiveParallelism clamps to len(Tables): a SINGLE-table refresh divides
	// by 1, so the full 5M applies there whatever this constant's sibling says.
	//
	// The bound below is what protects the process; this threshold is only what
	// tells someone it happened.
	daemonFoldWarnEventThreshold = 5_000_000

	// daemonFoldParallelism bounds how many tables fold concurrently. Zero means
	// runtime.NumCPU(), and peak resident memory is the SUM of the
	// concurrently-folding tables' change maps (the reason scaledEventThreshold
	// divides by parallelism at all, #842), each holding one entry per distinct
	// touched primary key (#1107). Inheriting the core count therefore ties this
	// daemon's peak memory to the size of the host it happens to run on, inside
	// the process that is also capturing. Two lets a slow table overlap with the
	// next one without letting the peak track the hardware; lower it before
	// raising it.
	//
	// It is not only a memory knob: fulltable.go sizes the index connection
	// pool as SetMaxOpenConns(2 * Parallelism), so moving this also moves the
	// fold's share of the index server's connections (4 here, against 2*NumCPU
	// before). Anyone tuning it is tuning both.
	daemonFoldParallelism = 2

	// daemonFoldRemediation replaces the volume warning's default advice, which
	// names --at, --parallelism and --warn-event-threshold. bintrail-console
	// registers none of the three: its only persistent flags are --log-level and
	// --log-format, and these folds' budgets are the constants above. Telling an
	// operator to lower a flag their binary does not have is worse than saying
	// nothing, so this names what they CAN actually reach.
	daemonFoldRemediation = "shorten the window this fold covers: for the scheduled refresh, " +
		"lower --baseline-refresh-interval so each fold starts from a fresher backup; " +
		"for a restore or a SQL export, pick a moment closer to an existing backup"
)

// refreshFoldConfig is the configuration one refresh cycle folds with.
//
// Split out of foldSnapshot so the settings it carries are checkable without
// standing up an index and a baseline: this is the last hop of the chain that
// starts at a console toggle, and it was the only one nothing could observe.
func refreshFoldConfig(req refreshRequest, at time.Time, tableList []string) reconstruct.FullTableConfig {
	return reconstruct.FullTableConfig{
		IndexDSN:              req.IndexDSN,
		BaselineSrc:           req.BaselineDir,
		Tables:                tableList,
		At:                    at,
		OutputDir:             req.BaselineDir,
		OutputFormat:          reconstruct.OutputFormatParquet,
		CarryForwardUnchanged: req.CarryForwardUnchanged,
		Parallelism:           daemonFoldParallelism,
		WarnEventThreshold:    daemonFoldWarnEventThreshold,
		RemediationHint:       daemonFoldRemediation,
		// AllowGaps stays FALSE. An unattended job must never publish a
		// knowingly-incomplete baseline: accepting a permanent capture loss is a
		// decision with consequences for every future reconstruct, and nobody is
		// watching this one to make it.
	}
}

// countCarried counts the tables a fold published by reusing the previous
// snapshot's file.
//
// A separate function for the same reason refreshFoldConfig is one: this is the
// last hop of the reuse feature and the only evidence it produced anything, and
// inside foldSnapshot nothing could reach it without standing up an index and a
// baseline. Returning len(reports) here, or 0, compiles and passes every test
// that does not call this directly.
func countCarried(reports []*reconstruct.TableReport) (carried int) {
	for _, rep := range reports {
		if rep != nil && rep.CarriedForward {
			carried++
		}
	}
	return carried
}

// foldSnapshot is the fold both the periodic refresh and the point-in-time
// restore share: reconstruct every table at `at` and publish the result as a
// new snapshot in the server's own baseline store, all-or-nothing.
//
// carried counts the tables published by reusing the previous snapshot's file.
// It is read out of the per-table reports rather than inferred from the
// setting, because asking for reuse is not getting it: a table with changes,
// with a capture gap, or on the S3 path is folded anyway.
func (s *baselineSupervisor) foldSnapshot(req refreshRequest, at time.Time, tableList []string) (tables, refused, carried int, err error) {
	reports, failures, runErr := foldTables(s.ctx, refreshFoldConfig(req, at, tableList))
	return foldOutcome(tableList, reports, failures, runErr)
}

// foldTables is reconstruct.ReconstructTablesDetailed behind a seam, shared by
// the refresh, the restore and the sql export — the three jobs whose work IS
// the fold.
//
// It exists because that call is otherwise the one thing in these job
// goroutines a unit test cannot reach: it needs a live index and a real
// baseline, and consoleapp has no fixture that stands those up (foldOutcome's
// doc states the same residual from the other side). Without the seam nothing
// below the `go` in each Trigger is drivable, which is exactly the gap #1472
// was: the panic guard on these goroutines would have had no test that reaches
// past the dispatch.
//
// Written by tests only, like checkMydumperPrivileges. Production never
// reassigns it, so the job goroutines only ever read it. A test that replaces
// it must not restore it until the job it started has reached a terminal
// state: the jobs run in their own goroutines, and restoring while one is
// still folding is a data race on this variable.
var foldTables = reconstruct.ReconstructTablesDetailed

// foldOutcome is everything foldSnapshot decides once the fold has run, split
// out because the fold itself needs a live index and a real baseline and this
// does not.
//
// Left inline, zeroing the carried count compiled and passed the entire suite:
// the only path that reads it runs against real MySQL, so the number the
// console reports had no unit-tier guard at all. That is the same shape as
// refreshFoldConfig one function up.
//
// carried is reported even when runErr is set, and that is deliberate rather
// than an oversight. Publication is all-or-nothing, so a failed run published
// nothing at all; the count still describes the work the fold did, and the UI
// renders it only under a succeeded state. ReconstructTablesDetailed routes a
// failed table into failures and never into reports, so this can never count a
// table that did not actually reuse its file.
//
// Residual, stated rather than papered over: the one-line delegation in
// foldSnapshot is still only reachable with a live index and a real baseline,
// and consoleapp has no fixture that stands those up. A mutation that bypasses
// this function survives the unit tier. What the split buys is that the
// unguarded surface is now a single call rather than the whole decision, and
// the equivalent end-to-end behaviour is pinned at the integration tier by
// internal/reconstruct's TestReconstructParquet_doesNotCarryForwardUnlessAsked.
func foldOutcome(tableList []string, reports []*reconstruct.TableReport,
	failures []reconstruct.TableFailure, runErr error) (tables, refused, carried int, err error) {
	carried = countCarried(reports)
	if runErr != nil {
		return len(tableList), len(failures), carried, runErr
	}
	return len(tableList), 0, carried, nil
}

// startBaselineRefreshLoop launches the opt-in periodic baseline refresh
// (#1171). intervalRaw empty = disabled, which is the default.
//
// Isolation matches the rotation and prune loops: it runs in its own goroutine,
// recovers from a panic, and logs failures without touching the stream or the
// supervisor. A baseline that stopped refreshing is a degradation; a daemon that
// stopped capturing is an outage, and the first must never cause the second.
func startBaselineRefreshLoop(ctx context.Context, reg *console.Registry, sup *baselineSupervisor,
	globalDSN, globalBaselineDir, intervalRaw string, carryDefault bool) error {
	if intervalRaw == "" {
		return nil
	}
	interval, err := cliutil.ParseInterval(intervalRaw)
	if err != nil {
		return fmt.Errorf("--baseline-refresh-interval: %w", err)
	}
	if interval <= 0 {
		return fmt.Errorf("--baseline-refresh-interval must be positive, got %q", intervalRaw)
	}
	if sup == nil {
		// Unreachable from watch.go, which builds the supervisor whenever this
		// interval is set. Kept so a future caller that forgets fails loudly
		// instead of running a console with a flag that is silently inert.
		return fmt.Errorf("internal: --baseline-refresh-interval was set without a baseline supervisor")
	}
	targets := baselineRefreshTargets(registryEntries(reg), globalDSN, globalBaselineDir)
	// Name the effective reuse setting AND where it came from, once, at the one
	// moment an operator is reading the log to see whether their configuration
	// took. A console override beats the command line silently by design, so
	// without this line the only symptom of a stale saved toggle is work that
	// keeps happening, or stops happening, for no stated reason.
	carryOn, carrySource := carryForwardProvenance(reg, carryDefault)
	slog.Info("baseline refresh loop enabled", "interval", interval, "servers", len(targets),
		"reuse_unchanged", carryOn, "reuse_set_by", carrySource)
	if len(targets) == 0 {
		// WARN, not a refusal — and the distinction is load-bearing. Every tick
		// recomputes the target set, so "nothing to refresh" is a state a daemon
		// legitimately starts in and grows out of: a source-less `watch` lists no
		// servers at all until they are added FROM THE CONSOLE, and per-server
		// baseline directories live in the registry, not on the command line.
		// Refusing here would mean a compose file carrying the interval could not
		// boot a fresh install — the operator would have to add a server through a
		// console that is not running. The visibility this warning gives is what
		// the refusal was actually for.
		slog.Warn("baseline refresh: no server is refreshable yet, so nothing will run until one has BOTH an " +
			"index DSN and a LOCAL baseline directory (a refresh reads the previous snapshot and writes the new " +
			"one on disk; an S3-only baseline destination cannot be refreshed in place). Servers added later are " +
			"picked up automatically.")
	}
	// RETENTION INTERPLAY (#616), stated at startup on purpose. A refreshed
	// snapshot is written locally and is NOT uploaded, and baseline.PruneLocal
	// only reclaims a snapshot whose _SUCCESS marker it can confirm in S3 — so
	// nothing this loop publishes is prunable, with or without an S3 destination
	// configured. Unattended that is one full-table snapshot per interval,
	// forever. An operator who discovers this from a full disk discovers it far
	// too late, and the loop has no business quietly deciding to upload on their
	// behalf.
	slog.Warn("baseline refresh: snapshots from this loop are written locally and never uploaded, so retention "+
		"cannot reclaim them (a prune needs a confirmed S3 copy of the snapshot). Upload and prune on your own "+
		"schedule, or size the disk for one full-table snapshot per server per interval, at the rate below.",
		diskArgs(interval, targets)...)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refreshTick(ctx, reg, sup, globalDSN, globalBaselineDir, interval, carryDefault)
			}
		}
	}()
	return nil
}

// reportRefreshDuration states how long ONE server's refresh took, and says so
// when it took longer than the interval that was asked for.
//
// This is per refresh rather than per tick, and that is the whole correction:
// a tick only DISPATCHES. TriggerRefresh ends in `go s.runRefresh(...)` and
// returns, so anything timed around the dispatch loop measures goroutine
// launch, is always microseconds, and can never exceed any interval
// ParseInterval accepts. An overrun warning built on that span is unreachable
// code, and the reassuring line beside it is false.
//
// Fires once per PUBLISHED refresh. Not per completed one: a run that refused
// costs about what a success costs, because the fan-out runs every table to
// completion before it reports the refusal, so reporting an overrun for it
// would put "raise the interval" above the capture gap that actually stopped
// it. And not per tick: a tick only dispatches.
//
// The duration is also the honest measure of what a full rewrite costs on real
// data: a refresh rewrites every table that CHANGED in full, however little of
// it
// changed. An estimate of that is a guess about someone else's data; this is
// theirs.
func reportRefreshDuration(server string, interval, took time.Duration) {
	if interval <= 0 || took <= interval {
		slog.Debug("baseline refresh finished", "server", server, "took", took, "interval", interval)
		return
	}
	slog.Warn("baseline refresh: this server's refresh took longer than the configured interval, so it cannot "+
		"run as often as requested. A refresh rewrites every table that changed in full, however little of it "+
		"changed (a table with no events at all is carried forward instead), "+
		"so this is the cost of the rewrite, not of the schedule. Raise the interval to match, or refresh "+
		"fewer tables.",
		"server", server, "took", took, "interval", interval)
}

// diskArgs builds the disk warning's attributes, omitting the projection when
// it is not meaningful rather than logging a misleading zero.
func diskArgs(interval time.Duration, targets []refreshRequest) []any {
	args := []any{"interval", interval}
	if n := snapshotsPer30Days(interval); n > 0 {
		args = append(args, "full_table_snapshots_per_server_per_30d", n)
	}
	return append(args, "dirs", refreshTargetDirs(targets))
}

// snapshotsPer30Days projects how many full-table snapshots the configured
// interval produces over a month, for the startup warning.
//
// The warning has said "one snapshot per interval, forever" since the interval
// floor was an hour, where the reader could do the arithmetic and the answer
// was 24 a day. Minutes make that a much worse number and a much easier one to
// skip over: "every 5m" and "8,640 a month per server, none of them
// reclaimable" are the same fact and land differently.
//
// Per SERVER, and the attribute name says so: a tick triggers one refresh for
// every eligible server, so a deployment monitoring several multiplies this.
// Named rather than multiplied because the target set is recomputed every
// tick, so a count fixed at startup would go stale.
//
// Thirty days rather than one, because the warning is about DISK and disk
// fills over weeks. A per-DAY projection also divides to zero for any interval
// longer than a day, so a --baseline-refresh-interval of 7d would have
// reported "0 per day", which reads as "none" and is the opposite of the
// truth.
//
// Returns 0 when the projection is not meaningful (a non-positive interval, or
// one longer than the horizon itself). The caller omits the figure entirely
// rather than print a zero, since the interval it logs alongside already tells
// that story. Reported as a count rather than bytes because a snapshot's size
// depends on the tables, which this loop does not know at startup.
func snapshotsPer30Days(interval time.Duration) int64 {
	if interval <= 0 {
		return 0
	}
	return int64(30 * 24 * time.Hour / interval)
}

// refreshTick is one tick: run a cycle, then report what it did.
//
// Extracted from the ticker's anonymous func on purpose. It is the only place
// the two counters are bound and forwarded, and inside the closure no test
// could reach it: swapping dispatched and skipped compiled and passed, because
// both are ints and every test drove runBaselineRefreshCycle and reportDispatch
// separately. That is the same shape this file already carries a correction
// for one function over, so it gets a seam rather than a comment.
func refreshTick(ctx context.Context, reg *console.Registry, sup *baselineSupervisor,
	globalDSN, globalBaselineDir string, interval time.Duration, carryDefault bool) {
	dispatched, skipped, carry := runBaselineRefreshCycle(ctx, reg, sup, globalDSN, globalBaselineDir, interval, carryDefault)
	// carry comes back from the cycle rather than being resolved again here: a
	// PUT landing between the two reads would make the logged value disagree
	// with what was actually dispatched, which is the one thing this log exists
	// to settle.
	reportDispatch(interval, dispatched, skipped, carry)
}

// refreshTargetsFor builds this cycle's requests with the effective settings
// already applied.
//
// The resolution lives next to the target construction rather than inside the
// cycle's loop body so that "what a request carries" is reachable without
// running a refresh: the loop body is otherwise only observable by letting a
// fold start.
func refreshTargetsFor(reg *console.Registry, globalDSN, globalBaselineDir string, carryDefault bool) []refreshRequest {
	return refreshTargetsWith(reg, globalDSN, globalBaselineDir, effectiveCarryForward(reg, carryDefault))
}

// refreshTargetsWith is the same thing with the setting ALREADY resolved, so
// one cycle resolves it once and logs exactly the value it dispatched with.
func refreshTargetsWith(reg *console.Registry, globalDSN, globalBaselineDir string, carry bool) []refreshRequest {
	reqs := baselineRefreshTargets(registryEntries(reg), globalDSN, globalBaselineDir)
	for i := range reqs {
		reqs[i].CarryForwardUnchanged = carry
	}
	return reqs
}

// effectiveCarryForward resolves what this cycle should do: a console-saved
// override wins over the daemon's own flag.
//
// Read per cycle, not cached at boot. A console override is meant to apply to a
// loop that is already running, which is the same contract the rotation panel
// has, and caching would make the panel look inert until a restart.
//
// A registry that cannot be consulted falls back to the daemon flag rather than
// to false: the operator's explicit command line is a better answer than a
// silent no.
func effectiveCarryForward(reg *console.Registry, daemonDefault bool) bool {
	on, _ := carryForwardProvenance(reg, daemonDefault)
	return on
}

// carryForwardProvenance resolves the same value and also names WHERE it came
// from, which is the half that has to be logged.
//
// The two sources disagree silently by design: a saved override of false beats
// a command line saying true, and that is the point of the tri-state. It also
// means an operator can pass the flag, watch every table get rewritten, and
// have nothing anywhere tell them a console toggle from months ago is the
// reason. The provenance string exists so one log line can.
func carryForwardProvenance(reg *console.Registry, daemonDefault bool) (on bool, source string) {
	if reg == nil {
		return daemonDefault, "daemon flag or environment"
	}
	if bc, ok := reg.BaselineRefresh(); ok {
		return bc.CarryForwardUnchanged, "console setting, which overrides the daemon flag"
	}
	return daemonDefault, "daemon flag or environment"
}

// runBaselineRefreshCycle triggers one refresh per eligible server.
//
// Deliberately NOT run once at startup, unlike the prune loop: a refresh is a
// full-table fold over every table, and doing that in the same seconds a daemon
// is establishing replication and opening its console would make every restart
// the most expensive moment in the process's life.
func runBaselineRefreshCycle(ctx context.Context, reg *console.Registry, sup *baselineSupervisor,
	globalDSN, globalBaselineDir string, interval time.Duration, carryDefault bool) (dispatched, skipped int, carry bool) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("baseline refresh cycle panicked; refreshes continue next tick", "panic", r)
		}
	}()
	carry = effectiveCarryForward(reg, carryDefault)
	if ctx.Err() != nil {
		return dispatched, skipped, carry
	}
	for _, req := range refreshTargetsWith(reg, globalDSN, globalBaselineDir, carry) {
		switch err := sup.TriggerRefresh(req, interval); {
		case err == nil:
			dispatched++
		case errors.Is(err, console.ErrBaselineRunning):
			// Expected: a refresh still folding, or a manual dump in flight.
			// Counted rather than only logged, because at a short interval this
			// stops being an edge case and becomes the steady state — it is the
			// evidence that the interval is shorter than a refresh takes, and
			// the caller needs the number to say so.
			//
			// The per-server line stays at Debug ALONGSIDE the count. Counting
			// alone traded "invisible but specific" for "visible but
			// anonymous": on a multi-server deployment `skipped=2` cannot be
			// acted on, because nothing else names which two, and the
			// "starting" line only fires for servers that were NOT skipped.
			skipped++
			slog.Debug("baseline refresh skipped this tick", "server", req.ServerName, "reason", err)
		default:
			// Nothing else is expected today. If that changes, it must not
			// become invisible: this used to swallow every error at Debug,
			// below the console binary's default level.
			slog.Warn("baseline refresh: could not start", "server", req.ServerName, "error", err)
		}
	}
	return dispatched, skipped, carry
}

// reportDispatch reports what a tick actually did, which is dispatch and
// nothing more.
//
// Quiet at Debug while every server started, because a healthy loop at a short
// interval would otherwise emit a line a minute forever. Visible as soon as
// anything was skipped, because a skip is the ONLY loop-level evidence that
// refreshes are not keeping up, and it was previously logged at Debug where the
// default level hides it. reportRefreshDuration carries the matching per-server
// detail when the slow refresh eventually lands.
func reportDispatch(interval time.Duration, dispatched, skipped int, carry bool) {
	if skipped == 0 {
		slog.Debug("baseline refresh: dispatched", "servers", dispatched, "interval", interval,
			"reuse_unchanged", carry)
		return
	}
	slog.Info("baseline refresh: a server was still busy with another baseline job, so this tick did not start "+
		"a refresh for it. That job is a refresh still folding, a manual dump, a restore or a SQL export: they "+
		"share one lock per server. If it is the previous refresh, the interval is shorter than a refresh "+
		"takes, and that refresh logs its own duration when it lands.",
		"dispatched", dispatched, "skipped", skipped, "interval", interval, "reuse_unchanged", carry)
}

// refreshTargetDirs lists the directories that will grow, for the retention
// warning. Naming globalBaselineDir instead would print "" whenever the
// refreshable servers came from the registry — a disk-growth warning that names
// no directory is the wrong half of the message.
func refreshTargetDirs(targets []refreshRequest) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range targets {
		if seen[t.BaselineDir] {
			continue
		}
		seen[t.BaselineDir] = true
		out = append(out, t.BaselineDir)
	}
	return out
}

func registryEntries(reg *console.Registry) []console.ServerEntry {
	if reg == nil {
		return nil
	}
	return reg.List()
}

// baselineRefreshTargets collects the servers a refresh can run for: an index
// DSN to fold from and a LOCAL baseline directory to fold into.
//
// A server with only an S3 baseline destination is skipped WITH a warning rather
// than silently: the refresh writes files, so an in-place S3 refresh is not
// something this loop can do, and an operator who configured S3-only baselines
// and set the interval would otherwise see nothing happen and no reason why.
func baselineRefreshTargets(entries []console.ServerEntry, globalDSN, globalBaselineDir string) []refreshRequest {
	var out []refreshRequest
	seen := map[string]bool{}
	add := func(id, name, dsn, dir string) {
		if dsn == "" || dir == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, refreshRequest{ServerID: id, ServerName: name, IndexDSN: dsn, BaselineDir: dir})
	}
	add("default", "boot", globalDSN, globalBaselineDir)
	for _, e := range entries {
		if e.DSN != "" && e.BaselineDir == "" && e.BaselineS3 != "" {
			slog.Warn("baseline refresh: server has an S3-only baseline destination and will not be refreshed "+
				"(a refresh reads and writes snapshot files on disk)", "server", e.Name)
			continue
		}
		add(e.ID, e.Name, e.DSN, e.BaselineDir)
	}
	return out
}
