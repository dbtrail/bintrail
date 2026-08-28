package consoleapp

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// Per-server backup schedule (#1442). The loop ticks once a minute, reads the
// registry every tick (a schedule saved from the page applies without a
// restart, the way a rotation override does), and starts a job when a
// server's schedule crosses a slot boundary. It is EDGE-triggered on the slot
// grid:
//
//   - a slot that passed while the daemon was down never fires: cron
//     semantics, and the only ones that keep a restart from turning into a
//     surprise full dump of production at 09:00 on a Monday;
//   - a schedule is observed from the moment it is SAVED (the API tells the
//     loop) or from boot (the loop seeds every schedule it finds), keyed by
//     the schedule's identity, so an add and an edit are both silent for
//     the slot already in progress and fire at the next one. That next one
//     is exactly the next_run the page showed when the operator saved, even
//     when it falls inside the minute before the first tick.
//
// Isolation matches the refresh and rotation loops: its own goroutine, a
// recover around each tick, and it never touches the stream. A schedule that
// stopped is a degradation; a daemon that stopped capturing is an outage.

// backupScheduleTick is how often the loop looks at the clock. A slot fires
// within this much of its instant.
const backupScheduleTick = time.Minute

// backupScheduler is the loop's state and the console's view of it.
type backupScheduler struct {
	sup *baselineSupervisor
	reg *console.Registry
	// fullBackups: the daemon's baseline-creation opt-in; a full-backup
	// schedule is skipped (and recorded as skipped) without it.
	fullBackups bool
	// carryDefault is the daemon's --baseline-carry-forward-unchanged, the
	// fallback when the console has not saved an override; a scheduled
	// rebuild honours the same setting the refresh loop and a restore do.
	carryDefault bool

	mu sync.Mutex
	// seen is the latest slot observed per server, together with the
	// identity of the schedule it was observed under. A different identity
	// is a first observation: that is what makes an edit silent.
	seen map[string]seenSlot
	// started is the last job this schedule started per server: which
	// supervisor slot to look at, the exact stamp the supervisor gave it (so
	// a later manual job in the same slot is never mistaken for ours), and,
	// once observed, its terminal status, kept so a later manual job taking
	// the slot does not erase the schedule's last outcome from the page.
	started map[string]scheduledStart
	// skipped is the last slot this schedule could not start per server.
	// The history has the durable copy; this one is what the page gets when
	// the history is unavailable.
	skipped map[string]scheduledSkip
	// fallback is the last slot per server where the rebuild was refused and
	// a full backup was taken instead, for the page.
	fallback map[string]scheduledFallback
	// warned holds the servers whose unreadable schedule was already reported,
	// so the log says it once rather than every minute.
	warned map[string]bool
}

type seenSlot struct {
	identity string
	slot     time.Time
}

type scheduledStart struct {
	method string
	// at is the loop's own stamp (RFC3339 UTC), reported as LastStartedAt.
	at string
	// since is the supervisor's Since for the job, read back right after
	// the trigger. Attribution compares on it exactly.
	since string
	// last is the job's status once it was observed in a terminal state,
	// nil until then.
	last *console.BaselineStatus
}

type scheduledSkip struct {
	at     string
	reason string
}

type scheduledFallback struct {
	at     string
	reason string
}

func newBackupScheduler(sup *baselineSupervisor, reg *console.Registry, fullBackups, carryDefault bool) *backupScheduler {
	return &backupScheduler{
		sup: sup, reg: reg, fullBackups: fullBackups, carryDefault: carryDefault,
		seen:     make(map[string]seenSlot),
		started:  make(map[string]scheduledStart),
		skipped:  make(map[string]scheduledSkip),
		fallback: make(map[string]scheduledFallback),
		warned:   make(map[string]bool),
	}
}

// newBackupScheduleReporter is the watch daemon's wiring in one place: nil
// supervisor (no baseline feature on this daemon) means no loop, and the
// interface it returns is then a true nil rather than a typed nil, which the
// console would otherwise take for a running loop and dereference. The
// second value is the same object for startBackupScheduleLoop.
func newBackupScheduleReporter(sup *baselineSupervisor, reg *console.Registry, fullBackups, carryDefault bool) (console.BackupScheduleReporter, *backupScheduler) {
	if sup == nil {
		return nil, nil
	}
	s := newBackupScheduler(sup, reg, fullBackups, carryDefault)
	return s, s
}

// FullBackups implements console.BackupScheduleReporter: the opt-in, and the
// supervisor's standing refusal (a lock-mode misconfiguration) when there
// is one.
func (b *backupScheduler) FullBackups() (bool, error) {
	return b.fullBackups, b.sup.configErr
}

// Observe implements console.BackupScheduleReporter: the API calls it when a
// schedule is saved, so the slot in progress at that instant is the one the
// loop treats as already seen. Without it the first TICK was the first
// observation, and a boundary between the save and that tick (up to a
// minute) was silently dropped while the page had just promised it as the
// next run. Cheap and lock-only; an unparseable schedule is ignored here and
// reported by the tick.
func (b *backupScheduler) Observe(serverID string, sched console.BackupSchedule, at time.Time) {
	p, err := sched.Parse()
	if err != nil {
		return
	}
	b.mu.Lock()
	b.seen[serverID] = seenSlot{identity: sched.Identity(), slot: p.SlotAtOrBefore(at.UTC())}
	b.mu.Unlock()
}

// observeAll seeds the observation of every schedule in the registry at one
// instant: boot. A boundary in the first minute of uptime then fires (the
// daemon was up for it), and one before boot does not.
func (b *backupScheduler) observeAll(at time.Time) {
	for _, e := range b.reg.List() {
		if e.BackupSchedule != nil {
			b.Observe(e.ID, *e.BackupSchedule, at)
		}
	}
}

// ScheduleState implements console.BackupScheduleReporter.
func (b *backupScheduler) ScheduleState(serverID string) console.BackupScheduleState {
	b.mu.Lock()
	st, started := b.started[serverID]
	sk, skipped := b.skipped[serverID]
	fb, fell := b.fallback[serverID]
	b.mu.Unlock()
	var out console.BackupScheduleState
	if skipped {
		out.LastSkippedAt, out.LastSkipReason = sk.at, sk.reason
	}
	if fell {
		out.LastFallbackAt, out.LastFallbackReason = fb.at, fb.reason
	}
	if !started {
		return out
	}
	out.LastStartedAt, out.LastMethod = st.at, st.method
	var cur console.BaselineStatus
	if st.method == console.BackupMethodRefresh {
		cur = b.sup.RefreshStatus(serverID)
	} else {
		cur = b.sup.Status(serverID)
	}
	switch {
	case cur.Since == st.since:
		// The slot is shared with manual jobs of the same kind. Only the job
		// whose Since is exactly the one read back at trigger time is ours.
		out.Last = &cur
		out.Running = cur.State == "running"
		if !out.Running {
			// Keep the outcome: a later manual job overwrites the slot, and
			// a job that panicked has no history record, so this copy is
			// the page's only evidence until the schedule's next run.
			b.mu.Lock()
			if cur2, ok := b.started[serverID]; ok && cur2.since == st.since {
				cur2.last = &cur
				b.started[serverID] = cur2
			}
			if st.method == console.BackupMethodRefresh && cur.State == "succeeded" {
				// The fallback line is an alarm about the rebuild path. A
				// later scheduled rebuild that went through is the evidence
				// the path works again, so the alarm ends here rather than
				// at the next restart.
				delete(b.fallback, serverID)
				out.LastFallbackAt, out.LastFallbackReason = "", ""
			}
			b.mu.Unlock()
		}
	case st.last != nil:
		out.Last = st.last
	}
	return out
}

// startBackupScheduleLoop launches the loop. sched nil = no baseline features
// on this daemon = no loop; the console then refuses the schedule endpoints.
func startBackupScheduleLoop(ctx context.Context, sched *backupScheduler) {
	if sched == nil {
		return
	}
	sched.observeAll(time.Now().UTC())
	slog.Info("backup schedule loop enabled", "tick", backupScheduleTick, "full_backups", sched.fullBackups)
	go func() {
		t := time.NewTicker(backupScheduleTick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				sched.tick(ctx, now.UTC())
			}
		}
	}()
}

// tick is one look at the clock: for every scheduled server, record the
// current slot and fire when it moved.
func (b *backupScheduler) tick(ctx context.Context, now time.Time) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("backup schedule tick panicked; schedules continue next tick", "panic", r)
		}
	}()
	if ctx.Err() != nil {
		return
	}
	entries := b.reg.List()
	live := make(map[string]bool, len(entries))
	for _, e := range entries {
		if e.BackupSchedule == nil {
			continue
		}
		live[e.ID] = true
		p, err := e.BackupSchedule.Parse()
		if err != nil {
			// The API never saves one of these, so this is a hand-edited
			// file. The listing reports it as not runnable; the log says it
			// once per server, at a level the default configuration shows.
			b.mu.Lock()
			first := !b.warned[e.ID]
			b.warned[e.ID] = true
			b.mu.Unlock()
			if first {
				slog.Warn("backup schedule: this server's schedule cannot be read and will not run until it is fixed",
					"server", e.Name, "error", err)
			}
			continue
		}
		b.mu.Lock()
		delete(b.warned, e.ID)
		b.mu.Unlock()
		if !b.crossed(e.ID, e.BackupSchedule.Identity(), p.SlotAtOrBefore(now)) {
			continue
		}
		b.fire(e, p, now)
	}
	// Forget servers whose schedule is gone: a schedule removed and later
	// re-added starts silent again rather than firing on a stale slot, and
	// its last outcome is not reported under a schedule that no longer
	// exists.
	b.mu.Lock()
	for id := range b.seen {
		if !live[id] {
			delete(b.seen, id)
		}
	}
	for id := range b.warned {
		if !live[id] {
			delete(b.warned, id)
		}
	}
	for id := range b.started {
		if !live[id] {
			delete(b.started, id)
		}
	}
	for id := range b.skipped {
		if !live[id] {
			delete(b.skipped, id)
		}
	}
	for id := range b.fallback {
		if !live[id] {
			delete(b.fallback, id)
		}
	}
	b.mu.Unlock()
}

// crossed records slot for the server under the schedule's identity and
// reports whether it moved past the previously observed one. The first
// observation of an identity records and reports false, so an add and an
// edit are both silent (the API's Observe normally gets there first).
func (b *backupScheduler) crossed(serverID, identity string, slot time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	prev, ok := b.seen[serverID]
	b.seen[serverID] = seenSlot{identity: identity, slot: slot}
	return ok && prev.identity == identity && slot.After(prev.slot)
}

// fire starts the scheduled job for e, or records why it could not. HOW is
// decided here per slot (console.ChooseBackupMethod): rebuild the newest
// backup from the recorded changes when that is the right producer, a full
// backup from the source otherwise. A rebuild the fold refuses (a capture
// gap, a schema change, nothing to rebuild from after all) falls back to a
// full backup at the same slot, because a fresh read of the source is
// exactly what heals those; the fallback is recorded so the page can say
// what happened and why.
func (b *backupScheduler) fire(e console.ServerEntry, p console.ParsedBackupSchedule, now time.Time) {
	gates := b.gates()
	if err := console.CheckBackupSchedule(e, *e.BackupSchedule, gates); err != nil {
		b.skip(e, now, console.RefusalReason(err))
		return
	}
	method, _, err := console.ChooseBackupMethod(b.sup.ctx, e, gates)
	if err != nil {
		b.skip(e, now, err.Error())
		return
	}
	stamp := now.Format(time.RFC3339)
	if method == console.BackupMethodRefresh {
		if err := b.startRebuild(e, p, stamp); err == nil {
			go b.fallBackIfRefused(e, stamp)
			return
		} else if !errors.Is(err, console.ErrBaselineRunning) {
			// Not the collision case: the rebuild could not even start. A
			// full backup would hit the same slot; report it as a skip.
			b.skip(e, now, err.Error())
			return
		} else {
			b.skip(e, now, "another backup job was running for this server at the scheduled time")
			return
		}
	}
	b.startFull(e, stamp, now)
}

// gates is what the checker needs to know about this daemon.
func (b *backupScheduler) gates() console.BackupScheduleGates {
	enabled, refusal := b.FullBackups()
	g := console.BackupScheduleGates{LoopRunning: true, FullBackups: enabled}
	if refusal != nil {
		g.FullBackupsErr = refusal.Error()
	}
	return g
}

// startRebuild triggers the fold and records the job as the schedule's.
func (b *backupScheduler) startRebuild(e console.ServerEntry, p console.ParsedBackupSchedule, stamp string) error {
	req := refreshRequest{
		ServerID: e.ID, ServerName: e.Name, IndexDSN: e.DSN, BaselineDir: e.BaselineDir,
		CarryForwardUnchanged: effectiveCarryForward(b.reg, b.carryDefault),
		Trigger:               console.BaselineRunTriggerScheduled,
	}
	// The interval is what the overrun warning measures against and names;
	// for a scheduled rebuild that is the schedule's own `every`, which is
	// where "raise the interval" is acted on.
	if err := b.sup.TriggerRefresh(req, p.Every); err != nil {
		return err
	}
	b.record(e, console.BackupMethodRefresh, stamp, b.sup.RefreshStatus(e.ID).Since)
	return nil
}

// startFull triggers a full backup and records the job as the schedule's, or
// records the skip.
func (b *backupScheduler) startFull(e console.ServerEntry, stamp string, now time.Time) {
	req := console.BaselineRequestFor(e)
	req.Trigger = console.BaselineRunTriggerScheduled
	switch err := b.sup.Trigger(req); {
	case err == nil:
		b.record(e, console.BackupMethodFull, stamp, b.sup.Status(e.ID).Since)
	case errors.Is(err, console.ErrBaselineRunning):
		// The collision the issue names: a manual backup, restore or export
		// (or the previous scheduled run) holds the server. Skip, do not
		// queue: a queued dump would fire at an unscheduled moment.
		b.skip(e, now, "another backup job was running for this server at the scheduled time")
	default:
		b.skip(e, now, err.Error())
	}
}

// record notes the job the schedule just started. The supervisor's own
// stamp is read back right after the trigger: it is the key ScheduleState
// attributes the slot by, and the trigger returned, so the slot is ours
// until the job finishes and something else claims it.
func (b *backupScheduler) record(e console.ServerEntry, method, stamp, since string) {
	b.mu.Lock()
	b.started[e.ID] = scheduledStart{method: method, at: stamp, since: since}
	b.mu.Unlock()
	slog.Info("backup schedule: started", "server", e.Name, "method", method, "every", e.BackupSchedule.Every)
}

// fallBackIfRefused waits for the rebuild started at stamp to finish and,
// if it failed, takes a full backup at the same slot. Any failure of the
// rebuild qualifies: its output is the same as a full backup's, and a full
// backup is what the operator scheduled in the first place. Nothing happens
// when the daemon is shutting down, when a full backup is not possible here
// (recorded as a skip with both reasons), or when another job took the
// server meanwhile (recorded as a skip).
func (b *backupScheduler) fallBackIfRefused(e console.ServerEntry, stamp string) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("backup schedule: fallback check panicked", "server", e.Name, "panic", r)
		}
	}()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		<-t.C
		// Checked on every tick rather than as a select arm beside it: with
		// both ready, select picks either, and a fold that failed BECAUSE
		// the daemon is shutting down is not a refusal. A full read of
		// production is not how to shut down. Nothing waits on this
		// goroutine, so leaving within a second of the cancel is enough.
		if b.sup.ctx.Err() != nil {
			return
		}
		st := b.ScheduleState(e.ID)
		if st.LastStartedAt != stamp || st.Last == nil {
			return // superseded, or the slot is no longer ours
		}
		if st.Running {
			continue
		}
		if st.Last.State != "failed" {
			return
		}
		reason := st.Last.LastError
		now := time.Now().UTC()
		if err := console.FullBackupPossible(e, b.gates()); err != nil {
			b.skip(e, now, "the rebuild was refused ("+reason+") and a full backup cannot start here: "+err.Error())
			return
		}
		slog.Warn("backup schedule: the rebuild was refused, taking a full backup instead",
			"server", e.Name, "reason", reason)
		b.mu.Lock()
		b.fallback[e.ID] = scheduledFallback{at: now.Format(time.RFC3339), reason: reason}
		b.mu.Unlock()
		b.startFull(e, now.Format(time.RFC3339), now)
		return
	}
}

// skip records a slot that did not start: in memory (the page's view when
// the history is unavailable), in the history (so it survives a restart)
// and in the log.
func (b *backupScheduler) skip(e console.ServerEntry, now time.Time, reason string) {
	stamp := now.Format(time.RFC3339)
	slog.Warn("backup schedule: scheduled backup did not start", "server", e.Name, "reason", reason)
	b.mu.Lock()
	b.skipped[e.ID] = scheduledSkip{at: stamp, reason: reason}
	b.mu.Unlock()
	if b.sup.history == nil {
		return
	}
	_, err := b.sup.history.AppendSkip(console.BaselineRunRecord{
		ServerID: e.ID, ServerName: e.Name, Kind: console.BaselineRunDump, SkipReason: reason,
		StartedAt: stamp, FinishedAt: stamp,
	})
	if err != nil {
		slog.Warn("backup schedule: could not record the skip in the history", "server", e.Name, "error", err)
	}
}
