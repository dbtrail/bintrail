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
// registry every tick (a saved schedule applies without a restart, the way a
// rotation override does), and starts a job when a server's schedule crosses
// a slot boundary. It is EDGE-triggered on the slot grid:
//
//   - a slot that passed while the daemon was down never fires: cron
//     semantics, and the only ones that keep a restart from turning into a
//     surprise full dump of production at 09:00 on a Monday;
//   - the first time a server's schedule is observed (boot, or just saved),
//     the current slot is recorded and NOT fired, so saving a schedule never
//     starts a backup on the spot. It runs at the next slot.
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
	// seen is the latest slot observed per server. Its presence is what
	// makes the first observation silent.
	seen map[string]time.Time
	// started is the last job this schedule started per server, for the
	// Running answer: which supervisor slot to look at, and since when.
	started map[string]scheduledStart
}

type scheduledStart struct {
	method string
	at     string
}

func newBackupScheduler(sup *baselineSupervisor, reg *console.Registry, fullBackups, carryDefault bool) *backupScheduler {
	return &backupScheduler{
		sup: sup, reg: reg, fullBackups: fullBackups, carryDefault: carryDefault,
		seen:    make(map[string]time.Time),
		started: make(map[string]scheduledStart),
	}
}

// FullBackups implements console.BackupScheduleReporter.
func (b *backupScheduler) FullBackups() bool { return b.fullBackups }

// ScheduleState implements console.BackupScheduleReporter.
func (b *backupScheduler) ScheduleState(serverID string) console.BackupScheduleState {
	b.mu.Lock()
	st, ok := b.started[serverID]
	b.mu.Unlock()
	if !ok {
		return console.BackupScheduleState{}
	}
	var cur console.BaselineStatus
	if st.method == console.BackupMethodRefresh {
		cur = b.sup.RefreshStatus(serverID)
	} else {
		cur = b.sup.Status(serverID)
	}
	// The slot is shared with manual jobs of the same kind, so "running"
	// alone could be the Create backup button. Since is stamped by the
	// supervisor at trigger time, right after the scheduler's own stamp; a
	// job that started before ours is not ours.
	running := cur.State == "running" && cur.Since >= st.at
	return console.BackupScheduleState{Running: running, LastStartedAt: st.at}
}

// startBackupScheduleLoop launches the loop. sup nil = no baseline features
// on this daemon = no loop; the console then refuses the schedule endpoints.
func startBackupScheduleLoop(ctx context.Context, sched *backupScheduler) {
	if sched == nil {
		return
	}
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
			// A hand-edited file; the API never saves one of these. The
			// listing reports it as not runnable, which is where an operator
			// looks; a per-tick log line would be noise on top of that.
			slog.Debug("backup schedule: unreadable schedule", "server", e.Name, "error", err)
			continue
		}
		if !b.crossed(e.ID, p.SlotAtOrBefore(now)) {
			continue
		}
		b.fire(e, p, now)
	}
	// Forget servers whose schedule is gone, so a schedule removed and later
	// re-added starts silent again rather than firing on a stale slot.
	b.mu.Lock()
	for id := range b.seen {
		if !live[id] {
			delete(b.seen, id)
		}
	}
	b.mu.Unlock()
}

// crossed records slot for the server and reports whether it moved past the
// previously observed one. The first observation records and reports false.
func (b *backupScheduler) crossed(serverID string, slot time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	prev, ok := b.seen[serverID]
	b.seen[serverID] = slot
	return ok && slot.After(prev)
}

// fire starts the scheduled job for e, or records why it could not.
func (b *backupScheduler) fire(e console.ServerEntry, p console.ParsedBackupSchedule, now time.Time) {
	gates := console.BackupScheduleGates{LoopRunning: true, FullBackups: b.fullBackups}
	if err := console.CheckBackupSchedule(e, *e.BackupSchedule, gates); err != nil {
		b.skip(e, p.Method, now, console.RefusalReason(err))
		return
	}
	stamp := now.Format(time.RFC3339)
	var err error
	switch p.Method {
	case console.BackupMethodRefresh:
		req := refreshRequest{
			ServerID: e.ID, ServerName: e.Name, IndexDSN: e.DSN, BaselineDir: e.BaselineDir,
			CarryForwardUnchanged: effectiveCarryForward(b.reg, b.carryDefault),
			Trigger:               console.BaselineRunTriggerScheduled,
		}
		err = b.sup.TriggerRefresh(req, p.Every)
	default:
		req := console.BaselineRequestFor(e)
		req.Trigger = console.BaselineRunTriggerScheduled
		err = b.sup.Trigger(req)
	}
	switch {
	case err == nil:
		b.mu.Lock()
		b.started[e.ID] = scheduledStart{method: p.Method, at: stamp}
		b.mu.Unlock()
		slog.Info("backup schedule: started", "server", e.Name, "method", p.Method, "every", e.BackupSchedule.Every)
	case errors.Is(err, console.ErrBaselineRunning):
		// The collision the issue names: a manual backup, restore or export
		// (or the previous scheduled run) holds the server. Skip, do not
		// queue: a queued dump would fire at an unscheduled moment.
		b.skip(e, p.Method, now, "another backup job was running for this server at the scheduled time")
	default:
		b.skip(e, p.Method, now, err.Error())
	}
}

// skip records a slot that did not start, in the history (so the page can
// show it, restart or not) and in the log.
func (b *backupScheduler) skip(e console.ServerEntry, method string, now time.Time, reason string) {
	slog.Warn("backup schedule: scheduled backup did not start", "server", e.Name, "method", method, "reason", reason)
	if b.sup.history == nil {
		return
	}
	kind := console.BaselineRunDump
	if method == console.BackupMethodRefresh {
		kind = console.BaselineRunRefresh
	}
	stamp := now.Format(time.RFC3339)
	_, err := b.sup.history.AppendSkip(console.BaselineRunRecord{
		ServerID: e.ID, ServerName: e.Name, Kind: kind, SkipReason: reason,
		StartedAt: stamp, FinishedAt: stamp,
	})
	if err != nil {
		slog.Warn("backup schedule: could not record the skip in the history", "server", e.Name, "error", err)
	}
}
