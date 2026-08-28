package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// newScheduleFixture builds a scheduler over a real supervisor and a real
// history file. The context is cancelled BEFORE the temp dirs are removed
// (Cleanup runs LIFO), but a fired job takes no context on its history
// write, so every test that fires must also await the job: see waitTerminal.
func newScheduleFixture(t *testing.T, fullBackups bool) (*backupScheduler, *console.Registry, *baselineSupervisor) {
	t.Helper()
	staging := t.TempDir()
	histDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, staging, baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(histDir + "/h.json")
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	reg, err := console.LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	return newBackupScheduler(sup, reg, fullBackups, false), reg, sup
}

// addScheduled adds an hourly schedule on a server with a source and a
// local backup directory. withBackup writes a fake previous backup into it,
// which is what makes the daemon choose a rebuild; without one it takes a
// full backup, and a source DSN that does not parse makes that fail before
// mydumper.
func addScheduled(t *testing.T, reg *console.Registry, withBackup bool) console.ServerEntry {
	t.Helper()
	dir := t.TempDir()
	source := "not a dsn"
	if withBackup {
		writeFakeSnapshot(t, dir)
		source = "src:pw@tcp(127.0.0.1:3306)/"
	}
	e, err := reg.Add(console.ServerEntry{Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx",
		SourceDSN: source, BaselineDir: dir,
		BackupSchedule: &console.BackupSchedule{Every: "1h", At: "00:00"}})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// waitTerminal polls the loop's own view until the job it started reached a
// terminal state. The job goroutine writes the history BEFORE it flips the
// status, so waiting on the history alone let a test return with the job
// still finishing, and its last writes then landed in a temp dir the test
// had already removed (CI: "TempDir RemoveAll cleanup: directory not empty").
func waitTerminal(t *testing.T, b *backupScheduler, id string) console.BackupScheduleState {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := b.ScheduleState(id)
		if st.Last != nil && !st.Running {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("the scheduled job never reached a terminal state in the loop's view: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitTerminalMethod is waitTerminal for a specific producer: a fallback
// changes the method, and the caller wants the second job.
func waitTerminalMethod(t *testing.T, b *backupScheduler, id, method string) console.BackupScheduleState {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := b.ScheduleState(id)
		if st.LastMethod == method && st.Last != nil && !st.Running {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("no terminal %s job in the loop's view: %+v", method, st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fireAt drives the two ticks that fire a fresh 1h schedule: the observation
// and the next boundary.
func fireAt(b *backupScheduler, t0 time.Time) {
	b.tick(context.Background(), t0)
	b.tick(context.Background(), t0.Add(time.Hour))
}

// holdFold replaces the fold seam with one that returns err (or panics when
// err is nil and panicWith is set) and restores it once the job under test
// is terminal; the caller must await the job before the test ends.
func holdFold(t *testing.T, fn func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error)) {
	t.Helper()
	prev := foldTables
	foldTables = fn
	t.Cleanup(func() { foldTables = prev })
}

// The first observation records the slot and never fires; a later, newer
// slot fires; the same slot again does not; a different identity (an edited
// schedule) is a first observation again.
func TestBackupScheduler_crossedIsEdgeTriggered(t *testing.T) {
	b, _, _ := newScheduleFixture(t, true)
	s1 := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	if b.crossed("a", "1h|00:00", s1) {
		t.Fatal("the first observation fired")
	}
	if b.crossed("a", "1h|00:00", s1) {
		t.Fatal("the same slot fired twice")
	}
	if !b.crossed("a", "1h|00:00", s1.Add(time.Hour)) {
		t.Fatal("a newer slot did not fire")
	}
	if b.crossed("a", "1h|00:00", s1) {
		t.Fatal("an older slot fired")
	}
	if b.crossed("a", "30m|00:00", s1.Add(2*time.Hour)) {
		t.Fatal("a changed schedule fired on its first observation")
	}
	if !b.crossed("a", "30m|00:00", s1.Add(3*time.Hour)) {
		t.Fatal("the changed schedule did not fire on its next slot")
	}
}

// Saving a schedule starts nothing: the tick that first sees it only
// records the slot. The next slot boundary is what fires, and a boundary
// that passed before the schedule was seen is not replayed.
func TestBackupScheduler_savingNeverFiresOnTheSpot(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	now := time.Date(2026, 8, 28, 9, 0, 30, 0, time.UTC) // 30s past the 09:00 slot
	b.tick(context.Background(), now)
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("the first tick started a job: %+v", st)
	}
	if run, skip := sup.history.LastScheduled(e.ID); run != nil || skip != nil {
		t.Fatal("the first tick wrote to the history")
	}
	b.tick(context.Background(), now.Add(30*time.Minute))
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("a tick inside the same slot started a job: %+v", st)
	}
	// 10:00: the boundary crossed while the loop was watching.
	b.tick(context.Background(), now.Add(time.Hour))
	if st := sup.RefreshStatus(e.ID); st.State == "idle" {
		t.Fatal("the slot boundary did not start the scheduled rebuild")
	}
	if st := waitTerminal(t, b, e.ID); st.LastStartedAt == "" || st.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("the loop did not record what it started: %+v", st)
	}
}

// Editing a schedule is as silent as adding one. Before the identity was
// part of the observation, "every 1d at 03:00" observed at 03:00 and edited
// to "every 6h" in the afternoon fired a full dump of production within a
// minute of Save, while the toast said it would run at the next slot.
func TestBackupScheduler_editingNeverFiresOnTheSpot(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	e.BackupSchedule = &console.BackupSchedule{Every: "1d", At: "03:00"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), time.Date(2026, 8, 28, 3, 0, 20, 0, time.UTC))
	// 15:30: the operator edits to every 6h at 03:00. The slot at or before
	// now under the new grid is 15:00, newer than the 03:00 seen under the
	// old one.
	e.BackupSchedule = &console.BackupSchedule{Every: "6h", At: "03:00"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), time.Date(2026, 8, 28, 15, 30, 0, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("editing the schedule started a job on the spot: %+v", st)
	}
	// 21:00 under the new grid: that one fires.
	b.tick(context.Background(), time.Date(2026, 8, 28, 21, 0, 10, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State == "idle" {
		t.Fatal("the edited schedule's next slot did not fire")
	}
	waitTerminal(t, b, e.ID)
}

// The API observes a schedule at the instant it is saved, so a boundary
// that falls between the save and the loop's next tick fires: the next_run
// the page reported is kept. Without Observe, the tick at 03:00:20 was the
// first observation and today's 03:00 was silently dropped.
func TestBackupScheduler_observeAtSaveKeepsThePromisedNextRun(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	sched := console.BackupSchedule{Every: "1d", At: "03:00"}
	e.BackupSchedule = &sched
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.Observe(e.ID, sched, time.Date(2026, 8, 28, 2, 59, 50, 0, time.UTC))
	b.tick(context.Background(), time.Date(2026, 8, 28, 3, 0, 20, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State == "idle" {
		t.Fatal("the boundary between the save and the first tick was dropped")
	}
	waitTerminal(t, b, e.ID)

	// Saved just AFTER the boundary: the slot in progress is the 03:00 one,
	// and the first tick must not fire it.
	b2, reg2, sup2 := newScheduleFixture(t, true)
	e2 := addScheduled(t, reg2, true)
	e2.BackupSchedule = &sched
	if err := reg2.Update(e2); err != nil {
		t.Fatal(err)
	}
	b2.Observe(e2.ID, sched, time.Date(2026, 8, 28, 3, 0, 10, 0, time.UTC))
	b2.tick(context.Background(), time.Date(2026, 8, 28, 3, 0, 30, 0, time.UTC))
	if st := sup2.RefreshStatus(e2.ID); st.State != "idle" {
		t.Fatalf("a save after the boundary fired the slot in progress: %+v", st)
	}
}

// At boot every schedule is observed at the boot instant: a boundary in the
// first minute of uptime fires (the daemon was up for it), a boundary before
// boot does not.
func TestBackupScheduler_observeAllAtBoot(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.observeAll(time.Date(2026, 8, 28, 8, 59, 40, 0, time.UTC))
	b.tick(context.Background(), time.Date(2026, 8, 28, 9, 0, 40, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State == "idle" {
		t.Fatal("a boundary in the first minute of uptime did not fire")
	}
	waitTerminal(t, b, e.ID)
}

// A slot the daemon's clock moved past while it was down is not replayed:
// the first tick after boot is a first observation.
func TestBackupScheduler_missedSlotsAreNotReplayed(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.observeAll(time.Date(2026, 8, 28, 14, 59, 0, 0, time.UTC))
	b.tick(context.Background(), time.Date(2026, 8, 28, 14, 59, 30, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("a slot from before boot was replayed: %+v", st)
	}
}

// How each run is made is the daemon's decision: a server with a previous
// backup on disk gets a rebuild, one without gets a full backup. Both stamp
// the trigger on their history record, which is what the page attributes
// runs by; deleting either stamp compiled and passed before this.
func TestBackupScheduler_choosesTheProducerAndStampsTheTrigger(t *testing.T) {
	for _, tc := range []struct {
		name       string
		withBackup bool
		wantMethod string
		wantKind   string
	}{
		{"previous backup on disk: rebuild", true, console.BackupMethodRefresh, console.BaselineRunRefresh},
		{"no backup yet: full backup", false, console.BackupMethodFull, console.BaselineRunDump},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
				return nil, nil, errors.New("seam: refused")
			})
			b, reg, sup := newScheduleFixture(t, true)
			e := addScheduled(t, reg, tc.withBackup)
			fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
			st := waitTerminalMethod(t, b, e.ID, tc.wantMethod)
			if st.Last.State != "failed" {
				t.Fatalf("the fixture was supposed to fail fast: %+v", st)
			}
			run, _ := sup.history.LastScheduled(e.ID)
			if run == nil || run.Trigger != console.BaselineRunTriggerScheduled {
				t.Fatalf("record = %+v, want it stamped scheduled", run)
			}
			// With the fallback, the rebuild's refusal is followed by a full
			// backup; the newest record is that one for the rebuild case.
			if tc.withBackup {
				waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
				run, _ = sup.history.LastScheduled(e.ID)
			}
			if run.Kind != tc.wantKind && !(tc.withBackup && run.Kind == console.BaselineRunDump) {
				t.Fatalf("record kind = %q, want %q", run.Kind, tc.wantKind)
			}
		})
	}
}

// A rebuild the fold refuses falls back to a full backup at the same slot,
// and the fallback is on record for the page. On a daemon that cannot take
// a full backup, the refusal becomes a skip that names both reasons.
func TestBackupScheduler_refusedRebuildFallsBackToAFullBackup(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, errors.New("capture gap in the reconstruction window")
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn" // the fallback full backup fails fast too
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if st.LastFallbackAt == "" || !strings.Contains(st.LastFallbackReason, "capture gap") {
		t.Fatalf("the fallback was not recorded: %+v", st)
	}
	if sup.Status(e.ID).State != "failed" {
		t.Fatalf("the full backup did not run after the refusal: %+v", sup.Status(e.ID))
	}
	if run, _ := sup.history.LastScheduled(e.ID); run == nil || run.Kind != console.BaselineRunDump {
		t.Fatalf("the newest scheduled record is not the fallback's full backup: %+v", run)
	}

	// Without the creation opt-in there is no fallback: a skip with both
	// reasons, and no dump started.
	b2, reg2, sup2 := newScheduleFixture(t, false)
	e2 := addScheduled(t, reg2, true)
	fireAt(b2, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	waitTerminal(t, b2, e2.ID)
	deadline := time.Now().Add(5 * time.Second)
	for b2.ScheduleState(e2.ID).LastSkippedAt == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	st2 := b2.ScheduleState(e2.ID)
	if !strings.Contains(st2.LastSkipReason, "capture gap") || !strings.Contains(st2.LastSkipReason, "not set to 1") {
		t.Fatalf("skip reason = %q, want the refusal and why a full backup cannot start", st2.LastSkipReason)
	}
	if sup2.Status(e2.ID).State != "idle" {
		t.Fatal("a full backup was started without the opt-in")
	}
}

// The scheduled rebuild carries the effective reuse setting: the console's
// saved override over the daemon flag, the same resolution the refresh loop
// and a restore use. Hardcoding either value compiled and passed.
func TestBackupScheduler_rebuildCarriesTheEffectiveCarryForward(t *testing.T) {
	for _, want := range []bool{true, false} {
		t.Run(map[bool]string{true: "override on", false: "override off"}[want], func(t *testing.T) {
			var mu sync.Mutex
			var got []bool
			holdFold(t, func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
				mu.Lock()
				got = append(got, cfg.CarryForwardUnchanged)
				mu.Unlock()
				return nil, nil, nil
			})
			b, reg, _ := newScheduleFixture(t, true)
			b.carryDefault = !want // the flag says the opposite; the override must win
			if err := reg.SetBaselineRefresh(&console.BaselineRefreshConfig{CarryForwardUnchanged: want}); err != nil {
				t.Fatal(err)
			}
			e := addScheduled(t, reg, true)
			fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
			waitTerminal(t, b, e.ID)
			mu.Lock()
			defer mu.Unlock()
			if len(got) != 1 || got[0] != want {
				t.Fatalf("fold config CarryForwardUnchanged = %v, want [%v]", got, want)
			}
		})
	}
}

// A scheduled job that PANICS reaches the page: the guard flips the slot to
// failed with the panic value, the loop attributes it, and the copy it keeps
// survives a later manual job taking the slot, which is the case the history
// cannot cover (the guard writes no record, by design). A panic is not a
// refusal: no fallback fires.
func TestBackupScheduler_panickedJobStaysVisible(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		panic("boom")
	})
	b, reg, sup := newScheduleFixture(t, false) // no opt-in: a fallback would be a skip, and there must be none
	e := addScheduled(t, reg, true)
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminal(t, b, e.ID)
	if st.Last.State != "failed" || !strings.Contains(st.Last.LastError, "boom") {
		t.Fatalf("panicked job = %+v, want failed with the panic value", st.Last)
	}
	if run, _ := sup.history.LastScheduled(e.ID); run != nil {
		t.Fatalf("the guard wrote a history record (%+v); this test assumes it does not, so the in-memory copy is the only evidence", run)
	}
	// A later manual rebuild overwrites the slot. The schedule's outcome
	// must not vanish with it.
	sup.refreshes[e.ID] = &console.BaselineStatus{State: "running", Since: "2099-01-01T00:00:00Z"}
	st = b.ScheduleState(e.ID)
	if st.Running || st.Last == nil || !strings.Contains(st.Last.LastError, "boom") {
		t.Fatalf("after a manual job took the slot: %+v, want the kept failure, not running", st)
	}
}

// The collision the issue names: another backup job holds the server at the
// scheduled time. The slot is skipped, not queued, and the skip is written
// to the history AND kept in memory so the page can show it either way.
func TestBackupScheduler_collisionSkipsAndRecords(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false)
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: "2026-08-28T08:30:00Z"}
	now := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), now.Add(-time.Hour))
	b.tick(context.Background(), now)
	run, skip := sup.history.LastScheduled(e.ID)
	if run != nil {
		t.Fatalf("a run was recorded through a busy server: %+v", run)
	}
	if skip == nil || skip.SkipReason == "" {
		t.Fatalf("skip = %+v, want a skip with a reason", skip)
	}
	if skip.FinishedAt != now.Format(time.RFC3339) {
		t.Fatalf("skip stamped %s, want the slot's tick %s", skip.FinishedAt, now.Format(time.RFC3339))
	}
	st := b.ScheduleState(e.ID)
	if st.LastSkippedAt != skip.FinishedAt || st.LastSkipReason != skip.SkipReason {
		t.Fatalf("in-memory skip = %+v, want the same slot and reason as the history", st)
	}
	// The manual job that was already running is not reported as ours.
	if st.Running || st.Last != nil {
		t.Fatalf("a manual job in flight was reported as the schedule's: %+v", st)
	}
	// Without a history the skip is still on record in memory.
	sup.history = nil
	b.tick(context.Background(), now.Add(time.Hour))
	if st := b.ScheduleState(e.ID); st.LastSkippedAt != now.Add(time.Hour).Format(time.RFC3339) {
		t.Fatalf("with no history the skip was lost: %+v", st)
	}
}

// A schedule the daemon cannot serve at this slot is skipped with the reason
// the page already knows, never silently: no backup to rebuild from and no
// creation opt-in, or the supervisor's standing refusal (a lock-mode
// misconfiguration) on a server whose backups go to S3.
func TestBackupScheduler_skipsWithTheReasonWhenNothingCanRun(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  bool
		cfg  error
		s3   bool
		want string
	}{
		{"no backup yet, opt-in off", false, nil, false, "not set to 1"},
		{"S3 destination, lock mode misconfigured", true, errors.New("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown mode \"lock-sometimes\""), true, "lock-sometimes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, tc.opt)
			sup.configErr = tc.cfg
			e := addScheduled(t, reg, false)
			if tc.s3 {
				e.BaselineDir, e.BaselineS3 = "", "s3://bucket/backups/"
				if err := reg.Update(e); err != nil {
					t.Fatal(err)
				}
			}
			fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
			if st := sup.Status(e.ID); st.State != "idle" {
				t.Fatalf("a dump was started: %+v", st)
			}
			_, skip := sup.history.LastScheduled(e.ID)
			if skip == nil || !strings.Contains(skip.SkipReason, tc.want) {
				t.Fatalf("skip = %+v, want a reason mentioning %q", skip, tc.want)
			}
		})
	}
}

// A removed schedule is forgotten (observation, warning, last run, last
// skip, last fallback), so re-adding one starts silent and reports nothing
// stale.
func TestBackupScheduler_removedScheduleIsForgotten(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	fireAt(b, t0)
	waitTerminal(t, b, e.ID)
	b.skipped[e.ID] = scheduledSkip{at: "x", reason: "y"}
	b.fallback[e.ID] = scheduledFallback{at: "x", reason: "y"}
	b.warned[e.ID] = true
	e.BackupSchedule = nil
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), t0.Add(2*time.Hour))
	if _, ok := b.seen[e.ID]; ok {
		t.Fatal("a server with no schedule is still observed")
	}
	if st := b.ScheduleState(e.ID); st.Last != nil || st.LastStartedAt != "" || st.LastSkippedAt != "" || st.LastFallbackAt != "" {
		t.Fatalf("a removed schedule still reports state: %+v", st)
	}
	if b.warned[e.ID] {
		t.Fatal("the warn-once mark outlived the schedule")
	}
	// Re-added with the SAME identity, between ticks: still a first
	// observation, so the tick two hours on records and does not fire.
	e.BackupSchedule = &console.BackupSchedule{Every: "1h", At: "00:00"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	before := sup.RefreshStatus(e.ID)
	b.tick(context.Background(), t0.Add(3*time.Hour))
	if after := sup.RefreshStatus(e.ID); after.Since != before.Since || sup.Status(e.ID).State != "idle" {
		t.Fatalf("a re-added schedule fired on first sight: %+v", after)
	}
}

// The history record a scheduled job writes carries the trigger, for both
// producers, and a manual one does not. Drives the supervisor half directly.
func TestScheduledRuns_stampTheTrigger(t *testing.T) {
	_, _, sup := newScheduleFixture(t, true)
	sup.jobs["d"] = &console.BaselineStatus{State: "running"}
	sup.run(console.BaselineRequest{ServerID: "d", ServerName: "d", SourceDSN: "not a dsn",
		Trigger: console.BaselineRunTriggerScheduled})
	if run, _ := sup.history.LastScheduled("d"); run == nil || run.Kind != console.BaselineRunDump || run.Error == "" {
		t.Fatalf("dump record = %+v, want a failed scheduled dump", run)
	}
	sup.jobs["m"] = &console.BaselineStatus{State: "running"}
	sup.run(console.BaselineRequest{ServerID: "m", ServerName: "m", SourceDSN: "not a dsn"})
	if run, _ := sup.history.LastScheduled("m"); run != nil {
		t.Fatalf("a manual dump was attributed to the schedule: %+v", run)
	}
	sup.refreshes["r"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "r", ServerName: "r", IndexDSN: "d", BaselineDir: t.TempDir(),
		Trigger: console.BaselineRunTriggerScheduled}, time.Now().UTC(), time.Hour)
	if run, _ := sup.history.LastScheduled("r"); run == nil || run.Kind != console.BaselineRunRefresh {
		t.Fatalf("refresh record = %+v, want a scheduled refresh", run)
	}
}

// Attribution is by the exact Since the supervisor stamped on the job the
// loop started. A job of the same kind that began before it, or a manual one
// that took the slot after it finished, is not the schedule's, in any state;
// the schedule's own outcome, once observed, outlives the slot.
func TestBackupScheduler_attributesOnlyTheJobItStarted(t *testing.T) {
	b, _, sup := newScheduleFixture(t, true)
	b.started["a"] = scheduledStart{method: console.BackupMethodRefresh, at: "2026-08-28T09:00:00Z", since: "2026-08-28T09:00:00Z"}
	sup.refreshes["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:00:00Z"}
	if st := b.ScheduleState("a"); !st.Running || st.Last == nil {
		t.Fatalf("the scheduled rebuild in flight is not reported: %+v", st)
	}
	sup.refreshes["a"].State = "failed"
	sup.refreshes["a"].LastError = "capture gap"
	if st := b.ScheduleState("a"); st.Running || st.Last == nil || st.Last.LastError != "capture gap" {
		t.Fatalf("the finished job's outcome did not reach the state: %+v", st)
	}
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:00:00Z"}
	if st := b.ScheduleState("a"); st.Running || st.Last == nil || st.Last.LastError != "capture gap" {
		t.Fatalf("a manual dump changed the schedule's reported outcome: %+v", st)
	}
	sup.refreshes["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T11:00:00Z"}
	if st := b.ScheduleState("a"); st.Running || st.Last == nil || st.Last.LastError != "capture gap" {
		t.Fatalf("a later manual job was reported as the schedule's, or erased its outcome: %+v", st)
	}
	b.started["b"] = scheduledStart{method: console.BackupMethodFull, at: "2026-08-28T10:00:00Z", since: "2026-08-28T10:00:00Z"}
	sup.jobs["b"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:30:00Z"}
	if st := b.ScheduleState("b"); st.Running || st.Last != nil {
		t.Fatalf("a dump that began before the scheduled slot was reported as the schedule's: %+v", st)
	}
	sup.jobs["b"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T10:00:00Z"}
	if st := b.ScheduleState("b"); !st.Running {
		t.Fatal("the scheduled dump in flight is not reported running")
	}
	if st := b.ScheduleState("never"); st.Running || st.LastStartedAt != "" || st.Last != nil {
		t.Fatal("an unknown server reported state")
	}
}

// Running after a REAL fire: the stamp the loop attributes by is read back
// from the supervisor, not reconstructed, so whatever format either side
// uses they agree. The full backup (no previous backup on disk) is held at
// the privilege-check seam so the job is observably in flight, then released
// and observed finished.
func TestBackupScheduler_runningAfterARealFire(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false)
	e.SourceDSN = "src:pw@tcp(127.0.0.1:3306)/" // parseable, so the dump reaches the seam
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	entered := make(chan struct{}, 1)
	prev := checkMydumperPrivileges
	checkMydumperPrivileges = func(ctx context.Context, dsn string, mode baseline.LockMode, remedy mydumperlock.Remedy, schemas []string) error {
		entered <- struct{}{}
		<-release
		return errors.New("held by the test")
	}
	// On every exit: let the job go, wait for it, THEN restore the seam.
	// Restoring while the goroutine may still read it is a data race.
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		deadline := time.Now().Add(10 * time.Second)
		for b.ScheduleState(e.ID).Running && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		checkMydumperPrivileges = prev
	})
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduled dump never reached the privilege check")
	}
	if st := b.ScheduleState(e.ID); !st.Running || st.Last == nil || st.LastMethod != console.BackupMethodFull {
		t.Fatalf("in flight: ScheduleState = %+v, want a running full backup attributed", st)
	}
	releaseOnce.Do(func() { close(release) })
	st := waitTerminal(t, b, e.ID)
	if st.Last.State != "failed" {
		t.Fatalf("finished: ScheduleState = %+v, want failed", st)
	}
	run, _ := sup.history.LastScheduled(e.ID)
	if run == nil || !strings.Contains(run.Error, "held by the test") {
		t.Fatalf("record = %+v, want the seam's error", run)
	}
}

// A panic inside a tick must not reach the daemon that is also capturing.
func TestBackupScheduler_tickSurvivesAPanic(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	addScheduled(t, reg, false)
	b.seen = nil // a nil map write panics inside crossed
	b.tick(context.Background(), time.Now().UTC())
}

// An unreadable schedule is reported once per server at Warn, not every
// minute at Debug, and again after it is fixed and broken again.
func TestBackupScheduler_unreadableScheduleWarnsOnce(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false)
	e.BackupSchedule = &console.BackupSchedule{Every: "soon"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().UTC()
	b.tick(context.Background(), t0)
	b.tick(context.Background(), t0.Add(time.Minute))
	b.tick(context.Background(), t0.Add(2*time.Minute))
	if n := strings.Count(buf.String(), "cannot be read"); n != 1 {
		t.Fatalf("warned %d times over three ticks, want once: %s", n, buf.String())
	}
	e.BackupSchedule = &console.BackupSchedule{Every: "1h"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), t0.Add(3*time.Minute))
	e.BackupSchedule = &console.BackupSchedule{Every: "later"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), t0.Add(4*time.Minute))
	if n := strings.Count(buf.String(), "cannot be read"); n != 2 {
		t.Fatalf("a schedule fixed and broken again was not reported again: %d warnings", n)
	}
}

// The watch wiring: a nil supervisor yields a nil INTERFACE, not a typed nil
// the console would take for a running loop and dereference on the first
// listing with a schedule.
func TestNewBackupScheduleReporter_nilSupervisorIsANilInterface(t *testing.T) {
	reg, _ := console.LoadRegistry("")
	rep, sched := newBackupScheduleReporter(nil, reg, true, false)
	if rep != nil || sched != nil {
		t.Fatalf("nil supervisor produced a reporter (%v, %v); the console would advertise a loop that does not exist", rep, sched)
	}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	rep, sched = newBackupScheduleReporter(sup, reg, true, false)
	if rep == nil || sched == nil || rep.(*backupScheduler) != sched {
		t.Fatal("a supervisor did not produce one scheduler behind both values")
	}
	if enabled, refusal := rep.FullBackups(); !enabled || refusal != nil {
		t.Fatalf("FullBackups = (%v, %v), want the opt-in carried through", enabled, refusal)
	}
	rep, sched = newBackupScheduleReporter(sup, reg, false, true)
	if enabled, _ := rep.FullBackups(); enabled || !sched.carryDefault {
		t.Fatal("the two flags were not carried through in order")
	}
}
