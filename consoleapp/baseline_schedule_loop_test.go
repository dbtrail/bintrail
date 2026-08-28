package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

func newScheduleFixture(t *testing.T, fullBackups bool) (*backupScheduler, *console.Registry, *baselineSupervisor) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(t.TempDir() + "/h.json")
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

func addScheduled(t *testing.T, reg *console.Registry, method string) console.ServerEntry {
	t.Helper()
	e, err := reg.Add(console.ServerEntry{Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx",
		SourceDSN: "src:pw@tcp(127.0.0.1:3306)/", BaselineDir: t.TempDir(),
		BackupSchedule: &console.BackupSchedule{Every: "1h", At: "00:00", Method: method}})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// waitScheduled polls the history for the schedule's own run record: the
// job runs in its own goroutine and fails fast (an unparseable source DSN,
// an empty baseline directory), so the record arrives within a moment.
func waitScheduled(t *testing.T, sup *baselineSupervisor, id string) *console.BaselineRunRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if run, _ := sup.history.LastScheduled(id); run != nil {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the scheduled job never recorded itself in the history")
	return nil
}

// The first observation records the slot and never fires; a later, newer
// slot fires; the same slot again does not; a different identity (an edited
// schedule) is a first observation again.
func TestBackupScheduler_crossedIsEdgeTriggered(t *testing.T) {
	b, _, _ := newScheduleFixture(t, true)
	s1 := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	if b.crossed("a", "1h|00:00|backup", s1) {
		t.Fatal("the first observation fired")
	}
	if b.crossed("a", "1h|00:00|backup", s1) {
		t.Fatal("the same slot fired twice")
	}
	if !b.crossed("a", "1h|00:00|backup", s1.Add(time.Hour)) {
		t.Fatal("a newer slot did not fire")
	}
	if b.crossed("a", "1h|00:00|backup", s1) {
		t.Fatal("an older slot fired")
	}
	if b.crossed("a", "30m|00:00|backup", s1.Add(2*time.Hour)) {
		t.Fatal("a changed schedule fired on its first observation")
	}
	if !b.crossed("a", "30m|00:00|backup", s1.Add(3*time.Hour)) {
		t.Fatal("the changed schedule did not fire on its next slot")
	}
}

// Saving a schedule starts nothing: the tick that first sees it only
// records the slot. The next slot boundary is what fires, and a boundary
// that passed before the schedule was seen is not replayed.
func TestBackupScheduler_savingNeverFiresOnTheSpot(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, console.BackupMethodRefresh)
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
	st := sup.RefreshStatus(e.ID)
	if st.State == "idle" {
		t.Fatal("the slot boundary did not start the scheduled rebuild")
	}
	if got := b.ScheduleState(e.ID); got.LastStartedAt == "" {
		t.Fatal("the loop did not record what it started")
	}
}

// Editing a schedule is as silent as adding one. Before the identity was
// part of the observation, "every 1d at 03:00" observed at 03:00 and edited
// to "every 6h" in the afternoon fired a full dump of production within a
// minute of Save, while the toast said it would run at the next slot.
func TestBackupScheduler_editingNeverFiresOnTheSpot(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, console.BackupMethodRefresh)
	e.BackupSchedule = &console.BackupSchedule{Every: "1d", At: "03:00", Method: console.BackupMethodRefresh}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), time.Date(2026, 8, 28, 3, 0, 20, 0, time.UTC))
	// 15:30: the operator edits to every 6h at 03:00. The slot at or before
	// now under the new grid is 15:00, newer than the 03:00 seen under the
	// old one.
	e.BackupSchedule = &console.BackupSchedule{Every: "6h", At: "03:00", Method: console.BackupMethodRefresh}
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
}

// A slot the daemon's clock moved past while it was down is not replayed:
// the first tick after boot is a first observation.
func TestBackupScheduler_missedSlotsAreNotReplayed(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, console.BackupMethodRefresh)
	// "Boot" hours after several slots went by.
	b.tick(context.Background(), time.Date(2026, 8, 28, 14, 59, 0, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("a slot from before boot was replayed: %+v", st)
	}
}

// What the loop hands the supervisor is what the page later attributes to
// the schedule: the Trigger stamp on the history record, for both methods.
// Deleting either stamp in fire compiled and passed before this, and the
// page would have said "It has not run yet" under a nightly schedule.
func TestBackupScheduler_fireStampsTheTrigger(t *testing.T) {
	for _, method := range []string{console.BackupMethodRefresh, console.BackupMethodFull} {
		t.Run(method, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, true)
			e := addScheduled(t, reg, method)
			if method == console.BackupMethodFull {
				// An unparseable source DSN makes the dump fail before mydumper.
				e.SourceDSN = "not a dsn"
				if err := reg.Update(e); err != nil {
					t.Fatal(err)
				}
			}
			t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
			b.tick(context.Background(), t0)
			b.tick(context.Background(), t0.Add(time.Hour))
			run := waitScheduled(t, sup, e.ID)
			wantKind := console.BaselineRunDump
			if method == console.BackupMethodRefresh {
				wantKind = console.BaselineRunRefresh
			}
			if run.Kind != wantKind || run.Trigger != console.BaselineRunTriggerScheduled {
				t.Fatalf("record = %+v, want kind %s stamped scheduled", run, wantKind)
			}
			if run.Error == "" {
				t.Fatal("the fixture was supposed to fail fast; it reports success, so the assertion above may be vacuous")
			}
			// And the in-memory view agrees, with the job's terminal state.
			st := b.ScheduleState(e.ID)
			if st.Running || st.Last == nil || st.Last.State != "failed" || st.LastMethod != method {
				t.Fatalf("ScheduleState = %+v, want the failed job attributed to the schedule", st)
			}
		})
	}
}

// The collision the issue names: another backup job holds the server at the
// scheduled time. The slot is skipped, not queued, and the skip is written
// to the history so the page can show it.
func TestBackupScheduler_collisionSkipsAndRecords(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, console.BackupMethodFull)
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: "2026-08-28T08:30:00Z"}
	now := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), now.Add(-time.Hour))
	b.tick(context.Background(), now)
	run, skip := sup.history.LastScheduled(e.ID)
	if run != nil {
		t.Fatalf("a run was recorded through a busy server: %+v", run)
	}
	if skip == nil || skip.Kind != console.BaselineRunDump || skip.SkipReason == "" {
		t.Fatalf("skip = %+v, want a dump skip with a reason", skip)
	}
	if skip.FinishedAt != now.Format(time.RFC3339) {
		t.Fatalf("skip stamped %s, want the slot's tick %s", skip.FinishedAt, now.Format(time.RFC3339))
	}
	// The manual job that was already running is not reported as ours.
	if st := b.ScheduleState(e.ID); st.Running || st.Last != nil {
		t.Fatalf("a manual job in flight was reported as the schedule's: %+v", st)
	}
}

// A full-backup schedule on a daemon without the creation opt-in is skipped
// with the reason the page already knows, never silently. Same for the
// supervisor's standing refusal (a lock-mode misconfiguration).
func TestBackupScheduler_fullBackupNeedsTheOptIn(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  bool
		cfg  error
		want string
	}{
		{"opt-in off", false, nil, "not set to 1"},
		{"lock mode misconfigured", true, errors.New("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown mode \"lock-sometimes\""), "lock-sometimes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, tc.opt)
			sup.configErr = tc.cfg
			e := addScheduled(t, reg, console.BackupMethodFull)
			now := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
			b.tick(context.Background(), now.Add(-time.Hour))
			b.tick(context.Background(), now)
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

// A removed schedule is forgotten, so re-adding one starts silent.
func TestBackupScheduler_removedScheduleIsForgotten(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, console.BackupMethodRefresh)
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), t0)
	e.BackupSchedule = nil
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), t0.Add(time.Hour))
	if _, ok := b.seen[e.ID]; ok {
		t.Fatal("a server with no schedule is still tracked")
	}
	e.BackupSchedule = &console.BackupSchedule{Every: "1h"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), t0.Add(2*time.Hour))
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("a re-added schedule fired on first sight: %+v", st)
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
// that took the slot after it finished, is not the schedule's, in any state.
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
	// A dump running is not the rebuild the schedule started.
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:00:00Z"}
	if st := b.ScheduleState("a"); st.Running {
		t.Fatal("a manual dump was reported as the scheduled rebuild")
	}
	// Same kind, later job: the manual Create backup after the scheduled
	// one finished. Neither running nor its outcome is the schedule's.
	b.started["a"] = scheduledStart{method: console.BackupMethodFull, at: "2026-08-28T10:00:00Z", since: "2026-08-28T10:00:00Z"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T11:00:00Z"}
	if st := b.ScheduleState("a"); st.Running || st.Last != nil {
		t.Fatalf("a later manual dump was reported as the schedule's: %+v", st)
	}
	// Same kind, earlier job (the slot was skipped): not ours either.
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:30:00Z"}
	if st := b.ScheduleState("a"); st.Running || st.Last != nil {
		t.Fatalf("a dump that began before the scheduled slot was reported as the schedule's: %+v", st)
	}
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T10:00:00Z"}
	if st := b.ScheduleState("a"); !st.Running {
		t.Fatal("the scheduled dump in flight is not reported running")
	}
	if st := b.ScheduleState("never"); st.Running || st.LastStartedAt != "" || st.Last != nil {
		t.Fatal("an unknown server reported state")
	}
}

// Running after a REAL fire: the stamp the loop attributes by is read back
// from the supervisor, not reconstructed, so whatever format either side
// uses they agree. The dump is held at the privilege-check seam so the job
// is observably in flight, then released and observed finished.
func TestBackupScheduler_runningAfterARealFire(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, console.BackupMethodFull)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	prev := checkMydumperPrivileges
	checkMydumperPrivileges = func(ctx context.Context, dsn string, mode baseline.LockMode, remedy mydumperlock.Remedy, schemas []string) error {
		entered <- struct{}{}
		<-release
		return errors.New("held by the test")
	}
	t.Cleanup(func() { checkMydumperPrivileges = prev })
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), t0)
	b.tick(context.Background(), t0.Add(time.Hour))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduled dump never reached the privilege check")
	}
	if st := b.ScheduleState(e.ID); !st.Running || st.Last == nil {
		t.Fatalf("in flight: ScheduleState = %+v, want running and attributed", st)
	}
	close(release)
	run := waitScheduled(t, sup, e.ID)
	if !strings.Contains(run.Error, "held by the test") {
		t.Fatalf("record = %+v, want the seam's error", run)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := b.ScheduleState(e.ID)
		if st.Last != nil && !st.Running {
			if st.Last.State != "failed" {
				t.Fatalf("finished: ScheduleState = %+v, want failed", st)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job never reached a terminal state in the loop's view: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A panic inside a tick must not reach the daemon that is also capturing.
func TestBackupScheduler_tickSurvivesAPanic(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	addScheduled(t, reg, console.BackupMethodRefresh)
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
	e := addScheduled(t, reg, console.BackupMethodRefresh)
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
	rep, _ = newBackupScheduleReporter(sup, reg, false, false)
	if enabled, _ := rep.FullBackups(); enabled {
		t.Fatal("the opt-in flag was not carried through")
	}
}
