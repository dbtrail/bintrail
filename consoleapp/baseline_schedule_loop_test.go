package consoleapp

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
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

// The first observation records the slot and never fires; a later, newer
// slot fires; the same slot again does not; a server whose schedule is gone
// is forgotten so a re-added schedule starts silent again.
func TestBackupScheduler_crossedIsEdgeTriggered(t *testing.T) {
	b, _, _ := newScheduleFixture(t, true)
	s1 := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	if b.crossed("a", s1) {
		t.Fatal("the first observation fired")
	}
	if b.crossed("a", s1) {
		t.Fatal("the same slot fired twice")
	}
	if !b.crossed("a", s1.Add(time.Hour)) {
		t.Fatal("a newer slot did not fire")
	}
	if b.crossed("a", s1) {
		t.Fatal("an older slot fired")
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
	if b.ScheduleState(e.ID).Running {
		t.Fatal("a manual job in flight was reported as the schedule's")
	}
}

// A full-backup schedule on a daemon without the creation opt-in is skipped
// with the reason the page already knows, never silently.
func TestBackupScheduler_fullBackupNeedsTheOptIn(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, false)
	e := addScheduled(t, reg, console.BackupMethodFull)
	now := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), now.Add(-time.Hour))
	b.tick(context.Background(), now)
	if st := sup.Status(e.ID); st.State != "idle" {
		t.Fatalf("a dump was started without the opt-in: %+v", st)
	}
	_, skip := sup.history.LastScheduled(e.ID)
	if skip == nil || skip.SkipReason == "" {
		t.Fatalf("the refusal was not recorded: %+v", skip)
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
// producers. Without it the page could not tell the schedule's runs from
// the button's. The dump path fails fast on an unparseable source DSN, the
// rebuild on an empty baseline directory; both still record.
func TestScheduledRuns_stampTheTrigger(t *testing.T) {
	_, _, sup := newScheduleFixture(t, true)
	sup.jobs["d"] = &console.BaselineStatus{State: "running"}
	sup.run(console.BaselineRequest{ServerID: "d", ServerName: "d", SourceDSN: "not a dsn",
		Trigger: console.BaselineRunTriggerScheduled})
	if run, _ := sup.history.LastScheduled("d"); run == nil || run.Kind != console.BaselineRunDump || run.Error == "" {
		t.Fatalf("dump record = %+v, want a failed scheduled dump", run)
	}
	// And a manual one is NOT attributed to the schedule.
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

// Running is answered against the supervisor slot of the method that was
// started, and only for a job that began at or after the scheduler's own
// stamp.
func TestBackupScheduler_runningMatchesTheJobItStarted(t *testing.T) {
	b, _, sup := newScheduleFixture(t, true)
	b.started["a"] = scheduledStart{method: console.BackupMethodRefresh, at: "2026-08-28T09:00:00Z"}
	sup.refreshes["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:00:00Z"}
	if !b.ScheduleState("a").Running {
		t.Fatal("the scheduled rebuild in flight is not reported running")
	}
	sup.refreshes["a"].State = "succeeded"
	if b.ScheduleState("a").Running {
		t.Fatal("a finished job is still reported running")
	}
	// A dump running is not the rebuild the schedule started.
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:00:01Z"}
	if b.ScheduleState("a").Running {
		t.Fatal("a manual dump was reported as the scheduled rebuild")
	}
	// Same kind, but begun BEFORE the schedule's stamp: a manual Create
	// backup that was already in flight (the slot itself was skipped). Not
	// ours. At or after the stamp, it is.
	b.started["a"] = scheduledStart{method: console.BackupMethodFull, at: "2026-08-28T10:00:00Z"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:30:00Z"}
	if b.ScheduleState("a").Running {
		t.Fatal("a dump that began before the scheduled slot was reported as the schedule's")
	}
	sup.jobs["a"].Since = "2026-08-28T10:00:00Z"
	if !b.ScheduleState("a").Running {
		t.Fatal("the scheduled dump in flight is not reported running")
	}
	if b.ScheduleState("never").Running || b.ScheduleState("never").LastStartedAt != "" {
		t.Fatal("an unknown server reported state")
	}
}

// A panic inside a tick must not reach the daemon that is also capturing.
func TestBackupScheduler_tickSurvivesAPanic(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	addScheduled(t, reg, console.BackupMethodRefresh)
	b.seen = nil // a nil map write panics inside crossed
	b.tick(context.Background(), time.Now().UTC())
}
