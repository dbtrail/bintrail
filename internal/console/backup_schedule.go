package console

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// Backup schedule (#1442): a per-server timer for the two ways this daemon can
// produce a backup unattended. Stored on the server's registry entry, edited
// from the Backups page, and consumed by the watch daemon's schedule loop,
// which reads the registry every tick so a schedule saved from the page
// applies without a restart. (The registry is in memory once loaded: a
// schedule typed into the file by hand is seen after the next start.)
//
// The grid is FIXED, not relative to the last run: slots sit at
// epoch + At + k*Every for every integer k. "every 1d at 03:00" is 03:00 UTC
// daily, "every 6h at 03:00" is 03:00/09:00/15:00/21:00, and the next run is
// computable from the clock alone, which is what lets the page show it and
// what keeps a restart from shifting the whole schedule to whenever the
// daemon happened to come back.

// BackupMethod values. The literals are the file/wire format.
const (
	// BackupMethodFull takes a full backup from the source database (mydumper,
	// or pgbaseline for PostgreSQL): the same job the Create backup button
	// runs, on a timer. It reads production and needs the console's
	// baseline-creation opt-in.
	BackupMethodFull = "backup"
	// BackupMethodRefresh rebuilds the newest backup from the recorded change
	// history alone (the same fold as --baseline-refresh-interval), reading
	// nothing from the source. It needs a previous backup on local disk to
	// start from.
	BackupMethodRefresh = "refresh"
)

// BackupScheduleMinEvery is the shortest interval a schedule accepts. A full
// backup every few minutes is a footgun on the source, and a rebuild that
// often is one on the disk (a rebuild's output is never uploaded, so
// retention cannot reclaim it); the floor is generous enough for every real
// cadence and low enough to try the feature out.
const BackupScheduleMinEvery = 15 * time.Minute

// backupScheduleMinEveryText is the floor as an operator types it, for the
// refusal message. Kept next to the constant so the two cannot drift apart
// unnoticed (a test pins them equal).
const backupScheduleMinEveryText = "15m"

// BackupSchedule is the per-server schedule as stored in the registry. The
// fields hold the operator-typed strings so they round-trip exactly; Parse
// turns them into durations with the same grammar as the daemon flags.
type BackupSchedule struct {
	// Every is the interval: Nm, Nh or Nd (cliutil.ParseInterval).
	Every string `yaml:"every"`
	// At is the UTC clock time the grid is aligned to, HH:MM. Empty means
	// 00:00. For whole days (1d, 7d) it is the time of day the backup runs;
	// for an interval that divides a day (6h, 15m) it is the alignment of the
	// slots; an interval that does not divide a day evenly (5h, 36h) drifts
	// through the day, and the page shows the next run so that is visible.
	At string `yaml:"at,omitempty"`
	// Method is BackupMethodFull or BackupMethodRefresh. Empty means full.
	Method string `yaml:"method,omitempty"`
	// Extra preserves future keys the way ServerEntry.Extra does.
	Extra map[string]any `yaml:",inline"`
}

// Identity is the schedule as a comparable string (every|at|method, with the
// defaults resolved), for a consumer that must notice a CHANGED schedule: the
// loop treats an edit as a new schedule, so the "first observation is silent"
// rule covers edits the way it covers adds. Unparseable schedules identify
// by their raw fields.
func (b BackupSchedule) Identity() string {
	if n, err := b.Normalized(); err == nil {
		return n.Every + "|" + n.At + "|" + n.Method
	}
	return b.Every + "|" + b.At + "|" + b.Method
}

// ParsedBackupSchedule is a validated schedule, ready for slot arithmetic.
type ParsedBackupSchedule struct {
	Every  time.Duration
	At     time.Duration // offset from midnight UTC
	Method string
}

// Parse validates the schedule and resolves its defaults. Every error names
// the field and the accepted grammar, because the message is what the Backups
// page shows back.
func (b BackupSchedule) Parse() (ParsedBackupSchedule, error) {
	var p ParsedBackupSchedule
	every, err := cliutil.ParseInterval(strings.TrimSpace(b.Every))
	if err != nil {
		return p, fmt.Errorf("every: %w", err)
	}
	if every < BackupScheduleMinEvery {
		return p, fmt.Errorf("every: %s is too often; the shortest schedule is %s",
			strings.TrimSpace(b.Every), backupScheduleMinEveryText)
	}
	at, err := parseClockTime(b.At)
	if err != nil {
		return p, fmt.Errorf("at: %w", err)
	}
	method, err := parseBackupMethod(b.Method)
	if err != nil {
		return p, err
	}
	return ParsedBackupSchedule{Every: every, At: at, Method: method}, nil
}

// Normalized returns the schedule with its defaults spelled out, for storage:
// "" becomes "00:00" and BackupMethodFull, so the file says what runs.
func (b BackupSchedule) Normalized() (BackupSchedule, error) {
	p, err := b.Parse()
	if err != nil {
		return BackupSchedule{}, err
	}
	return BackupSchedule{
		Every:  strings.TrimSpace(b.Every),
		At:     formatClockTime(p.At),
		Method: p.Method,
		Extra:  b.Extra,
	}, nil
}

// parseClockTime parses HH:MM (24h, UTC) into an offset from midnight. Empty
// is midnight.
func parseClockTime(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	hh, mm, ok := strings.Cut(s, ":")
	if !ok || len(hh) == 0 || len(hh) > 2 || len(mm) != 2 {
		return 0, fmt.Errorf("%q is not a time of day; use HH:MM in UTC, e.g. 03:00", s)
	}
	h, err1 := strconv.Atoi(hh)
	m, err2 := strconv.Atoi(mm)
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, fmt.Errorf("%q is not a time of day; use HH:MM in UTC, e.g. 03:00", s)
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, nil
}

func formatClockTime(d time.Duration) string {
	return fmt.Sprintf("%02d:%02d", int(d.Hours()), int(d.Minutes())%60)
}

func parseBackupMethod(s string) (string, error) {
	switch strings.TrimSpace(s) {
	case "", BackupMethodFull:
		return BackupMethodFull, nil
	case BackupMethodRefresh:
		return BackupMethodRefresh, nil
	}
	return "", fmt.Errorf("method: %q is not one of %q (full backup from the database) or %q (rebuild from the change history)",
		s, BackupMethodFull, BackupMethodRefresh)
}

// scheduleEpoch anchors the slot grid. Any fixed instant at midnight UTC
// works; the Unix epoch is the one nobody has to look up.
var scheduleEpoch = time.Unix(0, 0).UTC()

// SlotAtOrBefore returns the latest slot at or before now.
func (p ParsedBackupSchedule) SlotAtOrBefore(now time.Time) time.Time {
	base := scheduleEpoch.Add(p.At)
	elapsed := now.UTC().Sub(base)
	k := elapsed / p.Every
	if elapsed < 0 && elapsed%p.Every != 0 {
		k-- // integer division truncates toward zero; the grid wants the floor
	}
	return base.Add(k * p.Every)
}

// NextRun returns the first slot strictly after now.
func (p ParsedBackupSchedule) NextRun(now time.Time) time.Time {
	return p.SlotAtOrBefore(now).Add(p.Every)
}

// ErrBackupScheduleNotRunnable is the class of every "this schedule cannot run
// on this daemon as configured" refusal; errors.Is matches it. The reason is
// carried separately (ScheduleRefusal) because it is what the page shows.
var ErrBackupScheduleNotRunnable = errors.New("the schedule cannot run")

// ScheduleRefusal is CheckBackupSchedule's error: the class plus the reason.
type ScheduleRefusal struct{ Reason string }

func (r *ScheduleRefusal) Error() string {
	return ErrBackupScheduleNotRunnable.Error() + ": " + r.Reason
}

// Is makes errors.Is(err, ErrBackupScheduleNotRunnable) true.
func (r *ScheduleRefusal) Is(target error) bool { return target == ErrBackupScheduleNotRunnable }

func notRunnable(reason string) error { return &ScheduleRefusal{Reason: reason} }

// RefusalReason returns the reason inside a ScheduleRefusal, or the whole
// message for any other error.
func RefusalReason(err error) string {
	var r *ScheduleRefusal
	if errors.As(err, &r) {
		return r.Reason
	}
	return err.Error()
}

// The refusal texts the checker and the schedule endpoints share, so a saved
// schedule is reported with the same words a write is refused with.
const (
	scheduleRefusalReadOnly = "scheduled backups run in the watch daemon (bintrail-console watch), not the read-only console"
	scheduleRefusalNoLoop   = "backup features are turned off on this daemon (BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1 and there is no --baseline-refresh-interval), so nothing can run a schedule"
	scheduleRefusalNoDumps  = "creating backups from the console is turned off on this daemon (BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1); a schedule can still rebuild from the change history"
)

// BackupScheduleGates is what the daemon can do, as the schedule checker
// needs to know it. All of it is decided at boot.
type BackupScheduleGates struct {
	// LoopRunning: this process runs the schedule loop at all (a watch daemon
	// with a baseline supervisor).
	LoopRunning bool
	// FullBackups: this process may take full backups from the source (the
	// baseline-creation opt-in). A refresh schedule does not need it.
	FullBackups bool
	// FullBackupsErr, when set, is why a full backup cannot START even with
	// the opt-in on: the lock-mode misconfiguration the supervisor refuses
	// every MySQL dump with. Reported as the reason so a schedule does not
	// show a next run it can never keep.
	FullBackupsErr string
	// ReadOnlyConsole: this process is the standalone `serve` console, which
	// runs no loop of any kind; names the daemon in the reason.
	ReadOnlyConsole bool
}

// CheckBackupSchedule reports whether e's schedule can run on a daemon with
// these gates, and why not. Nil error means it can. Called on PUT (so a
// schedule that could never run is refused with the reason instead of saved)
// and on every listing (so a schedule the environment later invalidated is
// reported, not silently skipped by the loop).
func CheckBackupSchedule(e ServerEntry, sched BackupSchedule, gates BackupScheduleGates) error {
	p, err := sched.Parse()
	if err != nil {
		return notRunnable(err.Error())
	}
	if gates.ReadOnlyConsole {
		return notRunnable(scheduleRefusalReadOnly)
	}
	if !gates.LoopRunning {
		return notRunnable(scheduleRefusalNoLoop)
	}
	switch p.Method {
	case BackupMethodFull:
		if !gates.FullBackups {
			return notRunnable(scheduleRefusalNoDumps)
		}
		if gates.FullBackupsErr != "" && !e.IsPostgres() {
			// Same scope as the supervisor's refusal: a PostgreSQL dump never
			// consults the MySQL lock mode.
			return notRunnable(gates.FullBackupsErr)
		}
		if err := baselineTriggerPrecheck(e); err != nil {
			return notRunnable(err.Error())
		}
	case BackupMethodRefresh:
		if e.DSN == "" {
			return notRunnable("this server has no index connection to rebuild from")
		}
		if e.BaselineDir == "" {
			// Same constraint as the refresh loop and the point-in-time restore:
			// the fold reads the previous snapshot and writes the new one on
			// disk, so it needs the server's own local directory.
			if e.BaselineS3 != "" {
				return notRunnable("this server keeps its backups only in S3; a rebuild from the change history needs a local backup directory (Edit → Advanced)")
			}
			return notRunnable("this server has no backup directory of its own; set one first (Edit → Advanced)")
		}
	}
	return nil
}

// BackupScheduleState is the schedule loop's in-memory view of one server,
// for the Backups page: the job the schedule last started in this process,
// as the supervisor's slot reports it. The durable view (runs and skips
// across restarts) is the baseline run history; this one exists because the
// history can be unavailable (an unreadable file) or silent (a job that
// panicked writes no record), and a failed scheduled backup must reach the
// page either way.
type BackupScheduleState struct {
	// LastStartedAt is when the loop last started a job for this server
	// (RFC3339 UTC), empty if never in this process.
	LastStartedAt string
	// LastMethod is that job's method.
	LastMethod string
	// Last is the supervisor's status for THAT job, nil once the slot holds
	// another job (a later manual backup) or when nothing was started here.
	// Running is Last.State == "running".
	Last *BaselineStatus
	// Running: the job this schedule last started has not finished.
	Running bool
}

// BackupScheduleReporter is the schedule loop as the console sees it. nil when
// this process runs no loop (the read-only console, or a watch daemon with
// every baseline feature off), which the schedule endpoints refuse on and
// the listing reports as not runnable.
type BackupScheduleReporter interface {
	ScheduleState(serverID string) BackupScheduleState
	// FullBackups reports whether the loop may run BackupMethodFull schedules
	// (the daemon's baseline-creation opt-in), and, when it may, whether the
	// supervisor would still refuse to start one (a lock-mode
	// misconfiguration): nil when full backups can start.
	FullBackups() (enabled bool, refusal error)
}
