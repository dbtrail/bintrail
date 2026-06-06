package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/console"
	"github.com/dbtrail/bintrail/internal/indexer"
)

// monitorSupervisor is the control plane behind `bintrail up --console`: it
// implements console.MonitorController and runs one supervised streamOne per
// monitored registry entry. The approved architecture is index-DATABASE per
// source — each monitored entry streams into its own database
// (bintrail_idx_<entry-id>) on the daemon's index MySQL server, so per-source
// state stays structurally isolated (single-row stream_state per DB, no
// cross-source schema_snapshots confusion) and the console's existing
// multi-connection switcher lists it with zero new read code.
//
// The supervisor is a WRITER — it creates databases, tables, and runs
// EnsureSchema on the per-source DBs it provisions, exactly the role the cmd
// layer already plays for the boot DSN. The console's "registry servers are
// never migrated by request handlers" invariant is untouched: the console
// bundle for a monitored entry still opens the (already-provisioned) DB
// read-only.
type monitorSupervisor struct {
	// baseCtx is the daemon's lifecycle: streams derive from it, NOT from the
	// HTTP request that started them.
	baseCtx context.Context
	// bootIndexDSN is the daemon's index server connection; per-source
	// databases are derived from it (same server, same credentials,
	// different DBName).
	bootIndexDSN string
	// registry lists the other monitored entries — Doctor compares a
	// candidate source against them for replica/duplicate detection. May be
	// nil (tests); the check is skipped then.
	registry *console.Registry
	// streamFn runs one supervised stream; a seam for unit tests, streamOne
	// in production.
	streamFn func(ctx context.Context, cfg streamConfig) error

	mu   sync.Mutex
	jobs map[string]*monitorJob
	wg   sync.WaitGroup
}

// Supervisor health thresholds. Vars, not consts, so tests can shrink them.
var (
	// monitorStalledAfter: a running stream that has neither saved a
	// checkpoint nor flushed a batch for this long is reported "stalled".
	// The checkpoint ticker fires even with zero events, so an idle-but-
	// healthy source never trips this — only a genuinely wedged loop does.
	monitorStalledAfter = 5 * time.Minute
	// monitorGiveUpAfter: a stream that has been crash-looping continuously
	// for this long (no healthy run in between) stops retrying and reports
	// a permanent "failed" — the circuit breaker against a misconfigured
	// source retrying forever. Press Start (or restart the daemon) to re-arm.
	monitorGiveUpAfter = 6 * time.Hour
	// Crash-loop backoff: retry delay doubles from base to cap; a run that
	// survives monitorHealthyReset resets both the attempt counter and the
	// circuit-breaker clock.
	monitorBackoffBase  = 15 * time.Second
	monitorBackoffCap   = 5 * time.Minute
	monitorHealthyReset = 10 * time.Minute
)

// monitorJob is one supervised stream.
type monitorJob struct {
	cancel context.CancelFunc
	done   chan struct{}
	// indexDSN is the entry's per-source index database — set once at job
	// creation (before the job is published), immutable after. Stop uses it
	// to clear the durable gap-loss record with its own short-lived
	// connection (lockDB belongs to the run goroutine; sharing it from Stop
	// would race Start's provisioning window).
	indexDSN string
	// lockDB's single dedicated connection holds the advisory lock for this
	// entry; closing it releases the lock. Written by Start before the run
	// goroutine launches and read only by run's teardown — never from other
	// goroutines.
	lockDB *sql.DB

	mu      sync.Mutex
	state   string // stored: pending|running|failed|stopped
	lastErr string
	since   time.Time
	// lastProgress is when the stream last proved liveness (checkpoint saved
	// or batch flushed) — feeds the derived "stalled" state.
	lastProgress time.Time
	// lostPosition, when non-empty, records that an unfillable binlog gap
	// forced an auto-advance: events were permanently lost. The fact is
	// also persisted in stream_state (gap_lost_at/_detail) and re-hydrated
	// by Start, so it survives daemon restarts; only an explicit Stop (the
	// operator's acknowledgment) clears it — feeds the derived
	// "lost_position" state.
	lostPosition string
}

func (j *monitorJob) set(state, lastErr string) {
	j.mu.Lock()
	j.state, j.lastErr, j.since = state, lastErr, time.Now().UTC()
	j.mu.Unlock()
}

// storedState returns the raw state-machine value (pending|running|failed|
// stopped) without the derived stalled/lost_position presentation — for
// callers that need goroutine liveness, not operator-facing health.
func (j *monitorJob) storedState() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.state
}

// progress records stream liveness and performs the pending→running flip:
// the supervisor reports "pending" from launch until the stream saves its
// first checkpoint (or flushes its first batch) — before that the goroutine
// is still connecting/snapshotting and a RUNNING badge would lie (#407).
func (j *monitorJob) progress() {
	j.mu.Lock()
	j.lastProgress = time.Now().UTC()
	if j.state == "pending" {
		j.state, j.lastErr, j.since = "running", "", j.lastProgress
	}
	j.mu.Unlock()
}

func (j *monitorJob) markLostPosition(detail string) {
	j.mu.Lock()
	j.lostPosition = detail
	j.mu.Unlock()
}

// streamHooks wires this job as its stream's liveness observer.
func (j *monitorJob) streamHooks() *streamHooks {
	return &streamHooks{
		OnCheckpoint:     j.progress,
		OnIndexed:        func(int64) { j.progress() },
		OnGapAutoAdvance: j.markLostPosition,
	}
}

// snapshot reports the job's state, deriving the two "running but not
// healthy" presentations at read time (the stored state machine stays
// pending|running|failed|stopped):
//   - "stalled":       running, but no checkpoint/flush for monitorStalledAfter
//   - "lost_position": running, but a gap auto-advance lost events
//
// A wedged stream beats a historical data-loss note, so stalled wins when
// both apply.
func (j *monitorJob) snapshot() console.MonitorStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	st := console.MonitorStatus{State: j.state, LastError: j.lastErr}
	if j.state == "running" {
		if idle := time.Since(j.lastProgress); !j.lastProgress.IsZero() && idle > monitorStalledAfter {
			st.State = "stalled"
			st.LastError = fmt.Sprintf("no progress for %s: no events indexed and no checkpoint saved — the stream is connected but not advancing", idle.Round(time.Second))
		} else if j.lostPosition != "" {
			st.State = "lost_position"
			st.LastError = j.lostPosition
		}
	}
	if !j.since.IsZero() {
		st.Since = j.since.Format(time.RFC3339)
	}
	return st
}

// newMonitorSupervisor builds the control plane. reg may be nil (tests) —
// Doctor's replica/duplicate detection is skipped then.
func newMonitorSupervisor(baseCtx context.Context, bootIndexDSN string, reg *console.Registry) *monitorSupervisor {
	return &monitorSupervisor{
		baseCtx:      baseCtx,
		bootIndexDSN: bootIndexDSN,
		registry:     reg,
		streamFn:     streamOne,
		jobs:         map[string]*monitorJob{},
	}
}

// dbNameRE is the only shape the supervisor will CREATE DATABASE for. Derived
// names always match; a hand-edited registry DSN with anything fancier is
// refused rather than interpolated into DDL.
var dbNameRE = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)

// DeriveIndexDSN implements console.MonitorController: the daemon's index
// server with a per-entry database name.
func (m *monitorSupervisor) DeriveIndexDSN(entryID string) (string, error) {
	if !dbNameRE.MatchString(entryID) {
		return "", fmt.Errorf("entry id %q cannot form a database name", entryID)
	}
	cfg, err := mysql.ParseDSN(m.bootIndexDSN)
	if err != nil {
		return "", fmt.Errorf("daemon index DSN: %s", scrubMonitorErr(err, m.bootIndexDSN))
	}
	cfg.DBName = "bintrail_idx_" + entryID
	return cfg.FormatDSN(), nil
}

// Doctor implements console.MonitorController by running the same preflight
// as `bintrail doctor` and mapping the report to the console's wire shape.
// The entry's index DB may not exist yet — doctor treats that as fine (init
// creates it), per #384.
func (m *monitorSupervisor) Doctor(ctx context.Context, e console.ServerEntry) (*console.DoctorReport, error) {
	if e.SourceDSN == "" {
		return nil, errors.New("entry has no source configured")
	}
	r := buildDoctorReport(ctx, e.SourceDSN, e.DSN, e.Schemas)
	out := &console.DoctorReport{
		Passed:   r.Passed,
		Failed:   r.Failed,
		Warnings: r.Warnings,
		Skipped:  r.Skipped,
		Checks:   make([]console.DoctorCheck, len(r.Checks)),
	}
	for i, c := range r.Checks {
		out.Checks[i] = console.DoctorCheck{
			Name:        c.Name,
			Status:      string(c.Status),
			Detail:      scrubMonitorErrText(c.Detail, e.SourceDSN, e.DSN),
			Remediation: c.Remediation,
		}
	}

	// Replica/duplicate detection against the other monitored entries —
	// warn-only per the approved decision (#402): an amber card, never a
	// block. Supervisor-only: the standalone `bintrail doctor` has no
	// registry to compare against.
	if c := m.replicaOverlapCheck(ctx, e); c != nil {
		c.Detail = scrubMonitorErrText(c.Detail, e.SourceDSN, e.DSN)
		out.Checks = append(out.Checks, *c)
		switch c.Status {
		case "warn":
			out.Warnings++
		case "skip":
			out.Skipped++
		default:
			out.Passed++
		}
	}
	return out, nil
}

// Start implements console.MonitorController: provision the per-source index
// database (CREATE DATABASE + tables + schema migration), take the advisory
// lock, and launch the supervised stream on the daemon's lifecycle.
// Idempotent for an entry that is already running or starting.
func (m *monitorSupervisor) Start(ctx context.Context, e console.ServerEntry) error {
	if e.SourceDSN == "" {
		return errors.New("entry has no source configured")
	}
	if e.DSN == "" {
		return errors.New("entry has no index DSN (derive or set one first)")
	}

	m.mu.Lock()
	if j, ok := m.jobs[e.ID]; ok {
		// Gate on the STORED state, not the derived presentation: stalled
		// and lost_position are running variants (the goroutine still holds
		// the advisory lock; superseding it would deadlock on our own lock —
		// restart a stalled stream via Stop+Start), and checking the stored
		// machine means new derived states can never fall through to the
		// cancel below by omission.
		switch j.storedState() {
		case "running", "pending":
			m.mu.Unlock()
			return nil // idempotent
		}
		// A failed/stopped job is superseded below; make sure it is dead.
		j.cancel()
	}
	// Reserve the slot as pending while provisioning runs outside the lock.
	jobCtx, cancel := context.WithCancel(m.baseCtx)
	job := &monitorJob{cancel: cancel, done: make(chan struct{}), indexDSN: e.DSN}
	job.set("pending", "")
	m.jobs[e.ID] = job
	m.mu.Unlock()

	fail := func(err error) error {
		scrubbed := scrubMonitorErr(err, e.SourceDSN, e.DSN)
		job.set("failed", scrubbed)
		cancel()
		close(job.done)
		return errors.New(scrubbed)
	}

	// ── Provision the per-source index database ──────────────────────────
	idxCfg, err := mysql.ParseDSN(e.DSN)
	if err != nil {
		return fail(fmt.Errorf("index DSN: %w", err))
	}
	if idxCfg.DBName == "" || !dbNameRE.MatchString(idxCfg.DBName) {
		return fail(fmt.Errorf("index database name %q is not provisionable", idxCfg.DBName))
	}
	if err := ensureDatabase(idxCfg, idxCfg.DBName); err != nil {
		return fail(err)
	}
	idxDB, err := config.Connect(e.DSN)
	if err != nil {
		return fail(fmt.Errorf("connect provisioned index: %w", err))
	}
	if err := createIndexTables(ctx, idxDB, 48, false, nil); err != nil {
		idxDB.Close()
		return fail(err)
	}
	if err := indexer.EnsureSchema(idxDB); err != nil {
		idxDB.Close()
		return fail(fmt.Errorf("schema migration: %w", err))
	}
	// Re-hydrate a durable gap-loss record (#402): once the stream persisted
	// its advanced checkpoint, a restarted daemon sees no gap and the hook
	// never re-fires — the lost_position state must be restored from
	// stream_state or the data loss silently un-surfaces. Cleared only by an
	// explicit Stop (the operator's acknowledgment). ErrNoRows is the normal
	// fresh-start case; any other error means a recorded loss may go
	// un-surfaced this run, which deserves a breadcrumb.
	var gapDetail sql.NullString
	err = idxDB.QueryRowContext(ctx,
		`SELECT gap_lost_detail FROM stream_state WHERE id = 1`).Scan(&gapDetail)
	switch {
	case err == nil && gapDetail.Valid && gapDetail.String != "":
		job.markLostPosition(gapDetail.String)
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		slog.Warn("could not re-hydrate gap-loss record; a recorded data loss may not be re-surfaced this run",
			"entry", e.ID, "error", scrubMonitorErr(err, e.SourceDSN, e.DSN))
	}
	idxDB.Close()

	// ── Advisory lock: refuse to double-stream one entry ─────────────────
	// GET_LOCK is held by a dedicated connection on the index server; a
	// second daemon pointed at the same registry fails here with a clear
	// message instead of double-indexing the source. Closing lockDB (job
	// teardown) releases it.
	lockDB, err := config.Connect(e.DSN)
	if err != nil {
		return fail(fmt.Errorf("connect for advisory lock: %w", err))
	}
	lockDB.SetMaxOpenConns(1)
	lockDB.SetMaxIdleConns(1)
	lockDB.SetConnMaxIdleTime(0)
	lockDB.SetConnMaxLifetime(0)
	var got int
	lockName := "bintrail_monitor_" + e.ID
	if err := lockDB.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lockName).Scan(&got); err != nil {
		lockDB.Close()
		return fail(fmt.Errorf("acquire advisory lock: %w", err))
	}
	if got != 1 {
		lockDB.Close()
		return fail(fmt.Errorf("another bintrail process is already monitoring this server (advisory lock %s is held)", lockName))
	}
	job.lockDB = lockDB

	// ── Launch the supervised stream ─────────────────────────────────────
	serverID := e.SourceServerID
	if serverID == 0 {
		serverID, err = deriveServerID(e.SourceDSN)
		if err != nil {
			lockDB.Close()
			return fail(fmt.Errorf("derive server id: %w", err))
		}
	}
	cfg := streamConfig{
		IndexDSN:  e.DSN,
		SourceDSN: e.SourceDSN,
		ServerID:  serverID,
		BatchSize: 1000,
		Schemas:   e.Schemas,
		// MetricsSource keys this stream's Prometheus series; MetricsAddr
		// stays empty on purpose — the daemon serves ONE /metrics endpoint
		// for all supervised streams (per-stream binds would conflict).
		MetricsSource: e.ID,
		Checkpoint:    10,
		SSLMode:       "preferred",
		Format:        "text",
		GapTimeout:    30,
		Hooks:         job.streamHooks(),
	}

	m.wg.Add(1)
	go m.run(jobCtx, job, e, cfg)
	return nil
}

// run supervises one stream with crash-loop backoff: a stream that errors is
// restarted (15s doubling to a 5m cap, counter reset after 10 healthy
// minutes); the job reports "failed" with the scrubbed error between
// attempts. The job stays "pending" from launch until the stream's first
// checkpoint/flush flips it to "running" via the liveness hooks. A stream
// that crash-loops continuously for monitorGiveUpAfter trips the circuit
// breaker: permanent "failed", no more retries, advisory lock released —
// Start (or a daemon restart) re-arms it. Cancellation (stop verb / daemon
// shutdown) exits cleanly.
func (m *monitorSupervisor) run(ctx context.Context, job *monitorJob, e console.ServerEntry, cfg streamConfig) {
	defer m.wg.Done()
	defer close(job.done)
	defer func() {
		if job.lockDB != nil {
			job.lockDB.Close() // releases the advisory lock
		}
	}()

	attempt := 0
	var crashLoopSince time.Time // first failure of the current loop; zero = healthy
	for {
		job.set("pending", "")
		started := time.Now()
		err := m.streamFn(ctx, cfg)
		if ctx.Err() != nil || err == nil {
			job.set("stopped", "")
			return
		}
		if time.Since(started) > monitorHealthyReset {
			attempt = 0
			crashLoopSince = time.Time{}
		}
		if crashLoopSince.IsZero() {
			crashLoopSince = started
		}
		scrubbed := scrubMonitorErr(err, e.SourceDSN, e.DSN)
		if looping := time.Since(crashLoopSince); looping > monitorGiveUpAfter {
			slog.Error("monitored stream crash-looped past the give-up threshold; not retrying",
				"server", e.Name, "entry", e.ID, "looping_for", looping.Round(time.Minute), "error", scrubbed)
			job.set("failed", fmt.Sprintf("%s (gave up after %s of crash-looping — fix the issue, then press Start to retry)",
				scrubbed, looping.Round(time.Minute)))
			return
		}
		delay := min(monitorBackoffBase<<attempt, monitorBackoffCap)
		attempt++
		slog.Warn("monitored stream failed; retrying with backoff",
			"server", e.Name, "entry", e.ID, "delay", delay, "error", scrubbed)
		job.set("failed", scrubbed+" (retrying)")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			job.set("stopped", "")
			return
		}
	}
}

// Stop implements console.MonitorController. Idempotent; waits briefly for
// the stream to flush its final checkpoint. An explicit Stop is also the
// operator's acknowledgment of a recorded data loss: it clears the durable
// gap-loss record so the next Start begins clean. Daemon shutdown does NOT
// come through here (Shutdown cancels jobs directly), so a restart preserves
// the record.
func (m *monitorSupervisor) Stop(ctx context.Context, entryID string) error {
	m.mu.Lock()
	job, ok := m.jobs[entryID]
	if ok {
		delete(m.jobs, entryID)
	}
	m.mu.Unlock()
	if !ok {
		return nil
	}
	// Clear with a short-lived connection of our own: lockDB belongs to the
	// run goroutine (reading it here would race Start's provisioning window,
	// and it is already closed when the stream gave up or exited). On
	// failure the record survives — the next Start re-raises lost_position,
	// which fails safe: a real past loss is re-surfaced, never dropped.
	if job.indexDSN != "" {
		if db, err := config.Connect(job.indexDSN); err != nil {
			slog.Warn("could not clear gap-loss record on stop", "entry", entryID, "error", scrubMonitorErrText(err.Error(), job.indexDSN))
		} else {
			if _, err := db.ExecContext(ctx, `UPDATE stream_state
				SET gap_lost_at = NULL, gap_lost_detail = NULL WHERE id = 1`); err != nil {
				slog.Warn("could not clear gap-loss record on stop", "entry", entryID, "error", scrubMonitorErrText(err.Error(), job.indexDSN))
			}
			db.Close()
		}
	}
	job.cancel()
	select {
	case <-job.done:
	case <-time.After(15 * time.Second):
		return errors.New("stream did not stop within 15s; it will finish shutting down in the background")
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Status implements console.MonitorController.
func (m *monitorSupervisor) Status(entryID string) console.MonitorStatus {
	m.mu.Lock()
	job, ok := m.jobs[entryID]
	m.mu.Unlock()
	if !ok {
		return console.MonitorStatus{State: "stopped"}
	}
	return job.snapshot()
}

// Reconcile starts every registry entry whose desired state is "monitoring"
// — called once at daemon boot so a restart resumes exactly what the
// operator had running (streams pick up from their stream_state checkpoints).
// Failures are recorded on the job (visible in the UI) and logged, never
// fatal to the daemon.
func (m *monitorSupervisor) Reconcile(reg *console.Registry) {
	for _, e := range reg.List() {
		if !e.MonitorDesired || e.SourceDSN == "" {
			continue
		}
		if err := m.Start(m.baseCtx, e); err != nil {
			slog.Warn("boot reconcile: could not start monitoring", "server", e.Name, "entry", e.ID, "error", err)
		}
	}
}

// Shutdown stops every stream and waits for their final checkpoints.
func (m *monitorSupervisor) Shutdown() {
	m.mu.Lock()
	for _, j := range m.jobs {
		j.cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// scrubMonitorErr strips every DSN (and its password) from an error before it
// is stored on a job or returned to the console — monitor errors travel to
// the browser.
func scrubMonitorErr(err error, dsns ...string) string {
	return scrubMonitorErrText(err.Error(), dsns...)
}

func scrubMonitorErrText(msg string, dsns ...string) string {
	for _, dsn := range dsns {
		if dsn == "" {
			continue
		}
		msg = strings.ReplaceAll(msg, dsn, "<dsn>")
		if cfg, err := mysql.ParseDSN(dsn); err == nil && cfg.Passwd != "" {
			msg = strings.ReplaceAll(msg, cfg.Passwd, "***")
		}
	}
	return msg
}
