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

	mu   sync.Mutex
	jobs map[string]*monitorJob
	wg   sync.WaitGroup
}

// monitorJob is one supervised stream.
type monitorJob struct {
	cancel context.CancelFunc
	done   chan struct{}
	// lockDB's single dedicated connection holds the advisory lock for this
	// entry; closing it releases the lock.
	lockDB *sql.DB

	mu      sync.Mutex
	state   string // pending|running|failed|stopped
	lastErr string
	since   time.Time
}

func (j *monitorJob) set(state, lastErr string) {
	j.mu.Lock()
	j.state, j.lastErr, j.since = state, lastErr, time.Now().UTC()
	j.mu.Unlock()
}

func (j *monitorJob) snapshot() console.MonitorStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	st := console.MonitorStatus{State: j.state, LastError: j.lastErr}
	if !j.since.IsZero() {
		st.Since = j.since.Format(time.RFC3339)
	}
	return st
}

func newMonitorSupervisor(baseCtx context.Context, bootIndexDSN string) *monitorSupervisor {
	return &monitorSupervisor{
		baseCtx:      baseCtx,
		bootIndexDSN: bootIndexDSN,
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
		st := j.snapshot().State
		if st == "running" || st == "pending" {
			m.mu.Unlock()
			return nil // idempotent
		}
		// A failed/stopped job is superseded below; make sure it is dead.
		j.cancel()
	}
	// Reserve the slot as pending while provisioning runs outside the lock.
	jobCtx, cancel := context.WithCancel(m.baseCtx)
	job := &monitorJob{cancel: cancel, done: make(chan struct{})}
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
		IndexDSN:   e.DSN,
		SourceDSN:  e.SourceDSN,
		ServerID:   serverID,
		BatchSize:  1000,
		Schemas:    e.Schemas,
		Checkpoint: 10,
		SSLMode:    "preferred",
		Format:     "text",
		GapTimeout: 30,
	}

	m.wg.Add(1)
	go m.run(jobCtx, job, e, cfg)
	return nil
}

// run supervises one stream with crash-loop backoff: a stream that errors is
// restarted (15s doubling to a 5m cap, counter reset after 10 healthy
// minutes); the job reports "failed" with the scrubbed error between
// attempts. Cancellation (stop verb / daemon shutdown) exits cleanly.
func (m *monitorSupervisor) run(ctx context.Context, job *monitorJob, e console.ServerEntry, cfg streamConfig) {
	defer m.wg.Done()
	defer close(job.done)
	defer func() {
		if job.lockDB != nil {
			job.lockDB.Close() // releases the advisory lock
		}
	}()

	const (
		backoffBase  = 15 * time.Second
		backoffCap   = 5 * time.Minute
		healthyReset = 10 * time.Minute
	)
	attempt := 0
	for {
		job.set("running", "")
		started := time.Now()
		err := streamOne(ctx, cfg)
		if ctx.Err() != nil || err == nil {
			job.set("stopped", "")
			return
		}
		if time.Since(started) > healthyReset {
			attempt = 0
		}
		delay := min(backoffBase<<attempt, backoffCap)
		attempt++
		scrubbed := scrubMonitorErr(err, e.SourceDSN, e.DSN)
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
// the stream to flush its final checkpoint.
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
