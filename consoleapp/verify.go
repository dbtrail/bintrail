package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/pgverifysource"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// verifySupervisor implements console.VerifyController by running
// internal/verify's engine IN-PROCESS, one table at a time in a background
// goroutine — the same functions internal/cli/verify.go calls, looped here
// instead of printed (#677). It is fully self-sufficient like
// baselineSupervisor: it opens its own index/source connections per run
// rather than reaching into the console's per-server bundle, so it needs
// nothing from internal/console beyond the DTOs/interface it implements.
//
// One job at a time per server, tracked in-memory — like baseline, the
// durable record (when there is one) is the baseline snapshots themselves;
// a verify run's result has no artifact of its own, so a console restart
// loses it (mirrors BaselineStatus's "idle if never run in this process").
type verifySupervisor struct {
	ctx context.Context // daemon lifecycle; cancels an in-flight run on shutdown
	// history, when non-nil, receives one VerifyRunRecord per finished run
	// (#1191) — manual and scheduled alike, so the history is the one place
	// "when did this last verify" is answered.
	history *console.VerifyHistory
	// onFinish, when non-nil, observes the same record history gets (#1192's
	// notification hook). Set only at construction, so no run can race it.
	onFinish func(console.VerifyRunRecord)

	mu   sync.Mutex
	jobs map[string]*verifyJob
}

// verifyJob is the mutable per-server job state: the pollable status PLUS the
// bookkeeping Explain needs that must never reach the wire — the exact
// BaselinePair each mismatched table was verified against, and enough of the
// request to reopen a connection on demand. Re-deriving the pair via a fresh
// internal/verify.FindBaselinePair call at explain time would risk explaining
// a DIFFERENT pair than the one the displayed verdict came from, if a new
// baseline landed in between (FindBaselinePair always picks the two MOST
// RECENT snapshots).
//
// Every field is read/written ONLY while holding verifySupervisor.mu — a
// plain map (pairs) and a growing slice (status.Results) make an unlocked
// read from one goroutine race a locked write from another (see Explain's
// and appendResult's comments for the specific hazard this closes).
type verifyJob struct {
	status console.VerifyStatus
	mode   console.VerifyMode

	// serverName and trigger ("manual" | "scheduled") only feed the history
	// record written at finish; neither is part of the pollable status.
	serverName string
	trigger    string

	indexDSN  string
	noArchive bool

	// pairs caches the BaselinePair behind every table this run reported as a
	// mismatch, keyed "schema.table". Only populated for baseline-anchored
	// runs (live-source has no explain support in the engine).
	pairs map[string]verify.BaselinePair
}

// newVerifySupervisor builds a supervisor bound to the daemon context.
// history and onFinish may be nil (runs are then not recorded / not
// observed); both are fixed at construction on purpose — see their fields.
func newVerifySupervisor(ctx context.Context, history *console.VerifyHistory, onFinish func(console.VerifyRunRecord)) *verifySupervisor {
	return &verifySupervisor{ctx: ctx, history: history, onFinish: onFinish, jobs: make(map[string]*verifyJob)}
}

// Trigger starts a verify run in the background; returns
// console.ErrVerifyRunning if one is already in flight for this server.
func (s *verifySupervisor) Trigger(req console.VerifyRequest) error {
	baselineSrc, err := s.begin(req, console.VerifyTriggerManual)
	if err != nil {
		return err
	}
	go s.run(req, baselineSrc)
	return nil
}

// RunScheduled runs one verify synchronously on the caller's goroutine — the
// watch daemon's --verify-interval loop (#1191), which paces servers one at a
// time so scheduled cycles never stack DuckDB work. Same admission rule as
// Trigger: a run already in flight wins and this returns
// console.ErrVerifyRunning for the scheduler to record as a skip.
func (s *verifySupervisor) RunScheduled(req console.VerifyRequest) error {
	baselineSrc, err := s.begin(req, console.VerifyTriggerScheduled)
	if err != nil {
		return err
	}
	s.run(req, baselineSrc)
	return nil
}

// begin admits a run — refusing a concurrent one per server — and publishes
// the fresh "running" job state. Shared by the manual and scheduled paths so
// the one-at-a-time-per-server rule cannot drift between them.
func (s *verifySupervisor) begin(req console.VerifyRequest, trigger string) (string, error) {
	s.mu.Lock()
	if j, ok := s.jobs[req.ServerID]; ok && j.status.State == console.VerifyStateRunning {
		s.mu.Unlock()
		return "", console.ErrVerifyRunning
	}
	baselineSrc := req.BaselineDir
	if baselineSrc == "" {
		baselineSrc = req.BaselineS3
	}
	s.jobs[req.ServerID] = &verifyJob{
		status:     console.VerifyStatus{State: console.VerifyStateRunning, Mode: req.Mode, Since: nowStamp()},
		mode:       req.Mode,
		serverName: req.ServerName,
		trigger:    trigger,
		indexDSN:   req.IndexDSN,
		noArchive:  req.NoArchive,
	}
	s.mu.Unlock()

	slog.Info("verify: starting in-process run", "server", req.ServerName, "id", req.ServerID, "mode", req.Mode, "trigger", trigger)
	return baselineSrc, nil
}

// Status returns a copy of the latest known run state (idle if never run here).
func (s *verifySupervisor) Status(serverID string) console.VerifyStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[serverID]; ok {
		return j.status
	}
	return console.VerifyStatus{State: console.VerifyStateIdle}
}

// Explain re-runs the row-level drill-down for one table the last completed
// baseline-anchored run reported as a mismatch, using the EXACT BaselinePair
// that run verified — never a freshly re-derived one (see verifyJob's doc
// comment). It opens its own short-lived index connection: the triggering
// run's connection is already closed by the time an operator clicks Explain.
func (s *verifySupervisor) Explain(serverID, schema, table string) (*console.VerifyExplanation, error) {
	// Copy everything needed out of the job under the SAME critical section:
	// j.pairs is a plain map, mutated by cachePair from the run's goroutine
	// while a run is still in flight (mismatches on later tables cache in
	// after earlier ones have already been polled/explained) — reading it
	// after releasing the lock would race that write.
	s.mu.Lock()
	j, ok := s.jobs[serverID]
	var (
		indexDSN  string
		noArchive bool
		pair      verify.BaselinePair
		pairOK    bool
	)
	if ok && j.mode == console.VerifyModeBaselineAnchored {
		indexDSN, noArchive = j.indexDSN, j.noArchive
		pair, pairOK = j.pairs[schema+"."+table]
	}
	s.mu.Unlock()
	if !pairOK {
		return nil, console.ErrExplainUnavailable
	}

	db, err := config.Connect(indexDSN)
	if err != nil {
		return nil, fmt.Errorf("connect index: %w", err)
	}
	defer db.Close()
	resolver, err := verify.ResolverFor(db)
	if err != nil {
		return nil, fmt.Errorf("load schema snapshot: %w", err)
	}
	// The pair already carries the resolved Prev/New Parquet paths — no need
	// to re-resolve a baseline source dir/S3 prefix here.
	cfg := verify.BaselineConfig{
		IndexDB: db, Resolver: resolver, IndexDBName: indexDBName(indexDSN),
		NoArchive: noArchive, ArchiveFetcher: parquetquery.Fetch,
		SourceFlavor: query.SourceFlavor(db),
	}

	ex, err := verify.ExplainBaselinePairMismatch(s.ctx, cfg, pair)
	if err != nil {
		return nil, fmt.Errorf("explain %s.%s: %w", schema, table, err)
	}

	var buf bytes.Buffer
	ex.Write(&buf)
	diffs := make([]console.VerifyRowDiff, len(ex.Diffs))
	for i, d := range ex.Diffs {
		cells := make([]console.VerifyCellDiff, len(d.Cells))
		for j, c := range d.Cells {
			cells[j] = console.VerifyCellDiff{Column: c.Column, Recovery: c.Recovery, Baseline: c.Baseline}
		}
		diffs[i] = console.VerifyRowDiff{PK: d.PK, Kind: d.Kind, Cells: cells}
	}
	return &console.VerifyExplanation{
		Schema: ex.Schema, Table: ex.Table, Anchor: ex.Anchor,
		Total: ex.Total, Diffs: diffs, Rendered: buf.String(),
	}, nil
}

// run drives the verify engine to completion and publishes the final state.
// Per-table results are appended as each table completes (see appendResult)
// so Status polls see progress mid-run — internal/verify has no progress
// callback of its own; this loop IS the streaming.
func (s *verifySupervisor) run(req console.VerifyRequest, baselineSrc string) {
	// A panic here must NEVER take down the daemon: this background goroutine
	// shares the process with the stream and console under `watch`. Mirrors
	// rotation.StartLoop's and the baseline-prune loop's guard.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("verify: run panicked", "server", req.ServerID, "panic", r)
			s.finish(req.ServerID, fmt.Errorf("internal error: %v", r))
		}
	}()

	db, err := config.Connect(req.IndexDSN)
	if err != nil {
		s.finish(req.ServerID, fmt.Errorf("connect index: %w", err))
		return
	}
	defer db.Close()
	resolver, err := verify.ResolverFor(db)
	if err != nil {
		// Hard requirement, no fallback — mirrors internal/cli/verify.go: a
		// missing schema snapshot means verify cannot resolve primary keys.
		s.finish(req.ServerID, fmt.Errorf("load schema snapshot (run `bintrail snapshot`): %w", err))
		return
	}
	dbName := indexDBName(req.IndexDSN)
	flavor := query.SourceFlavor(db)

	var runErr error
	switch req.Mode {
	case console.VerifyModeLiveSource:
		// The index's recorded flavor routes the live fingerprint: the MySQL
		// consistent-snapshot scan, or the PG-native checksum (#1024).
		if flavor == console.FlavorPostgres {
			runErr = s.runLiveSourcePG(req, db, resolver, dbName)
		} else {
			runErr = s.runLiveSource(req, db, resolver, dbName)
		}
	case console.VerifyModeRecoverInputs:
		runErr = s.runRecoverInputs(req, db, resolver, dbName, flavor)
	case console.VerifyModeBaselineAnchored:
		runErr = s.runBaselineAnchored(req, baselineSrc, db, resolver, dbName, flavor)
	default:
		// Exhaustive on purpose: this dispatch and the handler's mode
		// validation must move in lockstep. An unrecognized mode fails loudly
		// — it must never silently run (and, since #1191, persist) the wrong
		// verification.
		runErr = fmt.Errorf("unknown verify mode %q", req.Mode)
	}
	s.finish(req.ServerID, runErr)
}

func (s *verifySupervisor) runBaselineAnchored(req console.VerifyRequest, baselineSrc string, indexDB *sql.DB, resolver *metadata.Resolver, dbName, flavor string) error {
	ctx := s.ctx
	pairs, unpaired, prevOnly, err := verify.FindBaselinePair(ctx, baselineSrc)
	if err != nil {
		return fmt.Errorf("list baselines: %w", err)
	}
	if len(pairs) == 0 && len(unpaired) == 0 {
		any, err := verify.AnyBaseline(ctx, baselineSrc)
		if err != nil {
			return fmt.Errorf("list baselines: %w", err)
		}
		if !any {
			return fmt.Errorf("no baselines found under the configured baseline destination")
		}
		s.setNote(req.ServerID, "only one baseline exists for this server yet — nothing to compare")
		return nil
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Schema != pairs[j].Schema {
			return pairs[i].Schema < pairs[j].Schema
		}
		return pairs[i].Table < pairs[j].Table
	})

	filter, seen := tableFilter(req.Tables)
	cfg := verify.BaselineConfig{
		IndexDB: indexDB, Resolver: resolver, IndexDBName: dbName,
		NoArchive: req.NoArchive, ArchiveFetcher: parquetquery.Fetch,
		SourceFlavor: flavor,
	}

	for _, p := range pairs {
		// A daemon shutdown mid-run must fail the run, not let the remaining
		// tables degrade into synthetic per-table errors under a "succeeded"
		// state — with #1191 that verdict is persisted, so the misreport would
		// outlive the restart.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("verify interrupted: %w", err)
		}
		key := p.Schema + "." + p.Table
		if filter != nil && !filter[key] {
			continue
		}
		delete(seen, key)
		res, err := verify.VerifyBaselinePair(ctx, cfg, p)
		if err != nil {
			res = verify.TableResult{Schema: p.Schema, Table: p.Table, Status: verify.StatusError, Detail: err.Error()}
		}
		if res.Status == verify.StatusMismatch {
			s.cachePair(req.ServerID, key, p)
		}
		s.appendResult(req.ServerID, toWireResult(res, res.Status == verify.StatusMismatch))
	}
	for _, st := range unpaired {
		key := st.Schema + "." + st.Table
		if filter != nil && !filter[key] {
			continue
		}
		delete(seen, key)
		s.appendResult(req.ServerID, toWireResult(verify.TableResult{
			Schema: st.Schema, Table: st.Table, Status: verify.StatusInconclusive,
			Detail: "new since the previous baseline — no predecessor image to reconstruct from",
		}, false))
	}
	for _, st := range prevOnly {
		key := st.Schema + "." + st.Table
		if filter != nil && !filter[key] {
			continue
		}
		delete(seen, key)
		s.appendResult(req.ServerID, toWireResult(verify.TableResult{
			Schema: st.Schema, Table: st.Table, Status: verify.StatusInconclusive,
			Detail: "absent from the newest baseline (dropped, or the newest baseline was a --tables subset)",
		}, false))
	}
	for key := range seen {
		schema, table, _ := strings.Cut(key, ".")
		s.appendResult(req.ServerID, toWireResult(verify.TableResult{
			Schema: schema, Table: table, Status: verify.StatusError,
			Detail: "requested via the tables filter but not present in the latest baseline pair",
		}, false))
	}
	return nil
}

func (s *verifySupervisor) runLiveSource(req console.VerifyRequest, indexDB *sql.DB, resolver *metadata.Resolver, dbName string) error {
	ctx := s.ctx
	sourceDB, err := config.Connect(req.SourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer sourceDB.Close()

	tables, err := liveSourceTargetTables(ctx, indexDB, req.Tables)
	if err != nil {
		return fmt.Errorf("resolve target tables: %w", err)
	}

	baselineSrc := req.BaselineDir
	if baselineSrc == "" {
		baselineSrc = req.BaselineS3
	}
	cfg := verify.Config{
		SourceDB: sourceDB, IndexDB: indexDB, Resolver: resolver,
		BaselineSource: baselineSrc, IndexDBName: dbName,
		NoArchive: req.NoArchive, ArchiveFetcher: parquetquery.Fetch,
	}
	for _, st := range tables {
		// See runBaselineAnchored: a shutdown mid-run fails the run loudly.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("verify interrupted: %w", err)
		}
		res, err := verify.VerifyTable(ctx, cfg, st.Schema, st.Table)
		if err != nil {
			res = verify.TableResult{Schema: st.Schema, Table: st.Table, Status: verify.StatusError, Detail: err.Error()}
		}
		// Live-source mismatches have no explain support in the engine.
		s.appendResult(req.ServerID, toWireResult(res, false))
	}
	return nil
}

// runLiveSourcePG is runLiveSource for a PostgreSQL source (#1024): the same
// per-table loop, driving the engine's PG sibling (verify.VerifyTablePG).
// Two deliberate differences from the MySQL loop:
//   - the source is reached through pgverifysource.LiveSource — a pinned PG
//     connection (the render-GUC pin is what makes the live scan
//     byte-comparable to the baseline/delta text) — opened once and used
//     serially across tables; this daemon already links the PG capture
//     stack, so unlike the core CLI no seam indirection is needed;
//   - target tables come from the resolver (verify.PGTargetTables), never
//     liveSourceTargetTables' MAX(snapshot_id) query: a PG index stores one
//     relation per snapshot_id, so that query would silently verify a single
//     table.
func (s *verifySupervisor) runLiveSourcePG(req console.VerifyRequest, indexDB *sql.DB, resolver *metadata.Resolver, dbName string) error {
	ctx := s.ctx
	sourceChecksum, closeSource, err := pgverifysource.LiveSource(ctx, req.SourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer func() { _ = closeSource() }()

	tables, err := verify.PGTargetTables(resolver, req.Tables)
	if err != nil {
		return fmt.Errorf("resolve target tables: %w", err)
	}
	// Defensive mirror of the CLI's guard: an empty enumeration must fail the
	// run loudly, not complete "clean" with zero table results — a verify that
	// verified nothing is the false assurance this tool exists to prevent.
	// (Normally unreachable: verify.ResolverFor errors first on an index with
	// no schema snapshot.)
	if len(tables) == 0 {
		return fmt.Errorf("no tables to verify (empty filter and no schema snapshot)")
	}

	baselineSrc := req.BaselineDir
	if baselineSrc == "" {
		baselineSrc = req.BaselineS3
	}
	// Conservative archive fetcher and zero DuckDB tuning on purpose: this
	// shares the daemon with the stream (#510/#511 keep --ultrafast off
	// daemons), same as every other supervisor run mode.
	cfg := verify.PGLiveConfig{
		SourceChecksum: sourceChecksum, IndexDB: indexDB, Resolver: resolver,
		BaselineSource: baselineSrc, IndexDBName: dbName,
		NoArchive: req.NoArchive, ArchiveFetcher: parquetquery.Fetch,
	}
	for _, st := range tables {
		// See runBaselineAnchored: a shutdown mid-run fails the run loudly.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("verify interrupted: %w", err)
		}
		res, err := verify.VerifyTablePG(ctx, cfg, st.Schema, st.Table)
		if err != nil {
			res = verify.TableResult{Schema: st.Schema, Table: st.Table, Status: verify.StatusError, Detail: err.Error()}
		}
		// Live-source mismatches have no explain support in the engine.
		s.appendResult(req.ServerID, toWireResult(res, false))
	}
	return nil
}

// recoverInputsLookback is how far back a console recover-inputs run walks
// each primary key's event chain — the CLI's --lookback default (30d). Fixed
// rather than configurable here: the scheduled runner is a background health
// check, and an operator who wants a different window has the CLI.
const recoverInputsLookback = 30 * 24 * time.Hour

// runRecoverInputs is the console face of `bintrail verify --check recover`
// (#1001): index-only — no baseline, no source read — which is why the
// scheduled runner (#1191) falls back to it for servers with no baseline
// configured. Window and per-table cap are the CLI's defaults; the
// conservative archive fetcher is deliberate (this shares the daemon with the
// stream — #510/#511 keep --ultrafast off daemons). Table enumeration is
// flavor-routed like the CLI's verifyTargetTablesForFlavor: on a PG index the
// MAX(snapshot_id) lookup silently names ONE relation, so the PG branch
// enumerates the per-table resolver instead (#1024 review).
func (s *verifySupervisor) runRecoverInputs(req console.VerifyRequest, indexDB *sql.DB, resolver *metadata.Resolver, dbName, flavor string) error {
	ctx := s.ctx
	var tables []query.SchemaTable
	var err error
	if flavor == console.FlavorPostgres {
		tables, err = verify.PGTargetTables(resolver, req.Tables)
	} else {
		tables, err = liveSourceTargetTables(ctx, indexDB, req.Tables)
	}
	if err != nil {
		return fmt.Errorf("resolve target tables: %w", err)
	}
	now := time.Now().UTC()
	cfg := verify.RecoverInputsConfig{
		IndexDB: indexDB, Resolver: resolver, IndexDBName: dbName,
		NoArchive: req.NoArchive, ArchiveFetcher: parquetquery.Fetch,
		Since: now.Add(-recoverInputsLookback), Until: now,
	}
	for _, st := range tables {
		// See runBaselineAnchored: a shutdown mid-run fails the run loudly.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("verify interrupted: %w", err)
		}
		res, err := verify.VerifyRecoverInputs(ctx, cfg, st.Schema, st.Table)
		if err != nil {
			res = verify.TableResult{Schema: st.Schema, Table: st.Table, Status: verify.StatusError, Detail: err.Error()}
		}
		// No explain support — the drill-down exists only for baseline-anchored
		// content mismatches, same as the CLI's --explain scope rule.
		s.appendResult(req.ServerID, toWireResult(res, false))
	}
	return nil
}

// liveSourceTargetTables mirrors internal/cli/verify.go's unexported
// verifyTargetTables: an explicit --tables-style filter, or every table in
// the latest schema snapshot. No new internal/verify logic — this is the same
// small orchestration query the CLI runs, kept local since the CLI's helper
// isn't exported.
func liveSourceTargetTables(ctx context.Context, indexDB *sql.DB, tables []string) ([]query.SchemaTable, error) {
	if len(tables) > 0 {
		out := make([]query.SchemaTable, 0, len(tables))
		for _, t := range tables {
			schema, table, ok := strings.Cut(t, ".")
			if !ok {
				return nil, fmt.Errorf("invalid table filter %q (want schema.table)", t)
			}
			out = append(out, query.SchemaTable{Schema: schema, Table: table})
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Schema != out[j].Schema {
				return out[i].Schema < out[j].Schema
			}
			return out[i].Table < out[j].Table
		})
		return out, nil
	}
	rows, err := indexDB.QueryContext(ctx,
		`SELECT DISTINCT schema_name, table_name FROM schema_snapshots
		 WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM schema_snapshots)
		 ORDER BY schema_name, table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []query.SchemaTable
	for rows.Next() {
		var st query.SchemaTable
		if err := rows.Scan(&st.Schema, &st.Table); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// tableFilter builds a lookup set from a "schema.table" list (nil filter = no
// restriction) plus a mutable "seen" copy the caller deletes from as each
// entry is matched — whatever remains at the end was requested but never
// found, mirroring the CLI's --tables-not-present-in-pair StatusError.
func tableFilter(tables []string) (filter map[string]bool, seen map[string]bool) {
	if len(tables) == 0 {
		return nil, nil
	}
	filter = make(map[string]bool, len(tables))
	seen = make(map[string]bool, len(tables))
	for _, t := range tables {
		filter[t] = true
		seen[t] = true
	}
	return filter, seen
}

// toWireResult maps the engine's TableResult onto the console DTO. Every
// result — engine-produced or synthesized by the run loop — funnels through
// here, so the status is always normalized by the one classification the CLI
// report also uses (verify.NormalizeStatus; #1127). Reason and Detail carry
// the same value: Reason matches the CLI JSON field name, Detail is the
// legacy #677 alias.
func toWireResult(res verify.TableResult, explainable bool) console.VerifyTableResult {
	status, reason := verify.NormalizeStatus(res.Status, res.Detail)
	return console.VerifyTableResult{
		Schema: res.Schema, Table: res.Table, Status: string(status),
		Reason: reason, Detail: reason,
		SourceRows: res.SourceRows, ReconstructRows: res.ReconstructRows, Anchor: res.Anchor,
		Explainable: explainable,
	}
}

// appendResult publishes one table's outcome under the job lock, so a
// concurrent Status() poll sees it immediately — the "as they land" progress
// #677 asks for.
func (s *verifySupervisor) appendResult(serverID string, tr console.VerifyTableResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[serverID]
	if !ok {
		return // job was cleared out from under us; drop (defensive, shouldn't happen)
	}
	j.status.Results = append(j.status.Results, tr)
	// One classification for every surface: verify.Summary.Count applies the
	// same buckets (and unknown→Error rule) the CLI's JSON report uses (#1127).
	// The round-trip struct conversion is the drift guard: it stops compiling
	// the moment console.VerifySummary and verify.Summary diverge.
	sum := verify.Summary(j.status.Summary)
	sum.Count(verify.Status(tr.Status))
	j.status.Summary = console.VerifySummary(sum)
}

// cachePair records the BaselinePair behind a mismatched table for a later
// on-demand Explain call. Must be called before appendResult's status update
// is polled by a racing Explain — both are under s.mu, and cachePair always
// runs first in the caller, so a client can never observe Explainable:true
// before the pair is actually cached.
func (s *verifySupervisor) cachePair(serverID, key string, p verify.BaselinePair) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[serverID]
	if !ok {
		return
	}
	if j.pairs == nil {
		j.pairs = make(map[string]verify.BaselinePair)
	}
	j.pairs[key] = p
}

// setNote records a benign informational message for the run in progress
// (e.g. "only one baseline yet") WITHOUT changing State — finish, called
// once at the tail of run() regardless of which branch it took, is the sole
// place that transitions State out of "running". Setting State here too
// would let it leave "running" before the goroutine's actual terminal call,
// opening a window where a second concurrent Trigger for the same server is
// wrongly admitted (State != "running" already) and finish's caller ends up
// racing two jobs' state under one map entry.
func (s *verifySupervisor) setNote(serverID, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[serverID]
	if !ok {
		return
	}
	j.status.Note = note
}

// finish marks a run's terminal state — the ONLY place State leaves
// "running" (see setNote). err (from the run's own setup, e.g. a connect
// failure) fails the whole run; per-table failures never reach here — they
// are recorded as StatusError results by appendResult instead, exactly like
// internal/cli/verify.go's per-table error isolation.
func (s *verifySupervisor) finish(serverID string, err error) {
	s.mu.Lock()
	j, ok := s.jobs[serverID]
	if !ok {
		j = &verifyJob{status: console.VerifyStatus{}}
		s.jobs[serverID] = j
	}
	j.status.FinishedAt = nowStamp()
	if err != nil {
		j.status.State = console.VerifyStateFailed
		j.status.LastError = err.Error()
	} else {
		j.status.State = console.VerifyStateSucceeded
	}
	rec := console.VerifyRunRecord{
		ServerID:     serverID,
		ServerName:   j.serverName,
		Trigger:      j.trigger,
		VerifyStatus: j.status,
	}
	s.mu.Unlock()

	if err != nil {
		slog.Error("verify: run failed", "server", serverID, "error", err)
	} else {
		slog.Info("verify: run complete", "server", serverID,
			"match", rec.Summary.Match, "mismatch", rec.Summary.Mismatch,
			"inconclusive", rec.Summary.Inconclusive, "error", rec.Summary.Error)
	}
	// History is written OUTSIDE the job lock — Append does file IO, and a
	// concurrent Status poll must never block on the disk. The run is terminal
	// at this point, so rec's Results slice can no longer grow under it.
	if s.history != nil {
		if herr := s.history.Append(rec); herr != nil {
			slog.Warn("verify: could not persist run to history", "server", serverID, "error", herr)
		}
	}
	if s.onFinish != nil {
		s.onFinish(rec)
	}
}

// indexDBName extracts the database name from an index DSN, mirroring
// internal/cli/verify.go's own tolerant handling: a parse failure just leaves
// it empty rather than failing the run (IndexDBName is used for planner
// diagnostics, not correctness).
func indexDBName(dsn string) string {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return ""
	}
	return cfg.DBName
}
