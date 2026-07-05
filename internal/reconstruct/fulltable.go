package reconstruct

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for parquet_scan baseline streaming
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
)

// FullTableConfig drives ReconstructTables — the full-table merge-on-read
// entry point for #187. One Config instance covers one run; each table in
// Tables is reconstructed concurrently with at most Parallelism goroutines
// running at a time (a goroutine is spawned for every table, but a buffered-
// channel semaphore caps the number that actually do work).
//
// Supported primary-key types (#212 + #214):
//
//   - integer: int, smallint, tinyint, mediumint, bigint (+ unsigned)
//   - string: char, varchar, text, tinytext, mediumtext, longtext
//   - enum, set
//   - datetime, timestamp — canonicalized from DuckDB time.Time to the
//     go-mysql string format the indexer stores
//   - date — canonicalized to "2006-01-02"
//   - year
//   - decimal, numeric — pass-through string (go-mysql delivers a
//     pre-formatted string when useDecimal is false, which is the
//     bintrail default; baseline stores DECIMAL as parquet.String, so
//     both sides agree byte-for-byte)
//
// PK columns with any other type (FLOAT, DOUBLE, BINARY, VARBINARY, BLOB,
// BIT, JSON, spatial types) are rejected at ReconstructTable entry with a
// hard error. Fixing those types requires modifying event.BuildPKValues
// or internal/baseline/reader_sql.go — both are non-additive changes to
// data already on disk — so they're deferred behind separate follow-up
// issues.
//
// UPDATE events that mutate the primary key itself are NOT handled
// correctly: the change map is keyed by the before-image PK, so a later
// event on the old PK value may overwrite the UPDATE in the map and the
// after-image row is dropped. Re-snapshot the baseline after schema
// changes that reshape PKs.
type FullTableConfig struct {
	IndexDSN    string    // DSN for the bintrail index database
	BaselineSrc string    // local directory or s3:// URL of baselines
	Tables      []string  // "db.table" entries
	At          time.Time // target point-in-time
	OutputDir   string    // mydumper dump output directory (must exist)
	ChunkSize   int64     // per-chunk SQL file size (0 → 256 MiB)
	Parallelism int       // max concurrent tables (0 → runtime.NumCPU())
	AllowGaps   bool      // false = strict abort on gaps (default for reconstruct)

	// WarnEventThreshold logs a loud warning when a table's fetched event count
	// exceeds it: full-table reconstruct holds every event plus one change-map
	// entry per touched PK in memory and can exhaust RAM at scale (#654). 0 =
	// disabled — the zero value, so direct library callers stay silent; the CLI
	// defaults it to 5,000,000 via --warn-event-threshold.
	WarnEventThreshold int64

	// ArchiveFetcher fetches archived binlog events for a table. nil →
	// parquetquery.Fetch (the container-safe DuckDB budget). The CLI sets it
	// to a tuned fetcher under --ultrafast so the flag is honored on the
	// full-table path, not just single-row reconstruct (#510).
	ArchiveFetcher query.ArchiveFetcher
}

// TableReport carries the per-table outcome stats that the CLI summary prints.
type TableReport struct {
	Schema, Table  string
	BaselineRows   int64 // rows streamed through from the baseline unchanged
	EventsApplied  int64 // total events observed from the event index
	InsertsEmitted int64 // rows appended after the baseline pass (new PKs)
	UpdatesApplied int64 // baseline rows whose PK matched an UPDATE/INSERT event
	DeletesSkipped int64 // baseline rows whose PK matched a DELETE event
	Files          []string
	Duration       time.Duration
	// BinlogOnly is true when the table had no baseline at all and was
	// recovered entirely from the binlog (#766's ErrNoBaseline fallback,
	// reconstructBinlogOnly). Files is non-empty in this case too, so
	// ReconstructTables' shared-metadata-file selector must check this flag
	// as well — a binlog-only report has no baseline GTID/binlog coordinates
	// to embed in the metadata file.
	BinlogOnly bool
}

// shouldWarnEvents reports whether a fetched event count should trigger the
// large-window memory warning (#654). threshold <= 0 disables the warning, so
// the zero-value FullTableConfig stays silent for direct library callers.
func shouldWarnEvents(n, threshold int64) bool {
	return threshold > 0 && n > threshold
}

// maybeWarnEventVolume emits the #654 large-window memory warning when the
// fetched event count exceeds threshold (0 disables). Extracted from
// ReconstructTable so the emission — not just the predicate — is unit-testable.
func maybeWarnEventVolume(schema, table string, n int, threshold int64) {
	if !shouldWarnEvents(int64(n), threshold) {
		return
	}
	slog.Warn("reconstruct: very large event window — full-table reconstruct holds every event "+
		"plus one change-map entry per touched row in memory and may exhaust RAM",
		"schema", schema, "table", table,
		"events", n, "threshold", threshold,
		"hint", "narrow the window with a later --at or a fresher baseline snapshot, or raise/silence "+
			"via --warn-event-threshold / BINTRAIL_RECONSTRUCT_WARN_EVENTS (0 disables)")
}

// ReconstructTables runs ReconstructTable concurrently for every entry in
// cfg.Tables, sharing a single *sql.DB + *query.Engine + *metadata.Resolver.
// Returns the list of reports in arbitrary order plus a joined error
// containing every per-table failure (via errors.Join).
func ReconstructTables(ctx context.Context, cfg FullTableConfig) ([]*TableReport, error) {
	if cfg.IndexDSN == "" {
		return nil, errors.New("FullTableConfig: IndexDSN is required")
	}
	if cfg.BaselineSrc == "" {
		return nil, errors.New("FullTableConfig: BaselineSrc is required")
	}
	if len(cfg.Tables) == 0 {
		return nil, errors.New("FullTableConfig: at least one table is required")
	}
	if cfg.OutputDir == "" {
		return nil, errors.New("FullTableConfig: OutputDir is required")
	}
	if cfg.At.IsZero() {
		cfg.At = time.Now().UTC()
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = runtime.NumCPU()
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 256 << 20
	}

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	db, err := config.Connect(cfg.IndexDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to index DB: %w", err)
	}
	defer db.Close()
	// Give per-table goroutines enough connections for concurrent fetches.
	db.SetMaxOpenConns(2 * cfg.Parallelism)

	// Run the idempotent schema migration before NewResolver. NewResolver
	// reads schema_snapshots.column_type (added in #212), and pre-upgrade
	// databases where EnsureSchema hasn't been called from some other
	// command yet would fail with Error 1054: Unknown column 'column_type'.
	// Every other consumer of NewResolver runs EnsureSchema first; doing it
	// at the library boundary here means library callers (not just the CLI)
	// also get the migration automatically.
	if err := indexer.EnsureSchema(db); err != nil {
		return nil, fmt.Errorf("ensure index schema: %w", err)
	}

	// Derive DBName for the query planner.
	var dbName string
	if dsnCfg, perr := mysqldriver.ParseDSN(cfg.IndexDSN); perr == nil {
		dbName = dsnCfg.DBName
	}

	// Load schema resolver once (latest snapshot). All PK encoding goes
	// through event.BuildPKValues with the resolver's ColumnMetas so the
	// keys are byte-identical to what the indexer stored in pk_values.
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		return nil, fmt.Errorf("load schema resolver: %w; run `bintrail snapshot` first", err)
	}

	engine := query.New(db)

	// Resolve archive sources once — the same set is used for every table.
	archSources, archErr := query.ResolveArchiveSources(ctx, db)
	if archErr != nil {
		if !cfg.AllowGaps {
			return nil, fmt.Errorf("resolve archive sources, cannot verify coverage: %w", archErr)
		}
		slog.Warn("archive source discovery failed; proceeding without archives", "error", archErr)
	}

	// Report slice is protected by a mutex for the parallel goroutines.
	reports := make([]*TableReport, 0, len(cfg.Tables))
	var (
		mu   sync.Mutex
		errs []error
	)

	sem := make(chan struct{}, cfg.Parallelism)
	var wg sync.WaitGroup

	for _, entry := range cfg.Tables {
		schema, table, ok := splitSchemaTable(entry)
		if !ok {
			return nil, fmt.Errorf("invalid --tables entry %q: must be schema.table", entry)
		}
		wg.Add(1)
		go func(schema, table string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}

			rep, err := ReconstructTable(ctx, cfg, schema, table, db, engine, archSources, resolver, dbName)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				slog.Error("full-table reconstruct failed",
					"schema", schema, "table", table, "error", err)
				errs = append(errs, fmt.Errorf("%s.%s: %w", schema, table, err))
				return
			}
			reports = append(reports, rep)
		}(schema, table)
	}
	wg.Wait()

	// Write the shared metadata file once after every table completes.
	// We use the metadata from the FIRST reconstructed table — all tables
	// share the same baseline set, so their positions should agree. If a
	// user somehow reconstructs tables from baselines of different ages,
	// the metadata file will reflect the first one and the rest will log
	// a warning in ReconstructTable itself.
	if len(reports) > 0 {
		// Pick the first report that actually produced output files from a
		// real baseline (skip tables that had no baseline: an empty report,
		// or a #766 binlog-only report — neither has baseline GTID/binlog
		// coordinates to embed in the metadata file).
		var metaReport *TableReport
		for _, r := range reports {
			if len(r.Files) > 0 && !r.BinlogOnly {
				metaReport = r
				break
			}
		}
		if metaReport != nil {
			tableName := metaReport.Schema + "." + metaReport.Table
			baselinePath, _, _, perr := FindBaseline(ctx, cfg.BaselineSrc, metaReport.Schema, metaReport.Table, cfg.At)
			if perr == nil {
				bmeta, merr := baseline.ReadParquetMetadataAny(ctx, baselinePath)
				if merr == nil {
					if err := WriteMetadataFile(cfg.OutputDir, cfg.At,
						bmeta.GTIDSet, bmeta.BinlogFile, bmeta.BinlogPos); err != nil {
						slog.Warn("could not write metadata file", "error", err)
					}
				} else {
					slog.Warn("could not read baseline metadata for metadata file",
						"table", tableName, "error", merr)
				}
			} else {
				slog.Warn("could not find baseline for metadata file",
					"table", tableName, "error", perr)
			}
		} else {
			slog.Warn("no reconstructed table has a real baseline (all empty or binlog-only); metadata file not written")
		}
	}

	if len(errs) > 0 {
		// errors.Join surfaces every per-table failure so operators running
		// with --log-level error see the full picture, not just the first
		// one. Every error is also logged individually above, but the
		// returned error is what the CLI wraps into its exit status.
		return reports, errors.Join(errs...)
	}
	return reports, nil
}

// ReconstructTable is the per-table worker. Safe to call concurrently with
// other ReconstructTable invocations that share the same db / engine /
// archSources / resolver.
func ReconstructTable(
	ctx context.Context,
	cfg FullTableConfig,
	schema, table string,
	db *sql.DB,
	engine *query.Engine,
	archSources []string,
	resolver *metadata.Resolver,
	dbName string,
) (*TableReport, error) {
	start := time.Now()
	rep := &TableReport{Schema: schema, Table: table}

	// ── 1. Find the baseline snapshot ──────────────────────────────────────
	// FindBaseline already logs a stale-fallback warning (#466) server-side.
	baselinePath, snapshotTime, _, err := FindBaseline(ctx, cfg.BaselineSrc, schema, table, cfg.At)
	if err != nil {
		if !errors.Is(err, ErrNoBaseline) {
			return nil, fmt.Errorf("find baseline: %w", err)
		}
		// No baseline exists for this table. This can happen when: (1) the
		// table was empty at dump time and the baseline predates 0-row
		// Parquet support, or (2) the table was created after the last
		// baseline snapshot. Case (2) can hold real binlog-only rows (#766),
		// so fall back to a binlog-only reconstruction instead of silently
		// emitting an empty report — parity with the shim's _snapshot→
		// _flashback degrade (internal/shim/snapshot.go runSnapshotFullTable).
		return reconstructBinlogOnly(ctx, cfg, schema, table, db, engine, resolver, dbName, rep, start)
	}
	slog.Debug("baseline selected",
		"schema", schema, "table", table,
		"path", baselinePath, "snapshot_time", snapshotTime.UTC().Format(time.RFC3339))

	// ── 2. Read baseline Parquet metadata ──────────────────────────────────
	bmeta, err := baseline.ReadParquetMetadataAny(ctx, baselinePath)
	if err != nil {
		return nil, fmt.Errorf("read baseline metadata: %w", err)
	}
	if bmeta.CreateTableSQL == "" {
		return nil, fmt.Errorf(
			"baseline at %s lacks bintrail.create_table_sql metadata; "+
				"re-run `bintrail baseline` to embed the CREATE TABLE statement",
			baselinePath)
	}

	// ── 3. Resolve PK columns from the schema resolver ─────────────────────
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("resolve schema for %s.%s: %w; run `bintrail snapshot` to refresh", schema, table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key in the loaded snapshot; full-table reconstruct requires a PK", schema, table)
	}
	// Refuse to proceed when a PK column uses a type the canonicalizer
	// cannot handle. Emitting a warning isn't enough because operators
	// running with --log-level error won't see it and would silently get
	// wrong output — the same class of bug the full-table reconstruct
	// hardening exists to prevent. Users with DECIMAL / BINARY / BLOB /
	// BIT / JSON / GEOMETRY / etc. PKs must track the follow-up work for
	// their type to be added.
	for _, pkCol := range pkCols {
		if !supportedPKType(pkCol.DataType) {
			return nil, fmt.Errorf(
				"full-table reconstruct: %s.%s PK column %q has type %q which is not in the supported PK type set; "+
					"file a follow-up issue if you need this type",
				schema, table, pkCol.Name, pkCol.DataType)
		}
	}

	// For DATETIME/TIMESTAMP PK columns, warn loudly if the column_type
	// metadata is missing — the canonicalizer will fall back to a
	// Nanosecond()==0 heuristic that is correct for DATETIME(0) but
	// silently wrong for DATETIME(N>0) whole-second values. Operators
	// should re-run `bintrail snapshot` to refresh schema_snapshots with
	// the new column_type field (added in the precision-aware PK fix).
	for _, pkCol := range pkCols {
		dt := strings.ToLower(strings.TrimSpace(pkCol.DataType))
		if (dt == "datetime" || dt == "timestamp") && pkCol.ColumnType == "" {
			slog.Warn("full-table reconstruct: DATETIME/TIMESTAMP PK column has no column_type in schema_snapshots; "+
				"using best-effort precision heuristic — DATETIME(N>0) whole-second values may silently miss the baseline. "+
				"Re-run `bintrail snapshot` to refresh.",
				"schema", schema, "table", table, "column", pkCol.Name)
		}
	}

	// ── 3b. Refuse if a TRUNCATE/DROP/RENAME hit this table in the window ──
	// TRUNCATE/DROP emit no row events (#764): without this check the merge
	// below would replay the baseline straight through and silently
	// resurrect rows the DDL actually deleted.
	if err := CheckDestructiveDDL(ctx, db, schema, table, snapshotTime, cfg.At); err != nil {
		return nil, err
	}

	// ── 3c. Refuse/warn on a stamped capture gap inside the window (#765) ──
	// stream_state.gap_lost_at records an irreparable capture gap (source
	// binlogs purged before the stream caught up); unlike the archive-coverage
	// gap the fetch below already guards against, no amount of archive
	// resolution can fill this — it must be checked directly.
	if err := CheckCaptureGap(ctx, db, schema, table, snapshotTime, cfg.At, cfg.AllowGaps); err != nil {
		return nil, err
	}

	// ── 4. Fetch events via the shared helper (gap-aware) ──────────────────
	fetchOpts := query.Options{
		Schema: schema,
		Table:  table,
		Since:  &snapshotTime,
		Until:  &cfg.At,
		// No PKValues filter — we want every event for this table.
	}
	// nil ArchiveFetcher → the container-safe parquetquery.Fetch. Resolved here
	// at the point of use so both ReconstructTables and any direct
	// ReconstructTable caller get the default (#510).
	fetcher := cfg.ArchiveFetcher
	if fetcher == nil {
		fetcher = parquetquery.Fetch
	}
	// Pass NoArchive=false unconditionally and let query.FetchMerged decide
	// whether to query archives — it already handles the empty-archive case
	// in its fast path. The previous `len(archSources)==0` gate was wrong:
	// it disabled archive routing entirely even when FetchMerged could have
	// resolved sources through its own code path.
	events, _, err := query.FetchMerged(ctx, db, engine, query.FetchMergedOptions{
		Opts:           fetchOpts,
		DBName:         dbName,
		NoArchive:      false,
		AllowGaps:      cfg.AllowGaps,
		ArchiveFetcher: fetcher,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch events: %w", err)
	}
	rep.EventsApplied = int64(len(events))

	// Large-window memory warning (#654). len(events) is the real, archive-inclusive
	// count (FetchMerged already merged live + Parquet), so no separate COUNT is
	// needed. Advisory only: it fires before the change-map build below and tells
	// the operator to narrow the next run; it cannot shrink the already-resident
	// slice (reconstruct warns, never refuses — the OOM at scale is unreproduced).
	maybeWarnEventVolume(schema, table, len(events), cfg.WarnEventThreshold)

	// ENUM/SET ordinals → labels (#476), each delta decoded with the
	// snapshot in effect at its event time (#475). Must run before the
	// merge so the mydumper output writes labels — the same form the
	// baseline rows and a real mydumper dump carry — instead of numeric
	// ordinals.
	MapEventEnumLabels(db, resolver, schema, table, events)

	// BLOB/TEXT base64 → real value, each delta decoded with the snapshot in
	// effect at its event time (#668; same epoch-aware approach as the ENUM/SET
	// pass above and the single-row reconstruct path, #666). Must run before
	// the merge map is built below so changes' RowAfter images carry decoded
	// values for free — typing the decode columns from a single latest-snapshot
	// resolve (the pre-#668 behavior) corrupts a column captured as VARCHAR at
	// an old epoch and widened to TEXT later.
	DecodeEventBinaries(db, schema, table, events)

	// ── 5. Build the change map: PK string → last event for that PK ───────
	// events is already sorted by (event_timestamp, event_id) via
	// query.MergeResults, so the last write wins naturally.
	changes := make(map[string]*query.ResultRow, len(events))
	for i := range events {
		changes[events[i].PKValues] = &events[i]
	}

	// ── 6. Materialize the baseline locally for DuckDB streaming ───────────
	localPath, cleanup, err := materializeBaselineLocal(ctx, baselinePath)
	if err != nil {
		return nil, fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()

	// ── 7-9. Merge baseline + changes into the mydumper writer ────────────
	// The merge loop is extracted so it can be unit-tested without MySQL.
	if err := mergeBaselineIntoWriter(ctx, mergeInput{
		LocalBaselinePath: localPath,
		CreateTableSQL:    bmeta.CreateTableSQL,
		Schema:            schema,
		Table:             table,
		PKCols:            pkCols,
		Changes:           changes,
		OutputDir:         cfg.OutputDir,
		ChunkSize:         cfg.ChunkSize,
	}, rep); err != nil {
		return nil, err
	}
	rep.Duration = time.Since(start)

	slog.Info("table reconstructed",
		"schema", schema, "table", table,
		"baseline_rows", rep.BaselineRows,
		"events_applied", rep.EventsApplied,
		"updates_applied", rep.UpdatesApplied,
		"inserts_emitted", rep.InsertsEmitted,
		"deletes_skipped", rep.DeletesSkipped,
		"duration_ms", rep.Duration.Milliseconds())
	return rep, nil
}

// mergeInput bundles everything mergeBaselineIntoWriter needs. Extracted so
// unit tests can exercise the merge loop without standing up MySQL.
type mergeInput struct {
	LocalBaselinePath string
	CreateTableSQL    string
	Schema            string
	Table             string
	PKCols            []metadata.ColumnMeta
	Changes           map[string]*query.ResultRow
	OutputDir         string
	ChunkSize         int64
}

// mergeBaselineIntoWriter streams the local baseline Parquet via DuckDB,
// applies the change map to produce the final row set, and writes the result
// through a MydumperWriter. Updates counters on rep in place. Drains the
// Changes map: after this function returns, entries still present are rows
// that were NOT found in the baseline (appended as new INSERTs).
//
// The actual merge is delegated to mergeBaselineImages so the same logic
// backs both this writer path and the shim's in-memory full-table _snapshot
// path (SnapshotFullTableImages). This function is the thin writer wrapper:
// it owns the MydumperWriter and turns each emitted row map into an ordered
// tuple the writer expects.
//
// The writer's Close() is deferred as a fallback: on any early return it
// still runs and unlinks half-written chunk files, so callers never observe
// stray partial output on disk.
func mergeBaselineIntoWriter(ctx context.Context, in mergeInput, rep *TableReport) (retErr error) {
	// Fail loud on a residual unchanged-TOAST marker (#592), before the writer
	// opens (same up-front stance as the #602 refusal below): every change in
	// the map is destined for the output, so a marker anywhere in it would be
	// written into the reconstructed dump as the marker's JSON — silent
	// corruption.
	if err := checkChangesToast(in.Changes); err != nil {
		return err
	}

	colNames, err := readBaselineColumns(ctx, in.LocalBaselinePath)
	if err != nil {
		return fmt.Errorf("read baseline columns: %w", err)
	}

	// Fail loud on a column ADDED after the baseline (#602). This path projects
	// every emitted row onto the baseline column set and writes the baseline's
	// CREATE TABLE as the schema header; it does not reconstruct intermediate
	// DDL. So a delta event's row_after key absent from the baseline columns
	// would be dropped silently by rowAfterOrdered (its value never reaches the
	// dump). Refuse instead, the same fail-loud choice the supportedPKType
	// guard in ReconstructTable makes: a warning isn't enough because an
	// operator running --log-level error would not see it and would get a dump
	// silently missing a column. Detected up front, before the writer opens, so
	// no partial chunk files are left on disk.
	if extra := postBaselineColumns(in.Changes, colNames); len(extra) > 0 {
		return fmt.Errorf(
			"full-table reconstruct: %s.%s has column(s) %s present in delta events but absent from the baseline schema "+
				"(added after the baseline snapshot); their values cannot be emitted without dropping data silently — "+
				"re-run `bintrail baseline` to capture a snapshot that includes the new column(s)",
			in.Schema, in.Table, strings.Join(extra, ", "))
	}

	mw, err := NewMydumperWriter(in.OutputDir, in.Schema, in.Table, colNames, in.ChunkSize)
	if err != nil {
		return fmt.Errorf("open mydumper writer: %w", err)
	}
	// Defer Close so every error path cleans up the current chunk file.
	// Close is idempotent; the success path below also calls it explicitly
	// before capturing rep.Files.
	defer func() {
		if cerr := mw.Close(); cerr != nil && retErr == nil {
			retErr = fmt.Errorf("close mydumper writer: %w", cerr)
		}
	}()

	if err := mw.WriteSchema(in.CreateTableSQL); err != nil {
		return err
	}

	// BLOB/TEXT base64 of the delta-event after-images is already decoded by
	// the caller (DecodeEventBinaries, run epoch-aware over the full events
	// slice before the Changes map was built, #668) — in.Changes entries alias
	// those same event structs. Baseline pass-through rows are untouched here:
	// their TEXT values arrive from the DuckDB scan as Go strings and must NOT
	// be decoded (#660).

	// emit re-orders each emitted row map into the baseline Parquet column
	// order the writer was constructed with. For baseline pass-through rows
	// the map is keyed by exactly those columns (so rowAfterOrdered is an
	// order-preserving identity); for event after-images it aligns the
	// event's row_after to the baseline schema, filling drift columns with
	// NULL — identical to the pre-refactor behaviour.
	stats, err := mergeBaselineImages(ctx, mergeCore{
		LocalBaselinePath: in.LocalBaselinePath,
		Schema:            in.Schema,
		Table:             in.Table,
		PKCols:            in.PKCols,
		Changes:           in.Changes,
	}, func(rowMap map[string]any) error {
		return mw.WriteRow(rowAfterOrdered(rowMap, colNames, in.Schema, in.Table))
	})
	if err != nil {
		return err
	}
	rep.BaselineRows = stats.BaselineRows
	rep.UpdatesApplied = stats.UpdatesApplied
	rep.InsertsEmitted = stats.InsertsEmitted
	rep.DeletesSkipped = stats.DeletesSkipped

	// Close explicitly to promote close errors into the happy-path return
	// value (the deferred Close above is a no-op after this succeeds, and
	// a no-op from an already-closed writer is safe).
	if err := mw.Close(); err != nil {
		return fmt.Errorf("close mydumper writer: %w", err)
	}
	rep.Files = mw.Files()
	return nil
}

// mergeCore is the writer-agnostic input to mergeBaselineImages.
type mergeCore struct {
	LocalBaselinePath string
	Schema            string
	Table             string
	PKCols            []metadata.ColumnMeta
	Changes           map[string]*query.ResultRow
}

// mergeStats counts what mergeBaselineImages did, for the offline
// TableReport. The shim path ignores it.
type mergeStats struct {
	BaselineRows   int64
	UpdatesApplied int64
	InsertsEmitted int64
	DeletesSkipped int64
}

// mergeBaselineImages streams the local baseline Parquet via DuckDB, applies
// the change map, and calls emit once per row of the reconstructed table, in
// baseline Parquet column order:
//
//   - a baseline row not present in Changes is emitted as-is (pass-through);
//   - a baseline row whose PK is in Changes is emitted as the event's
//     row_after image (UPDATE/INSERT wins over the baseline), or skipped
//     entirely when the latest event is a DELETE;
//   - PKs left in Changes after the baseline scan are post-snapshot inserts
//     (rows that did not exist at baseline time) and are emitted last, in
//     deterministic PK order.
//
// The emitted map is keyed by column name. Pass-through rows carry the
// baseline's values verbatim (DuckDB scan types — time.Time, []byte, …);
// event rows carry the binlog event's row_after (JSON-decoded). Callers that
// build a wire resultset normalise the values; the writer path re-orders them.
// emit returning a non-nil error stops the merge and propagates that error
// (used by the shim to enforce its row cap). Drains in.Changes.
//
// This is the shared core for both the offline `bintrail reconstruct` writer
// (mergeBaselineIntoWriter) and the shim full-table _snapshot path
// (SnapshotFullTableImages, via runSnapshotFullTable), so the two reconstruction
// surfaces can never drift in the merge ALGORITHM (baseline/event matching,
// ordering, drain). Per-value decoding is NOT done here — every caller decodes
// its own Changes map before it ever reaches this function: the writer caller
// upstream in ReconstructTable, on the full events slice before the Changes
// map is even built (DecodeEventBinaries, #668); the shim caller upstream in
// runSnapshotFullTable, via mapEventImages (#661); `verify`'s callers
// (reconstructDigest and ExplainBaselinePairMismatch) the same way, upstream
// of their own Changes map builds (#672). Every SnapshotFullTableImages caller
// now decodes before this function ever sees a value.
func mergeBaselineImages(ctx context.Context, in mergeCore, emit func(map[string]any) error) (mergeStats, error) {
	var stats mergeStats

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return stats, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()

	safePath := strings.ReplaceAll(in.LocalBaselinePath, "'", "''")
	q := fmt.Sprintf("SELECT * FROM parquet_scan('%s')", safePath)
	drows, err := ddb.QueryContext(ctx, q)
	if err != nil {
		return stats, fmt.Errorf("duckdb baseline query: %w", err)
	}
	defer drows.Close()

	dcols, err := drows.Columns()
	if err != nil {
		return stats, fmt.Errorf("duckdb columns: %w", err)
	}

	scan := make([]any, len(dcols))
	ptrs := make([]any, len(dcols))
	for i := range scan {
		ptrs[i] = &scan[i]
	}

	for drows.Next() {
		if err := drows.Scan(ptrs...); err != nil {
			return stats, fmt.Errorf("scan baseline row: %w", err)
		}
		// zipMap reads the scanned values into a fresh map; database/sql
		// clones []byte into the *any destinations and reassigns (never
		// mutates) scan[i] on the next Next(), so the map is safe to retain
		// past this iteration — which the shim relies on when it buffers
		// emitted rows.
		rowMap := zipMap(dcols, scan)
		// Canonicalise PK values before hashing so they match what the
		// indexer stored in binlog_events.pk_values. Without this,
		// DATETIME/TIMESTAMP PKs silently miss the change map because
		// DuckDB returns time.Time while the indexer stored a
		// go-mysql-formatted string (#212). canonicalizePKMap does not
		// mutate rowMap, so the emitted pass-through row keeps its original
		// values.
		pkMap, err := canonicalizePKMap(rowMap, in.PKCols)
		if err != nil {
			return stats, fmt.Errorf("canonicalize baseline PK for %s.%s: %w", in.Schema, in.Table, err)
		}
		pk := event.BuildPKValues(in.PKCols, pkMap)

		if ev, ok := in.Changes[pk]; ok {
			delete(in.Changes, pk)
			switch ev.EventType {
			case event.EventDelete:
				stats.DeletesSkipped++
				continue
			case event.EventUpdate, event.EventInsert:
				// Defensive: a non-DELETE event with nil RowAfter would
				// otherwise emit an all-NULL tuple. This indicates a
				// corrupt event or parser bug, not a normal code path.
				if ev.RowAfter == nil {
					slog.Error("event has nil RowAfter; skipping to avoid emitting all-NULL tuple",
						"schema", in.Schema, "table", in.Table, "pk", pk,
						"event_type", ev.EventType, "event_id", ev.EventID)
					continue
				}
				if err := emit(ev.RowAfter); err != nil {
					return stats, err
				}
				stats.UpdatesApplied++
			}
		} else {
			if err := emit(rowMap); err != nil {
				return stats, err
			}
			stats.BaselineRows++
		}
	}
	if err := drows.Err(); err != nil {
		return stats, fmt.Errorf("iterate baseline rows: %w", err)
	}

	// Append events for PKs that weren't in the baseline (rows inserted
	// after the snapshot). Deterministic order: sort by PK string so tests
	// can assert on the output without flakiness.
	newPKs := make([]string, 0, len(in.Changes))
	for pk := range in.Changes {
		newPKs = append(newPKs, pk)
	}
	sort.Strings(newPKs)
	for _, pk := range newPKs {
		ev := in.Changes[pk]
		if ev.EventType == event.EventDelete {
			continue
		}
		if ev.RowAfter == nil {
			slog.Error("event has nil RowAfter; skipping to avoid emitting all-NULL tuple",
				"schema", in.Schema, "table", in.Table, "pk", pk,
				"event_type", ev.EventType, "event_id", ev.EventID)
			continue
		}
		if err := emit(ev.RowAfter); err != nil {
			return stats, err
		}
		stats.InsertsEmitted++
	}

	return stats, nil
}

// SnapshotFullTableInput drives SnapshotFullTableImages.
type SnapshotFullTableInput struct {
	// BaselinePath is the baseline Parquet path from FindBaseline — either a
	// local path or an s3:// URL (downloaded to a temp file before merge).
	BaselinePath string
	Schema       string
	Table        string
	// PKCols are the table's primary-key column metas, used to canonicalize
	// baseline PK values so they match the indexer's pk_values strings. The
	// caller is expected to have guarded every PKCol with SupportedPKType.
	PKCols []metadata.ColumnMeta
	// Changes maps pk_values string → the latest binlog event for that PK in
	// (baseline, AsOf]. Drained by the merge.
	Changes map[string]*query.ResultRow
}

// SnapshotFullTableImages reconstructs the full row state of a table at the
// AsOf instant by merging the baseline snapshot with the change map, calling
// emit once per surviving row (baseline column order, values verbatim from
// DuckDB / the binlog event). It is the in-memory sibling of
// mergeBaselineIntoWriter, for the shim's full-table _snapshot path: instead
// of writing mydumper output it streams row maps to the caller, which builds
// the wire resultset and enforces its own row cap (by returning an error from
// emit).
//
// s3:// baselines are downloaded to a temp file first; the temp dir is removed
// before return. Real failures (unreadable baseline, DuckDB error, a PK value
// the canonicalizer can't translate) are returned as errors for the caller to
// surface; the caller decides separately — before calling this — whether a
// missing baseline or an unsupported PK type should instead fall back to a
// binlog-only path.
func SnapshotFullTableImages(ctx context.Context, in SnapshotFullTableInput, emit func(map[string]any) error) error {
	// Residual unchanged-TOAST marker (#592) → refuse before materializing the
	// baseline (possibly an S3 download); see mergeBaselineIntoWriter.
	if err := checkChangesToast(in.Changes); err != nil {
		return err
	}

	localPath, cleanup, err := materializeBaselineLocal(ctx, in.BaselinePath)
	if err != nil {
		return fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()

	_, err = mergeBaselineImages(ctx, mergeCore{
		LocalBaselinePath: localPath,
		Schema:            in.Schema,
		Table:             in.Table,
		PKCols:            in.PKCols,
		Changes:           in.Changes,
	}, emit)
	return err
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// checkChangesToast scans a full-table change map for a residual
// unchanged-TOAST marker (#592). Both full-table entry points (the mydumper
// writer and the shim's in-memory _snapshot) call it BEFORE any IO — every
// change is destined for the output (an UPDATE/INSERT overwrites the baseline
// row with its row_after, a leftover change is appended as a new row), so a
// marker anywhere in the map means the output would carry the marker's JSON
// instead of a real value.
func checkChangesToast(changes map[string]*query.ResultRow) error {
	for _, ev := range changes {
		if err := checkEventToast(*ev); err != nil {
			return err
		}
	}
	return nil
}

// reconstructBinlogOnly is the ErrNoBaseline fallback for full-table
// reconstruct (#766). A table created after the last baseline snapshot has no
// Parquet to merge against; previously ReconstructTable stopped there and
// returned an empty report, silently discarding every row that only ever
// existed in the binlog — even a table with thousands of rows. This mirrors
// the shim's binlog-only degrade (internal/shim/snapshot.go
// runSnapshotFullTable falling back to runFullTable): fetch every event for
// the table up to cfg.At and emit the latest surviving row per PK, skipping
// DELETEs. There is no baseline Parquet to read a CREATE TABLE statement
// from, and fabricating one from schema_snapshots column metadata risks
// silently shipping a wrong PK/engine/charset/index definition as fact — so
// the schema file records why it's missing instead, and the caller must
// supply the table structure before loading the accompanying data file(s).
func reconstructBinlogOnly(
	ctx context.Context,
	cfg FullTableConfig,
	schema, table string,
	db *sql.DB,
	engine *query.Engine,
	resolver *metadata.Resolver,
	dbName string,
	rep *TableReport,
	start time.Time,
) (*TableReport, error) {
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("resolve schema for %s.%s: %w; run `bintrail snapshot` to refresh", schema, table, err)
	}
	if len(tm.PKColumnMetas()) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key in the loaded snapshot; full-table reconstruct requires a PK", schema, table)
	}

	// Generated columns can't be set explicitly in an INSERT, so exclude them
	// from the emitted schema — same exclusion `bintrail baseline` applies.
	colNames := make([]string, 0, len(tm.Columns))
	for _, c := range tm.Columns {
		if c.IsGenerated {
			continue
		}
		colNames = append(colNames, c.Name)
	}
	if len(colNames) == 0 {
		return nil, fmt.Errorf("%s.%s has no non-generated columns in the loaded snapshot", schema, table)
	}

	fetcher := cfg.ArchiveFetcher
	if fetcher == nil {
		fetcher = parquetquery.Fetch
	}
	events, _, err := query.FetchMerged(ctx, db, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema: schema,
			Table:  table,
			Until:  &cfg.At,
			// No Since — there is no baseline instant to anchor from; fetch
			// the whole retained binlog-only window up to cfg.At.
		},
		DBName:         dbName,
		NoArchive:      false,
		AllowGaps:      cfg.AllowGaps,
		ArchiveFetcher: fetcher,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch events: %w", err)
	}
	rep.EventsApplied = int64(len(events))
	maybeWarnEventVolume(schema, table, len(events), cfg.WarnEventThreshold)

	MapEventEnumLabels(db, resolver, schema, table, events)
	DecodeEventBinaries(db, schema, table, events)

	// Latest event per PK; events is already sorted by (event_timestamp,
	// event_id) via query.MergeResults, so the last write wins naturally.
	changes := make(map[string]*query.ResultRow, len(events))
	for i := range events {
		changes[events[i].PKValues] = &events[i]
	}

	// A table that only ever existed after the last baseline was very likely
	// CREATEd during the retained binlog window, and the schema-drift guard
	// (#700) records every CREATE TABLE's exact DDL text in schema_changes.
	// Prefer that real, captured statement over a fabricated one; only fall
	// back to the explanatory placeholder when no such record exists (e.g.
	// the table predates schema_changes, or genuinely was never baselined
	// for another reason).
	createSQL, ddlFound, err := findCapturedCreateTableDDL(ctx, db, schema, table, cfg.At)
	if err != nil {
		slog.Warn("could not query schema_changes for a captured CREATE TABLE; using placeholder schema file",
			"schema", schema, "table", table, "error", err)
	}
	if ddlFound {
		// The captured DDL is from CREATE time; it does not reflect any ALTER
		// applied later, up to cfg.At (that class of point-in-time schema
		// drift is the separately-tracked #600/#601/#602 family). Label it so
		// a reader doesn't mistake it for a verified-current definition.
		createSQL = "-- bintrail: CREATE TABLE captured from schema_changes at table-creation time\n" +
			"-- (binlog-only fallback, #766). If the table was ALTERed afterwards, this may\n" +
			"-- not match the columns in the accompanying data file(s) — verify before loading.\n" +
			createSQL
	} else {
		createSQL = binlogOnlySchemaPlaceholder(schema, table)
	}

	rep.BinlogOnly = true
	if err := writeBinlogOnlyChanges(cfg.OutputDir, schema, table, colNames, cfg.ChunkSize, createSQL, changes, rep); err != nil {
		return nil, err
	}
	rep.Duration = time.Since(start)

	slog.Warn("full-table reconstruct: no baseline found for table; recovered via binlog-only fallback "+
		"(rows outside the retained binlog window, if any, are not included)",
		"schema", schema, "table", table,
		"events_applied", rep.EventsApplied,
		"inserts_emitted", rep.InsertsEmitted,
		"deletes_skipped", rep.DeletesSkipped)

	return rep, nil
}

// binlogOnlySchemaPlaceholder is the schema-file text used when no captured
// CREATE TABLE DDL is available: fabricating one from column metadata risks
// silently shipping a wrong PK/engine/charset/index definition as fact.
func binlogOnlySchemaPlaceholder(schema, table string) string {
	return fmt.Sprintf(
		"-- bintrail: no baseline snapshot exists for %s.%s (table created after the last\n"+
			"-- `bintrail baseline` run, or never baselined), and no CREATE TABLE statement for\n"+
			"-- it was found in schema_changes either. The rows in the accompanying data\n"+
			"-- file(s) were recovered entirely from the binlog (binlog-only fallback, #766);\n"+
			"-- the table structure is deliberately NOT fabricated here. Create the table\n"+
			"-- structure yourself before loading the data file(s).\n",
		schema, table)
}

// findCapturedCreateTableDDL looks up the most recent CREATE TABLE statement
// the schema-drift guard (#700) recorded for schema.table at-or-before at, in
// schema_changes.ddl_query. found is false (with a nil error) when no such
// row exists — the caller falls back to binlogOnlySchemaPlaceholder.
func findCapturedCreateTableDDL(ctx context.Context, db *sql.DB, schema, table string, at time.Time) (ddl string, found bool, err error) {
	row := db.QueryRowContext(ctx, `
		SELECT ddl_query FROM schema_changes
		WHERE schema_name = ? AND table_name = ? AND ddl_type = ? AND detected_at <= ?
		ORDER BY detected_at DESC, id DESC
		LIMIT 1`,
		schema, table, event.DDLCreateTable, at)
	if err := row.Scan(&ddl); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return ddl, true, nil
}

// writeBinlogOnlyChanges writes the reconstructBinlogOnly output: a schema
// file (createSQL — either a real captured CREATE TABLE or the explanatory
// placeholder, decided by the caller) and one data row per surviving entry in
// changes, in deterministic PK order. Updates rep.InsertsEmitted/
// DeletesSkipped/Files in place. Split out from reconstructBinlogOnly so it's
// unit-testable without a MySQL index connection (it does no IO beyond
// outputDir).
func writeBinlogOnlyChanges(
	outputDir, schema, table string,
	colNames []string,
	chunkSize int64,
	createSQL string,
	changes map[string]*query.ResultRow,
	rep *TableReport,
) error {
	if err := checkChangesToast(changes); err != nil {
		return err
	}

	mw, err := NewMydumperWriter(outputDir, schema, table, colNames, chunkSize)
	if err != nil {
		return fmt.Errorf("open mydumper writer: %w", err)
	}
	defer func() { _ = mw.Close() }() // no-op after the explicit Close below on the happy path

	if err := mw.WriteSchema(createSQL); err != nil {
		return err
	}

	// Deterministic order: sort by PK string so tests can assert on output
	// without flakiness (mirrors the "new PKs" tail loop in mergeBaselineImages).
	pks := make([]string, 0, len(changes))
	for pk := range changes {
		pks = append(pks, pk)
	}
	sort.Strings(pks)
	for _, pk := range pks {
		ev := changes[pk]
		if ev.EventType == event.EventDelete {
			rep.DeletesSkipped++
			continue
		}
		if ev.RowAfter == nil {
			slog.Error("event has nil RowAfter; skipping to avoid emitting all-NULL tuple",
				"schema", schema, "table", table, "pk", pk,
				"event_type", ev.EventType, "event_id", ev.EventID)
			continue
		}
		if err := mw.WriteRow(rowAfterOrdered(ev.RowAfter, colNames, schema, table)); err != nil {
			return err
		}
		rep.InsertsEmitted++
	}

	if err := mw.Close(); err != nil {
		return fmt.Errorf("close mydumper writer: %w", err)
	}
	rep.Files = mw.Files()
	return nil
}

// splitSchemaTable parses "db.table" into (db, table, true). Rejects entries
// with zero or more than one dot.
func splitSchemaTable(entry string) (string, string, bool) {
	parts := strings.SplitN(entry, ".", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	if parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	if strings.Contains(parts[1], ".") {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// materializeBaselineLocal ensures the baseline Parquet is available on the
// local filesystem. Local paths are returned as-is with a no-op cleanup. S3
// URLs are downloaded to a temp file via DuckDB's httpfs + COPY so DuckDB
// can then query the resulting local file without an outbound connection.
func materializeBaselineLocal(ctx context.Context, path string) (string, func(), error) {
	if !strings.HasPrefix(path, "s3://") {
		// At-rest integrity (#636): validate the local file against its snapshot's
		// _MANIFEST before any reader trusts it (DuckDB validates nothing). Fail
		// loud on corruption; a legacy snapshot with no manifest is a no-op. S3
		// baselines are not validated here yet — the COPY below re-encodes them, so
		// the temp is not byte-identical to the object — a follow-up.
		if err := baselineintegrity.ValidateLocalFile(path); err != nil {
			return "", nil, err
		}
		return path, func() {}, nil
	}
	baselineintegrity.WarnS3IntegrityNotValidated() // #636 covers local baselines only (S3 re-encode → follow-up)
	// Download via DuckDB httpfs. Keep the temp file around until cleanup().
	tmpDir, err := os.MkdirTemp("", "bintrail-baseline-*")
	if err != nil {
		return "", nil, fmt.Errorf("mkdir temp: %w", err)
	}
	tmpPath := filepath.Join(tmpDir, "baseline.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("load httpfs: %w", err)
	}
	duckdbutil.EnableS3CredentialChain(ctx, db)
	safeSrc := strings.ReplaceAll(path, "'", "''")
	safeDst := strings.ReplaceAll(tmpPath, "'", "''")
	copyQ := fmt.Sprintf("COPY (SELECT * FROM parquet_scan('%s')) TO '%s' (FORMAT PARQUET)", safeSrc, safeDst)
	if _, err := db.ExecContext(ctx, copyQ); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("download s3 baseline: %w", err)
	}

	cleanup := func() { os.RemoveAll(tmpDir) }
	return tmpPath, cleanup, nil
}

// ReadBaselineColumns returns the column names of a baseline Parquet file (local
// or s3://). This is the authoritative non-generated column set for fingerprinting
// the table: mydumper excludes true STORED/VIRTUAL generated columns from the dump
// but keeps ordinary expression-default (DEFAULT_GENERATED) columns, so the Parquet
// schema is exactly the set to hash — deriving it from a schema snapshot's
// is_generated flag instead would wrongly drop DEFAULT_GENERATED columns (the
// trap consistency.ConsistentTableChecksum documents) and silently under-verify them.
func ReadBaselineColumns(ctx context.Context, path string) ([]string, error) {
	localPath, cleanup, err := materializeBaselineLocal(ctx, path)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return readBaselineColumns(ctx, localPath)
}

// readBaselineColumns opens the local Parquet file with DuckDB and returns
// the column names in the order parquet_scan() emits them. This order is
// the canonical column order for the emitted INSERT statements.
func readBaselineColumns(ctx context.Context, localPath string) ([]string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	safePath := strings.ReplaceAll(localPath, "'", "''")
	q := fmt.Sprintf("SELECT * FROM parquet_scan('%s') LIMIT 0", safePath)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("describe baseline: %w", err)
	}
	defer rows.Close()
	return rows.Columns()
}

// zipMap pairs cols with vals to produce a map[string]any view of a row.
// In mergeBaselineImages the result is used both for PK canonicalization and,
// for a pass-through baseline row, as the emitted row itself — so the map is
// retained downstream (see the retention-safety note in that loop). It must
// not be backed by a buffer reused across DuckDB Scan iterations.
func zipMap(cols []string, vals []any) map[string]any {
	out := make(map[string]any, len(cols))
	for i, c := range cols {
		out[c] = vals[i]
	}
	return out
}

// postBaselineColumns returns, sorted and de-duplicated, the column names
// that appear in some non-DELETE event's row_after image but are absent from
// the baseline column set — i.e. columns ADDED to the source table after the
// baseline snapshot. The mydumper writer projects every emitted row onto the
// baseline columns (rowAfterOrdered), so these columns' values would be
// dropped silently; mergeBaselineIntoWriter calls this up front to refuse the
// run instead (#602). DELETE events carry no row_after and are skipped.
func postBaselineColumns(changes map[string]*query.ResultRow, colNames []string) []string {
	baseline := make(map[string]struct{}, len(colNames))
	for _, c := range colNames {
		baseline[c] = struct{}{}
	}
	extra := make(map[string]struct{})
	for _, ev := range changes {
		if ev == nil || ev.EventType == event.EventDelete || ev.RowAfter == nil {
			continue
		}
		for col := range ev.RowAfter {
			if _, ok := baseline[col]; !ok {
				extra[col] = struct{}{}
			}
		}
	}
	if len(extra) == 0 {
		return nil
	}
	out := make([]string, 0, len(extra))
	for col := range extra {
		out = append(out, col)
	}
	sort.Strings(out)
	return out
}

// These helpers reverse the storage-side base64 encoding of BLOB/TEXT
// delta-event values for the mydumper reconstruct path (#653/#660). go-mysql
// delivers BLOB and TEXT (both MYSQL_TYPE_BLOB) as []byte, which marshalRow
// base64-encodes into the binlog_events JSON; a delta event's row_after
// therefore carries them as base64 STRINGS, which would otherwise reach
// FormatSQLValue's string branch and be written to the mydumper dump verbatim.
//
// Provenance matters: decoding is applied ONLY to event-sourced values (the
// Changes map, where every value came from binlog_events JSON), never to a
// merged row at emit time — a baseline TEXT value reaches the DuckDB scan as a
// Go string (parquet.String()), indistinguishable from a base64 event value, so
// decoding by Go-type at emit time would corrupt valid-base64 baseline text.
//
// base64StoredKind and decodeStoredBase64 mirror the decode added for the recover
// path in #653 (sibling PR #662, not yet in main); they are duplicated here
// because that copy is unexported. binaryColsFromTableMeta is the
// fulltable-specific builder. A follow-up should hoist one shared copy once both
// land (#661 is the third consumer).
// "json" is included (non-binary) as a defense-in-depth companion to #736:
// marshalRow now only promotes a []byte to raw JSON when it looks like a
// JSON container ({ or [), so a JSON column whose top-level value is itself a
// bare scalar (rare, but legal) falls through to this same base64 path
// instead of failing to round-trip.
//
// "binary"/"varbinary" are included (binary) since #756: metadata.MapRow now
// reinterprets those two DataTypes as []byte (they arrive from go-mysql as a
// raw Go string with no charset, which json.Marshal could silently corrupt to
// U+FFFD), so they take the same []byte-to-base64 storage path as BLOB and
// must be decoded the same way here.
//
// Retroactive-reclassification risk (#756, accepted): unlike BLOB/TEXT (always
// []byte-and-base64 from day one), a BINARY/VARBINARY event indexed BEFORE
// this fix was stored as a plain, non-base64 string. decodeStoredBase64 can't
// tell that apart from a post-fix base64 string, so a pre-fix value whose raw
// bytes happen to satisfy the base64 alphabet+padding decodes to different,
// wrong bytes with no error — astronomically unlikely for random binary
// content, but plausible for a VARBINARY column storing ASCII-like data. See
// the fuller rationale on the sibling copy in internal/recovery/recovery.go.
func base64StoredKind(dataType string) (binary, ok bool) {
	switch strings.ToLower(dataType) {
	case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary":
		return true, true
	case "text", "tinytext", "mediumtext", "longtext", "json":
		return false, true
	default:
		return false, false
	}
}

// decodeStoredBase64 also repairs events indexed before marshalRow was fixed
// (#736) to gate on looksLikeJSONContainer: such an event may hold a
// BLOB/TEXT value mis-promoted to a bare JSON scalar (e.g. the literal string
// "false" stored as the JSON boolean false), decoding here as a Go
// bool/json.Number instead of a string. That value IS the column's original
// textual literal, so it is restored directly. A value that decoded to Go
// nil (originally the string "null") is NOT repairable — indistinguishable
// from a genuine SQL NULL — and is left as nil. This nil case, and a bare
// JSON *string* scalar (bytes like `"YWJj"`, quotes included) that was
// mis-promoted the same way, are historical-only gaps: by the time this
// runs, the pre-#736 marshalRow had already parsed the outer quotes away as
// ordinary JSON-string syntax, so the value arriving here is the
// already-quote-stripped text (`YWJj`), indistinguishable from genuine
// base64 content and wrongly re-decoded on top of the original corruption —
// not repairable, a real fix belongs at the storage encoding, out of scope
// here. A genuine JSON column captured AFTER this fix with a bare
// string-scalar value does NOT hit this gap: it takes the ordinary
// []byte-to-base64 path (same as any TEXT/BLOB), and this function correctly
// reverses it to the original bytes, quotes included — which is exactly the
// text MySQL needs to re-parse the value back into that JSON column.
func decodeStoredBase64(v any, binary bool) any {
	var text string
	switch val := v.(type) {
	case string:
		b, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return v
		}
		if binary {
			return b
		}
		return string(b)
	case bool:
		text = strconv.FormatBool(val)
	case json.Number:
		text = string(val)
	default:
		return v
	}
	if binary {
		return []byte(text)
	}
	return text
}

// binaryColsFromTableMeta maps each BLOB/TEXT column of a table to whether it is
// binary, for decoding delta-event row_after values. Returns nil when no column
// needs decoding.
func binaryColsFromTableMeta(tm *metadata.TableMeta) map[string]bool {
	var m map[string]bool
	for _, c := range tm.Columns {
		if binary, ok := base64StoredKind(c.DataType); ok {
			if m == nil {
				m = make(map[string]bool)
			}
			m[c.Name] = binary
		}
	}
	return m
}

// rowAfterOrdered walks colNames and looks up each name in rowAfter (a
// map[string]any from a binlog event's row_after image), returning a slice
// of values aligned to the baseline Parquet column order. A baseline column
// absent from this event's row_after becomes nil (SQL NULL) with an slog.Warn
// — e.g. a column DROPPED after the baseline (newer events stop carrying it)
// or a partial image. The opposite direction — a column ADDED after the
// baseline, present in row_after but not in colNames — is handled up front by
// the postBaselineColumns guard in mergeBaselineIntoWriter, which refuses the
// run rather than letting this function drop the value silently (#602).
//
// Both baseline pass-through rows and delta-event after-images flow through
// here, so it must NOT base64-decode BLOB/TEXT values: a baseline TEXT value is
// a Go string (DuckDB scans parquet.String() as string) indistinguishable from
// a base64 event value, and decoding it would corrupt valid-base64 baseline
// text. Event values are decoded upstream, epoch-aware, before the change map
// is even built (see DecodeEventBinaries in ReconstructTable, #668).
func rowAfterOrdered(rowAfter map[string]any, colNames []string, schema, table string) []any {
	out := make([]any, len(colNames))
	for i, col := range colNames {
		v, ok := rowAfter[col]
		if !ok {
			slog.Warn("event row_after missing column present in baseline; emitting NULL",
				"schema", schema, "table", table, "column", col)
			out[i] = nil
			continue
		}
		out[i] = v
	}
	return out
}
