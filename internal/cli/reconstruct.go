package cli

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

var reconstructCmd = &cobra.Command{
	Use:   "reconstruct",
	Short: "Reconstruct the state of a row at a given point in time",
	Long: `Combine a baseline Parquet snapshot with indexed binlog events to reconstruct
the exact state of a row at a target timestamp.

Requires a baseline directory or S3 location produced by "bintrail baseline".
The most recent snapshot at or before --at is automatically selected.

Events are fetched from both live MySQL partitions and any Parquet archives
auto-discovered via archive_state. Pass --no-archive to query MySQL only.
By default, a coverage gap (an hour rotated out of MySQL with no archive)
aborts the reconstruction; pass --allow-gaps to proceed with a warning.

Full-table mode (--output-format mydumper) reconstructs entire tables at a
target point in time and emits a mydumper-compatible dump directory
(schema file + chunked INSERT files + metadata) restorable with a plain
mysql client. Use --tables schema.table,... to select which tables to
reconstruct and --output-dir for the destination.

Examples:
  # Current state of a row (baseline + all binlog events up to now)
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines

  # Full-table point-in-time dump for multiple tables
  bintrail reconstruct --index-dsn "..." \
    --tables mydb.orders,mydb.users --baseline-dir /data/baselines \
    --at "2026-04-01 15:30:00" \
    --output-format mydumper --output-dir ./pitr-dump

  # State at a past timestamp
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines \
    --at "2026-02-15 14:30:00"

  # Full change history (one entry per binlog event)
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines --history

  # Baseline snapshot only — no binlog replay, no --index-dsn needed
  bintrail reconstruct --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines --baseline-only

  # Free-form DuckDB SQL against a baseline directory
  bintrail reconstruct \
    --sql "SELECT * FROM parquet_scan('/data/baselines/2026-02-28T00-00-00Z/mydb/orders.parquet') LIMIT 10"

  # S3 baseline (uses standard AWS credential chain)
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-s3 s3://bucket/baselines`,
	RunE: runReconstruct,
}

var (
	recIndexDSN     string
	recSchema       string
	recTable        string
	recPK           string
	recPKColumns    string
	recAt           string
	recBaselineDir  string
	recBaselineS3   string
	recBaselineOnly bool
	recHistory      bool
	recSQL          string
	recFormat       string
	recNoArchive    bool
	recAllowGaps    bool

	// Full-table mydumper output mode (#187).
	recOutputFormat string
	recOutputDir    string
	recTables       string
	recChunkSize    string
	recParallelism  int
	recWarnEvents   int64
)

func init() {
	reconstructCmd.Flags().StringVar(&recIndexDSN, "index-dsn", "", "DSN for the index MySQL database (not required with --baseline-only or --sql)")
	reconstructCmd.Flags().StringVar(&recSchema, "schema", "", "Schema (database) name")
	reconstructCmd.Flags().StringVar(&recTable, "table", "", "Table name")
	reconstructCmd.Flags().StringVar(&recPK, "pk", "", "Primary key value(s), pipe-delimited for composite PKs (e.g. 12345 or 12345|2)")
	reconstructCmd.Flags().StringVar(&recPKColumns, "pk-columns", "", "Comma-separated PK column name(s) matching --pk order (e.g. id or order_id,item_id)")
	reconstructCmd.Flags().StringVar(&recAt, "at", "", "Target timestamp for reconstruction (default: now); accepts 2006-01-02 15:04:05 or RFC3339")
	reconstructCmd.Flags().StringVar(&recBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots produced by bintrail baseline")
	reconstructCmd.Flags().StringVar(&recBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline Parquet snapshots (e.g. s3://bucket/baselines/); uses the standard AWS credential chain")
	reconstructCmd.Flags().BoolVar(&recBaselineOnly, "baseline-only", false, "Return the baseline row without applying binlog events (no --index-dsn needed)")
	reconstructCmd.Flags().BoolVar(&recHistory, "history", false, "Return all intermediate states (one entry per binlog event) instead of just the final state")
	reconstructCmd.Flags().StringVar(&recSQL, "sql", "", "Execute arbitrary DuckDB SQL and print results (bypasses --schema/table/pk/at; --baseline-dir/s3 only controls whether the httpfs extension is loaded for S3 access)")
	reconstructCmd.Flags().StringVar(&recFormat, "format", "json", "Output format: json, table, or csv")
	reconstructCmd.Flags().BoolVar(&recNoArchive, "no-archive", false, "Disable auto-routing to Parquet archives (MySQL-only event fetch)")
	reconstructCmd.Flags().BoolVar(&recAllowGaps, "allow-gaps", false, "Proceed even when the event index has missing hours in the baseline-to-target range (may produce incomplete reconstruction)")
	// Full-table mydumper mode (#187).
	reconstructCmd.Flags().StringVar(&recOutputFormat, "output-format", "", "Output format for full-table mode: 'mydumper' to produce a mydumper-compatible dump directory (default: single-row mode)")
	reconstructCmd.Flags().StringVar(&recOutputDir, "output-dir", "", "Output directory for --output-format=mydumper (will be created if missing)")
	reconstructCmd.Flags().StringVar(&recTables, "tables", "", "Comma-separated schema.table list for --output-format=mydumper (e.g. mydb.orders,mydb.users)")
	reconstructCmd.Flags().StringVar(&recChunkSize, "chunk-size", "256MB", "Max size per SQL chunk file in full-table mode (e.g. 64MB, 1GB)")
	reconstructCmd.Flags().IntVar(&recParallelism, "parallelism", 0, "Max tables to reconstruct concurrently in full-table mode (default: runtime.NumCPU())")
	reconstructCmd.Flags().Int64Var(&recWarnEvents, "warn-event-threshold", 5_000_000, "Full-table mode: log a memory warning when a table's reconstruct window exceeds this many events (full-table reconstruct holds them all in RAM, #654; 0 disables)")
	AddDuckDBTuningFlags(reconstructCmd)
	BindCommandEnv(reconstructCmd)

}

func runReconstruct(cmd *cobra.Command, args []string) error {
	start := time.Now()

	// ── --output-format mydumper mode: full-table reconstruct (#187) ───────────
	if recOutputFormat != "" {
		if recOutputFormat != "mydumper" {
			return fmt.Errorf("--output-format: only 'mydumper' is supported, got %q", recOutputFormat)
		}
		return runReconstructFullTable(cmd, start)
	}

	// ── --sql mode: execute arbitrary DuckDB SQL ───────────────────────────────
	if recSQL != "" {
		return runReconstructSQL(cmd, start)
	}

	// ── Validate flags ─────────────────────────────────────────────────────────
	if !cliutil.IsValidFormat(recFormat) {
		return fmt.Errorf("invalid --format %q; must be json, table, or csv", recFormat)
	}
	if recSchema == "" {
		return fmt.Errorf("--schema is required")
	}
	if recTable == "" {
		return fmt.Errorf("--table is required")
	}
	if recPK == "" {
		return fmt.Errorf("--pk is required")
	}
	if recPKColumns == "" {
		return fmt.Errorf("--pk-columns is required")
	}
	if recBaselineDir == "" && recBaselineS3 == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required")
	}
	if !recBaselineOnly && recIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required unless --baseline-only is set")
	}
	if recHistory && recBaselineOnly {
		return fmt.Errorf("--history and --baseline-only are mutually exclusive")
	}

	// ── Parse --at ─────────────────────────────────────────────────────────────
	at := time.Now().UTC()
	if recAt != "" {
		parsed, err := cliutil.ParseTime(recAt)
		if err != nil {
			return fmt.Errorf("--at: %w", err)
		}
		if parsed != nil {
			at = *parsed
		}
	}

	// ── Build pkFilter from --pk and --pk-columns ──────────────────────────────
	// Note: --pk uses | as the composite PK separator. Literal | in PK values
	// is not supported (strings.Split cannot honour the \| escaping convention).
	pkCols := strings.Split(recPKColumns, ",")
	pkVals := strings.Split(recPK, "|")
	if len(pkCols) != len(pkVals) {
		return fmt.Errorf("--pk has %d value(s) but --pk-columns has %d column(s); they must match",
			len(pkVals), len(pkCols))
	}
	pkFilter := make(map[string]string, len(pkCols))
	for i, col := range pkCols {
		pkFilter[strings.TrimSpace(col)] = pkVals[i]
	}

	// ── Choose baseline source ─────────────────────────────────────────────────
	baselineSrc := recBaselineDir
	if baselineSrc == "" {
		baselineSrc = recBaselineS3
	}

	// ── Find and read the baseline snapshot ────────────────────────────────────
	// The stale-fallback warning (#466) is already logged inside FindBaseline;
	// the CLI relies on that server-side log.
	baselinePath, snapshotTime, _, err := reconstruct.FindBaseline(cmd.Context(), baselineSrc, recSchema, recTable, at)
	if err != nil {
		return err
	}
	slog.Debug("found baseline snapshot", "path", baselinePath, "snapshot_time", snapshotTime.UTC().Format(time.RFC3339))

	// Read baseline binlog position metadata (local files only).
	var bmeta baseline.DumpMetadata
	if !strings.HasPrefix(baselinePath, "s3://") {
		var metaErr error
		bmeta, metaErr = baseline.ReadParquetMetadata(baselinePath)
		if metaErr != nil {
			slog.Warn("could not read baseline metadata", "error", metaErr)
		} else if bmeta.BinlogFile != "" {
			slog.Debug("baseline binlog position",
				"file", bmeta.BinlogFile, "pos", bmeta.BinlogPos, "gtid", bmeta.GTIDSet)
		}
	}

	baselineRow, err := reconstruct.ReadBaselineRow(cmd.Context(), baselinePath, pkFilter)
	if err != nil {
		return fmt.Errorf("read baseline: %w", err)
	}
	if baselineRow == nil {
		return fmt.Errorf("no row found in baseline %q matching pk filter %v", baselinePath, pkFilter)
	}

	// ── Baseline-only mode ─────────────────────────────────────────────────────
	if recBaselineOnly {
		if err := writeReconstructOutput(baselineRow, nil, snapshotTime, at, false, recFormat, os.Stdout); err != nil {
			return err
		}
		slog.Info("reconstruct complete",
			"mode", "baseline-only",
			"snapshot", snapshotTime.UTC().Format(time.RFC3339),
			"duration_ms", time.Since(start).Milliseconds())
		return nil
	}

	// ── Fetch binlog events from live MySQL + archives ────────────────────────
	// Routed through query.FetchMerged so archived events are not silently
	// missed — the single-row path previously called engine.Fetch directly
	// (#209). Strict mode (AllowGaps=false) aborts on any condition that
	// would silently degrade coverage.
	db, err := config.Connect(recIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index database: %w", err)
	}
	defer db.Close()

	// Idempotent schema migration: the query engine SELECTs
	// post-initial-schema binlog_events columns (query_text/query_hash,
	// #699); without this, single-row/--history reconstruct against a
	// pre-upgrade index 1054s while full-table mode (which already calls
	// EnsureSchema in reconstruct.ReconstructTable) self-heals.
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("ensure index schema: %w", err)
	}

	// Refuse if a TRUNCATE/DROP/RENAME hit this table in the window: it emits
	// no row events, so folding the fetched deltas onto the baseline below
	// would silently resolve a truncated-away row as if it still existed at
	// --at (#764; same guard as the full-table path and the shim's
	// _snapshot).
	if err := reconstruct.CheckDestructiveDDL(cmd.Context(), db, recSchema, recTable, snapshotTime, at); err != nil {
		return err
	}

	engine := query.New(db)

	// Refuse/warn on a stamped capture gap inside the window (#765): a
	// query.GapError below only catches archive-coverage gaps (rotated hours
	// with no archive); stream_state.gap_lost_at records events permanently
	// lost at the source, which no archive can fill.
	if err := reconstruct.CheckCaptureGap(cmd.Context(), db, recSchema, recTable, snapshotTime, at, recAllowGaps); err != nil {
		return err
	}

	// The planner needs a database name derived from the DSN.
	var dbName string
	if cfg, parseErr := mysqldriver.ParseDSN(recIndexDSN); parseErr != nil {
		slog.Warn("could not parse DSN for query planning", "error", parseErr)
	} else {
		dbName = cfg.DBName
	}

	opts := query.Options{
		Schema:   recSchema,
		Table:    recTable,
		PKValues: recPK,
		Since:    &snapshotTime,
		Until:    &at,
	}
	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}
	events, _, err := query.FetchMerged(cmd.Context(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      recNoArchive,
		AllowGaps:      recAllowGaps,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
	})
	if err != nil {
		// Surface CLI hints only at the CLI layer; the library types
		// (query.GapError, query.SourceEmptyError) stay command-neutral.
		var gapErr *query.GapError
		if errors.As(err, &gapErr) {
			return fmt.Errorf("%w; pass --allow-gaps to proceed with an incomplete reconstruction", err)
		}
		var emptyErr *query.SourceEmptyError
		if errors.As(err, &emptyErr) {
			return fmt.Errorf("%w; run `bintrail archive reconcile` to re-sync archive_state with storage, or pass --allow-gaps to proceed without that source", err)
		}
		return fmt.Errorf("fetch binlog events: %w", err)
	}
	slog.Debug("fetched binlog events", "count", len(events))

	// ENUM/SET ordinals → labels (#476), each delta decoded with the
	// snapshot in effect at its event time (#475). No latest-resolver
	// fallback here: if the epoch lookup fails, ordinals pass through
	// raw — the pre-#476 CLI output.
	reconstruct.MapEventEnumLabels(db, nil, recSchema, recTable, events)
	// BLOB/TEXT columns are stored base64-encoded; decode them on the deltas
	// before the ApplyAt/BuildHistory fold so the output carries the real value,
	// not its base64 text (#666). Baseline rows are read raw and untouched.
	reconstruct.DecodeEventBinaries(db, recSchema, recTable, events)

	// Warn if there is a gap between the baseline position and the first indexed
	// event — events in that gap are missing from the reconstruction. The
	// comparable anchor is flavor-dependent (#593): PostgreSQL baselines anchor
	// on the numeric WAL LSN (baseline.MetaKeyLSN); MySQL/MariaDB on binlog
	// file+pos. PG LSN TEXT ("0/1A2B3C4") is NOT lexically ordered, so the
	// binlog_file column must never be compared for a PG source — see
	// reconstruct.GapDetected and resolveGapCheck.
	if len(events) > 0 {
		first := events[0]
		flavor, lineageGuard, anchorPresent, eventPosMissing := resolveGapCheck(
			query.SourceFlavor(db), bmeta, first.BinlogFile, first.StartPos)
		if lineageGuard {
			slog.Warn("source flavor unknown but baseline carries an LSN anchor — treating source as postgres for gap detection (LSN text is never compared lexically)",
				"baseline_lsn", bmeta.LSN)
		}
		switch {
		case !anchorPresent && flavor == "postgres":
			// The permanent steady state for every PG baseline until the
			// LSN-writing producer ships (#593 slice C): the reconstruction may
			// contain an undetectable hole, which deserves the same visibility
			// as the #318 missing-event-position warn below — not an INFO line
			// invisible at --log-level warn. No remediation command exists yet
			// ("bintrail baseline" is the MySQL/mydumper pipeline), so the
			// message must not recommend one.
			slog.Warn("gap detection unavailable — this baseline predates LSN anchoring (no bintrail.baseline_lsn metadata); a gap between the baseline and the first indexed event would go undetected",
				"flavor", flavor)
		case !anchorPresent:
			slog.Info("gap detection skipped — baseline lacks position metadata; consider re-running 'bintrail baseline' to embed position data",
				"flavor", flavor)
		case eventPosMissing:
			// A first event with no comparable position — NULL binlog_file
			// (MySQL) or a zero LSN (PostgreSQL; 0 is not a valid WAL position)
			// — skips the gap check rather than silently degrade to "no gap".
			// See dbtrail/bintrail#318.
			slog.Warn("gap detection skipped — first indexed event lacks position metadata",
				"event_id", first.EventID,
				"baseline_file", bmeta.BinlogFile,
				"baseline_pos", bmeta.BinlogPos,
				"baseline_lsn", bmeta.LSN,
				"flavor", flavor)
		case reconstruct.GapDetected(flavor, first.BinlogFile, first.StartPos, bmeta.BinlogFile, bmeta.BinlogPos, bmeta.LSN):
			slog.Warn("gap between baseline and first indexed event — reconstruction may be incomplete",
				"baseline_file", bmeta.BinlogFile,
				"baseline_pos", bmeta.BinlogPos,
				"baseline_gtid", bmeta.GTIDSet,
				"baseline_lsn", bmeta.LSN,
				"first_event_file", first.BinlogFile,
				"first_event_pos", first.StartPos,
				"flavor", flavor)
		}
	}

	// ── Reconstruct and format output ──────────────────────────────────────────
	if err := writeReconstructOutput(baselineRow, events, snapshotTime, at, recHistory, recFormat, os.Stdout); err != nil {
		return err
	}

	slog.Info("reconstruct complete",
		"schema", recSchema, "table", recTable, "pk", recPK,
		"at", at.UTC().Format(time.RFC3339),
		"snapshot", snapshotTime.UTC().Format(time.RFC3339),
		"events_applied", len(events),
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// runReconstructSQL handles the --sql mode.
func runReconstructSQL(cmd *cobra.Command, start time.Time) error {
	if !cliutil.IsValidFormat(recFormat) {
		return fmt.Errorf("invalid --format %q; must be json, table, or csv", recFormat)
	}
	source := recBaselineDir
	if source == "" {
		source = recBaselineS3
	}
	results, cols, err := reconstruct.ExecSQL(cmd.Context(), source, recSQL)
	if err != nil {
		return err
	}
	switch recFormat {
	case "json":
		if err := reconstruct.WriteSQLResultsJSON(results, os.Stdout); err != nil {
			return err
		}
	case "csv":
		if err := reconstruct.WriteSQLResultsCSV(results, cols, os.Stdout); err != nil {
			return err
		}
	default:
		if err := reconstruct.WriteSQLResultsTable(results, cols, os.Stdout); err != nil {
			return err
		}
	}
	slog.Info("reconstruct SQL complete",
		"rows", len(results),
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// writeReconstructOutput formats the reconstructed state (or history) to w.
func writeReconstructOutput(baselineRow map[string]any, events []query.ResultRow, snapshotTime, at time.Time, history bool, format string, w io.Writer) error {
	if history {
		entries, err := reconstruct.BuildHistory(baselineRow, snapshotTime, events, at)
		if err != nil {
			return err
		}
		switch format {
		case "json":
			return reconstruct.WriteHistoryJSON(entries, w)
		case "csv":
			return reconstruct.WriteHistoryCSV(entries, w)
		default:
			return reconstruct.WriteHistoryTable(entries, w)
		}
	}
	state, err := reconstruct.ApplyAt(baselineRow, events, at)
	if err != nil {
		return err
	}
	switch format {
	case "json":
		return reconstruct.WriteStateJSON(state, w)
	case "csv":
		return reconstruct.WriteStateCSV(state, w)
	default:
		return reconstruct.WriteStateTable(state, w)
	}
}

// runReconstructFullTable handles --output-format mydumper. It validates
// flag combinations, builds a FullTableConfig, invokes
// reconstruct.ReconstructTables, and prints a one-line summary per table.
func runReconstructFullTable(cmd *cobra.Command, start time.Time) error {
	// ── Validate incompatible flags ────────────────────────────────────────
	if recPK != "" || recPKColumns != "" {
		return fmt.Errorf("--output-format=mydumper is incompatible with --pk / --pk-columns (full-table mode reconstructs every row)")
	}
	if recHistory {
		return fmt.Errorf("--output-format=mydumper is incompatible with --history")
	}
	if recBaselineOnly {
		return fmt.Errorf("--output-format=mydumper is incompatible with --baseline-only")
	}
	if recSQL != "" {
		return fmt.Errorf("--output-format=mydumper is incompatible with --sql")
	}
	if recSchema != "" || recTable != "" {
		return fmt.Errorf("--output-format=mydumper uses --tables for schema.table selection, not --schema/--table")
	}

	// ── Validate required flags ────────────────────────────────────────────
	if recTables == "" {
		return fmt.Errorf("--tables is required with --output-format=mydumper (e.g. --tables mydb.orders,mydb.users)")
	}
	if recOutputDir == "" {
		return fmt.Errorf("--output-dir is required with --output-format=mydumper")
	}
	if recIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required with --output-format=mydumper")
	}
	if recBaselineDir == "" && recBaselineS3 == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required with --output-format=mydumper")
	}

	// ── Parse --at ─────────────────────────────────────────────────────────
	at := time.Now().UTC()
	if recAt != "" {
		parsed, err := cliutil.ParseTime(recAt)
		if err != nil {
			return fmt.Errorf("--at: %w", err)
		}
		if parsed != nil {
			at = *parsed
		}
	}

	// ── Parse --chunk-size ─────────────────────────────────────────────────
	chunkSize, err := cliutil.ParseByteSize(recChunkSize)
	if err != nil {
		return fmt.Errorf("--chunk-size: %w", err)
	}
	if recWarnEvents < 0 {
		return fmt.Errorf("--warn-event-threshold must be >= 0 (0 disables)")
	}

	// ── Parse --tables (comma-separated schema.table list) ────────────────
	tables := splitAndTrim(recTables, ",")
	if len(tables) == 0 {
		return fmt.Errorf("--tables: no entries after trimming")
	}
	for _, entry := range tables {
		if !strings.Contains(entry, ".") {
			return fmt.Errorf("--tables entry %q must be schema.table", entry)
		}
	}

	// ── Pick baseline source (local dir takes precedence) ─────────────────
	baselineSrc := recBaselineDir
	if baselineSrc == "" {
		baselineSrc = recBaselineS3
	}

	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}

	// ── Run ────────────────────────────────────────────────────────────────
	cfg := reconstruct.FullTableConfig{
		IndexDSN:           recIndexDSN,
		BaselineSrc:        baselineSrc,
		Tables:             tables,
		At:                 at,
		OutputDir:          recOutputDir,
		ChunkSize:          chunkSize,
		Parallelism:        recParallelism,
		AllowGaps:          recAllowGaps,
		WarnEventThreshold: recWarnEvents,
		ArchiveFetcher:     TunedArchiveFetcher(duckTuning),
	}
	reports, err := reconstruct.ReconstructTables(cmd.Context(), cfg)
	if err != nil {
		// Same CLI-layer hint as single-row reconstruct (the %w chain
		// survives ReconstructTables' errors.Join, so errors.As still
		// unwraps the library type).
		var emptyErr *query.SourceEmptyError
		if errors.As(err, &emptyErr) {
			return fmt.Errorf("full-table reconstruct: %w; run `bintrail archive reconcile` to re-sync archive_state with storage, or pass --allow-gaps to proceed without that source", err)
		}
		return fmt.Errorf("full-table reconstruct: %w", err)
	}

	// ── Summary ────────────────────────────────────────────────────────────
	var totalRows, totalEvents int64
	for _, rep := range reports {
		totalRows += rep.BaselineRows + rep.UpdatesApplied + rep.InsertsEmitted
		totalEvents += rep.EventsApplied
		slog.Info("table dump complete",
			"schema", rep.Schema, "table", rep.Table,
			"baseline_rows", rep.BaselineRows,
			"updates_applied", rep.UpdatesApplied,
			"inserts_emitted", rep.InsertsEmitted,
			"deletes_skipped", rep.DeletesSkipped,
			"events_applied", rep.EventsApplied,
			"files", len(rep.Files),
			"duration_ms", rep.Duration.Milliseconds())
	}
	slog.Info("full-table reconstruct complete",
		"tables", len(reports),
		"total_rows", totalRows,
		"total_events_applied", totalEvents,
		"output_dir", recOutputDir,
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// splitAndTrim splits s on sep and strips whitespace from each entry,
// dropping empty results.
func splitAndTrim(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// resolveGapCheck computes the flavor-dependent decisions of runReconstruct's
// baseline↔first-event gap check. effectiveFlavor is what
// reconstruct.GapDetected must be called with; lineageGuard reports that the
// stream_state flavor read came back empty while the baseline carries an LSN
// anchor — the baseline itself proves a PostgreSQL lineage, so PG semantics
// are forced rather than ever falling through to the MySQL lexical compare on
// LSN text (that path being unreachable today only because PG baselines leave
// BinlogFile empty is a convention, not an invariant — this guard makes it
// structural, #593). anchorPresent reports whether the baseline has a
// comparable anchor for its flavor (binlog file vs LSN); eventPosMissing
// whether the first event lacks one (NULL binlog_file for MySQL; a zero
// StartPos for PG — 0 is not a valid WAL position).
func resolveGapCheck(flavor string, bmeta baseline.DumpMetadata, firstFile string, firstStartPos uint64) (effectiveFlavor string, lineageGuard, anchorPresent, eventPosMissing bool) {
	if flavor == "" && bmeta.LSN != 0 {
		flavor = "postgres"
		lineageGuard = true
	}
	anchorPresent = bmeta.BinlogFile != ""
	eventPosMissing = firstFile == ""
	if flavor == "postgres" {
		anchorPresent = bmeta.LSN != 0
		eventPosMissing = firstStartPos == 0
	}
	return flavor, lineageGuard, anchorPresent, eventPosMissing
}
