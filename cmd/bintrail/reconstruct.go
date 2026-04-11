package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/query"
	"github.com/dbtrail/bintrail/internal/reconstruct"
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

Examples:
  # Current state of a row (baseline + all binlog events up to now)
  bintrail reconstruct --index-dsn "..." --schema mydb --table orders \
    --pk 12345 --pk-columns id --baseline-dir /data/baselines

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
	bindCommandEnv(reconstructCmd)

	rootCmd.AddCommand(reconstructCmd)
}

func runReconstruct(cmd *cobra.Command, args []string) error {
	start := time.Now()

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
	baselinePath, snapshotTime, err := reconstruct.FindBaseline(cmd.Context(), baselineSrc, recSchema, recTable, at)
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
	// Fix for #209: single-row reconstruct previously called engine.Fetch
	// directly, which silently missed events that had been rotated out of
	// MySQL and archived to Parquet. Delegating to query.FetchMerged routes
	// through the same planner + merge pipeline as `bintrail recover`.
	db, err := config.Connect(recIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index database: %w", err)
	}
	defer db.Close()

	engine := query.New(db)

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
	events, _, err := query.FetchMerged(cmd.Context(), db, engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         dbName,
		NoArchive:      recNoArchive,
		AllowGaps:      recAllowGaps,
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err != nil {
		return fmt.Errorf("fetch binlog events: %w", err)
	}
	slog.Debug("fetched binlog events", "count", len(events))

	// Warn if there is a gap between the baseline binlog position and the
	// first indexed event — events in that gap are missing from the reconstruction.
	if bmeta.BinlogFile == "" && len(events) > 0 {
		slog.Info("gap detection skipped — baseline lacks binlog position metadata; consider re-running 'bintrail baseline' to embed position data")
	}
	if bmeta.BinlogFile != "" && len(events) > 0 {
		first := events[0]
		gap := first.BinlogFile > bmeta.BinlogFile ||
			(first.BinlogFile == bmeta.BinlogFile && first.StartPos > uint64(bmeta.BinlogPos))
		if gap {
			slog.Warn("gap between baseline and first indexed event — reconstruction may be incomplete",
				"baseline_file", bmeta.BinlogFile,
				"baseline_pos", bmeta.BinlogPos,
				"baseline_gtid", bmeta.GTIDSet,
				"first_event_file", first.BinlogFile,
				"first_event_pos", first.StartPos)
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
		entries := reconstruct.BuildHistory(baselineRow, snapshotTime, events, at)
		switch format {
		case "json":
			return reconstruct.WriteHistoryJSON(entries, w)
		case "csv":
			return reconstruct.WriteHistoryCSV(entries, w)
		default:
			return reconstruct.WriteHistoryTable(entries, w)
		}
	}
	state := reconstruct.ApplyAt(baselineRow, events, at)
	switch format {
	case "json":
		return reconstruct.WriteStateJSON(state, w)
	case "csv":
		return reconstruct.WriteStateCSV(state, w)
	default:
		return reconstruct.WriteStateTable(state, w)
	}
}
