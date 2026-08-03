package cli

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/status"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show index state: indexed files, partition info, and event counts",
	Long: `Displays the current state of the binlog index in three sections:

  - Indexed Files  : which binlog files have been processed and their status
  - Partitions     : all time-range partitions with estimated row counts
  - Summary        : aggregate file and event counts

The Stream section also reports continuity — the cheap "did I lose any events?"
verdict: "no gaps in the captured range" (a contiguity check, not a liveness
one), or a loud "GAP LOST" when an unfillable gap forced an auto-advance with
permanent loss. Pass --fail-on-gap to exit non-zero on that loss — or when
continuity can't be confirmed (fails closed), or when in-scope statement-format
DML drops were recorded (the same permanent-loss class) — for CI/cron alerting;
by default a gap never changes the exit code.

Partition row counts are estimates read from information_schema (no table scan).

Example:
  bintrail status --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"`,
	RunE: runStatus,
}

var (
	stIndexDSN    string
	stFormat      string
	stBaselineDir string
	stFailOnGap   bool
)

func init() {
	statusCmd.Flags().StringVar(&stIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	statusCmd.Flags().StringVar(&stFormat, "format", "text", "Output format: text or json")
	statusCmd.Flags().StringVar(&stBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots (optional, shows baseline binlog positions)")
	statusCmd.Flags().BoolVar(&stFailOnGap, "fail-on-gap", false, "Exit non-zero if the stream lost data (a binlog gap or statement-format DML drops), or its continuity can't be confirmed (fails closed); for CI/cron alerting. By default a gap never changes the exit code")
	_ = statusCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(statusCmd)
	// Registration on a root command happens via AddReadCommands(root), which
	// each binary's main() calls — see commands.go (#529).
}

func runStatus(cmd *cobra.Command, args []string) error {
	start := time.Now()

	if !cliutil.IsValidOutputFormat(stFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", stFormat)
	}

	cfg, err := mysql.ParseDSN(stIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}

	slog.Debug("connecting to index database", "database", dbName)
	t := time.Now()
	db, err := config.Connect(stIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()
	slog.Debug("connected", "duration_ms", time.Since(t).Milliseconds())

	data, err := status.CollectStatus(cmd.Context(), db, dbName)
	if err != nil {
		return err
	}

	// Discover baseline Parquet files if --baseline-dir is provided.
	if stBaselineDir != "" {
		baselines, bErr := baseline.DiscoverBaselines(stBaselineDir)
		if bErr != nil {
			slog.Warn("could not discover baselines", "dir", stBaselineDir, "error", bErr)
		} else {
			for _, b := range baselines {
				var size int64
				if fi, sErr := os.Stat(b.Path); sErr == nil {
					size = fi.Size()
				} else {
					slog.Warn("could not stat baseline file for size", "path", b.Path, "error", sErr)
				}
				data.Baselines = append(data.Baselines, status.BaselineInfo{
					SnapshotTime: b.SnapshotTime,
					Database:     b.Database,
					Table:        b.Table,
					BinlogFile:   b.BinlogFile,
					BinlogPos:    b.BinlogPos,
					GTIDSet:      b.GTIDSet,
					Path:         b.Path,
					Size:         size,
				})
			}
		}
	}

	slog.Info("status complete", "duration_ms", time.Since(start).Milliseconds())

	if stFormat == "json" {
		if err := data.WriteJSON(os.Stdout); err != nil {
			return err
		}
	} else {
		data.Write(os.Stdout)
	}

	// Opt-in alertable exit: --fail-on-gap turns a non-OK continuity verdict into a
	// non-zero exit for CI/cron — AFTER the report is written so the operator still
	// sees the full status. It FAILS CLOSED: a stamped gap, OR an inability to
	// confirm the gap state (no stream row / a swallowed load error → nil stream,
	// or a legacy index missing the gap columns) all alert — the flag exists to
	// catch trouble, so "couldn't check" must not read as "fine". Off by default,
	// so status keeps exiting 0 as before (break-nothing for existing scripts).
	if stFailOnGap {
		cmd.SilenceUsage = true
		switch {
		case data.StreamErr != nil:
			return fmt.Errorf("stream continuity: could not read stream state (%w); failing closed under --fail-on-gap", data.StreamErr)
		case data.Stream == nil:
			return fmt.Errorf("stream continuity: could not confirm gap state (no stream state loaded); failing closed under --fail-on-gap")
		case !data.Stream.GapColumnsPresent:
			return fmt.Errorf("stream continuity: could not confirm gap state (legacy index missing gap-detection columns; migrate the schema); failing closed under --fail-on-gap")
		case data.Stream.GapLostAt.Valid:
			return fmt.Errorf("stream continuity: events permanently lost (gap detected at %s); index is valid only up to the gap, resume requires re-baseline",
				data.Stream.GapLostAt.Time.Format(status.TSFmt))
		}
		// #999: a non-zero IN-SCOPE statement-format DML count is the same loss
		// class as gap_lost — those changes are permanently absent from the
		// index — so it joins the fail-closed contract. A NULL/empty ledger
		// (legacy index / no skip-aware daemon) deliberately does NOT alert
		// here: unlike the gap columns above, capture_skips post-dates this
		// flag, and failing every pre-#1034 deployment's cron would bury the
		// signal in false alarms (the gap-column unknowns already alerted
		// before capture_skips existed, so their fail-closed stance breaks no
		// one). A ledger that IS present but unreadable gets no such pass —
		// a skip-aware daemon wrote it, it may be hiding a loss count, and
		// "couldn't check" must not read as "fine" (the sibling branches'
		// stance).
		if skips, ok := data.Stream.ParseCaptureSkips(); ok {
			if st := skips[status.CaptureSkipReasonStatementFormatDML]; st.Count > 0 {
				loc := ""
				if st.LastFile != "" || st.LastStatementType != "" {
					file := st.LastFile
					if file == "" {
						file = "?"
					}
					loc = fmt.Sprintf(" (last: %s at %s:%d, connection id %d)", st.LastStatementType, file, st.LastPos, st.LastConnectionID)
				}
				return fmt.Errorf("capture health: %d statement-format DML event(s) permanently uncaptured%s; set binlog_format=ROW server-wide on the source, then acknowledge by clearing stream_state.capture_skips with the daemon stopped (the counter is monotonic); failing closed under --fail-on-gap", st.Count, loc)
			}
		} else if data.Stream.CaptureSkips.Valid && strings.TrimSpace(data.Stream.CaptureSkips.String) != "" {
			return fmt.Errorf("capture health: capture_skips ledger present but unreadable; cannot confirm statement-format DML drops; failing closed under --fail-on-gap")
		}
	}
	return nil
}
