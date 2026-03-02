package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/cliutil"
	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/status"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show index state: indexed files, partition info, and event counts",
	Long: `Displays the current state of the binlog index in three sections:

  - Indexed Files  : which binlog files have been processed and their status
  - Partitions     : all time-range partitions with estimated row counts
  - Summary        : aggregate file and event counts

Partition row counts are estimates read from information_schema (no table scan).

Example:
  bintrail status --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"`,
	RunE: runStatus,
}

var (
	stIndexDSN string
	stFormat   string
)

func init() {
	statusCmd.Flags().StringVar(&stIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	statusCmd.Flags().StringVar(&stFormat, "format", "text", "Output format: text or json")
	_ = statusCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(statusCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
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

	db, err := config.Connect(stIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	ctx := cmd.Context()

	files, err := status.LoadIndexState(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to load index state: %w", err)
	}

	partStats, err := status.LoadPartitionStats(ctx, db, dbName)
	if err != nil {
		return fmt.Errorf("failed to load partition info: %w", err)
	}

	// Best-effort: archive_state may not exist in older index databases.
	archives, err := status.LoadArchiveStats(ctx, db)
	if err != nil {
		slog.Warn("could not load archive stats", "error", err)
		archives = nil
	}

	// Best-effort: schema_changes may not exist in older index databases.
	coverage, err := status.LoadCoverage(ctx, db)
	if err != nil {
		slog.Warn("could not load coverage info", "error", err)
		coverage = nil
	}

	if stFormat == "json" {
		return status.WriteStatusJSON(os.Stdout, files, partStats, archives, coverage)
	}
	status.WriteStatus(os.Stdout, files, partStats, archives, coverage)
	return nil
}
