package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/baseline"
)

var baselineCmd = &cobra.Command{
	Use:   "baseline",
	Short: "Convert mydumper output to Parquet baseline snapshots",
	Long: `Reads mydumper per-table dump files (both SQL INSERT and TSV formats) and
converts them into Parquet files, one per table. The output preserves full
column typing and is suitable for audit reconstruction when combined with
binlog change events indexed by 'bintrail index'.

No database connection is required — this command operates purely on files.

Output structure:
  <output>/<timestamp>/<database>/<table>.parquet`,
	RunE: runBaseline,
}

var (
	bslInput       string
	bslOutput      string
	bslTimestamp   string
	bslTables      string
	bslCompression string
	bslRowGroupSize int
)

func init() {
	baselineCmd.Flags().StringVar(&bslInput, "input", "", "mydumper output directory (required)")
	baselineCmd.Flags().StringVar(&bslOutput, "output", "", "Parquet output base directory (required)")
	baselineCmd.Flags().StringVar(&bslTimestamp, "timestamp", "", "Snapshot timestamp override (ISO 8601; default: from mydumper metadata)")
	baselineCmd.Flags().StringVar(&bslTables, "tables", "", "Comma-separated db.table filter (e.g. mydb.orders,mydb.items; default: all)")
	baselineCmd.Flags().StringVar(&bslCompression, "compression", "zstd", "Parquet compression codec: zstd, snappy, none")
	baselineCmd.Flags().IntVar(&bslRowGroupSize, "row-group-size", 500_000, "Rows per Parquet row group")
	_ = baselineCmd.MarkFlagRequired("input")
	_ = baselineCmd.MarkFlagRequired("output")

	rootCmd.AddCommand(baselineCmd)
}

func runBaseline(cmd *cobra.Command, args []string) error {
	var ts time.Time
	if bslTimestamp != "" {
		var err error
		ts, err = time.Parse(time.RFC3339, bslTimestamp)
		if err != nil {
			// Try without timezone suffix
			ts, err = time.ParseInLocation("2006-01-02T15:04:05", bslTimestamp, time.UTC)
			if err != nil {
				ts, err = time.ParseInLocation("2006-01-02 15:04:05", bslTimestamp, time.UTC)
				if err != nil {
					return fmt.Errorf("--timestamp %q: expected ISO 8601 format (e.g. 2025-02-28T00:00:00Z)", bslTimestamp)
				}
			}
		}
	}

	var tables []string
	for part := range strings.SplitSeq(bslTables, ",") {
		if t := strings.TrimSpace(part); t != "" {
			tables = append(tables, t)
		}
	}

	cfg := baseline.Config{
		InputDir:     bslInput,
		OutputDir:    bslOutput,
		Timestamp:    ts,
		Tables:       tables,
		Compression:  bslCompression,
		RowGroupSize: bslRowGroupSize,
	}

	stats, err := baseline.Run(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	fmt.Printf("Baseline complete.\n")
	fmt.Printf("  tables    : %d\n", stats.TablesProcessed)
	fmt.Printf("  rows      : %d\n", stats.RowsWritten)
	fmt.Printf("  files     : %d\n", stats.FilesWritten)
	slog.Info("baseline complete",
		"tables", stats.TablesProcessed,
		"rows_written", stats.RowsWritten,
		"files_written", stats.FilesWritten)
	return nil
}

// parseTableFilter splits a comma-separated "db.table" list.
func parseTableFilter(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for part := range strings.SplitSeq(s, ",") {
		if t := strings.TrimSpace(part); t != "" {
			result = append(result, t)
		}
	}
	return result
}
