package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/config"
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

var stIndexDSN string

func init() {
	statusCmd.Flags().StringVar(&stIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	_ = statusCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(statusCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
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

	files, err := loadIndexState(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to load index state: %w", err)
	}

	partStats, err := loadPartitionStats(ctx, db, dbName)
	if err != nil {
		return fmt.Errorf("failed to load partition info: %w", err)
	}

	writeStatus(os.Stdout, files, partStats)
	return nil
}

// ─── Data types ───────────────────────────────────────────────────────────────

type indexStateRow struct {
	BinlogFile    string
	Status        string
	EventsIndexed int64
	FileSize      int64
	LastPosition  int64
	StartedAt     time.Time
	CompletedAt   sql.NullTime
	ErrorMessage  sql.NullString
}

type partitionStat struct {
	Name        string
	Description string // LESS THAN value (integer UNIX ts string) or "MAXVALUE"
	TableRows   int64  // estimate from information_schema
	Ordinal     int
}

// ─── Queries ──────────────────────────────────────────────────────────────────

func loadIndexState(ctx context.Context, db *sql.DB) ([]indexStateRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT binlog_file, status, events_indexed, file_size, last_position,
		       started_at, completed_at, error_message
		FROM index_state
		ORDER BY started_at, binlog_file`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []indexStateRow
	for rows.Next() {
		var r indexStateRow
		if err := rows.Scan(
			&r.BinlogFile, &r.Status, &r.EventsIndexed, &r.FileSize, &r.LastPosition,
			&r.StartedAt, &r.CompletedAt, &r.ErrorMessage,
		); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

func loadPartitionStats(ctx context.Context, db *sql.DB, dbName string) ([]partitionStat, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, IFNULL(PARTITION_DESCRIPTION, ''),
		       PARTITION_ORDINAL_POSITION, COALESCE(TABLE_ROWS, 0)
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'
		ORDER BY PARTITION_ORDINAL_POSITION`,
		dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []partitionStat
	for rows.Next() {
		var p partitionStat
		if err := rows.Scan(&p.Name, &p.Description, &p.Ordinal, &p.TableRows); err != nil {
			return nil, err
		}
		stats = append(stats, p)
	}
	return stats, rows.Err()
}

// ─── Display ─────────────────────────────────────────────────────────────────

const statusTSFmt = "2006-01-02 15:04:05"

func writeStatus(w io.Writer, files []indexStateRow, parts []partitionStat) {
	// ── Section 1: Indexed Files ──────────────────────────────────────────────
	fmt.Fprintln(w, "=== Indexed Files ===")
	if len(files) == 0 {
		fmt.Fprintln(w, "  (no files indexed yet)")
	} else {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "FILE\tSTATUS\tEVENTS\tSTARTED_AT\tCOMPLETED_AT\tERROR")
		fmt.Fprintln(tw, "────\t──────\t──────\t──────────\t────────────\t─────")
		for _, f := range files {
			completedAt := "-"
			if f.CompletedAt.Valid {
				completedAt = f.CompletedAt.Time.Format(statusTSFmt)
			}
			errMsg := "-"
			if f.ErrorMessage.Valid && f.ErrorMessage.String != "" {
				errMsg = truncate(f.ErrorMessage.String, 60)
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n",
				f.BinlogFile, f.Status, f.EventsIndexed,
				f.StartedAt.Format(statusTSFmt),
				completedAt, errMsg,
			)
		}
		tw.Flush()
	}

	// ── Section 2: Partitions ─────────────────────────────────────────────────
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Partitions ===")
	if len(parts) == 0 {
		fmt.Fprintln(w, "  (no partitions found — run 'bintrail init' first)")
	} else {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "PARTITION\tLESS_THAN\tROWS (est.)")
		fmt.Fprintln(tw, "─────────\t─────────\t───────────")
		var totalRows int64
		for _, p := range parts {
			fmt.Fprintf(tw, "%s\t%s\t%d\n", p.Name, descriptionToHuman(p.Description), p.TableRows)
			totalRows += p.TableRows
		}
		tw.Flush()
		fmt.Fprintf(w, "Total events (est.): %d\n", totalRows)
	}

	// ── Section 3: Summary ────────────────────────────────────────────────────
	if len(files) > 0 {
		counts := map[string]int{}
		var totalEvents int64
		for _, f := range files {
			counts[f.Status]++
			totalEvents += f.EventsIndexed
		}
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== Summary ===")
		fmt.Fprintf(w, "Files:  %d completed, %d in_progress, %d failed\n",
			counts["completed"], counts["in_progress"], counts["failed"])
		fmt.Fprintf(w, "Events: %d indexed\n", totalEvents)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// descriptionToHuman converts a PARTITION_DESCRIPTION value to a readable string.
// RANGE partitions using UNIX_TIMESTAMP() store the evaluated integer; MAXVALUE is literal.
func descriptionToHuman(desc string) string {
	if desc == "" || strings.EqualFold(desc, "MAXVALUE") {
		return "MAXVALUE"
	}
	ts, err := strconv.ParseInt(desc, 10, 64)
	if err != nil {
		return desc // not an integer — return raw value
	}
	return time.Unix(ts, 0).UTC().Format(statusTSFmt + " UTC")
}

// truncate shortens s to at most n bytes, appending "…" if truncated.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
