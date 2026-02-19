package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/config"
)

var rotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Drop old partitions and optionally add new future ones",
	Long: `Manage the time-range partitions on the binlog_events table.

Old partitions are dropped based on the --retain threshold. New future partitions
can be added with --add-future so upcoming events land in named partitions rather
than the catch-all p_future partition.

Examples:
  # Drop partitions older than 7 days
  bintrail rotate --index-dsn "..." --retain 7d

  # Drop old partitions and add 3 new future ones
  bintrail rotate --index-dsn "..." --retain 7d --add-future 3

  # Only add new future partitions (no drops)
  bintrail rotate --index-dsn "..." --add-future 7`,
	RunE: runRotate,
}

var (
	rotIndexDSN  string
	rotRetain    string
	rotAddFuture int
)

func init() {
	rotateCmd.Flags().StringVar(&rotIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	rotateCmd.Flags().StringVar(&rotRetain, "retain", "", "Drop partitions older than this duration (e.g. 7d, 24h)")
	rotateCmd.Flags().IntVar(&rotAddFuture, "add-future", 0, "Number of daily partitions to add for future dates")
	_ = rotateCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(rotateCmd)
}

func runRotate(cmd *cobra.Command, args []string) error {
	if rotRetain == "" && rotAddFuture == 0 {
		return fmt.Errorf("at least one of --retain or --add-future is required")
	}

	var retainDur time.Duration
	if rotRetain != "" {
		var err error
		retainDur, err = parseRetain(rotRetain)
		if err != nil {
			return fmt.Errorf("--retain: %w", err)
		}
	}

	cfg, err := mysql.ParseDSN(rotIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}

	db, err := config.Connect(rotIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	ctx := cmd.Context()

	// ── Load current partition list ───────────────────────────────────────────
	partitions, err := listPartitions(ctx, db, dbName)
	if err != nil {
		return fmt.Errorf("failed to list partitions: %w", err)
	}

	// ── Drop old partitions ───────────────────────────────────────────────────
	if rotRetain != "" {
		cutoff := time.Now().UTC().Add(-retainDur).Truncate(24 * time.Hour)
		var toDrop []string
		for _, p := range partitions {
			d, ok := partitionDate(p.Name)
			if !ok {
				continue // skip p_future and any unrecognised names
			}
			if d.Before(cutoff) {
				toDrop = append(toDrop, p.Name)
			}
		}

		if len(toDrop) == 0 {
			fmt.Fprintf(os.Stderr, "no partitions older than %s to drop\n", rotRetain)
		} else {
			if err := dropPartitions(ctx, db, dbName, toDrop); err != nil {
				return fmt.Errorf("failed to drop partitions: %w", err)
			}
			for _, name := range toDrop {
				fmt.Fprintf(os.Stderr, "dropped partition %s\n", name)
			}
			// Refresh list so nextPartitionStart sees current state.
			partitions, err = listPartitions(ctx, db, dbName)
			if err != nil {
				return fmt.Errorf("failed to refresh partition list: %w", err)
			}
		}
	}

	// ── Warn if p_future already holds data ───────────────────────────────────
	hasFutureData, err := partitionHasData(ctx, db, dbName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not check p_future data: %v\n", err)
	} else if hasFutureData {
		fmt.Fprintln(os.Stderr, "warning: p_future partition contains data — events are arriving "+
			"outside all named partition ranges; consider adding more future partitions with --add-future")
	}

	// ── Add new future partitions ─────────────────────────────────────────────
	if rotAddFuture > 0 {
		startDate := nextPartitionStart(partitions)
		if err := addFuturePartitions(ctx, db, dbName, startDate, rotAddFuture); err != nil {
			return fmt.Errorf("failed to add future partitions: %w", err)
		}
		for i := range rotAddFuture {
			fmt.Fprintf(os.Stderr, "added partition %s\n", partitionName(startDate.AddDate(0, 0, i)))
		}
	}

	return nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// parseRetain parses a retain string like "7d" or "24h" into a time.Duration.
// Supported units: 'd' (days), 'h' (hours). The number must be a positive integer.
func parseRetain(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid format %q; expected Nd (days) or Nh (hours), e.g. 7d", s)
	}
	unit := s[len(s)-1]
	numStr := s[:len(s)-1]
	var n int
	if _, err := fmt.Sscanf(numStr, "%d", &n); err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid format %q; expected Nd (days) or Nh (hours), e.g. 7d", s)
	}
	switch unit {
	case 'd':
		return time.Duration(n) * 24 * time.Hour, nil
	case 'h':
		return time.Duration(n) * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid unit %q in %q; use 'd' for days or 'h' for hours", unit, s)
	}
}

// partitionInfo holds metadata for a single table partition.
type partitionInfo struct {
	Name        string
	Description string // LESS THAN value or "MAXVALUE"
	Ordinal     int
}

// listPartitions returns all partitions for binlog_events ordered by ordinal position.
func listPartitions(ctx context.Context, db *sql.DB, dbName string) ([]partitionInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, IFNULL(PARTITION_DESCRIPTION, ''), PARTITION_ORDINAL_POSITION
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'
		ORDER BY PARTITION_ORDINAL_POSITION`,
		dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []partitionInfo
	for rows.Next() {
		var p partitionInfo
		if err := rows.Scan(&p.Name, &p.Description, &p.Ordinal); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}
	return partitions, rows.Err()
}

// partitionDate parses the date from a partition name like "p_20260219".
// Returns the date and true on success; zero and false for p_future or other names.
func partitionDate(name string) (time.Time, bool) {
	if len(name) != 10 || !strings.HasPrefix(name, "p_") {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("p_20060102", name, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// partitionName returns the partition name for a given date ("p_YYYYMMDD").
func partitionName(d time.Time) string {
	return d.UTC().Format("p_20060102")
}

// dropPartitions drops one or more named partitions in a single ALTER TABLE statement.
func dropPartitions(ctx context.Context, db *sql.DB, dbName string, names []string) error {
	q := fmt.Sprintf("ALTER TABLE `%s`.`binlog_events` DROP PARTITION %s",
		dbName, strings.Join(names, ", "))
	_, err := db.ExecContext(ctx, q)
	return err
}

// partitionHasData reports whether the p_future catch-all partition holds any rows.
// Uses SELECT 1 ... LIMIT 1 rather than COUNT(*) for efficiency on large tables.
func partitionHasData(ctx context.Context, db *sql.DB, dbName string) (bool, error) {
	q := fmt.Sprintf("SELECT 1 FROM `%s`.`binlog_events` PARTITION (p_future) LIMIT 1", dbName)
	var dummy int
	err := db.QueryRowContext(ctx, q).Scan(&dummy)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// nextPartitionStart returns the date for the first new partition to add.
// It finds the latest p_YYYYMMDD partition and returns the following day.
// Falls back to today (UTC) if no named daily partitions exist.
func nextPartitionStart(partitions []partitionInfo) time.Time {
	var maxDate time.Time
	for _, p := range partitions {
		d, ok := partitionDate(p.Name)
		if !ok {
			continue
		}
		if d.After(maxDate) {
			maxDate = d
		}
	}
	if maxDate.IsZero() {
		return time.Now().UTC().Truncate(24 * time.Hour)
	}
	return maxDate.AddDate(0, 0, 1)
}

// addFuturePartitions reorganizes p_future to insert n new daily partitions
// beginning at startDate, then appends a new p_future MAXVALUE catch-all.
func addFuturePartitions(ctx context.Context, db *sql.DB, dbName string, startDate time.Time, n int) error {
	parts := make([]string, 0, n+1)
	for i := range n {
		d := startDate.AddDate(0, 0, i)
		nextDay := d.AddDate(0, 0, 1)
		parts = append(parts, fmt.Sprintf(
			"PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))",
			partitionName(d),
			nextDay.UTC().Format("2006-01-02"),
		))
	}
	parts = append(parts, "PARTITION p_future VALUES LESS THAN MAXVALUE")

	q := fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` REORGANIZE PARTITION p_future INTO (\n\t%s\n)",
		dbName,
		strings.Join(parts, ",\n\t"),
	)
	_, err := db.ExecContext(ctx, q)
	return err
}
