package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/archive"
	"github.com/bintrail/bintrail/internal/config"
)

var rotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Drop old partitions and add replacement future ones",
	Long: `Manage the time-range partitions on the binlog_events table.

Old partitions are dropped based on the --retain threshold. For every partition
dropped, one new hourly partition is automatically added for the future so that
the total partition count stays constant. Use --add-future to add extra partitions
on top of the automatic replacements.

Examples:
  # Drop partitions older than 7 days (auto-adds 168 future hourly partitions)
  bintrail rotate --index-dsn "..." --retain 7d

  # Drop old partitions and add 3 extra future ones beyond the replacements
  bintrail rotate --index-dsn "..." --retain 7d --add-future 3

  # Only add new future partitions (no drops)
  bintrail rotate --index-dsn "..." --add-future 7`,
	RunE: runRotate,
}

var (
	rotIndexDSN           string
	rotRetain             string
	rotAddFuture          int
	rotArchiveDir         string
	rotArchiveCompression string
)

func init() {
	rotateCmd.Flags().StringVar(&rotIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	rotateCmd.Flags().StringVar(&rotRetain, "retain", "", "Drop partitions older than this duration (e.g. 7d, 24h)")
	rotateCmd.Flags().IntVar(&rotAddFuture, "add-future", 0, "Extra hourly partitions to add beyond auto-replacements (0 = only add replacements for dropped partitions)")
	rotateCmd.Flags().StringVar(&rotArchiveDir, "archive-dir", "", "Directory to write Parquet archives before dropping partitions (empty = no archiving)")
	rotateCmd.Flags().StringVar(&rotArchiveCompression, "archive-compression", "zstd", "Compression for archive Parquet files (zstd, snappy, gzip, none)")
	_ = rotateCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(rotateCmd)
}

func runRotate(cmd *cobra.Command, args []string) error {
	start := time.Now()

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
	var droppedCount int
	if rotRetain != "" {
		cutoff := time.Now().UTC().Add(-retainDur).Truncate(time.Hour)
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
			fmt.Fprintf(os.Stdout, "no partitions older than %s to drop\n", rotRetain)
		} else {
			// Archive partitions to Parquet before dropping, if requested.
			if rotArchiveDir != "" {
				for _, name := range toDrop {
					outPath := filepath.Join(rotArchiveDir, name+".parquet")
					n, err := archive.ArchivePartition(ctx, db, dbName, name, outPath, rotArchiveCompression)
					if err != nil {
						return fmt.Errorf("archive partition %s: %w", name, err)
					}
					fmt.Fprintf(os.Stdout, "archived partition %s (%d rows) → %s\n", name, n, outPath)
				}
			}

			if err := dropPartitions(ctx, db, dbName, toDrop); err != nil {
				return fmt.Errorf("failed to drop partitions: %w", err)
			}
			for _, name := range toDrop {
				fmt.Fprintf(os.Stdout, "dropped partition %s\n", name)
			}
			droppedCount = len(toDrop)
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
		slog.Warn("could not check p_future data", "error", err)
	} else if hasFutureData {
		slog.Warn("p_future partition contains data — events are arriving outside all named partition ranges; consider adding more future partitions with --add-future")
	}

	// ── Add new future partitions ─────────────────────────────────────────────
	// Auto-replace every dropped partition with a new future hourly partition,
	// plus any extras requested via --add-future.
	toAdd := droppedCount + rotAddFuture
	if toAdd > 0 {
		startDate := nextPartitionStart(partitions)
		if err := addFuturePartitions(ctx, db, dbName, startDate, toAdd); err != nil {
			return fmt.Errorf("failed to add future partitions: %w", err)
		}
		for i := range toAdd {
			fmt.Fprintf(os.Stdout, "added partition %s\n", partitionName(startDate.Add(time.Duration(i)*time.Hour)))
		}
	}

	slog.Info("rotation complete",
		"partitions_dropped", droppedCount,
		"partitions_added", toAdd,
		"duration_ms", time.Since(start).Milliseconds())

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

// partitionDate parses the hour from a partition name like "p_2026021914".
// Returns the time and true on success; zero and false for p_future or other names.
func partitionDate(name string) (time.Time, bool) {
	if len(name) != 12 || !strings.HasPrefix(name, "p_") {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("p_2006010215", name, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// partitionName returns the partition name for a given hour ("p_YYYYMMDDHH").
func partitionName(d time.Time) string {
	return d.UTC().Format("p_2006010215")
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

// nextPartitionStart returns the hour for the first new partition to add.
// It finds the latest p_YYYYMMDDHH partition and returns the following hour.
// Falls back to the current hour (UTC) if no named hourly partitions exist.
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
		return time.Now().UTC().Truncate(time.Hour)
	}
	return maxDate.Add(time.Hour)
}

// addFuturePartitions reorganizes p_future to insert n new hourly partitions
// beginning at startDate, then appends a new p_future MAXVALUE catch-all.
func addFuturePartitions(ctx context.Context, db *sql.DB, dbName string, startDate time.Time, n int) error {
	parts := make([]string, 0, n+1)
	for i := range n {
		d := startDate.Add(time.Duration(i) * time.Hour)
		nextHour := d.Add(time.Hour)
		parts = append(parts, fmt.Sprintf(
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s'))",
			partitionName(d),
			nextHour.UTC().Format("2006-01-02 15:04:05"),
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
