package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/archive"
	"github.com/bintrail/bintrail/internal/baseline"
	"github.com/bintrail/bintrail/internal/cliutil"
	"github.com/bintrail/bintrail/internal/config"
)

var rotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Drop old partitions and add replacement future ones",
	Long: `Manage the time-range partitions on the binlog_events table.

Old partitions are dropped based on the --retain threshold. For every partition
dropped, one new hourly partition is automatically added for the future so that
the total partition count stays constant. Use --add-future to add extra partitions
on top of the automatic replacements. Use --no-replace to suppress auto-replacement
and only add the explicit --add-future count (useful when storage is limited).

Examples:
  # Drop partitions older than 7 days (auto-adds 168 future hourly partitions)
  bintrail rotate --index-dsn "..." --retain 7d

  # Drop old partitions and add 3 extra future ones beyond the replacements
  bintrail rotate --index-dsn "..." --retain 7d --add-future 3

  # Only add new future partitions (no drops)
  bintrail rotate --index-dsn "..." --add-future 7

  # Drop without auto-replacing (pure drop, storage-conscious)
  bintrail rotate --index-dsn "..." --retain 7d --no-replace

  # Run as a daemon, rotating every hour
  bintrail rotate --index-dsn "..." --retain 7d --daemon

  # Run as a daemon with a custom interval
  bintrail rotate --index-dsn "..." --retain 7d --daemon --interval 6h`,
	RunE: runRotate,
}

var (
	rotIndexDSN           string
	rotRetain             string
	rotAddFuture          int
	rotNoReplace          bool
	rotArchiveDir         string
	rotArchiveCompression string
	rotBintrailID         string
	rotArchiveS3          string
	rotArchiveS3Region    string
	rotDaemon             bool
	rotInterval           string
	rotFormat             string
)

func init() {
	rotateCmd.Flags().StringVar(&rotIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	rotateCmd.Flags().StringVar(&rotRetain, "retain", "", "Drop partitions older than this duration (e.g. 7d, 24h)")
	rotateCmd.Flags().IntVar(&rotAddFuture, "add-future", 0, "Extra hourly partitions to add beyond auto-replacements (0 = only add replacements for dropped partitions)")
	rotateCmd.Flags().BoolVar(&rotNoReplace, "no-replace", false, "Do not auto-add future partitions to replace dropped ones")
	rotateCmd.Flags().StringVar(&rotArchiveDir, "archive-dir", "", "Directory to write Parquet archives before dropping partitions (required with --bintrail-id)")
	rotateCmd.Flags().StringVar(&rotArchiveCompression, "archive-compression", "zstd", "Compression for archive Parquet files (zstd, snappy, gzip, none)")
	rotateCmd.Flags().StringVar(&rotBintrailID, "bintrail-id", "", "Server identity UUID (required when --archive-dir is set); archives are written under bintrail_id=<uuid>/event_date=<date>/")
	rotateCmd.Flags().StringVar(&rotArchiveS3, "archive-s3", "", "S3 destination URL to upload Parquet archives after writing (requires --archive-dir; e.g. s3://my-bucket/archives/)")
	rotateCmd.Flags().StringVar(&rotArchiveS3Region, "archive-s3-region", "", "AWS region for --archive-s3 (default: from AWS_REGION env var or ~/.aws/config)")
	rotateCmd.Flags().BoolVar(&rotDaemon, "daemon", false, "Run continuously, repeating rotation on the --interval schedule until SIGINT/SIGTERM")
	rotateCmd.Flags().StringVar(&rotInterval, "interval", "1h", "How often to run rotation in daemon mode (e.g. 1h, 30m)")
	rotateCmd.Flags().StringVar(&rotFormat, "format", "text", "Output format: text or json")
	_ = rotateCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(rotateCmd)
}

func runRotate(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(rotFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", rotFormat)
	}
	if rotRetain == "" && rotAddFuture == 0 {
		return fmt.Errorf("at least one of --retain or --add-future is required")
	}
	if rotArchiveDir != "" && rotBintrailID == "" {
		return fmt.Errorf("--bintrail-id is required when --archive-dir is set")
	}
	if rotArchiveS3 != "" && rotArchiveDir == "" {
		return fmt.Errorf("--archive-s3 requires --archive-dir")
	}
	if rotArchiveDir != "" {
		if err := baseline.ValidateCodec(rotArchiveCompression); err != nil {
			return fmt.Errorf("--archive-compression: %w", err)
		}
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

	if rotDaemon {
		if _, err := time.ParseDuration(rotInterval); err != nil {
			return fmt.Errorf("--interval: %w", err)
		}
	}

	doRotation := func(ctx context.Context) error {
		db, err := config.Connect(rotIndexDSN)
		if err != nil {
			return fmt.Errorf("failed to connect to index database: %w", err)
		}
		defer db.Close()
		dropped, added, err := performRotation(ctx, db, dbName, retainDur)
		if err != nil {
			return err
		}
		if rotFormat == "json" {
			return outputJSON(struct {
				PartitionsDropped int `json:"partitions_dropped"`
				PartitionsAdded   int `json:"partitions_added"`
			}{PartitionsDropped: dropped, PartitionsAdded: added})
		}
		return nil
	}

	if !rotDaemon {
		return doRotation(cmd.Context())
	}

	interval, _ := time.ParseDuration(rotInterval) // already validated above
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("rotate daemon started", "interval", interval)
	if err := doRotation(ctx); err != nil && ctx.Err() == nil {
		slog.Error("rotation failed", "error", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("rotate daemon stopping")
			return nil
		case <-ticker.C:
			// Suppress JSON output in daemon mode — only the initial rotation outputs JSON.
			savedFmt := rotFormat
			rotFormat = "text"
			func() {
				defer func() { rotFormat = savedFmt }()
				if err := doRotation(ctx); err != nil && ctx.Err() == nil {
					slog.Error("rotation failed", "error", err)
				}
			}()
		}
	}
}

// performRotation executes one full rotation cycle against an open DB connection.
// It uses the package-level flag vars (rotRetain, rotAddFuture, rotNoReplace, etc.)
// so that daemon and one-shot modes share identical rotation logic.
// Returns (partitions_dropped, partitions_added, error).
func performRotation(ctx context.Context, db *sql.DB, dbName string, retainDur time.Duration) (int, int, error) {
	start := time.Now()

	// ── Load current partition list ─────────────────────────────────────────────
	partitions, err := listPartitions(ctx, db, dbName)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list partitions: %w", err)
	}

	// ── Drop old partitions ───────────────────────────────────────────────────
	var droppedCount int
	if retainDur > 0 {
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
			if rotFormat != "json" {
				fmt.Fprintf(os.Stdout, "no partitions older than %s to drop\n", rotRetain)
			}
		} else {
			// Archive partitions to Parquet before dropping, if requested.
			if rotArchiveDir != "" {
				// Set up S3 client once for all uploads (nil when --archive-s3 is not set).
				var s3Client *s3.Client
				var s3Bucket, s3Prefix string
				if rotArchiveS3 != "" {
					s3Bucket, s3Prefix, err = parseS3URL(rotArchiveS3)
					if err != nil {
						return 0, 0, fmt.Errorf("invalid --archive-s3: %w", err)
					}
					s3Client, err = newS3Client(ctx, rotArchiveS3Region)
					if err != nil {
						return 0, 0, fmt.Errorf("init S3 client: %w", err)
					}
				}

				for _, name := range toDrop {
					outPath, err := hiveArchivePath(rotArchiveDir, rotBintrailID, name)
					if err != nil {
						return 0, 0, fmt.Errorf("build archive path for %s: %w", name, err)
					}
					if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
						return 0, 0, fmt.Errorf("create archive directory for %s: %w", name, err)
					}
					n, err := archive.ArchivePartition(ctx, db, dbName, name, outPath, rotArchiveCompression)
					if err != nil {
						return 0, 0, fmt.Errorf("archive partition %s: %w", name, err)
					}
					if rotFormat != "json" {
						fmt.Fprintf(os.Stdout, "archived partition %s (%d rows) \u2192 %s\n", name, n, outPath)
					}

					if s3Client != nil {
						key, err := buildS3Key(rotArchiveDir, outPath, s3Prefix)
						if err != nil {
							return 0, 0, fmt.Errorf("build S3 key for %s: %w", name, err)
						}
						if err := uploadFile(ctx, s3Client, outPath, s3Bucket, key); err != nil {
							return 0, 0, fmt.Errorf("upload %s to S3: %w", name, err)
						}
						slog.Debug("uploaded archive to S3", "partition", name, "bucket", s3Bucket, "key", key)
					}
				}
			}

			if err := dropPartitions(ctx, db, dbName, toDrop); err != nil {
				return 0, 0, fmt.Errorf("failed to drop partitions: %w", err)
			}
			for _, name := range toDrop {
				if rotFormat != "json" {
					fmt.Fprintf(os.Stdout, "dropped partition %s\n", name)
				}
			}
			droppedCount = len(toDrop)
			// Refresh list so nextPartitionStart sees current state.
			partitions, err = listPartitions(ctx, db, dbName)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to refresh partition list: %w", err)
			}
		}
	}

	// ── Warn if p_future already holds data ───────────────────────────────────
	hasFutureData, err := partitionHasData(ctx, db, dbName)
	if err != nil {
		slog.Warn("could not check p_future data", "error", err)
	} else if hasFutureData {
		slog.Warn("p_future partition contains data \u2014 events are arriving outside all named partition ranges; consider adding more future partitions with --add-future")
	}

	// ── Add new future partitions ─────────────────────────────────────────────
	// Auto-replace every dropped partition with a new future hourly partition,
	// plus any extras requested via --add-future.
	// --no-replace suppresses the auto-replacement; only --add-future count is used.
	toAdd := rotAddFuture
	if !rotNoReplace {
		toAdd += droppedCount
	}
	if toAdd > 0 {
		startDate := nextPartitionStart(partitions)
		if err := addFuturePartitions(ctx, db, dbName, startDate, toAdd); err != nil {
			return 0, 0, fmt.Errorf("failed to add future partitions: %w", err)
		}
		for i := range toAdd {
			if rotFormat != "json" {
				fmt.Fprintf(os.Stdout, "added partition %s\n", partitionName(startDate.Add(time.Duration(i)*time.Hour)))
			}
		}
	}

	slog.Info("rotation complete",
		"partitions_dropped", droppedCount,
		"partitions_added", toAdd,
		"duration_ms", time.Since(start).Milliseconds())

	return droppedCount, toAdd, nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// hiveArchivePath returns the Hive-partitioned path for a binlog_events partition
// archive. The layout is:
//
//	<archiveDir>/bintrail_id=<uuid>/event_date=<YYYY-MM-DD>/events_<HH>.parquet
//
// Each hourly partition maps to exactly one file. The hour suffix disambiguates
// multiple files under the same event_date= directory.
func hiveArchivePath(archiveDir, bintrailID, partitionName string) (string, error) {
	d, ok := partitionDate(partitionName)
	if !ok {
		return "", fmt.Errorf("cannot parse partition date from %q", partitionName)
	}
	return filepath.Join(
		archiveDir,
		"bintrail_id="+bintrailID,
		"event_date="+d.UTC().Format("2006-01-02"),
		fmt.Sprintf("events_%02d.parquet", d.UTC().Hour()),
	), nil
}

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
