package main

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

var rotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Drop old partitions and add replacement future ones",
	Long: `Manage the time-range partitions on the binlog_events table.

Old partitions are dropped based on the --retain threshold. For every partition
dropped, one new hourly partition is automatically added for the future so that
the total partition count stays constant. Use --add-future N to maintain a
declarative headroom of at least N future hourly partitions beyond the current
hour (top-up only; already-sufficient headroom is left alone). Use --no-replace
to suppress auto-replacement and only top up toward the --add-future target
(useful when storage is limited).

Examples:
  # Drop partitions older than 7 days (auto-adds 168 future hourly partitions)
  bintrail rotate --index-dsn "..." --retain 7d

  # Drop old partitions and maintain at least 3 future partitions of headroom
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
	rotRetry              bool
)

func init() {
	rotateCmd.Flags().StringVar(&rotIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	rotateCmd.Flags().StringVar(&rotRetain, "retain", "", "Drop partitions older than this duration (e.g. 7d, 24h)")
	rotateCmd.Flags().IntVar(&rotAddFuture, "add-future", 0, "Maintain at least N future hourly partitions beyond the current hour (declarative target; top-up only)")
	rotateCmd.Flags().BoolVar(&rotNoReplace, "no-replace", false, "Do not auto-add future partitions to replace dropped ones (only top up toward --add-future)")
	rotateCmd.Flags().StringVar(&rotArchiveDir, "archive-dir", "", "Directory to write Parquet archives before dropping partitions (required with --bintrail-id)")
	rotateCmd.Flags().StringVar(&rotArchiveCompression, "archive-compression", "zstd", "Compression for archive Parquet files (zstd, snappy, gzip, none)")
	rotateCmd.Flags().StringVar(&rotBintrailID, "bintrail-id", "", "Server identity UUID (required when --archive-dir is set); archives are written under bintrail_id=<uuid>/event_date=<date>/")
	rotateCmd.Flags().StringVar(&rotArchiveS3, "archive-s3", "", "S3 destination URL to upload Parquet archives after writing (requires --archive-dir; e.g. s3://my-bucket/archives/)")
	rotateCmd.Flags().StringVar(&rotArchiveS3Region, "archive-s3-region", "", "AWS region for --archive-s3 (default: from AWS_REGION env var or ~/.aws/config)")
	rotateCmd.Flags().BoolVar(&rotDaemon, "daemon", false, "Run continuously, repeating rotation on the --interval schedule until SIGINT/SIGTERM")
	rotateCmd.Flags().StringVar(&rotInterval, "interval", "1h", "How often to run rotation in daemon mode (e.g. 1h, 30m)")
	rotateCmd.Flags().StringVar(&rotFormat, "format", "text", "Output format: text or json")
	rotateCmd.Flags().BoolVar(&rotRetry, "retry", false, "Skip archiving partitions whose Parquet file already exists and S3 uploads that already succeeded")
	_ = rotateCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(rotateCmd)

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
		retainDur, err = cliutil.ParseRetain(rotRetain)
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
		if err := indexer.EnsureSchema(db); err != nil {
			return fmt.Errorf("schema migration: %w", err)
		}
		res, err := rotation.Perform(ctx, db, dbName, rotation.Options{
			RetainDur:          retainDur,
			RetainRaw:          rotRetain,
			AddFuture:          rotAddFuture,
			NoReplace:          rotNoReplace,
			ArchiveDir:         rotArchiveDir,
			ArchiveCompression: rotArchiveCompression,
			BintrailID:         rotBintrailID,
			ArchiveS3:          rotArchiveS3,
			ArchiveS3Region:    rotArchiveS3Region,
			Retry:              rotRetry,
			Format:             rotFormat,
			// ProtectUnarchived stays false: the explicit rotate command drops
			// exactly what the operator asked for (the guard is built-in
			// rotation's, set by up's loop).
		})
		if err != nil {
			return err
		}
		if rotFormat == "json" {
			return cliutil.OutputJSON(struct {
				PartitionsDropped int `json:"partitions_dropped"`
				PartitionsAdded   int `json:"partitions_added"`
			}{PartitionsDropped: res.Dropped, PartitionsAdded: res.Added})
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
