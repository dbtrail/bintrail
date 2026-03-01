package main

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	bslInput        string
	bslOutput       string
	bslTimestamp    string
	bslTables       string
	bslCompression  string
	bslRowGroupSize int
	bslUpload       string
)

func init() {
	baselineCmd.Flags().StringVar(&bslInput, "input", "", "mydumper output directory (required)")
	baselineCmd.Flags().StringVar(&bslOutput, "output", "", "Parquet output base directory (required)")
	baselineCmd.Flags().StringVar(&bslTimestamp, "timestamp", "", "Snapshot timestamp override (ISO 8601; default: from mydumper metadata)")
	baselineCmd.Flags().StringVar(&bslTables, "tables", "", "Comma-separated db.table filter (e.g. mydb.orders,mydb.items; default: all)")
	baselineCmd.Flags().StringVar(&bslCompression, "compression", "zstd", "Parquet compression codec: zstd, snappy, gzip, none")
	baselineCmd.Flags().IntVar(&bslRowGroupSize, "row-group-size", 500_000, "Rows per Parquet row group")
	baselineCmd.Flags().StringVar(&bslUpload, "upload", "", "S3 destination URL to upload Parquet files after generation (e.g. s3://my-bucket/baselines/)")
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

	cfg := baseline.Config{
		InputDir:     bslInput,
		OutputDir:    bslOutput,
		Timestamp:    ts,
		Tables:       parseTableFilter(bslTables),
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

	if bslUpload != "" {
		uploaded, err := uploadBaselineToS3(cmd.Context(), bslOutput, bslUpload)
		if err != nil {
			return fmt.Errorf("S3 upload: %w", err)
		}
		fmt.Printf("  uploaded  : %d files → %s\n", uploaded, bslUpload)
		slog.Info("baseline S3 upload complete", "files", uploaded, "destination", bslUpload)
	}

	return nil
}

// parseS3URL parses an S3 URL of the form s3://bucket or s3://bucket/prefix
// and returns the bucket name and prefix (without leading slash).
func parseS3URL(u string) (bucket, prefix string, err error) {
	if !strings.HasPrefix(u, "s3://") {
		return "", "", fmt.Errorf("must start with s3://, got %q", u)
	}
	rest := strings.TrimPrefix(u, "s3://")
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("bucket name is empty in %q", u)
	}
	return bucket, prefix, nil
}

// uploadBaselineToS3 walks outputDir and uploads every file to the S3 URL,
// preserving the relative directory structure under the prefix. Returns the
// number of files uploaded. Uses the standard AWS credential chain.
func uploadBaselineToS3(ctx context.Context, outputDir, s3URL string) (int, error) {
	bucket, prefix, err := parseS3URL(s3URL)
	if err != nil {
		return 0, fmt.Errorf("invalid --upload URL: %w", err)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return 0, fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg)

	var count int
	err = filepath.WalkDir(outputDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		rel, err := filepath.Rel(outputDir, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if prefix != "" {
			key = strings.TrimSuffix(prefix, "/") + "/" + key
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()

		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   f,
		}); err != nil {
			return fmt.Errorf("upload %s → s3://%s/%s: %w", path, bucket, key, err)
		}
		slog.Debug("uploaded", "file", path, "bucket", bucket, "key", key)
		count++
		return nil
	})
	return count, err
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
