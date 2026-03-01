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
	bslUploadRegion string
)

func init() {
	baselineCmd.Flags().StringVar(&bslInput, "input", "", "mydumper output directory (required)")
	baselineCmd.Flags().StringVar(&bslOutput, "output", "", "Parquet output base directory (required)")
	baselineCmd.Flags().StringVar(&bslTimestamp, "timestamp", "", "Snapshot timestamp override (ISO 8601; default: from mydumper metadata)")
	baselineCmd.Flags().StringVar(&bslTables, "tables", "", "Comma-separated db.table filter (e.g. mydb.orders,mydb.items; default: all)")
	baselineCmd.Flags().StringVar(&bslCompression, "compression", "zstd", "Parquet compression codec: zstd, snappy, gzip, none")
	baselineCmd.Flags().IntVar(&bslRowGroupSize, "row-group-size", 500_000, "Rows per Parquet row group")
	baselineCmd.Flags().StringVar(&bslUpload, "upload", "", "S3 destination URL to upload Parquet files after generation (e.g. s3://my-bucket/baselines/)")
	baselineCmd.Flags().StringVar(&bslUploadRegion, "upload-region", "", "AWS region for --upload (default: from AWS_REGION env var or ~/.aws/config)")
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
		uploaded, err := uploadBaselineToS3(cmd.Context(), bslOutput, bslUpload, bslUploadRegion)
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

// newS3Client creates an S3 client using the default AWS credential chain.
// region is optional — if empty, the SDK resolves it from AWS_REGION env var
// or ~/.aws/config.
func newS3Client(ctx context.Context, region string) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return s3.NewFromConfig(awsCfg), nil
}

// buildS3Key constructs the S3 object key for a file by computing its path
// relative to baseDir and prepending prefix (if non-empty). This is the
// shared key-building logic used by both uploadBaselineToS3 and the rotate
// archive loop.
func buildS3Key(baseDir, filePath, prefix string) (string, error) {
	rel, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		return "", err
	}
	key := filepath.ToSlash(rel)
	if prefix != "" {
		key = strings.TrimSuffix(prefix, "/") + "/" + key
	}
	return key, nil
}

// uploadBaselineToS3 walks outputDir and uploads every file to the S3 URL,
// preserving the relative directory structure under the prefix. region is
// optional — if empty, the AWS SDK resolves it from AWS_REGION env var or
// ~/.aws/config. Returns the number of files uploaded.
func uploadBaselineToS3(ctx context.Context, outputDir, s3URL, region string) (int, error) {
	bucket, prefix, err := parseS3URL(s3URL)
	if err != nil {
		return 0, fmt.Errorf("invalid --upload URL: %w", err)
	}

	client, err := newS3Client(ctx, region)
	if err != nil {
		return 0, err
	}

	var count int
	err = filepath.WalkDir(outputDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		key, err := buildS3Key(outputDir, path, prefix)
		if err != nil {
			return err
		}
		if err := uploadFile(ctx, client, path, bucket, key); err != nil {
			return err
		}
		slog.Debug("uploaded", "file", path, "bucket", bucket, "key", key)
		count++
		return nil
	})
	return count, err
}

// uploadFile opens a single local file and uploads it to S3. It is a separate
// function so that defer f.Close() runs when uploadFile returns — not when the
// WalkDir callback returns — preventing file descriptor accumulation over
// large directory trees.
func uploadFile(ctx context.Context, client *s3.Client, path, bucket, key string) error {
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
