package main

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/storage"
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
	bslFormat       string
	bslRetry        bool
	bslEncrypt      bool
	bslEncryptKey   string
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
	baselineCmd.Flags().StringVar(&bslFormat, "format", "text", "Output format: text or json")
	baselineCmd.Flags().BoolVar(&bslRetry, "retry", false, "Skip tables whose output Parquet file already exists and S3 objects that were already uploaded")
	baselineCmd.Flags().BoolVar(&bslEncrypt, "encrypt", false, "Decrypt encrypted dump files before processing (requires openssl on $PATH)")
	baselineCmd.Flags().StringVar(&bslEncryptKey, "encrypt-key", "", "Path to encryption key file (default: ~/.config/bintrail/dump.key)")
	_ = baselineCmd.MarkFlagRequired("input")
	_ = baselineCmd.MarkFlagRequired("output")
	bindCommandEnv(baselineCmd)

	rootCmd.AddCommand(baselineCmd)
}

func runBaseline(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(bslFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", bslFormat)
	}
	if err := baseline.ValidateCodec(bslCompression); err != nil {
		return fmt.Errorf("--compression: %w", err)
	}

	// Decrypt encrypted dump files if --encrypt is set.
	if bslEncrypt {
		keyPath, err := resolveEncryptKey(bslEncryptKey)
		if err != nil {
			return err
		}
		cleanup, err := decryptDumpFiles(bslInput, keyPath)
		if err != nil {
			return fmt.Errorf("decrypt dump files: %w", err)
		}
		defer cleanup()
	}

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
		Retry:        bslRetry,
	}

	stats, err := baseline.Run(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	slog.Info("baseline complete",
		"tables", stats.TablesProcessed,
		"rows_written", stats.RowsWritten,
		"files_written", stats.FilesWritten)

	var uploaded int
	if bslUpload != "" {
		var err error
		uploaded, err = uploadBaselineToS3(cmd.Context(), bslOutput, bslUpload, bslUploadRegion, bslRetry)
		if err != nil {
			return fmt.Errorf("S3 upload: %w", err)
		}
		if bslFormat != "json" {
			fmt.Printf("  uploaded  : %d files → %s\n", uploaded, bslUpload)
		}
		slog.Info("baseline S3 upload complete", "files", uploaded, "destination", bslUpload)
	}

	if bslFormat == "json" {
		result := struct {
			Tables       int    `json:"tables"`
			RowsWritten  int64  `json:"rows_written"`
			FilesWritten int    `json:"files_written"`
			Uploaded     int    `json:"uploaded,omitempty"`
			UploadDest   string `json:"upload_destination,omitempty"`
		}{
			Tables:       stats.TablesProcessed,
			RowsWritten:  stats.RowsWritten,
			FilesWritten: stats.FilesWritten,
		}
		if bslUpload != "" {
			result.Uploaded = uploaded
			result.UploadDest = bslUpload
		}
		return cliutil.OutputJSON(result)
	}

	fmt.Printf("Baseline complete.\n")
	fmt.Printf("  tables    : %d\n", stats.TablesProcessed)
	fmt.Printf("  rows      : %d\n", stats.RowsWritten)
	fmt.Printf("  files     : %d\n", stats.FilesWritten)
	return nil
}

// uploadBaselineToS3 walks outputDir and uploads every file to the S3 URL,
// preserving the relative directory structure under the prefix. region is
// optional — if empty, the AWS SDK resolves it from AWS_REGION env var or
// ~/.aws/config. When retry is true, files that already exist in S3 are
// skipped (checked via HeadObject). Returns the number of files uploaded.
func uploadBaselineToS3(ctx context.Context, outputDir, s3URL, region string, retry bool) (int, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return 0, fmt.Errorf("invalid --upload URL: %w", err)
	}

	client, err := storage.NewS3Client(ctx, region)
	if err != nil {
		return 0, err
	}

	var count int
	err = filepath.WalkDir(outputDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		key, err := storage.BuildS3Key(outputDir, path, prefix)
		if err != nil {
			return err
		}
		if retry {
			exists, err := storage.S3ObjectExists(ctx, client, bucket, key)
			if err != nil {
				return err
			}
			if exists {
				slog.Info("skipping existing S3 object (--retry)", "bucket", bucket, "key", key)
				return nil
			}
		}
		if err := storage.UploadFile(ctx, client, path, bucket, key); err != nil {
			return err
		}
		slog.Debug("uploaded", "file", path, "bucket", bucket, "key", key)
		count++
		return nil
	})
	return count, err
}

// decryptDumpFiles walks inputDir and decrypts every .enc file using openssl,
// writing the decrypted output alongside with the .enc extension stripped.
// Returns a cleanup function that removes the decrypted files.
func decryptDumpFiles(inputDir, keyPath string) (func(), error) {
	absKey, err := filepath.Abs(keyPath)
	if err != nil {
		return nil, fmt.Errorf("resolve key path: %w", err)
	}

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("read input directory: %w", err)
	}

	var decrypted []string
	cleanup := func() {
		for _, f := range decrypted {
			os.Remove(f)
		}
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".enc") {
			continue
		}
		encPath := filepath.Join(inputDir, e.Name())
		outPath := strings.TrimSuffix(encPath, ".enc")

		cmd := exec.Command("openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2",
			"-pass", "file:"+absKey, "-in", encPath, "-out", outPath)
		if output, err := cmd.CombinedOutput(); err != nil {
			cleanup()
			return nil, fmt.Errorf("decrypt %s: %w\n%s", e.Name(), err, output)
		}
		decrypted = append(decrypted, outPath)
		slog.Debug("decrypted", "file", e.Name())
	}

	if len(decrypted) == 0 {
		slog.Warn("no .enc files found in input directory; is the dump encrypted?", "dir", inputDir)
	}

	return cleanup, nil
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
