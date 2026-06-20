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
//
// The upload mirrors the local Run marker contract (#467) so a mid-upload death
// leaves a snapshot that S3 discovery treats as INCOMPLETE, not complete:
//
//  1. _INCOMPLETE FIRST, per snapshot dir (a zero-byte object — the local
//     _INCOMPLETE marker was already removed once Run succeeded, so there is no
//     local file to walk).
//  2. every data file.
//  3. _SUCCESS LAST. "_SUCCESS" can sort before sibling schema dirs depending
//     on the database name's first byte ('_' is 0x5F — before lowercase letters
//     but after digits and uppercase), so a single-pass lexical WalkDir could
//     publish it before all data is up. We defer it UNCONDITIONALLY, which keeps
//     the S3 snapshot un-marked-complete until its data is fully present.
//  4. best-effort _INCOMPLETE delete. s3IncompleteSnapshots only flags a
//     snapshot incomplete when _INCOMPLETE is present AND _SUCCESS is absent, so
//     a leftover _INCOMPLETE next to a published _SUCCESS is harmless — a failed
//     delete never demotes a completed snapshot. (No S3 DeleteObject of partial
//     data is attempted; resume via --retry overwrites in place.)
func uploadBaselineToS3(ctx context.Context, outputDir, s3URL, region string, retry bool) (int, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return 0, fmt.Errorf("invalid --upload URL: %w", err)
	}

	client, err := storage.NewS3Client(ctx, region)
	if err != nil {
		return 0, err
	}

	// Route the four S3 operations through an injectable seam so the ordering
	// invariant can be unit-tested with a recording mock (#524 review).
	ops := s3ops{
		putEmpty:     func(ctx context.Context, key string) error { return storage.PutEmptyObject(ctx, client, bucket, key) },
		uploadFile:   func(ctx context.Context, path, key string) error { return storage.UploadFile(ctx, client, path, bucket, key) },
		objectExists: func(ctx context.Context, key string) (bool, error) { return storage.S3ObjectExists(ctx, client, bucket, key) },
		deleteObject: func(ctx context.Context, key string) error { return storage.DeleteObject(ctx, client, bucket, key) },
	}
	return runBaselineUpload(ctx, outputDir, prefix, retry, ops)
}

// s3ops abstracts the four S3 operations the baseline upload performs, so the
// crash-safe ordering invariant (_INCOMPLETE first → data files → _SUCCESS last
// → best-effort _INCOMPLETE delete) can be pinned by a recording mock without a
// live client (#524 review).
type s3ops struct {
	putEmpty     func(ctx context.Context, key string) error
	uploadFile   func(ctx context.Context, path, key string) error
	objectExists func(ctx context.Context, key string) (bool, error)
	deleteObject func(ctx context.Context, key string) error
}

// runBaselineUpload performs the crash-safe upload ordering against ops. See the
// uploadBaselineToS3 doc for the four-step contract it guarantees.
func runBaselineUpload(ctx context.Context, outputDir, prefix string, retry bool, ops s3ops) (int, error) {
	upload := func(path string) error {
		key, err := storage.BuildS3Key(outputDir, path, prefix)
		if err != nil {
			return err
		}
		if retry {
			exists, err := ops.objectExists(ctx, key)
			if err != nil {
				return err
			}
			if exists {
				slog.Info("skipping existing S3 object (--retry)", "key", key)
				return nil
			}
		}
		if err := ops.uploadFile(ctx, path, key); err != nil {
			return err
		}
		slog.Debug("uploaded", "file", path, "key", key)
		return nil
	}

	// Snapshot dirs to upload, identified by a local _SUCCESS marker — only
	// completed snapshots reach here post-Run. Each one's _INCOMPLETE marker is
	// keyed off the snapshot dir, NOT off a walked file (Run already removed it).
	snapDirs, err := snapshotDirsWithSuccess(outputDir)
	if err != nil {
		return 0, err
	}
	incompleteKey := func(snapDir string) (string, error) {
		return storage.BuildS3Key(outputDir, filepath.Join(snapDir, baseline.IncompleteMarker), prefix)
	}

	// 1. Publish _INCOMPLETE FIRST so an interrupted upload reads as incomplete.
	for _, snapDir := range snapDirs {
		key, err := incompleteKey(snapDir)
		if err != nil {
			return 0, err
		}
		if err := ops.putEmpty(ctx, key); err != nil {
			return 0, err
		}
	}

	// 2 & 3. Upload data files; defer the _SUCCESS marker(s) to the very end.
	var count int
	var successMarkers []string
	err = filepath.WalkDir(outputDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		if d.Name() == baseline.SuccessMarker {
			successMarkers = append(successMarkers, path) // defer to the end
			return nil
		}
		if err := upload(path); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return count, err
	}
	for _, path := range successMarkers {
		if err := upload(path); err != nil {
			return count, err
		}
		count++
	}

	// 4. Best-effort _INCOMPLETE cleanup — harmless to leave (see the func doc).
	for _, snapDir := range snapDirs {
		key, err := incompleteKey(snapDir)
		if err != nil {
			slog.Warn("could not build _INCOMPLETE marker key for cleanup", "snapshot", snapDir, "error", err)
			continue
		}
		if err := ops.deleteObject(ctx, key); err != nil {
			slog.Warn("could not remove S3 _INCOMPLETE marker after upload (harmless; _SUCCESS decides completeness)",
				"key", key, "error", err)
		}
	}
	return count, nil
}

// snapshotDirsWithSuccess returns the immediate child snapshot directories of
// outputDir that carry a local _SUCCESS marker (i.e. completed snapshots). The
// baseline layout is <output>/<timestamp>/..., so only one level is scanned.
func snapshotDirsWithSuccess(outputDir string) ([]string, error) {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil, fmt.Errorf("read output directory %q: %w", outputDir, err)
	}
	var dirs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		snapDir := filepath.Join(outputDir, e.Name())
		if _, err := os.Stat(filepath.Join(snapDir, baseline.SuccessMarker)); err == nil {
			dirs = append(dirs, snapDir)
		}
	}
	return dirs, nil
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
