// Package parquetquery implements a DuckDB-backed query engine for Parquet archive files.
// It reads Parquet files written by bintrail rotate --archive-dir (local) or stored in S3,
// and returns results in the same format as internal/query.
package parquetquery

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// Fetch queries Parquet archive files (local or S3) using DuckDB and returns matching events.
// source is either a local directory path or an S3 URL prefix (s3://bucket/prefix/).
// Archives follow the Hive-partitioned layout written by bintrail rotate
// (event_date=YYYY-MM-DD/event_hour=HH/events.parquet).
func Fetch(ctx context.Context, opts query.Options, source string) ([]query.ResultRow, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	// Use the OS temp directory for DuckDB scratch files. By default DuckDB
	// creates a .tmp directory in the CWD, which fails in containers where
	// the working directory (often /) is read-only.
	if _, err := db.ExecContext(ctx, "SET temp_directory = '"+os.TempDir()+"'"); err != nil {
		slog.Warn("could not set DuckDB temp_directory", "error", err)
	}

	// Constrain DuckDB resource usage for container environments.
	// DuckDB requires ~125MB per thread; 2 threads allows parallelism
	// across row groups while staying within container memory limits.
	// preserve_insertion_order is safe to disable because our queries
	// have explicit ORDER BY.
	for _, stmt := range []string{
		"SET threads = 2",
		"SET memory_limit = '4GB'",
		"SET preserve_insertion_order = false",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			slog.Warn("could not configure DuckDB", "statement", stmt, "error", err)
		}
	}

	// For S3, download each file locally via the AWS SDK and query with
	// DuckDB from disk. This avoids DuckDB's httpfs extension which holds
	// entire S3 files in memory (outside memory_limit tracking), causing
	// OOM kills in containers. Local reads use OS page cache / mmap.
	if strings.HasPrefix(source, "s3://") {
		files, s3Client, err := listS3ParquetScoped(ctx, source, opts.Since, opts.Until)
		if err != nil {
			return nil, fmt.Errorf("list S3 archive files: %w", err)
		}
		// Pre-filter files by Hive partition values (event_date/event_hour)
		// to avoid downloading parquet files outside the requested time range.
		files = filterFilesByTimeRange(files, opts.Since, opts.Until)
		// Sort chronologically so we can terminate early when --limit is satisfied.
		files = sortFilesByHour(files)
		slog.Debug("files after time-range pruning", "count", len(files))
		if len(files) == 0 {
			slog.Warn("no .parquet files found in S3 archive source", "source", source)
			return nil, nil
		}

		dl := newS3Downloader(s3Client)

		// Pipeline: prefetch up to maxInFlightDownloads files in parallel
		// while DuckDB queries the current one. Queries remain strictly
		// sequential — only one DuckDB query is active at a time, so peak
		// RAM (DuckDB's per-query working set) is unchanged from a serial
		// implementation. Peak temp files on disk at any instant:
		// maxInFlightDownloads + 1 (one being queried, the rest buffered
		// as completed prefetches).
		const maxInFlightDownloads = 2
		slots := make([]chan dlResult, len(files))
		for i := range slots {
			slots[i] = make(chan dlResult, 1)
		}
		dlCtx, cancelDl := context.WithCancel(ctx)
		defer cancelDl()
		go prefetchAll(dlCtx, files, slots, maxInFlightDownloads, dl.download)

		var results []query.ResultRow
		for i := range files {
			dr, ok := <-slots[i]
			if !ok {
				// Slot closed without a value (download canceled); stop.
				break
			}
			if dr.err != nil {
				cancelDl()
				drainSlots(slots[i+1:])
				return nil, fmt.Errorf("download archive file %s: %w", dr.src, dr.err)
			}

			fileResults, qErr := queryLocalFile(ctx, db, dr.path, dr.src, opts)
			removeTempFile(dr.path)
			if qErr != nil {
				cancelDl()
				drainSlots(slots[i+1:])
				return nil, qErr
			}
			results = append(results, fileResults...)
			slog.Debug("queried archive file", "file", dr.src, "rows", len(fileResults))

			// Per-file early termination: stop as soon as no remaining file
			// can produce a row earlier than what we already have.
			if opts.Limit > 0 && len(results) >= opts.Limit && i+1 < len(files) {
				if canTerminateEarly(results, files[i+1:], opts.Limit) {
					slog.Debug("early termination: collected enough results",
						"collected", len(results), "limit", opts.Limit,
						"remaining_files", len(files)-i-1)
					cancelDl()
					drainSlots(slots[i+1:])
					break
				}
			}
		}
		return results, nil
	}

	glob := buildGlob(source)
	cols, colErr := parquetColumns(ctx, db, glob)
	if colErr != nil {
		return nil, fmt.Errorf("read parquet schema: %w", colErr)
	}
	q, args := buildQueryForFile(glob, opts, cols)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("parquet query: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// listS3ParquetScoped lists .parquet files under an S3 prefix, optionally scoping
// to date-specific prefixes when since/until are provided and span ≤31 days.
// This avoids listing all files in the archive when only a narrow time range is needed.
// It returns the S3 client configured for the bucket's region so callers can reuse
// it for downloads without loading the AWS config again.
func listS3ParquetScoped(ctx context.Context, source string, since, until *time.Time) (files []string, client *s3.Client, err error) {
	bucket, prefix, err := parseS3Source(source)
	if err != nil {
		return nil, nil, err
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("load AWS config: %w", err)
	}

	// Detect the bucket's actual region via GetBucketLocation (must be called
	// from us-east-1). This prevents 301 PermanentRedirect errors when the
	// configured region doesn't match the bucket's location.
	bucketRegion := cfg.Region
	locClient := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = "us-east-1"
	})
	loc, locErr := locClient.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: &bucket,
	})
	if locErr != nil {
		slog.Warn("could not detect S3 bucket region, using default", "bucket", bucket, "error", locErr)
	} else {
		r := string(loc.LocationConstraint)
		if r == "" {
			r = "us-east-1" // GetBucketLocation returns empty for us-east-1
		}
		if r != cfg.Region {
			slog.Debug("S3 bucket in different region, switching", "bucket", bucket, "bucket_region", r, "default_region", cfg.Region)
		}
		bucketRegion = r
	}

	client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = bucketRegion
	})

	// Generate date-scoped prefixes when time range is narrow enough.
	// This avoids listing thousands of irrelevant files for large archives.
	datePrefixes := generateDatePrefixes(prefix, since, until)
	if datePrefixes == nil {
		// Wide range or no time bounds — list everything under the base prefix.
		datePrefixes = []string{prefix}
	}
	slog.Debug("S3 listing prefixes", "count", len(datePrefixes))

	// List objects under each date prefix concurrently.
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4)
	for _, dp := range datePrefixes {
		g.Go(func() error {
			paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
				Bucket: &bucket,
				Prefix: &dp,
			})
			for paginator.HasMorePages() {
				page, pageErr := paginator.NextPage(gctx)
				if pageErr != nil {
					return fmt.Errorf("list S3 objects under s3://%s/%s: %w", bucket, dp, pageErr)
				}
				mu.Lock()
				for _, obj := range page.Contents {
					if obj.Key != nil && strings.HasSuffix(*obj.Key, ".parquet") {
						files = append(files, fmt.Sprintf("s3://%s/%s", bucket, *obj.Key))
					}
				}
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	slog.Debug("listed S3 archive files", "source", source, "count", len(files))
	return files, client, nil
}

// maxScopedDays is the maximum number of days for prefix-scoped S3 listing.
// Beyond this, we fall back to listing the full prefix.
const maxScopedDays = 31

// generateDatePrefixes returns date-scoped S3 prefixes for Hive-partitioned
// archives (event_date=YYYY-MM-DD/). Returns nil when the range is too wide
// or no time bounds are provided, signaling the caller to list the full prefix.
// Assumes event_date= directories are directly under basePrefix (the layout
// written by bintrail rotate --archive-dir).
func generateDatePrefixes(basePrefix string, since, until *time.Time) []string {
	if since == nil && until == nil {
		return nil
	}
	now := time.Now().UTC()
	start := now.AddDate(0, 0, -maxScopedDays).Truncate(24 * time.Hour)
	if since != nil {
		start = since.Truncate(24 * time.Hour)
	}
	// Use the end-of-day (truncate to day) for the until bound.
	// When until is nil, include today fully.
	end := now.Truncate(24 * time.Hour)
	if until != nil {
		end = until.Truncate(24 * time.Hour)
	}

	days := int(end.Sub(start).Hours()/24) + 1
	if days <= 0 {
		days = 1
	}
	if days > maxScopedDays {
		return nil
	}

	prefixes := make([]string, 0, days)
	for d := range days {
		day := start.AddDate(0, 0, d)
		prefixes = append(prefixes, basePrefix+"event_date="+day.Format("2006-01-02")+"/")
	}
	return prefixes
}

// s3Downloader holds a reusable S3 client to avoid re-creating AWS config
// on every file download.
type s3Downloader struct {
	client *s3.Client
}

func newS3Downloader(client *s3.Client) *s3Downloader {
	return &s3Downloader{client: client}
}

// download fetches an S3 parquet file to a temporary local file.
// The caller must os.Remove the file when done.
func (d *s3Downloader) download(ctx context.Context, s3URL string) (string, error) {
	rest := strings.TrimPrefix(s3URL, "s3://")
	bucket, key, _ := strings.Cut(rest, "/")
	if bucket == "" || key == "" {
		return "", fmt.Errorf("invalid S3 URL %q", s3URL)
	}

	resp, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return "", fmt.Errorf("download s3://%s/%s: %w", bucket, key, err)
	}
	defer resp.Body.Close()

	tmp, err := os.CreateTemp("", "bintrail-*.parquet")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", fmt.Errorf("write temp file: %w", err)
	}
	tmp.Close()
	var size int64
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}
	slog.Debug("downloaded archive file", "s3", s3URL, "local", tmp.Name(), "bytes", size)
	return tmp.Name(), nil
}

// dlResult carries the outcome of a single S3 file download to the consumer.
// path is the temp file path on disk (caller deletes); empty when err is set.
type dlResult struct {
	path string
	src  string
	err  error
}

// downloadFn fetches a remote file to a local temp path. The implementation
// returns ("", err) on failure or (path, nil) on success. Abstracted as a
// function (rather than a method on *s3Downloader) so tests can inject fakes.
type downloadFn func(ctx context.Context, src string) (string, error)

// prefetchAll downloads files into their slots with bounded parallelism.
// Each slot is closed exactly once — by the per-file goroutine if it ran,
// by prefetchAll directly if cancellation prevented launch, or by the
// goroutine's defer when cancellation arrives after download completes —
// so the consumer's <-slots[i] always unblocks.
func prefetchAll(ctx context.Context, files []string, slots []chan dlResult, maxInFlight int, download downloadFn) {
	sem := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup
	for i, f := range files {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// Close slots we never launched so the consumer doesn't block.
			for k := i; k < len(slots); k++ {
				close(slots[k])
			}
			wg.Wait()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			defer close(slots[i])
			path, err := download(ctx, f)
			if ctx.Err() != nil {
				// Consumer is gone; clean up our temp file rather than send.
				// Any error here is downstream of cancellation — a real S3
				// download error (closed connection, request canceled by the
				// transport, etc.) often does NOT wrap context.Canceled, so
				// classifying by error type produces false positives. Real
				// persistent S3 problems will resurface on the consumer's
				// next non-canceled query via dr.err.
				if err != nil {
					slog.Debug("download discarded after cancel", "src", f, "error", err)
				}
				removeTempFile(path)
				return
			}
			slots[i] <- dlResult{path: path, src: f, err: err}
		}()
	}
	wg.Wait()
}

// drainSlots consumes any pending prefetched results and removes their temp
// files. Called after the consumer stops reading (early termination or error)
// so we don't leak files prefetched before cancellation took effect.
func drainSlots(slots []chan dlResult) {
	for _, ch := range slots {
		if dr, ok := <-ch; ok {
			removeTempFile(dr.path)
		}
	}
}

// removeTempFile deletes a single temp file path, ignoring missing-file errors.
func removeTempFile(path string) {
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Warn("failed to remove temp file", "path", path, "error", err)
	}
}

// queryLocalFile runs a single DuckDB query against a downloaded parquet file
// and returns the matching rows. The caller deletes the file after this returns.
func queryLocalFile(ctx context.Context, db *sql.DB, path, srcURL string, opts query.Options) ([]query.ResultRow, error) {
	cols, colErr := parquetColumns(ctx, db, path)
	if colErr != nil {
		return nil, fmt.Errorf("read parquet schema %s: %w", srcURL, colErr)
	}
	q, args := buildQueryForFile(path, opts, cols)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("parquet query %s: %w", srcURL, err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// sortFilesByHour sorts S3 file paths chronologically by their Hive partition
// values (event_date/event_hour). Files with unparseable paths are placed at the end.
func sortFilesByHour(files []string) []string {
	sorted := make([]string, len(files))
	copy(sorted, files)
	slices.SortFunc(sorted, func(a, b string) int {
		ta, aOK := parseFileHour(a)
		tb, bOK := parseFileHour(b)
		if !aOK && !bOK {
			return cmp.Compare(a, b)
		}
		if !aOK {
			return 1 // unparseable goes to end
		}
		if !bOK {
			return -1
		}
		return ta.Compare(tb)
	})
	return sorted
}

// canTerminateEarly returns true when we've collected enough results for the
// limit and all remaining files are from later hours. Since files are processed
// in chronological order, if the limit-th result's timestamp is before the
// next file's hour start, no future file can contain rows that would displace it.
func canTerminateEarly(results []query.ResultRow, remainingFiles []string, limit int) bool {
	if len(results) < limit || len(remainingFiles) == 0 {
		return false
	}
	// Sort results to find the limit-th by (event_timestamp, event_id).
	sorted := make([]query.ResultRow, len(results))
	copy(sorted, results)
	slices.SortFunc(sorted, func(a, b query.ResultRow) int {
		if c := a.EventTimestamp.Compare(b.EventTimestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.EventID, b.EventID)
	})
	cutoff := sorted[limit-1].EventTimestamp

	// If the next remaining file's hour starts after the cutoff, we can stop.
	nextHour, ok := parseFileHour(remainingFiles[0])
	if !ok {
		return false // can't determine, keep going
	}
	return nextHour.After(cutoff)
}

// parseS3Source extracts the bucket and prefix from an S3 URL.
// The prefix always ends with "/" for listing purposes.
func parseS3Source(source string) (bucket, prefix string, err error) {
	rest := strings.TrimPrefix(source, "s3://")
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("empty bucket in S3 source %q", source)
	}
	// Ensure prefix ends with "/" so ListObjectsV2 scopes correctly.
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return bucket, prefix, nil
}

// buildQueryFromFiles constructs a DuckDB SQL query using an explicit list of
// S3 file paths instead of a glob pattern. This avoids DuckDB's broken S3 glob
// expansion for paths containing Hive partition keys (= signs).
func buildQueryFromFiles(files []string, opts query.Options) (string, []any) {
	where, args := buildFilters(opts)

	// Build the file list as a DuckDB array literal: ['s3://...', 's3://...']
	var escaped []string
	for _, f := range files {
		escaped = append(escaped, "'"+strings.ReplaceAll(f, "'", "''")+"'")
	}
	fileList := "[" + strings.Join(escaped, ", ") + "]"

	q := "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp," +
		" gtid, connection_id, schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version" +
		" FROM parquet_scan(" + fileList + ", hive_partitioning=true, union_by_name=true)"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY event_timestamp, event_id"
	if opts.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, opts.Limit)
	}

	return q, args
}

// buildGlob converts a local directory path to a glob pattern that selects all
// Parquet archive files under that location. Only used for local paths — S3
// sources use listS3Parquet + buildQueryFromFiles instead.
func buildGlob(source string) string {
	s := strings.TrimSuffix(source, "/")
	if strings.HasSuffix(s, ".parquet") {
		return source
	}
	return s + "/**/*.parquet"
}

// buildUnsortedQuery constructs a DuckDB query for a single local parquet file
// without ORDER BY. Used for per-file S3 queries where the caller handles
// sorting after collecting all results. Skipping ORDER BY lets DuckDB stream
// rows without buffering the full result set, dramatically reducing memory.
func buildUnsortedQuery(path string, opts query.Options) (string, []any) {
	where, args := buildFilters(opts)
	safePath := strings.ReplaceAll(path, "'", "''")

	q := "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp," +
		" gtid, connection_id, schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version" +
		" FROM parquet_scan('" + safePath + "', union_by_name=true)"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}

	return q, args
}

// buildQuery constructs a DuckDB SQL query from a glob pattern (local paths only).
// The glob is embedded directly in the SQL because DuckDB table functions do not
// support bind parameters for the file path argument.
func buildQuery(glob string, opts query.Options) (string, []any) {
	where, args := buildFilters(opts)

	// Escape single quotes in the glob path to prevent SQL injection.
	safeGlob := strings.ReplaceAll(glob, "'", "''")

	q := "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp," +
		" gtid, connection_id, schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version" +
		" FROM parquet_scan('" + safeGlob + "', hive_partitioning=true, union_by_name=true)"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY event_timestamp, event_id"
	if opts.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, opts.Limit)
	}

	return q, args
}

// buildFilters extracts WHERE clause fragments and bind args from query options.
func buildFilters(opts query.Options) ([]string, []any) {
	var where []string
	var args []any

	if opts.Schema != "" {
		where = append(where, "schema_name = ?")
		args = append(args, opts.Schema)
	}
	if opts.Table != "" {
		where = append(where, "table_name = ?")
		args = append(args, opts.Table)
	}
	if opts.PKValues != "" {
		where = append(where, "pk_values = ?")
		args = append(args, opts.PKValues)
	}
	if opts.EventType != nil {
		where = append(where, "event_type = ?")
		args = append(args, int32(*opts.EventType))
	}
	if opts.GTID != "" {
		where = append(where, "gtid = ?")
		args = append(args, opts.GTID)
	}
	if opts.Since != nil {
		where = append(where, "event_timestamp >= ?")
		args = append(args, *opts.Since)
	}
	if opts.Until != nil {
		where = append(where, "event_timestamp <= ?")
		args = append(args, *opts.Until)
	}
	if opts.ChangedColumn != "" {
		needle, _ := json.Marshal(opts.ChangedColumn)
		where = append(where, "json_contains(changed_columns, ?)")
		args = append(args, string(needle))
	}

	return where, args
}

// parquetColumns returns the set of column names present in a parquet source.
// Works with both single files (S3 temp downloads) and glob patterns (local
// archives). For globs, union_by_name merges schemas across all matching
// files so the result reflects the full column superset.
func parquetColumns(ctx context.Context, db *sql.DB, path string) (map[string]bool, error) {
	safePath := strings.ReplaceAll(path, "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM parquet_scan('"+safePath+"', hive_partitioning=true, union_by_name=true) LIMIT 0")
	if err != nil {
		return nil, err
	}
	names, err := rows.Columns()
	rows.Close()
	if err != nil {
		return nil, err
	}
	cols := make(map[string]bool, len(names))
	for _, n := range names {
		cols[n] = true
	}
	return cols, nil
}

// buildQueryForFile constructs a DuckDB query for a single parquet file,
// substituting typed NULLs for optional columns not present in the file.
// This handles backward compatibility when reading archive files written
// before a schema-adding release (e.g., pre-v0.4.4 files lack connection_id).
func buildQueryForFile(path string, opts query.Options, cols map[string]bool) (string, []any) {
	where, args := buildFilters(opts)
	safePath := strings.ReplaceAll(path, "'", "''")

	connCol := "connection_id"
	if !cols["connection_id"] {
		connCol = "NULL::INT32 AS connection_id"
	}

	q := "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp," +
		" gtid, " + connCol + ", schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version" +
		" FROM parquet_scan('" + safePath + "', hive_partitioning=true, union_by_name=true)"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY event_timestamp, event_id"
	if opts.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, opts.Limit)
	}
	return q, args
}

// filterFilesByTimeRange prunes the file list based on Hive partition values
// (event_date=YYYY-MM-DD/event_hour=HH) extracted from the paths. Files whose
// hour does not overlap with [since, until] are excluded. Files without
// parseable partition values are kept (safe fallback).
func filterFilesByTimeRange(files []string, since, until *time.Time) []string {
	if since == nil && until == nil {
		return files
	}
	var filtered []string
	for _, f := range files {
		hourStart, ok := parseFileHour(f)
		if !ok {
			filtered = append(filtered, f) // can't determine; include to be safe
			continue
		}
		hourEnd := hourStart.Add(time.Hour)
		if since != nil && hourEnd.Before(*since) {
			continue // entire hour is before the since cutoff
		}
		if until != nil && hourStart.After(*until) {
			continue // entire hour is after the until cutoff
		}
		filtered = append(filtered, f)
	}
	return filtered
}

// parseFileHour extracts the hour start time from Hive partition path segments
// (event_date=YYYY-MM-DD/event_hour=HH). Returns zero time and false if the
// path does not contain both segments.
func parseFileHour(path string) (time.Time, bool) {
	var dateStr, hourStr string
	for _, seg := range strings.Split(path, "/") {
		if strings.HasPrefix(seg, "event_date=") {
			dateStr = strings.TrimPrefix(seg, "event_date=")
		} else if strings.HasPrefix(seg, "event_hour=") {
			hourStr = strings.TrimPrefix(seg, "event_hour=")
		}
	}
	if dateStr == "" || hourStr == "" {
		return time.Time{}, false
	}
	t, err := time.Parse("2006-01-02 15", dateStr+" "+hourStr)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// scanRows converts DuckDB result rows into []query.ResultRow.
func scanRows(rows *sql.Rows) ([]query.ResultRow, error) {
	var results []query.ResultRow
	for rows.Next() {
		var (
			eventID        int64
			binlogFile     string
			startPos       int64
			endPos         int64
			eventTimestamp time.Time
			gtid           sql.NullString
			connID         sql.NullInt64
			schemaName     string
			tableName      string
			eventType      int32
			pkValues       string
			changedCols    sql.NullString
			rowBefore      sql.NullString
			rowAfter       sql.NullString
			schemaVersion  int32
		)
		if err := rows.Scan(
			&eventID, &binlogFile, &startPos, &endPos, &eventTimestamp,
			&gtid, &connID, &schemaName, &tableName, &eventType, &pkValues,
			&changedCols, &rowBefore, &rowAfter, &schemaVersion,
		); err != nil {
			return nil, fmt.Errorf("scan parquet result: %w", err)
		}

		r := query.ResultRow{
			EventID:        uint64(eventID),
			BinlogFile:     binlogFile,
			StartPos:       uint64(startPos),
			EndPos:         uint64(endPos),
			EventTimestamp: eventTimestamp,
			SchemaName:     schemaName,
			TableName:      tableName,
			EventType:      parser.EventType(eventType),
			PKValues:       pkValues,
			SchemaVersion:  uint32(schemaVersion),
		}
		if gtid.Valid {
			r.GTID = &gtid.String
		}
		if connID.Valid {
			v := uint32(connID.Int64)
			r.ConnectionID = &v
		}
		if changedCols.Valid && changedCols.String != "" {
			_ = json.Unmarshal([]byte(changedCols.String), &r.ChangedColumns)
		}
		if rowBefore.Valid && rowBefore.String != "" {
			_ = json.Unmarshal([]byte(rowBefore.String), &r.RowBefore)
		}
		if rowAfter.Valid && rowAfter.String != "" {
			_ = json.Unmarshal([]byte(rowAfter.String), &r.RowAfter)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}
