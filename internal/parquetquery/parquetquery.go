// Package parquetquery implements a DuckDB-backed query engine for Parquet archive files.
// It reads Parquet files written by bintrail rotate --archive-dir (local) or stored in S3,
// and returns results in the same format as internal/query.
package parquetquery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/bintrail/bintrail/internal/parser"
	"github.com/bintrail/bintrail/internal/query"
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
	// DuckDB requires 125MB per thread minimum; 1 thread keeps baseline
	// low so more memory is available for data. preserve_insertion_order
	// is safe to disable because our queries have explicit ORDER BY.
	for _, stmt := range []string{
		"SET threads = 1",
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
		files, bucketRegion, err := listS3Parquet(ctx, source)
		if err != nil {
			return nil, fmt.Errorf("list S3 archive files: %w", err)
		}
		// Pre-filter files by Hive partition values (event_date/event_hour)
		// to avoid downloading parquet files outside the requested time range.
		// DuckDB can't map event_timestamp filters to partition columns.
		files = filterFilesByTimeRange(files, opts.Since, opts.Until)
		slog.Debug("files after time-range pruning", "count", len(files))
		if len(files) == 0 {
			slog.Warn("no .parquet files found in S3 archive source", "source", source)
			return nil, nil
		}
		// Download and query one file at a time. DuckDB reads the local
		// file, then we delete it before moving to the next — peak memory
		// is one file's decompressed data, not all files combined.
		// No ORDER BY here: the caller (query.MergeResults) sorts after
		// collecting all results. Skipping ORDER BY lets DuckDB stream
		// matching rows without buffering the full result set.
		var results []query.ResultRow
		for _, f := range files {
			tmpPath, err := downloadS3ToTemp(ctx, f, bucketRegion)
			if err != nil {
				return nil, fmt.Errorf("download archive file: %w", err)
			}
			q, args := buildUnsortedQuery(tmpPath, opts)
			rows, err := db.QueryContext(ctx, q, args...)
			if err != nil {
				os.Remove(tmpPath)
				return nil, fmt.Errorf("parquet query %s: %w", f, err)
			}
			batch, err := scanRows(rows)
			rows.Close()
			os.Remove(tmpPath)
			if err != nil {
				return nil, err
			}
			results = append(results, batch...)
			slog.Debug("queried archive file", "file", f, "rows", len(batch))
		}
		return results, nil
	}

	glob := buildGlob(source)
	q, args := buildQuery(glob, opts)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("parquet query: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// listS3Parquet uses the AWS SDK to list all .parquet files under an S3 prefix.
// This bypasses DuckDB's glob expansion which fails on Hive-partitioned paths.
// It also auto-detects the bucket's region via GetBucketLocation to avoid 301
// PermanentRedirect errors when AWS_DEFAULT_REGION differs from the bucket's
// actual region. The detected region is returned so callers can configure
// DuckDB's s3_region accordingly.
func listS3Parquet(ctx context.Context, source string) (files []string, bucketRegion string, err error) {
	bucket, prefix, err := parseS3Source(source)
	if err != nil {
		return nil, "", err
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("load AWS config: %w", err)
	}

	// Detect the bucket's actual region via GetBucketLocation (must be called
	// from us-east-1). This prevents 301 PermanentRedirect errors when the
	// configured region doesn't match the bucket's location.
	bucketRegion = cfg.Region
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

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = bucketRegion
	})

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, bucketRegion, fmt.Errorf("list S3 objects under s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, ".parquet") {
				files = append(files, fmt.Sprintf("s3://%s/%s", bucket, *obj.Key))
			}
		}
	}
	slog.Debug("listed S3 archive files", "source", source, "count", len(files))
	return files, bucketRegion, nil
}

// downloadS3ToTemp downloads an S3 parquet file to a temporary local file and
// returns its path. The caller must os.Remove the file when done. This avoids
// DuckDB's httpfs extension which holds entire files in memory.
func downloadS3ToTemp(ctx context.Context, s3URL, region string) (string, error) {
	rest := strings.TrimPrefix(s3URL, "s3://")
	bucket, key, _ := strings.Cut(rest, "/")
	if bucket == "" || key == "" {
		return "", fmt.Errorf("invalid S3 URL %q", s3URL)
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = region
	})

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
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
		" gtid, schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version" +
		" FROM parquet_scan(" + fileList + ", hive_partitioning=true)"
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
		" gtid, schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version" +
		" FROM parquet_scan('" + safePath + "')"
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
		" gtid, schema_name, table_name, event_type, pk_values," +
		" changed_columns, row_before, row_after, schema_version" +
		" FROM parquet_scan('" + safeGlob + "', hive_partitioning=true)"
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
			&gtid, &schemaName, &tableName, &eventType, &pkValues,
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
