// Package parquetquery implements a DuckDB-backed query engine for Parquet archive files.
// It reads Parquet files written by bintrail rotate --archive-dir (local) or stored in S3,
// and returns results in the same format as internal/query.
package parquetquery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
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

	// S3 sources require the httpfs extension for S3 protocol support and the
	// aws extension for credential resolution (reads AWS_ACCESS_KEY_ID,
	// AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_DEFAULT_REGION from env).
	// Without aws, DuckDB attempts anonymous S3 access which silently returns
	// zero results.
	if strings.HasPrefix(source, "s3://") {
		if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
			return nil, fmt.Errorf("load duckdb httpfs extension: %w", err)
		}
		if _, err := db.ExecContext(ctx, "INSTALL aws; LOAD aws;"); err != nil {
			return nil, fmt.Errorf("install/load duckdb aws extension: %w", err)
		}
		if _, err := db.ExecContext(ctx, "CALL load_aws_credentials();"); err != nil {
			slog.Warn("could not load AWS credentials into DuckDB, falling back to anonymous S3 access", "error", err)
		}
	}

	// For S3, list objects via the AWS SDK and pass explicit file paths to
	// DuckDB. DuckDB's glob expansion does not work reliably on S3 paths
	// containing Hive partition keys (= signs). For local paths, use the
	// standard glob approach which works correctly.
	var q string
	var args []any
	if strings.HasPrefix(source, "s3://") {
		files, err := listS3Parquet(ctx, source)
		if err != nil {
			return nil, fmt.Errorf("list S3 archive files: %w", err)
		}
		if len(files) == 0 {
			return nil, nil
		}
		q, args = buildQueryFromFiles(files, opts)
	} else {
		glob := buildGlob(source)
		q, args = buildQuery(glob, opts)
	}

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("parquet query: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// listS3Parquet uses the AWS SDK to list all .parquet files under an S3 prefix.
// This bypasses DuckDB's glob expansion which fails on Hive-partitioned paths.
func listS3Parquet(ctx context.Context, source string) ([]string, error) {
	bucket, prefix, err := parseS3Source(source)
	if err != nil {
		return nil, err
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)

	var files []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list S3 objects under s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, ".parquet") {
				files = append(files, fmt.Sprintf("s3://%s/%s", bucket, *obj.Key))
			}
		}
	}
	slog.Debug("listed S3 archive files", "source", source, "count", len(files))
	return files, nil
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
