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

	glob := buildGlob(source)
	q, args := buildQuery(glob, opts)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("parquet query: %w", err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// buildGlob converts source (a directory path or S3 URL) to a glob pattern that
// selects Parquet archive files under that location. For local paths it uses a
// recursive glob (**/*.parquet). For S3 paths it uses explicit single-level
// wildcards (/*/*/*.parquet) because DuckDB's httpfs extension does not support
// recursive globs; the source must be at the bintrail_id=<uuid> level to match
// the expected event_date=.../event_hour=.../events.parquet layout.
func buildGlob(source string) string {
	s := strings.TrimSuffix(source, "/")
	if strings.HasSuffix(s, ".parquet") {
		return source
	}
	// DuckDB's httpfs extension does not support ** (recursive) globs on S3.
	// Use explicit single-level wildcards matching the two Hive partition levels
	// below the bintrail_id=<uuid> source path:
	//   event_date=YYYY-MM-DD/event_hour=HH/events.parquet
	if strings.HasPrefix(s, "s3://") {
		return s + "/*/*/*.parquet"
	}
	return s + "/**/*.parquet"
}

// buildQuery constructs a DuckDB SQL query and its arguments for the given options.
// The glob is embedded directly in the SQL because DuckDB table functions do not
// support bind parameters for the file path argument.
func buildQuery(glob string, opts query.Options) (string, []any) {
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
		// No SHA2 index in Parquet — plain equality on pk_values.
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
		// Mirror the MySQL JSON_CONTAINS pattern: needle is the JSON-encoded column name,
		// e.g. "status" → `"status"` (with double quotes), matching inside the JSON array.
		needle, _ := json.Marshal(opts.ChangedColumn)
		where = append(where, "json_contains(changed_columns, ?)")
		args = append(args, string(needle))
	}

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
