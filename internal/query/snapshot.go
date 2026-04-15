// Package query — snapshot.go: merges mydumper baseline Parquet rows as a
// third source for `bintrail query --include-snapshot`.
//
// Baseline rows are emitted as synthetic events with EventType=EventSnapshot
// and EventTimestamp=baseline_creation_ts (read from the Parquet file's
// `bintrail.snapshot_timestamp` key-value metadata). They slot into the
// existing MergeAndTrim pipeline alongside live-MySQL and archive events.
package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/parser"
)

// snapshotEventIDBase is OR'd with the row index to synthesise a ResultRow
// EventID that cannot collide with a real binlog event_id (always a binlog
// byte offset, << 1<<62). MergeResults dedups by event_id, so uniqueness
// across snapshot rows is enough.
const snapshotEventIDBase uint64 = 1 << 62

// FetchSnapshot reads a single mydumper baseline Parquet file and returns
// matching rows as synthetic SNAPSHOT events. path must point at a
// <schema>/<table>.parquet file produced by `bintrail baseline` (local or
// s3:// URL).
//
// Filters that don't apply to baseline rows (gtid, changed-column, flag)
// exclude the whole snapshot source. --event-type SNAPSHOT keeps only
// snapshot rows; any other event-type filter excludes them. --since/--until
// apply to the baseline's recorded creation timestamp.
//
// Returns nil, nil when filters exclude all snapshot rows — this is not an
// error and callers should proceed with no-snapshot results.
func FetchSnapshot(ctx context.Context, path string, opts Options) ([]ResultRow, error) {
	// Short-circuit: filters that cannot match baseline rows.
	if opts.EventType != nil && *opts.EventType != parser.EventSnapshot {
		return nil, nil
	}
	if opts.GTID != "" || opts.ChangedColumn != "" || opts.Flag != "" {
		return nil, nil
	}

	meta, err := baseline.ReadParquetMetadataAny(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read baseline metadata %s: %w", path, err)
	}
	ts, err := readSnapshotTimestamp(ctx, path)
	if err != nil {
		return nil, err
	}
	// baseline_creation_ts filter per issue #234.
	if opts.Since != nil && ts.Before(*opts.Since) {
		return nil, nil
	}
	if opts.Until != nil && ts.After(*opts.Until) {
		return nil, nil
	}
	_ = meta // reserved for future use (binlog file/pos surfacing)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if strings.HasPrefix(path, "s3://") {
		if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
			return nil, fmt.Errorf("load httpfs: %w", err)
		}
	}

	safePath := strings.ReplaceAll(path, "'", "''")
	q := "SELECT * FROM parquet_scan('" + safePath + "')"
	where, args := snapshotFilters(opts)
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	if opts.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("snapshot query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("snapshot columns: %w", err)
	}

	var results []ResultRow
	idx := uint64(0)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan snapshot row: %w", err)
		}
		rowAfter := make(map[string]any, len(cols))
		for i, c := range cols {
			rowAfter[c] = normalizeSnapshotValue(vals[i])
		}
		results = append(results, ResultRow{
			EventID:        snapshotEventIDBase | idx,
			EventTimestamp: ts,
			SchemaName:     opts.Schema,
			TableName:      opts.Table,
			EventType:      parser.EventSnapshot,
			RowAfter:       rowAfter,
		})
		idx++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot rows: %w", err)
	}
	return results, nil
}

// readSnapshotTimestamp extracts bintrail.snapshot_timestamp from the Parquet
// file metadata. Returns a non-nil error when the file lacks the key — the
// caller cannot apply the --since/--until filter without it.
func readSnapshotTimestamp(ctx context.Context, path string) (time.Time, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return time.Time{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if strings.HasPrefix(path, "s3://") {
		if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
			return time.Time{}, fmt.Errorf("load httpfs: %w", err)
		}
	}
	safePath := strings.ReplaceAll(path, "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT key, value FROM parquet_kv_metadata('"+safePath+"')")
	if err != nil {
		return time.Time{}, fmt.Errorf("read baseline metadata: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var keyBytes, valBytes []byte
		if err := rows.Scan(&keyBytes, &valBytes); err != nil {
			return time.Time{}, fmt.Errorf("scan metadata row: %w", err)
		}
		if string(keyBytes) != "bintrail.snapshot_timestamp" {
			continue
		}
		t, err := time.Parse(time.RFC3339, string(valBytes))
		if err != nil {
			return time.Time{}, fmt.Errorf("parse snapshot_timestamp %q: %w", string(valBytes), err)
		}
		return t.UTC(), nil
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("iterate metadata: %w", err)
	}
	return time.Time{}, fmt.Errorf("baseline %s missing bintrail.snapshot_timestamp metadata — re-run `bintrail baseline`", path)
}

// snapshotFilters returns WHERE fragments and bind args matching opts.ColumnEq
// and opts.PKValues/PKValuesIn. Schema/table/since/until are not applied here
// (they are validated against the baseline itself before the query runs).
func snapshotFilters(opts Options) ([]string, []any) {
	var where []string
	var args []any
	for _, ce := range opts.ColumnEq {
		if !IsSafeColumnName(ce.Column) {
			slog.Error("snapshot: rejected unsafe column name; emitting no-match clause", "column", ce.Column)
			where = append(where, "1=0")
			continue
		}
		ident := `"` + strings.ReplaceAll(ce.Column, `"`, `""`) + `"`
		if ce.IsNull {
			where = append(where, ident+" IS NULL")
			continue
		}
		// Cast to VARCHAR so string-typed --column-eq values match typed
		// Parquet columns (int, date, etc.) the same way the binlog index's
		// JSON_EXTRACT_STRING path does.
		where = append(where, "CAST("+ident+" AS VARCHAR) = ?")
		args = append(args, ce.Value)
	}
	return where, args
}

// normalizeSnapshotValue converts DuckDB scan outputs into types that
// encoding/json can marshal cleanly. Byte slices that aren't valid JSON are
// stringified; time.Time is formatted MySQL-style; everything else passes
// through.
func normalizeSnapshotValue(v any) any {
	switch x := v.(type) {
	case []byte:
		if json.Valid(x) {
			var decoded any
			if err := json.Unmarshal(x, &decoded); err == nil {
				return decoded
			}
		}
		return string(x)
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04:05")
	default:
		return v
	}
}
