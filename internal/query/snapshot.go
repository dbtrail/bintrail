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

	"github.com/dbtrail/bintrail/internal/parser"
)

// snapshotEventIDBase is OR'd with the row index to synthesise a ResultRow
// EventID high enough that collision with MySQL's AUTO_INCREMENT `event_id`
// (BIGINT UNSIGNED — see internal/indexer) is effectively impossible in
// practice: at 2^62 a real index would need to accumulate ~4.6e18 rows to
// reach this range. MergeResults dedups by event_id, so uniqueness across
// snapshot rows is enough.
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
// error. An slog.Info line is emitted at each exclusion so operators who
// pass --include-snapshot but see "No results" can tell why.
func FetchSnapshot(ctx context.Context, path string, opts Options) ([]ResultRow, error) {
	if opts.EventType != nil && *opts.EventType != parser.EventSnapshot {
		slog.Info("snapshot source excluded by filter", "reason", "--event-type ≠ SNAPSHOT")
		return nil, nil
	}
	if opts.GTID != "" {
		slog.Info("snapshot source excluded by filter", "reason", "--gtid set (baseline rows carry no GTID)")
		return nil, nil
	}
	if opts.ChangedColumn != "" {
		slog.Info("snapshot source excluded by filter", "reason", "--changed-column set (baseline rows have no changed-columns metadata)")
		return nil, nil
	}
	if opts.Flag != "" {
		slog.Info("snapshot source excluded by filter", "reason", "--flag set (baseline rows do not carry table_flags)")
		return nil, nil
	}

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

	ts, err := readSnapshotTimestamp(ctx, db, path)
	if err != nil {
		return nil, err
	}
	if opts.Since != nil && ts.Before(*opts.Since) {
		slog.Info("snapshot source excluded by filter", "reason", "baseline timestamp before --since", "snapshot_ts", ts, "since", *opts.Since)
		return nil, nil
	}
	if opts.Until != nil && ts.After(*opts.Until) {
		slog.Info("snapshot source excluded by filter", "reason", "baseline timestamp after --until", "snapshot_ts", ts, "until", *opts.Until)
		return nil, nil
	}

	where, args, err := snapshotFilters(opts)
	if err != nil {
		return nil, err
	}
	safePath := strings.ReplaceAll(path, "'", "''")
	q := "SELECT * FROM parquet_scan('" + safePath + "')"
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
// file metadata using the shared db handle. Returns a non-nil error when the
// file lacks the key — the caller cannot apply the --since/--until filter
// without it.
func readSnapshotTimestamp(ctx context.Context, db *sql.DB, path string) (time.Time, error) {
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

// snapshotFilters returns WHERE fragments and bind args matching opts.ColumnEq.
// PKValues/PKValuesIn are NOT applied here — this PR does not build PK_values
// for snapshot rows, and combining --pk/--pks with --include-snapshot is
// rejected at the CLI validation layer. Schema/table/since/until are handled
// against the baseline's own metadata before the query runs.
//
// A ColumnEq entry whose column fails IsSafeColumnName returns an error —
// emitting a silent "1=0" clause would leave the operator seeing "No results"
// with a reason that's only in structured logs.
func snapshotFilters(opts Options) ([]string, []any, error) {
	var where []string
	var args []any
	for _, ce := range opts.ColumnEq {
		if !IsSafeColumnName(ce.Column) {
			return nil, nil, fmt.Errorf("snapshot: unsafe column name %q in --column-eq; must match [A-Za-z_][A-Za-z0-9_]*", ce.Column)
		}
		ident := `"` + strings.ReplaceAll(ce.Column, `"`, `""`) + `"`
		if ce.IsNull {
			where = append(where, ident+" IS NULL")
			continue
		}
		// Cast to VARCHAR so string-typed --column-eq values match typed
		// Parquet columns (int, date, etc.) the same way the binlog index's
		// JSON_UNQUOTE(JSON_EXTRACT(...)) path coerces stored values to
		// strings before comparison.
		where = append(where, "CAST("+ident+" AS VARCHAR) = ?")
		args = append(args, ce.Value)
	}
	return where, args, nil
}

// normalizeSnapshotValue converts DuckDB scan outputs into types that
// encoding/json can marshal cleanly. Byte slices are parsed as JSON only when
// the payload's first non-whitespace byte is '{' or '[' — this avoids silently
// promoting a TEXT column containing the literal "null" to Go nil, or "123"
// to a number, which would diverge from the binlog index's string-preserving
// storage. time.Time is formatted MySQL-style; everything else passes through.
func normalizeSnapshotValue(v any) any {
	switch x := v.(type) {
	case []byte:
		if looksLikeJSONContainer(x) && json.Valid(x) {
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

// looksLikeJSONContainer reports whether b's first non-whitespace byte is
// '{' or '[' — a cheap prefix test that distinguishes JSON object/array
// payloads (which MySQL stores in JSON columns) from bare string/numeric
// literals that json.Valid would also accept.
func looksLikeJSONContainer(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}
