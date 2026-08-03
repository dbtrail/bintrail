package archive

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for parquet_scan
)

// RestorePartition streams one archived Parquet file back into binlog_events
// — the read-side mirror of ArchivePartition. event_id is inserted EXPLICITLY
// (query.MergeResults dedups by it, so archived identity must survive the
// round trip; InnoDB advances the AUTO_INCREMENT counter past explicit
// values). pk_hash is a stored generated column and is never inserted.
// Returns the number of rows loaded.
func RestorePartition(ctx context.Context, db *sql.DB, localPath string, batchSize int) (int64, error) {
	// MySQL prepared statements cap at 65,535 placeholders (uint16) — the
	// #956 class the indexer already guards with a derived MaxBatchSize. 18
	// columns × 5,000 rows = 90,000 placeholders = ER 1390 on every thin-row
	// archive, so the clamp is derived from the column slice (a 19th column
	// must not silently re-break it).
	maxTuples := 65535 / len(BinlogEventColumns)
	if batchSize <= 0 || batchSize > maxTuples {
		batchSize = maxTuples
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, fmt.Errorf("open DuckDB: %w", err)
	}
	defer duck.Close()

	pathLit := "'" + strings.ReplaceAll(localPath, "'", "''") + "'"
	// Column presence is introspected so archives written before a column
	// existed (connection_id, #699 query_text/query_hash, commit_ts_us)
	// restore with NULLs instead of being locked out — the same tolerance
	// parquetquery applies when reading them. The SELECT stays BY NAME, so
	// positional misalignment is impossible either way; a missing CORE
	// column still fails loud at the MySQL insert (NOT NULL).
	present, err := parquetColumns(ctx, duck, pathLit)
	if err != nil {
		return 0, fmt.Errorf("inspect archive %s: %w", localPath, err)
	}
	sel := make([]string, len(BinlogEventColumns))
	quoted := make([]string, len(BinlogEventColumns))
	for i, c := range BinlogEventColumns {
		if present[c.Name] {
			sel[i] = c.Name
		} else {
			sel[i] = "NULL AS " + c.Name
		}
		quoted[i] = "`" + c.Name + "`"
	}
	rows, err := duck.QueryContext(ctx,
		"SELECT "+strings.Join(sel, ", ")+" FROM parquet_scan("+pathLit+")")
	if err != nil {
		return 0, fmt.Errorf("scan archive %s: %w", localPath, err)
	}
	defer rows.Close()

	insertPrefix := "INSERT INTO binlog_events (" + strings.Join(quoted, ", ") + ") VALUES "
	tuple := "(" + strings.TrimSuffix(strings.Repeat("?,", len(quoted)), ",") + ")"

	// Flush on row count OR approximate payload size: binlog_events rows
	// carry full before/after JSON images, so a count-only batch of fat rows
	// can exceed a stock 64M max_allowed_packet (the #652 failure class) —
	// 16MiB leaves the same headroom drill uses.
	const maxBatchBytes = 16 << 20
	var total int64
	var args []any
	var tuples int
	var batchBytes int
	flush := func() error {
		if tuples == 0 {
			return nil
		}
		stmt := insertPrefix + strings.TrimSuffix(strings.Repeat(tuple+",", tuples), ",")
		if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("insert batch into binlog_events: %w", err)
		}
		total += int64(tuples)
		args, tuples, batchBytes = args[:0], 0, 0
		return nil
	}

	scan := make([]any, len(quoted))
	ptrs := make([]any, len(quoted))
	for i := range scan {
		ptrs[i] = &scan[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return total, fmt.Errorf("scan archived row: %w", err)
		}
		for _, v := range scan {
			args = append(args, v)
			switch t := v.(type) {
			case string:
				batchBytes += len(t)
			case []byte:
				batchBytes += len(t)
			default:
				batchBytes += 16
			}
		}
		tuples++
		if tuples >= batchSize || batchBytes >= maxBatchBytes {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return total, fmt.Errorf("read archive %s: %w", localPath, err)
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, nil
}

// parquetColumns lists the column names present in a Parquet file.
func parquetColumns(ctx context.Context, duck *sql.DB, pathLit string) (map[string]bool, error) {
	rows, err := duck.QueryContext(ctx, "SELECT * FROM parquet_scan("+pathLit+") LIMIT 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[n] = true
	}
	return out, nil
}
