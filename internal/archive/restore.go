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
	if batchSize <= 0 {
		batchSize = 5000
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, fmt.Errorf("open DuckDB: %w", err)
	}
	defer duck.Close()

	cols := make([]string, len(BinlogEventColumns))
	quoted := make([]string, len(BinlogEventColumns))
	for i, c := range BinlogEventColumns {
		cols[i] = c.Name
		quoted[i] = "`" + c.Name + "`"
	}
	// The column list is read EXPLICITLY (not SELECT *) so an older archive
	// missing later columns fails loud here rather than loading misaligned.
	rows, err := duck.QueryContext(ctx,
		"SELECT "+strings.Join(cols, ", ")+" FROM parquet_scan('"+strings.ReplaceAll(localPath, "'", "''")+"')")
	if err != nil {
		return 0, fmt.Errorf("scan archive %s: %w", localPath, err)
	}
	defer rows.Close()

	insertPrefix := "INSERT INTO binlog_events (" + strings.Join(quoted, ", ") + ") VALUES "
	tuple := "(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"

	var total int64
	var args []any
	var tuples int
	flush := func() error {
		if tuples == 0 {
			return nil
		}
		stmt := insertPrefix + strings.TrimSuffix(strings.Repeat(tuple+",", tuples), ",")
		if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("insert batch into binlog_events: %w", err)
		}
		total += int64(tuples)
		args, tuples = args[:0], 0
		return nil
	}

	scan := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range scan {
		ptrs[i] = &scan[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return total, fmt.Errorf("scan archived row: %w", err)
		}
		for _, v := range scan {
			args = append(args, v)
		}
		tuples++
		if tuples >= batchSize {
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
