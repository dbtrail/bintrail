// Package archive writes a binlog_events partition to a Parquet file before
// it is dropped. The output uses the same column schema as baseline Parquet
// files so the two datasets can be joined for full audit reconstruction.
package archive

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/bintrail/bintrail/internal/baseline"
)

// binlogEventColumns defines the 13 non-generated binlog_events columns in
// MySQL table order (pk_hash is a stored generated column and is omitted).
var binlogEventColumns = []baseline.Column{
	{Name: "event_id", MySQLType: "bigint", ParquetType: baseline.MysqlToParquetNode("bigint")},
	{Name: "binlog_file", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "start_pos", MySQLType: "bigint", ParquetType: baseline.MysqlToParquetNode("bigint")},
	{Name: "end_pos", MySQLType: "bigint", ParquetType: baseline.MysqlToParquetNode("bigint")},
	{Name: "event_timestamp", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	{Name: "gtid", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "schema_name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "table_name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "event_type", MySQLType: "tinyint", ParquetType: baseline.MysqlToParquetNode("tinyint")},
	{Name: "pk_values", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "changed_columns", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "row_before", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "row_after", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
}

// ArchivePartition writes all rows from the named partition of binlog_events
// to a Parquet file at outputPath. The db must have been opened with
// parseTime=true (i.e. via config.Connect) so DATETIME columns scan as
// time.Time. Returns the number of rows written.
//
// On error the partial output file is removed before returning.
func ArchivePartition(ctx context.Context, db *sql.DB, dbName, partition, outputPath, compression string) (int64, error) {
	cfg := baseline.WriterConfig{
		Compression:  compression,
		RowGroupSize: 500_000,
		Metadata: map[string]string{
			"bintrail.archive.partition": partition,
			"bintrail.archive.timestamp": time.Now().UTC().Format(time.RFC3339),
			"bintrail.archive.version":   "1.0.0",
		},
	}

	w, err := baseline.NewWriter(outputPath, binlogEventColumns, cfg)
	if err != nil {
		return 0, fmt.Errorf("create parquet writer: %w", err)
	}

	var closed bool
	defer func() {
		if !closed {
			w.Close()             //nolint
			os.Remove(outputPath) //nolint
		}
	}()

	q := fmt.Sprintf(
		"SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp,"+
			" gtid, schema_name, table_name, event_type, pk_values,"+
			" changed_columns, row_before, row_after"+
			" FROM `%s`.`binlog_events` PARTITION (`%s`) ORDER BY event_id",
		dbName, partition,
	)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("query partition %s: %w", partition, err)
	}
	defer rows.Close()

	var rowCount int64
	for rows.Next() {
		var (
			eventID        uint64
			binlogFile     string
			startPos       uint64
			endPos         uint64
			eventTimestamp time.Time
			gtid           sql.NullString
			schemaName     string
			tableName      string
			eventType      uint8
			pkValues       string
			changedColumns []byte // nil = NULL
			rowBefore      []byte // nil = NULL
			rowAfter       []byte // nil = NULL
		)
		if err := rows.Scan(
			&eventID, &binlogFile, &startPos, &endPos, &eventTimestamp,
			&gtid, &schemaName, &tableName, &eventType, &pkValues,
			&changedColumns, &rowBefore, &rowAfter,
		); err != nil {
			return rowCount, fmt.Errorf("scan row: %w", err)
		}

		values := []string{
			strconv.FormatUint(eventID, 10),
			binlogFile,
			strconv.FormatUint(startPos, 10),
			strconv.FormatUint(endPos, 10),
			eventTimestamp.UTC().Format("2006-01-02 15:04:05"),
			gtid.String,
			schemaName,
			tableName,
			strconv.FormatUint(uint64(eventType), 10),
			pkValues,
			string(changedColumns),
			string(rowBefore),
			string(rowAfter),
		}
		nulls := []bool{
			false, false, false, false, false,
			!gtid.Valid,
			false, false, false, false,
			changedColumns == nil,
			rowBefore == nil,
			rowAfter == nil,
		}

		if err := w.WriteRow(values, nulls); err != nil {
			return rowCount, fmt.Errorf("write row: %w", err)
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return rowCount, fmt.Errorf("iterate rows: %w", err)
	}

	closed = true
	if err := w.Close(); err != nil {
		os.Remove(outputPath) //nolint
		return rowCount, fmt.Errorf("close writer: %w", err)
	}
	return rowCount, nil
}
