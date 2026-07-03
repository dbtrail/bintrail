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

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// BinlogEventColumns defines the 15 non-generated binlog_events columns in
// MySQL table order (pk_hash is a stored generated column and is omitted).
// Exported for reuse by the buffer package when writing Parquet files.
//
// event_id / start_pos / end_pos are BIGINT UNSIGNED in MySQL but are scanned as
// sql.NullInt64 here (ArchivePartition) and never exceed int64 in practice —
// AUTO_INCREMENT and binlog byte offsets stay well under 2^63 — so the signed
// Int64 column they map to via MysqlToParquetNode is lossless. connection_id is
// the one column that genuinely needs the unsigned widening below: it is INT
// UNSIGNED (a CONNECTION_ID() can exceed int32's 2147483647) and is written via
// FormatUint by the buffer path, so a signed Int(32) column would reject it.
var BinlogEventColumns = []baseline.Column{
	{Name: "event_id", MySQLType: "bigint", ParquetType: baseline.MysqlToParquetNode("bigint")},
	{Name: "binlog_file", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "start_pos", MySQLType: "bigint", ParquetType: baseline.MysqlToParquetNode("bigint")},
	{Name: "end_pos", MySQLType: "bigint", ParquetType: baseline.MysqlToParquetNode("bigint")},
	{Name: "event_timestamp", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	{Name: "gtid", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "connection_id", MySQLType: "int", Unsigned: true, ParquetType: baseline.MysqlToParquetNode2("int", true)},
	{Name: "schema_name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "table_name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "event_type", MySQLType: "tinyint", ParquetType: baseline.MysqlToParquetNode("tinyint")},
	{Name: "pk_values", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "changed_columns", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "row_before", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "row_after", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "schema_version", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	{Name: "query_text", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	{Name: "query_hash", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
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

	w, err := baseline.NewWriter(outputPath, BinlogEventColumns, cfg)
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
			" gtid, connection_id, schema_name, table_name, event_type, pk_values,"+
			" changed_columns, row_before, row_after, schema_version, query_text, query_hash"+
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
		// Every NOT NULL column is scanned defensively and its
		// Valid bit is propagated into the nulls[] slice so the Parquet
		// writer preserves true NULL. See dbtrail/bintrail#318 for the
		// production observation that "NOT NULL" cannot be trusted in
		// drifted/external-pipeline-fed indexes. event_id stays a bare
		// uint64 since AUTO_INCREMENT cannot return NULL.
		var (
			eventID        uint64
			binlogFile     sql.NullString
			startPos       sql.NullInt64
			endPos         sql.NullInt64
			eventTimestamp sql.NullTime
			gtid           sql.NullString
			connID         sql.NullInt64
			schemaName     sql.NullString
			tableName      sql.NullString
			eventType      sql.NullInt32
			pkValues       sql.NullString
			changedColumns []byte // nil = NULL
			rowBefore      []byte // nil = NULL
			rowAfter       []byte // nil = NULL
			schemaVersion  sql.NullInt32
			queryText      sql.NullString
			queryHash      sql.NullString
		)
		if err := rows.Scan(
			&eventID, &binlogFile, &startPos, &endPos, &eventTimestamp,
			&gtid, &connID, &schemaName, &tableName, &eventType, &pkValues,
			&changedColumns, &rowBefore, &rowAfter, &schemaVersion, &queryText, &queryHash,
		); err != nil {
			return rowCount, fmt.Errorf("scan row: %w", err)
		}

		connIDStr := ""
		if connID.Valid {
			connIDStr = strconv.FormatInt(connID.Int64, 10)
		}
		eventTimestampStr := ""
		if eventTimestamp.Valid {
			eventTimestampStr = eventTimestamp.Time.UTC().Format("2006-01-02 15:04:05")
		}

		values := []string{
			strconv.FormatUint(eventID, 10),
			binlogFile.String,
			strconv.FormatInt(startPos.Int64, 10),
			strconv.FormatInt(endPos.Int64, 10),
			eventTimestampStr,
			gtid.String,
			connIDStr,
			schemaName.String,
			tableName.String,
			strconv.FormatInt(int64(eventType.Int32), 10),
			pkValues.String,
			string(changedColumns),
			string(rowBefore),
			string(rowAfter),
			strconv.FormatInt(int64(schemaVersion.Int32), 10),
			queryText.String,
			queryHash.String,
		}
		nulls := []bool{
			false, // event_id (AUTO_INCREMENT, cannot be NULL)
			!binlogFile.Valid,
			!startPos.Valid,
			!endPos.Valid,
			!eventTimestamp.Valid,
			!gtid.Valid,
			!connID.Valid,
			!schemaName.Valid,
			!tableName.Valid,
			!eventType.Valid,
			!pkValues.Valid,
			changedColumns == nil,
			rowBefore == nil,
			rowAfter == nil,
			!schemaVersion.Valid,
			!queryText.Valid,
			!queryHash.Valid,
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
