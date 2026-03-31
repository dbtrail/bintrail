package byos

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/storage"
)

// payloadColumns defines the Parquet schema for BYOS payload files.
// Column order here is the MySQL (input) order — baseline.Writer sorts
// them alphabetically for the Parquet file.
var payloadColumns = []baseline.Column{
	{Name: "pk_hash", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "pk_values", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "schema_name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "table_name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "event_type", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	{Name: "event_timestamp", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
	{Name: "row_before", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "row_after", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "changed_columns", MySQLType: "json", ParquetType: baseline.MysqlToParquetNode("json")},
	{Name: "schema_version", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
}

// PayloadWriter writes payload records to a storage backend as Parquet files,
// partitioned by server ID, schema.table, and date.
type PayloadWriter struct {
	backend  storage.Backend
	serverID string
}

// NewPayloadWriter creates a writer that uploads payload Parquet files to
// the given storage backend. serverID is the source MySQL server identifier
// used as the top-level partition key.
func NewPayloadWriter(backend storage.Backend, serverID string) *PayloadWriter {
	return &PayloadWriter{
		backend:  backend,
		serverID: serverID,
	}
}

// WriteRecords groups records by schema.table, writes each group to a
// temporary Parquet file, and uploads it to the storage backend using the
// partitioning scheme: {server_id}/{schema}.{table}/{date}/events_{nanos}.parquet
func (w *PayloadWriter) WriteRecords(ctx context.Context, records []PayloadRecord) error {
	// Group records by schema.table so each Parquet file contains a single table.
	groups := make(map[string][]PayloadRecord)
	for i := range records {
		key := records[i].SchemaName + "." + records[i].TableName
		groups[key] = append(groups[key], records[i])
	}

	for _, recs := range groups {
		if err := w.writeGroup(ctx, recs); err != nil {
			return err
		}
	}
	return nil
}

// writeGroup writes a slice of same-table records to a temp Parquet file
// and uploads it via the storage backend.
func (w *PayloadWriter) writeGroup(ctx context.Context, records []PayloadRecord) error {
	if len(records) == 0 {
		return nil
	}

	first := records[0]
	key := PayloadKey(w.serverID, first.SchemaName, first.TableName, first.EventTimestamp)

	tmp, err := os.CreateTemp("", "byos-payload-*.parquet")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)

	cfg := baseline.WriterConfig{
		Compression:  "zstd",
		RowGroupSize: 500_000,
		Metadata: map[string]string{
			"bintrail.byos.server_id": w.serverID,
			"bintrail.byos.schema":    first.SchemaName,
			"bintrail.byos.table":     first.TableName,
		},
	}
	pw, err := baseline.NewWriter(tmpPath, payloadColumns, cfg)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}

	for i := range records {
		values, nulls, err := marshalPayloadRow(&records[i])
		if err != nil {
			return fmt.Errorf("marshal payload row: %w", err)
		}
		if err := pw.WriteRow(values, nulls); err != nil {
			pw.Close() //nolint
			return fmt.Errorf("write row: %w", err)
		}
	}
	if err := pw.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("open temp file for upload: %w", err)
	}
	defer f.Close()

	if err := w.backend.Put(ctx, key, f); err != nil {
		return fmt.Errorf("upload payload: %w", err)
	}
	return nil
}

// PayloadKey builds the S3 object key for a payload Parquet file using the
// partitioning scheme: {server_id}/{schema}.{table}/{date}/events_{nanos}.parquet
func PayloadKey(serverID, schema, table string, ts time.Time) string {
	date := ts.UTC().Format("2006-01-02")
	nanos := ts.UnixNano()
	return fmt.Sprintf("%s/%s.%s/%s/events_%d.parquet", serverID, schema, table, date, nanos)
}

// marshalPayloadRow converts a PayloadRecord to the string values and null
// flags expected by baseline.Writer.WriteRow. Column order must match
// payloadColumns.
func marshalPayloadRow(r *PayloadRecord) ([]string, []bool, error) {
	rowBefore, err := marshalJSON(r.RowBefore)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal row_before: %w", err)
	}
	rowAfter, err := marshalJSON(r.RowAfter)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal row_after: %w", err)
	}
	changed, err := marshalJSON(r.ChangedColumns)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal changed_columns: %w", err)
	}

	values := []string{
		r.PKHash,
		r.PKValues,
		r.SchemaName,
		r.TableName,
		r.EventType,
		r.EventTimestamp.UTC().Format("2006-01-02 15:04:05"),
		string(rowBefore),
		string(rowAfter),
		string(changed),
		strconv.FormatUint(uint64(r.SchemaVersion), 10),
	}
	nulls := []bool{
		false, // pk_hash
		false, // pk_values
		false, // schema_name
		false, // table_name
		false, // event_type
		false, // event_timestamp
		r.RowBefore == nil,
		r.RowAfter == nil,
		r.ChangedColumns == nil,
		false, // schema_version
	}
	return values, nulls, nil
}

// marshalJSON encodes v to JSON, returning nil for nil input.
func marshalJSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
