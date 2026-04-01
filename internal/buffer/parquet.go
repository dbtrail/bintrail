package buffer

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/dbtrail/bintrail/internal/archive"
	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/query"
)

const tsFormat = "2006-01-02 15:04:05"

// WriteParquet writes result rows to a Parquet file at outputPath using the
// same 14-column schema as archive.ArchivePartition. The output files are
// compatible with parquetquery.Fetch for querying.
func WriteParquet(rows []query.ResultRow, outputPath, compression string) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	cfg := baseline.WriterConfig{
		Compression:  compression,
		RowGroupSize: 500_000,
		Metadata: map[string]string{
			"bintrail.buffer.timestamp": time.Now().UTC().Format(time.RFC3339),
			"bintrail.buffer.version":   "1.0.0",
		},
	}

	w, err := baseline.NewWriter(outputPath, archive.BinlogEventColumns, cfg)
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

	var count int64
	for i := range rows {
		r := &rows[i]
		values, nulls := rowToParquet(r)
		if err := w.WriteRow(values, nulls); err != nil {
			return count, fmt.Errorf("write row: %w", err)
		}
		count++
	}

	closed = true
	if err := w.Close(); err != nil {
		os.Remove(outputPath) //nolint
		return count, fmt.Errorf("close writer: %w", err)
	}
	return count, nil
}

// rowToParquet converts a ResultRow to the string values and null flags
// expected by baseline.Writer.WriteRow. Column order matches
// archive.BinlogEventColumns (14 columns).
func rowToParquet(r *query.ResultRow) ([]string, []bool) {
	gtidStr := ""
	gtidNull := true
	if r.GTID != nil {
		gtidStr = *r.GTID
		gtidNull = false
	}

	changedCols, changedNull := marshalJSONField(r.ChangedColumns)
	rowBefore, beforeNull := marshalJSONField(r.RowBefore)
	rowAfter, afterNull := marshalJSONField(r.RowAfter)

	values := []string{
		strconv.FormatUint(r.EventID, 10),
		r.BinlogFile,
		strconv.FormatUint(r.StartPos, 10),
		strconv.FormatUint(r.EndPos, 10),
		r.EventTimestamp.UTC().Format(tsFormat),
		gtidStr,
		r.SchemaName,
		r.TableName,
		strconv.FormatUint(uint64(r.EventType), 10),
		r.PKValues,
		changedCols,
		rowBefore,
		rowAfter,
		strconv.FormatUint(uint64(r.SchemaVersion), 10),
	}

	nulls := []bool{
		false, false, false, false, false,
		gtidNull,
		false, false, false, false,
		changedNull,
		beforeNull,
		afterNull,
		false,
	}

	return values, nulls
}

// marshalJSONField marshals v to JSON. Returns ("", true) when v is nil.
func marshalJSONField(v any) (string, bool) {
	if v == nil {
		return "", true
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", true
	}
	return string(b), false
}
