package baseline

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// WriterConfig carries Parquet writer options.
type WriterConfig struct {
	Compression  string // "zstd", "snappy", "gzip", "none"
	RowGroupSize int    // rows per row group
	// Metadata fields
	SnapshotTimestamp string
	SourceDatabase    string
	SourceTable       string
	MydumperFormat    string // "sql" or "tab"
	BintrailVersion   string
}

// Writer wraps a parquet.Writer for a single table's output file.
type Writer struct {
	pw         *parquet.Writer
	file       *os.File
	// parquetCols holds columns sorted alphabetically (Parquet column order).
	parquetCols []Column
	// mysqlOrder[parquetIdx] = original MySQL column index in the source data.
	mysqlOrder []int
}

// NewWriter creates a new Parquet writer for the given table.
// cols is the list of columns in original MySQL order.
// The output file is created at path; parent directories are created as needed.
func NewWriter(path string, cols []Column, cfg WriterConfig) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create parquet file: %w", err)
	}

	// Build column index mapping: MySQL order → Parquet (alphabetical) order.
	// parquet.Group sorts Fields() alphabetically, so we must track the mapping.
	parquetCols, mysqlOrder := sortColumnsForParquet(cols)

	// Build parquet.Group from sorted columns.
	group := make(parquet.Group, len(parquetCols))
	for _, c := range parquetCols {
		group[c.Name] = c.ParquetType
	}
	schema := parquet.NewSchema("row", group)

	// Build writer options.
	opts := []parquet.WriterOption{
		schema,
		parquet.MaxRowsPerRowGroup(int64(cfg.RowGroupSize)),
		parquet.KeyValueMetadata("bintrail.snapshot_timestamp", cfg.SnapshotTimestamp),
		parquet.KeyValueMetadata("bintrail.source_database", cfg.SourceDatabase),
		parquet.KeyValueMetadata("bintrail.source_table", cfg.SourceTable),
		parquet.KeyValueMetadata("bintrail.mydumper_format", cfg.MydumperFormat),
		parquet.KeyValueMetadata("bintrail.bintrail_version", cfg.BintrailVersion),
	}
	if codec := resolveCodec(cfg.Compression); codec != nil {
		opts = append(opts, parquet.Compression(codec))
	}

	pw := parquet.NewWriter(f, opts...)
	return &Writer{
		pw:          pw,
		file:        f,
		parquetCols: parquetCols,
		mysqlOrder:  mysqlOrder,
	}, nil
}

// WriteRow converts a row of string values (in MySQL column order) into a
// Parquet row and writes it to the file.
func (w *Writer) WriteRow(values []string, nulls []bool) error {
	row := make(parquet.Row, len(w.parquetCols))
	for parquetIdx, col := range w.parquetCols {
		mysqlIdx := w.mysqlOrder[parquetIdx]
		var v parquet.Value
		isNull := mysqlIdx >= len(nulls) || nulls[mysqlIdx]
		if isNull {
			v = parquet.NullValue().Level(0, 0, parquetIdx)
		} else {
			raw := ""
			if mysqlIdx < len(values) {
				raw = values[mysqlIdx]
			}
			converted, err := convertValue(col, raw)
			if err != nil {
				// On conversion error, store as NULL to avoid data loss.
				v = parquet.NullValue().Level(0, 0, parquetIdx)
			} else {
				v = converted.Level(0, 1, parquetIdx)
			}
		}
		row[parquetIdx] = v
	}
	_, err := w.pw.WriteRows([]parquet.Row{row})
	return err
}

// Close flushes and closes the Parquet writer and the underlying file.
func (w *Writer) Close() error {
	if err := w.pw.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return w.file.Close()
}

// sortColumnsForParquet returns (sorted []Column, mysqlOrder []int) where
// sorted is the alphabetically sorted column slice (matching parquet.Group order),
// and mysqlOrder[parquetIdx] = the original MySQL column index.
func sortColumnsForParquet(cols []Column) ([]Column, []int) {
	type indexed struct {
		col      Column
		mysqlIdx int
	}
	items := make([]indexed, len(cols))
	for i, c := range cols {
		items[i] = indexed{c, i}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].col.Name < items[j].col.Name
	})
	sorted := make([]Column, len(items))
	order := make([]int, len(items))
	for i, item := range items {
		sorted[i] = item.col
		order[i] = item.mysqlIdx
	}
	return sorted, order
}

// convertValue converts a string value to the appropriate parquet.Value for the
// column's MySQL type. Caller sets Level after.
func convertValue(col Column, raw string) (parquet.Value, error) {
	switch col.MySQLType {
	case "tinyint", "smallint", "mediumint", "int", "integer":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int32Value(int32(n)), nil

	case "bigint":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int64Value(n), nil

	case "float":
		f, err := strconv.ParseFloat(strings.TrimSpace(raw), 32)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.FloatValue(float32(f)), nil

	case "double", "real":
		f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.DoubleValue(f), nil

	case "datetime", "timestamp":
		us, err := parseDatetimeToMicros(raw)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int64Value(us), nil

	case "date":
		days, err := parseDateToDays(raw)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int32Value(int32(days)), nil

	case "year":
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int32Value(int32(n)), nil

	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bit":
		return parquet.ByteArrayValue([]byte(raw)), nil

	default:
		// String types and fallback.
		return parquet.ByteArrayValue([]byte(raw)), nil
	}
}

// parseDatetimeToMicros parses MySQL DATETIME/TIMESTAMP strings to microseconds
// since Unix epoch (UTC).
func parseDatetimeToMicros(s string) (int64, error) {
	s = strings.TrimSpace(s)
	// MySQL formats: "2006-01-02 15:04:05" or "2006-01-02 15:04:05.000000"
	var t time.Time
	var err error
	if strings.Contains(s, ".") {
		t, err = time.ParseInLocation("2006-01-02 15:04:05.999999", s, time.UTC)
	} else {
		t, err = time.ParseInLocation("2006-01-02 15:04:05", s, time.UTC)
	}
	if err != nil {
		return 0, fmt.Errorf("parse datetime %q: %w", s, err)
	}
	return t.UnixMicro(), nil
}

// parseDateToDays parses a MySQL DATE string to days since Unix epoch.
func parseDateToDays(s string) (int32, error) {
	s = strings.TrimSpace(s)
	t, err := time.ParseInLocation("2006-01-02", s, time.UTC)
	if err != nil {
		return 0, fmt.Errorf("parse date %q: %w", s, err)
	}
	days := int64(t.Unix()) / 86400
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, fmt.Errorf("date %q out of int32 range", s)
	}
	return int32(days), nil
}

// resolveCodec returns the compress.Codec for the given name, or nil for "none"/unknown.
func resolveCodec(name string) compress.Codec {
	switch strings.ToLower(name) {
	case "zstd", "":
		return &zstd.Codec{}
	case "snappy":
		return &snappy.Codec{}
	default:
		return nil
	}
}
