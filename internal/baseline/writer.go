package baseline

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// errZeroDate marks MySQL's all-zero DATE/DATETIME/TIMESTAMP pseudo-NULL
// sentinel (`0000-00-00`, `0000-00-00 00:00:00`, fractional `.000000` variant).
// These are LEGAL legacy MySQL values (a column with DEFAULT '0000-00-00
// 00:00:00' carries them in every row) that Go's time parser rejects ("month
// out of range"). They are representable as NULL — MySQL itself treats the zero
// date as its pseudo-NULL — so WriteRow maps them to a deliberate NULL plus a
// once-per-column warning, rather than aborting the whole baseline run. This is
// distinct from a genuinely unrepresentable value (non-numeric garbage in an
// integer column, an out-of-range literal), which would be silently corrupted
// by a NULL and must still fail loud.
var errZeroDate = errors.New("zero date sentinel (mydumper pseudo-NULL)")

// WriterConfig carries Parquet writer options.
type WriterConfig struct {
	Compression  string            // "zstd", "snappy", "gzip", "none"
	RowGroupSize int               // rows per row group
	Metadata     map[string]string // key-value pairs written to Parquet file metadata
}

// Writer wraps a parquet.Writer for a single table's output file.
type Writer struct {
	pw   *parquet.Writer
	file *os.File
	// parquetCols holds columns sorted alphabetically (Parquet column order).
	parquetCols []Column
	// mysqlOrder[parquetIdx] = original MySQL column index in the source data.
	mysqlOrder []int
	// zeroDateWarned tracks parquet column indexes already warned about a
	// zero-date sentinel, so a legacy table with DEFAULT '0000-00-00' (the
	// sentinel in every row) emits one warning per column, not per row.
	zeroDateWarned map[int]bool
}

// NewWriter creates a new Parquet writer for the given table.
// cols is the list of columns in original MySQL order.
// The output file is created at path; parent directories are created as needed.
func NewWriter(path string, cols []Column, cfg WriterConfig) (*Writer, error) {
	if err := ValidateCodec(cfg.Compression); err != nil {
		return nil, err
	}
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

	// Build parquet.Group from sorted columns. A RawText column (PostgreSQL
	// baseline, #593) is unconditionally an optional string — its values are
	// stored verbatim, so the MySQL type mapping in ParquetType is bypassed.
	group := make(parquet.Group, len(parquetCols))
	for _, c := range parquetCols {
		if c.RawText {
			group[c.Name] = parquet.Optional(parquet.String())
			continue
		}
		group[c.Name] = c.ParquetType
	}
	schema := parquet.NewSchema("row", group)

	// Build writer options.
	opts := []parquet.WriterOption{
		schema,
		parquet.MaxRowsPerRowGroup(int64(cfg.RowGroupSize)),
	}
	for k, v := range cfg.Metadata {
		opts = append(opts, parquet.KeyValueMetadata(k, v))
	}
	if codec := resolveCodec(cfg.Compression); codec != nil {
		opts = append(opts, parquet.Compression(codec))
	}

	pw := parquet.NewWriter(f, opts...)
	return &Writer{
		pw:             pw,
		file:           f,
		parquetCols:    parquetCols,
		mysqlOrder:     mysqlOrder,
		zeroDateWarned: make(map[int]bool),
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
			switch {
			case errors.Is(err, errZeroDate):
				// MySQL's all-zero date/datetime is a LEGAL pseudo-NULL that the
				// Go time parser rejects. It is representable as NULL (that IS
				// its semantics), so map it to a deliberate NULL and warn once
				// per column — visible, not a silent drop. Aborting here would
				// kill the baseline of any legacy table carrying DEFAULT
				// '0000-00-00 00:00:00' (issue #506 review carve-out).
				if !w.zeroDateWarned[parquetIdx] {
					w.zeroDateWarned[parquetIdx] = true
					slog.Warn("baseline: zero date mapped to NULL",
						"column", col.Name, "type", col.MySQLType, "value", raw)
				}
				v = parquet.NullValue().Level(0, 0, parquetIdx)
			case err != nil:
				// Fail loud: silently coercing an unconvertible value to NULL
				// publishes a lossy baseline (issue #503 item-3). This is a
				// data-recovery tool — abort the run rather than hand back a
				// snapshot that quietly dropped a value. Context lets the
				// operator locate the offending row. (A value that would be
				// silently CORRUPTED by NULL — e.g. non-numeric garbage in an
				// int column, an out-of-range literal — still aborts; only the
				// zero-date pseudo-NULL above does not. In-range UNSIGNED values
				// no longer reach here: they are widened by convertValue.)
				return fmt.Errorf("baseline: column %q (%s) value %q: %w",
					col.Name, col.MySQLType, raw, err)
			default:
				v = converted.Level(0, 1, parquetIdx)
			}
		}
		row[parquetIdx] = v
	}
	_, err := w.pw.WriteRows([]parquet.Row{row})
	return err
}

// SetMetadata sets a key/value pair in the Parquet file metadata. It may be
// called after rows are written and before Close — the metadata is serialized
// into the file footer at Close — so it can carry values only known once all
// rows have been seen (e.g. the row count and content digest, #633).
func (w *Writer) SetMetadata(key, value string) {
	w.pw.SetKeyValueMetadata(key, value)
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
	if col.RawText {
		// PostgreSQL baseline (#593): the value is already the source's text
		// rendering and is stored verbatim — no parsing, no type conversion.
		return parquet.ByteArrayValue([]byte(raw)), nil
	}
	switch col.MySQLType {
	case "tinyint", "smallint", "mediumint":
		// These fit int32 whether signed or unsigned (max 16777215 for
		// MEDIUMINT UNSIGNED), so a plain signed parse is lossless.
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int32Value(int32(n)), nil

	case "int", "integer":
		if col.Unsigned {
			// INT UNSIGNED reaches 4294967295, which overflows int32.
			// Parse as uint32 and widen into the INT64 column (issue #506).
			n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 32)
			if err != nil {
				return parquet.Value{}, err
			}
			return parquet.Int64Value(int64(n)), nil
		}
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int32Value(int32(n)), nil

	case "bigint":
		if col.Unsigned {
			// BIGINT UNSIGNED reaches 18446744073709551615, which overflows
			// int64. Parse as uint64 and store the bit pattern in the UINT64
			// column: int64(MaxUint64) round-trips back via uint64(v.Int64())
			// (issue #506).
			n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
			if err != nil {
				return parquet.Value{}, err
			}
			return parquet.Int64Value(int64(n)), nil
		}
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
		if isZeroDate(raw) {
			return parquet.Value{}, errZeroDate
		}
		us, err := parseDatetimeToMicros(raw)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int64Value(us), nil

	case "date":
		if isZeroDate(raw) {
			return parquet.Value{}, errZeroDate
		}
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

// isZeroDate reports whether raw is MySQL's all-zero date pseudo-NULL sentinel:
// `0000-00-00`, `0000-00-00 00:00:00`, and the `.000000` fractional variant. The
// `0000-00-00` prefix uniquely identifies the family (a real date never starts
// with a zero year), so one prefix check covers DATE, DATETIME, and TIMESTAMP.
// TIME is deliberately excluded — `00:00:00` is legal midnight, stored as a
// string, and round-trips fine; it is not a pseudo-NULL.
func isZeroDate(raw string) bool {
	return strings.HasPrefix(strings.TrimSpace(raw), "0000-00-00")
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

// ValidateCodec checks that the compression codec name is supported.
// Valid values: "zstd" (default), "snappy", "gzip", "none", or "".
func ValidateCodec(name string) error {
	switch strings.ToLower(name) {
	case "zstd", "", "snappy", "gzip", "none":
		return nil
	default:
		return fmt.Errorf("unsupported compression codec %q; valid values: zstd, snappy, gzip, none", name)
	}
}

// resolveCodec returns the compress.Codec for the given name, or nil for "none".
// Callers should validate with ValidateCodec first.
func resolveCodec(name string) compress.Codec {
	switch strings.ToLower(name) {
	case "zstd", "":
		return &zstd.Codec{}
	case "snappy":
		return &snappy.Codec{}
	case "gzip":
		return &gzip.Codec{}
	default:
		return nil
	}
}
