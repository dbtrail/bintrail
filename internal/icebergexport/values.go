package icebergexport

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// rowAppender fills one Arrow record batch column by column from row maps.
//
// The two row sources it serves have different Go representations for the
// same MySQL column (see columnKind), so every append goes through the
// per-kind conversion below rather than a type switch on the builder alone.
// A value that cannot be converted is an error, never a silent NULL: a NULL
// written where a value was is the one corruption a reader cannot detect.
type rowAppender struct {
	cols []column
	b    *array.RecordBuilder
	n    int
}

func newRowAppender(mem memory.Allocator, schema *arrow.Schema, cols []column) (*rowAppender, error) {
	if len(schema.Fields()) != len(cols) {
		return nil, fmt.Errorf("arrow schema has %d fields, export has %d columns", len(schema.Fields()), len(cols))
	}
	// Each column's kind must match the Arrow field it will be appended into;
	// appendValue asserts the builder type, and a mismatch there would be a
	// panic in the middle of a write instead of a refusal here.
	for i, c := range cols {
		if name := schema.Field(i).Name; !strings.EqualFold(name, c.Name) {
			return nil, fmt.Errorf("column %d: the Iceberg table has %q where the export has %q; the table's schema is not the one this export produces", i+1, name, c.Name)
		}
		if got, want := schema.Field(i).Type.ID(), arrowTypeID(c.Kind); got != want {
			return nil, fmt.Errorf("column %q: the Iceberg table stores %s but the export would write %s; the table's schema is not the one this export produces", c.Name, got, want)
		}
	}
	return &rowAppender{cols: cols, b: array.NewRecordBuilder(mem, schema)}, nil
}

// arrowTypeID is the Arrow type each kind is written through, the same
// mapping table.SchemaToArrowSchema applies to icebergType.
func arrowTypeID(k kind) arrow.Type {
	switch k {
	case kindInt32:
		return arrow.INT32
	case kindInt64:
		return arrow.INT64
	case kindFloat32:
		return arrow.FLOAT32
	case kindFloat64:
		return arrow.FLOAT64
	case kindDecimal:
		return arrow.DECIMAL128
	case kindTimestamp:
		return arrow.TIMESTAMP
	case kindDate:
		return arrow.DATE32
	case kindBinary:
		return arrow.BINARY
	case kindString:
		return arrow.STRING
	}
	return arrow.NULL
}

// append adds one row. Every column must be present in the map: row images
// carry every column of the snapshot they were captured under, NULLs
// included, so an absent key means the event was captured under a schema
// that did not have the column, and writing NULL there would put a value the
// source never held into a column that may not even allow it. Only the
// spelling may differ, by case.
func (a *rowAppender) append(row map[string]any) error {
	for i, c := range a.cols {
		v, ok := row[c.Name]
		if !ok {
			if v, ok = lookupFold(row, c.Name); !ok {
				return fmt.Errorf("column %q is absent from the row image (the event was captured under a schema without it); reload the table from a fresh baseline", c.Name)
			}
		}
		if err := appendValue(a.b.Field(i), c, v); err != nil {
			return fmt.Errorf("column %q: %w", c.Name, err)
		}
	}
	a.n++
	return nil
}

// lookupFold finds a key by case-insensitive match, for row images whose
// column spelling differs from the CREATE TABLE's only in case.
func lookupFold(row map[string]any, name string) (any, bool) {
	for k, v := range row {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return nil, false
}

// flush returns the batch built so far and resets the appender.
func (a *rowAppender) flush() arrow.RecordBatch {
	rec := a.b.NewRecordBatch()
	a.n = 0
	return rec
}

func (a *rowAppender) release() { a.b.Release() }

// appendValue converts v to the column's kind and appends it.
func appendValue(b array.Builder, c column, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch c.Kind {
	case kindInt32:
		n, err := toInt64(v)
		if err != nil {
			return err
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return fmt.Errorf("value %d does not fit a 32-bit integer", n)
		}
		b.(*array.Int32Builder).Append(int32(n))
	case kindInt64:
		n, err := toInt64(v)
		if err != nil {
			return err
		}
		b.(*array.Int64Builder).Append(n)
	case kindFloat32:
		f, err := toFloat64(v)
		if err != nil {
			return err
		}
		b.(*array.Float32Builder).Append(float32(f))
	case kindFloat64:
		f, err := toFloat64(v)
		if err != nil {
			return err
		}
		b.(*array.Float64Builder).Append(f)
	case kindDecimal:
		s, err := toDecimalString(v)
		if err != nil {
			return err
		}
		if err := b.(*array.Decimal128Builder).AppendValueFromString(s); err != nil {
			return fmt.Errorf("decimal %q: %w", s, err)
		}
	case kindTimestamp:
		t, isNull, err := toTime(v)
		if err != nil {
			return err
		}
		if isNull {
			b.AppendNull()
			return nil
		}
		b.(*array.TimestampBuilder).AppendTime(t)
	case kindDate:
		t, isNull, err := toTime(v)
		if err != nil {
			return err
		}
		if isNull {
			b.AppendNull()
			return nil
		}
		b.(*array.Date32Builder).Append(arrow.Date32FromTime(t))
	case kindBinary:
		bs, err := toBytes(v)
		if err != nil {
			return err
		}
		b.(*array.BinaryBuilder).Append(bs)
	case kindString:
		s, err := toString(v)
		if err != nil {
			return err
		}
		b.(*array.StringBuilder).Append(s)
	default:
		return fmt.Errorf("column has no kind")
	}
	return nil
}

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int8:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case uint8:
		return int64(t), nil
	case uint16:
		return int64(t), nil
	case uint32:
		return int64(t), nil
	case uint:
		if uint64(t) > math.MaxInt64 {
			return 0, fmt.Errorf("value %d does not fit a signed 64-bit integer", t)
		}
		return int64(t), nil
	case uint64:
		if t > math.MaxInt64 {
			return 0, fmt.Errorf("value %d does not fit a signed 64-bit integer", t)
		}
		return int64(t), nil
	case float32:
		return wholeFloat(float64(t))
	case float64:
		return wholeFloat(t)
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return n, nil
		}
		f, err := t.Float64()
		if err != nil {
			return 0, fmt.Errorf("%q is not an integer", string(t))
		}
		return wholeFloat(f)
	case []byte:
		return toInt64(string(t))
	case string:
		s := strings.TrimSpace(t)
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("%q is not an integer", t)
		}
		return wholeFloat(f)
	}
	return 0, fmt.Errorf("cannot read %T as an integer", v)
}

func wholeFloat(f float64) (int64, error) {
	if f != math.Trunc(f) || math.IsInf(f, 0) || math.IsNaN(f) {
		return 0, fmt.Errorf("%v is not a whole number", f)
	}
	return int64(f), nil
}

func toFloat64(v any) (float64, error) {
	switch t := v.(type) {
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	case json.Number:
		return t.Float64()
	case []byte:
		return strconv.ParseFloat(strings.TrimSpace(string(t)), 64)
	case string:
		return strconv.ParseFloat(strings.TrimSpace(t), 64)
	}
	n, err := toInt64(v)
	if err != nil {
		return 0, fmt.Errorf("cannot read %T as a number", v)
	}
	return float64(n), nil
}

// toDecimalString renders v as the decimal text Arrow's builder parses and
// rescales. A float is rendered with the shortest round-trip form, which is
// what the baseline writer would have stored for the same value.
func toDecimalString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t), nil
	case []byte:
		return strings.TrimSpace(string(t)), nil
	case json.Number:
		return string(t), nil
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case uint64:
		return strconv.FormatUint(t, 10), nil
	case uint:
		return strconv.FormatUint(uint64(t), 10), nil
	}
	n, err := toInt64(v)
	if err != nil {
		return "", fmt.Errorf("cannot read %T as a decimal", v)
	}
	return strconv.FormatInt(n, 10), nil
}

// timeLayouts are the spellings a DATETIME/TIMESTAMP/DATE value arrives in:
// the row-event JSON ("2006-01-02 15:04:05[.ffffff]", rendered in UTC by the
// capture), a rendered baseline value, and RFC 3339 for good measure.
var timeLayouts = []string{
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999Z07:00",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05.999999",
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// toTime reads a time value as UTC wall-clock. MySQL's zero dates have no
// Iceberg equivalent and become NULL, the same choice the baseline writer
// makes when it converts a dump.
func toTime(v any) (t time.Time, isNull bool, err error) {
	switch x := v.(type) {
	case time.Time:
		return x.UTC(), false, nil
	case []byte:
		return toTime(string(x))
	case string:
		s := strings.TrimSpace(x)
		if strings.HasPrefix(s, "0000-00-00") {
			return time.Time{}, true, nil // the documented zero-date mapping, and only that
		}
		for _, layout := range timeLayouts {
			if parsed, perr := time.ParseInLocation(layout, s, time.UTC); perr == nil {
				return parsed.UTC(), false, nil
			}
		}
		return time.Time{}, false, fmt.Errorf("%q is not a date or time", x)
	}
	return time.Time{}, false, fmt.Errorf("cannot read %T as a time", v)
}

func toString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case json.Number:
		return string(t), nil
	case bool:
		// A JSON row image can carry a literal bool where MySQL has none; the
		// baseline side renders the same column as its integer.
		if t {
			return "1", nil
		}
		return "0", nil
	case time.Time:
		return t.UTC().Format("2006-01-02 15:04:05.999999"), nil
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case map[string]any, []any, json.RawMessage:
		// A JSON column: the row image decodes it into a value; re-encode it
		// as the text the baseline holds.
		bs, err := json.Marshal(t)
		if err != nil {
			return "", err
		}
		return string(bs), nil
	}
	if n, err := toInt64(v); err == nil {
		return strconv.FormatInt(n, 10), nil
	}
	return "", fmt.Errorf("cannot read %T as text", v)
}

func toBytes(v any) ([]byte, error) {
	switch t := v.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	case json.Number:
		return []byte(string(t)), nil
	}
	return nil, fmt.Errorf("cannot read %T as bytes", v)
}
