package icebergexport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

// lookupKey returns the key under which row holds name, exact first and then
// case-folded (the baseline's Parquet names and the CREATE TABLE spelling
// can differ by case), or false when the row has neither.
func lookupKey(row map[string]any, name string) (string, bool) {
	if _, ok := row[name]; ok {
		return name, true
	}
	for k := range row {
		if strings.EqualFold(k, name) {
			return k, true
		}
	}
	return "", false
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
		var s string
		var err error
		if c.isJSON() {
			s, err = jsonText(v)
		} else {
			s, err = toString(v)
		}
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
		// A JSON value in a column whose MySQL type is not known to be JSON
		// (a schema snapshot without data_type): the canonical rendering,
		// so it at least agrees with what a typed column would write.
		return jsonText(t)
	}
	if n, err := toInt64(v); err == nil {
		return strconv.FormatInt(n, 10), nil
	}
	return "", fmt.Errorf("cannot read %T as text", v)
}

// jsonText renders one JSON column value as the export's canonical text.
//
// The two sources hand a JSON column over in different shapes, and both
// leave here as the ONE rendering encodeJSON produces, so a reader that
// compares the column as text (GROUP BY, DISTINCT, a diff between two
// exports) sees one value where the source has one:
//
//   - the baseline holds MySQL's own rendering of the document (keys in
//     MySQL's order, a space after every comma and colon); writeBaselineRows
//     has already parsed and re-encoded it into a json.RawMessage;
//   - a row image carries a document (an object or an array) decoded into Go
//     values (maps, slices, json.Number, bool), because the indexer embeds
//     only JSON containers (#736);
//   - a row image carries a top-level SCALAR (`"abc"`, `42`, `true`, `null`)
//     as the TEXT go-mysql rendered it, quotes included: the indexer stored
//     it base64 and the epoch decoder returned that text. A Go string is
//     therefore JSON text to parse, not a string scalar to quote. Text that
//     does not parse is the pre-#736 shape, a string scalar stored bare, and
//     only then is it quoted (a bare legacy string that happens to parse,
//     such as "123", renders as the number; that index lost the distinction
//     when it stored it).
//
// Anything else (bytes, times, structs) is an error: encoding/json would
// render it without complaint, as base64 or as an object, and a JSON column
// silently holding that is the corruption a reader cannot detect.
func jsonText(v any) (string, error) {
	switch t := v.(type) {
	case json.RawMessage:
		return string(t), nil
	case string:
		if raw, err := canonicalJSONText(t); err == nil {
			return string(raw), nil
		}
		return encodeJSONString(t)
	case map[string]any, []any, json.Number, bool:
		return encodeJSONString(t)
	}
	// A float64 is refused with the rest: neither source produces one (both
	// row decoders keep numbers as json.Number), and rendering it would round
	// a 20-digit integer that the baseline wrote exactly.
	return "", fmt.Errorf("cannot render %T as JSON", v)
}

func encodeJSONString(v any) (string, error) {
	bs, err := encodeJSON(v)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

// canonicalJSONText parses one JSON document (MySQL's rendering from the
// baseline, or go-mysql's rendering of a scalar from a row image) and
// re-emits it through encodeJSON. Numbers keep their literal text
// (UseNumber), so `1.50` stays `1.50` and a 20-digit integer is not rounded
// through float64. A JSON null re-emits as `null`; it is a value, and SQL
// NULL never reaches here. Text that is not exactly one JSON document is an
// error for the caller to attribute.
func canonicalJSONText(text string) (json.RawMessage, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("not JSON: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("not one JSON document: trailing data after %q", truncate(text, 80))
	}
	return encodeJSON(v)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// encodeJSON is the export's one JSON rendering: encoding/json with keys
// sorted, no whitespace, and `<`, `>`, `&` left as they are (the HTML
// escaping encoding/json applies by default would make `"<x>"` read as
// `"<x>"`, which is the same value and a different text).
func encodeJSON(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return json.RawMessage(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
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
