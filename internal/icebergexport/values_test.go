package icebergexport

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// buildOne appends one value to a one-column batch and returns the column's
// string rendering (or "NULL").
func buildOne(t *testing.T, col baseline.Column, v any) (string, error) {
	t.Helper()
	cols, err := buildColumns([]baseline.Column{col}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := table.SchemaToArrowSchema(icebergSchema(cols), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	app, err := newRowAppender(memory.DefaultAllocator, sc, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer app.release()
	if err := app.append(map[string]any{col.Name: v}); err != nil {
		return "", err
	}
	rec := app.flush()
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Fatalf("rows = %d", rec.NumRows())
	}
	c := rec.Column(0)
	if c.IsNull(0) {
		return "NULL", nil
	}
	return render(c, 0), nil
}

// render prints one cell the way a reader would see the value: decimals at
// the column's scale, timestamps as naive UTC wall-clock, binary as hex.
// Arrow's own ValueStr is a debugging form (RFC 3339, base64, trimmed
// decimals) and would make the assertions about representation vacuous.
func render(c arrow.Array, i int) string {
	switch a := c.(type) {
	case *array.Decimal128:
		return a.Value(i).ToString(a.DataType().(*arrow.Decimal128Type).Scale)
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return a.Value(i).ToTime(unit).UTC().Format("2006-01-02 15:04:05.999999")
	case *array.Date32:
		return a.Value(i).ToTime().UTC().Format("2006-01-02")
	case *array.Binary:
		return strings.ToUpper(hex.EncodeToString(a.Value(i)))
	}
	return c.ValueStr(i)
}

func TestAppendValue_bothSourcesAgree(t *testing.T) {
	ts := time.Date(2026, 8, 28, 12, 34, 56, 0, time.UTC)
	cases := []struct {
		name string
		col  baseline.Column
		v    any
		want string
	}{
		// integers: DuckDB scan types vs JSON numbers vs text
		{"int32 from int32", baseline.Column{Name: "c", MySQLType: "int"}, int32(7), "7"},
		{"int32 from json.Number", baseline.Column{Name: "c", MySQLType: "int"}, json.Number("7"), "7"},
		{"int32 from text", baseline.Column{Name: "c", MySQLType: "int"}, "7", "7"},
		{"int64 from uint64 baseline of int unsigned", baseline.Column{Name: "c", MySQLType: "int", Unsigned: true}, int64(4000000000), "4000000000"},
		{"bigint unsigned from uint64", baseline.Column{Name: "c", MySQLType: "bigint", Unsigned: true}, uint64(18446744073709551615), "18446744073709551615"},
		{"bigint unsigned from json.Number", baseline.Column{Name: "c", MySQLType: "bigint", Unsigned: true}, json.Number("18446744073709551615"), "18446744073709551615"},
		{"bool image into int", baseline.Column{Name: "c", MySQLType: "tinyint"}, true, "1"},
		// decimal: text (baseline) vs json.Number (delta), rescaled to the column
		{"decimal from text", baseline.Column{Name: "c", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2}, "10.50", "10.50"},
		{"decimal from json.Number", baseline.Column{Name: "c", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2}, json.Number("10.5"), "10.50"},
		{"decimal from float", baseline.Column{Name: "c", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2}, 10.5, "10.50"},
		// time: time.Time (baseline via DuckDB) vs naive text (delta JSON)
		{"datetime from time.Time", baseline.Column{Name: "c", MySQLType: "datetime"}, ts, "2026-08-28 12:34:56"},
		{"datetime from naive text is UTC", baseline.Column{Name: "c", MySQLType: "datetime"}, "2026-08-28 12:34:56", "2026-08-28 12:34:56"},
		{"datetime with fraction", baseline.Column{Name: "c", MySQLType: "datetime"}, "2026-08-28 12:34:56.250000", "2026-08-28 12:34:56.25"},
		{"datetime zero date is NULL", baseline.Column{Name: "c", MySQLType: "datetime"}, "0000-00-00 00:00:00", "NULL"},
		{"date from text", baseline.Column{Name: "c", MySQLType: "date"}, "2026-08-28", "2026-08-28"},
		{"date from time.Time", baseline.Column{Name: "c", MySQLType: "date"}, ts, "2026-08-28"},
		// text
		{"varchar", baseline.Column{Name: "c", MySQLType: "varchar"}, "hello", "hello"},
		{"varchar from bytes", baseline.Column{Name: "c", MySQLType: "varchar"}, []byte("hello"), "hello"},
		{"json column from decoded image", baseline.Column{Name: "c", MySQLType: "json"}, map[string]any{"a": json.Number("1")}, `{"a":1}`},
		// json (#1508): the row image's decoded value and the baseline's
		// re-encoded text both leave as the one canonical rendering; keys
		// sorted, no spaces, `<` as is, numbers as written, scalars quoted.
		{"json nested from image", baseline.Column{Name: "c", MySQLType: "json"}, map[string]any{"b": json.Number("1.50"), "a": []any{json.Number("1"), "<x>&y"}}, `{"a":[1,"<x>&y"],"b":1.50}`},
		{"json scalar text from image (base64-stored, decoded to text) is re-emitted", baseline.Column{Name: "c", MySQLType: "json"}, `"abc"`, `"abc"`},
		{"json bare legacy string from image (pre-#736) is quoted", baseline.Column{Name: "c", MySQLType: "json"}, "abc", `"abc"`},
		{"json null text from image", baseline.Column{Name: "c", MySQLType: "json"}, "null", `null`},
		{"json number scalar from image", baseline.Column{Name: "c", MySQLType: "json"}, json.Number("42"), `42`},
		{"json bool scalar from image", baseline.Column{Name: "c", MySQLType: "json"}, true, `true`},
		{"json canonical text from the baseline passes through", baseline.Column{Name: "c", MySQLType: "json"}, json.RawMessage(`{"a":1}`), `{"a":1}`},
		{"json value in a column of unknown type", baseline.Column{Name: "c"}, map[string]any{"b": json.Number("1"), "a": "<x>"}, `{"a":"<x>","b":1}`},
		{"enum label passes through", baseline.Column{Name: "c", MySQLType: "enum"}, "paid", "paid"},
		{"time as text", baseline.Column{Name: "c", MySQLType: "time"}, "12:34:56", "12:34:56"},
		// binary
		{"blob from bytes", baseline.Column{Name: "c", MySQLType: "blob"}, []byte{0x00, 0xff}, "00FF"},
		// null
		{"nil is NULL", baseline.Column{Name: "c", MySQLType: "varchar"}, nil, "NULL"},
		// floats
		{"double from json.Number", baseline.Column{Name: "c", MySQLType: "double"}, json.Number("1.25"), "1.25"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildOne(t, tc.col, tc.v)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestJSON_bothPathsAgreeByteForByte drives the same JSON value down the two
// roads one column has: the baseline's text (MySQL's rendering, parsed by
// canonicalJSONText the way writeBaselineRows does) and the row image (the
// bytes binlog_events holds, decoded by query.UnmarshalRowImage the way the
// delta path does). The two cells must be the same bytes (#1508).
func TestJSON_bothPathsAgreeByteForByte(t *testing.T) {
	col := baseline.Column{Name: "c", MySQLType: "json"}
	cases := []struct {
		name  string
		mysql string // what SELECT prints, and the dump holds
		image string // the value inside the row image
		want  string
	}{
		{"nested object", `{"b": 1, "a": [1, 2], "s": "<x>&y", "n": 1.50}`, `{"b":1,"a":[1,2],"s":"<x>&y","n":1.50}`, `{"a":[1,2],"b":1,"n":1.50,"s":"<x>&y"}`},
		{"string scalar", `"abc"`, `"abc"`, `"abc"`},
		{"number scalar keeps its text", `1.50`, `1.50`, `1.50`},
		{"big integer is not rounded", `12345678901234567890`, `12345678901234567890`, `12345678901234567890`},
		{"bool scalar", `true`, `true`, `true`},
		{"null scalar is a value on both sides", `null`, `null`, `null`},
		{"empty object", `{}`, `{}`, `{}`},
		{"array with a scalar under html escaping", `[1, "<a>"]`, `[1,"<a>"]`, `[1,"<a>"]`},
		{"unicode as written", `{"k": "é"}`, `{"k":"é"}`, `{"k":"é"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := canonicalJSONText(tc.mysql)
			if err != nil {
				t.Fatalf("canonicalJSONText: %v", err)
			}
			fromBaseline, err := buildOne(t, col, raw)
			if err != nil {
				t.Fatalf("baseline path: %v", err)
			}
			// What the delta path hands appendValue: the indexer embeds a
			// container (#736), so it arrives decoded; a top-level scalar is
			// stored base64 and the epoch decoder returns its text.
			var fromImage any = tc.image
			if s := strings.TrimSpace(tc.image); strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
				fromImage = query.UnmarshalRowImage([]byte(`{"c":` + tc.image + `}`))["c"]
			}
			fromDelta, err := buildOne(t, col, fromImage)
			if err != nil {
				t.Fatalf("delta path: %v", err)
			}
			if fromBaseline != tc.want || fromDelta != tc.want {
				t.Fatalf("baseline %q, delta %q, want both %q", fromBaseline, fromDelta, tc.want)
			}
		})
	}
}

func TestCanonicalJSONText_refusesNonJSON(t *testing.T) {
	for _, text := range []string{`{"a": 1`, `abc`, `{"a":1} {"b":2}`, ``} {
		if _, err := canonicalJSONText(text); err == nil {
			t.Errorf("%q: want an error, a JSON column never holds it", text)
		}
	}
	// A JSON null is a value the column can hold; it keeps its literal.
	raw, err := canonicalJSONText(`null`)
	if err != nil || string(raw) != "null" {
		t.Fatalf("null = %q, %v", raw, err)
	}
	// A shape neither source produces is an error, never base64 or an object.
	for _, v := range []any{[]byte("x"), time.Now(), struct{ A int }{1}} {
		if _, err := jsonText(v); err == nil {
			t.Errorf("jsonText(%T): want an error", v)
		}
	}
}

func TestAppendValue_datetimeIgnoresProcessTimezone(t *testing.T) {
	// The 5-hour shift trap: a naive DATETIME text must not be read in the
	// process's local zone. Pin a zone far from UTC for the duration.
	prev := time.Local
	loc, err := time.LoadLocation("America/Bogota")
	if err != nil {
		t.Skip("zone database unavailable")
	}
	time.Local = loc
	defer func() { time.Local = prev }()

	got, err := buildOne(t, baseline.Column{Name: "c", MySQLType: "datetime"}, "2026-08-28 12:34:56")
	if err != nil {
		t.Fatal(err)
	}
	if got != "2026-08-28 12:34:56" {
		t.Fatalf("got %q under TZ=America/Bogota, want the naive value unchanged", got)
	}
}

func TestAppendValue_refusals(t *testing.T) {
	cases := []struct {
		name string
		col  baseline.Column
		v    any
		want string
	}{
		{"int32 overflow", baseline.Column{Name: "c", MySQLType: "int"}, int64(1) << 40, "does not fit"},
		{"fractional into int", baseline.Column{Name: "c", MySQLType: "int"}, json.Number("1.5"), "not a whole number"},
		{"garbage into int", baseline.Column{Name: "c", MySQLType: "int"}, "seven", "not an integer"},
		{"garbage into datetime", baseline.Column{Name: "c", MySQLType: "datetime"}, "yesterday", "not a date"},
		{"struct into text", baseline.Column{Name: "c", MySQLType: "varchar"}, struct{}{}, "cannot read"},
		// Only a zero DATE is a documented NULL; an empty string is not one.
		{"empty string into datetime", baseline.Column{Name: "c", MySQLType: "datetime"}, "", "not a date"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildOne(t, tc.col, tc.v)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestRowAppender_caseFoldsAndRefusesAbsentColumns(t *testing.T) {
	cols, err := buildColumns([]baseline.Column{{Name: "Id", MySQLType: "int"}, {Name: "note", MySQLType: "varchar"}}, []string{"Id"})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := table.SchemaToArrowSchema(icebergSchema(cols), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	app, err := newRowAppender(memory.DefaultAllocator, sc, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer app.release()
	// "id" spelled in lower case reaches the "Id" column; an explicit nil is a NULL.
	if err := app.append(map[string]any{"id": int32(1), "NOTE": nil}); err != nil {
		t.Fatal(err)
	}
	rec := app.flush()
	defer rec.Release()
	if got := rec.Column(0).(*array.Int32).Value(0); got != 1 {
		t.Fatalf("Id = %d, want 1 (case-folded lookup)", got)
	}
	if !rec.Column(1).IsNull(0) {
		t.Fatal("note should be NULL when the image carries an explicit nil")
	}
	// A column ABSENT from the image is not a NULL: the event was captured
	// under a schema without it, and writing NULL would invent a value.
	err = app.append(map[string]any{"id": int32(2)})
	if err == nil || !strings.Contains(err.Error(), "absent from the row image") {
		t.Fatalf("err = %v, want a refusal naming the absent column", err)
	}
}

// TestNewRowAppender_refusesSwappedColumns: the appender writes by ordinal,
// so two same-typed columns in the other order would write each other's
// values with no type check to notice.
func TestNewRowAppender_refusesSwappedColumns(t *testing.T) {
	ab, err := buildColumns([]baseline.Column{{Name: "id", MySQLType: "int"}, {Name: "a", MySQLType: "varchar"}, {Name: "b", MySQLType: "varchar"}}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	ba, err := buildColumns([]baseline.Column{{Name: "id", MySQLType: "int"}, {Name: "b", MySQLType: "varchar"}, {Name: "a", MySQLType: "varchar"}}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := table.SchemaToArrowSchema(icebergSchema(ab), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newRowAppender(memory.DefaultAllocator, sc, ba)
	if err == nil || !strings.Contains(err.Error(), `has "a" where the export has "b"`) {
		t.Fatalf("err = %v, want the swapped-column refusal", err)
	}
}

func TestNewRowAppender_refusesArrowTypeMismatch(t *testing.T) {
	// The table says string, the export would write int: a refusal, not a
	// panic in the builder type assertion mid-write.
	cols, err := buildColumns([]baseline.Column{{Name: "id", MySQLType: "int"}}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	asString, err := buildColumns([]baseline.Column{{Name: "id", MySQLType: "varchar"}}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	sc, err := table.SchemaToArrowSchema(icebergSchema(asString), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newRowAppender(memory.DefaultAllocator, sc, cols)
	if err == nil || !strings.Contains(err.Error(), "the Iceberg table stores") {
		t.Fatalf("err = %v, want a type-mismatch refusal", err)
	}
}
