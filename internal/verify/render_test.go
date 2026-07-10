package verify

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

func col(dataType, columnType string) metadata.ColumnMeta {
	return metadata.ColumnMeta{Name: "c", DataType: dataType, ColumnType: columnType}
}

func TestRenderCell_MatchesTextProtocolForm(t *testing.T) {
	ts := time.Date(2021, 1, 1, 0, 0, 0, 123456000, time.UTC) // .123456
	dt0 := time.Date(2022, 6, 15, 12, 30, 45, 0, time.UTC)
	d := time.Date(2021, 3, 4, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		v    any
		col  metadata.ColumnMeta
		want []byte // nil = SQL NULL
	}{
		{"null", nil, col("varchar", "varchar(64)"), nil},
		{"int64", int64(42), col("int", "int"), []byte("42")},
		{"int32 baseline", int32(7), col("int", "int"), []byte("7")},
		{"uint64 max", uint64(18446744073709551615), col("bigint", "bigint unsigned"), []byte("18446744073709551615")},
		{"json.Number big", json.Number("9007199254740993"), col("bigint", "bigint"), []byte("9007199254740993")},
		{"json.Number decimal", json.Number("1.50"), col("decimal", "decimal(10,2)"), []byte("1.50")},
		{"decimal as string", "1.50", col("decimal", "decimal(10,2)"), []byte("1.50")},
		{"utf8mb4 string", "café", col("varchar", "varchar(64)"), []byte("café")},
		{"empty string", "", col("varchar", "varchar(64)"), []byte("")},
		{"binary bytes", []byte{0x61, 0x00, 0x62}, col("varbinary", "varbinary(16)"), []byte{0x61, 0x00, 0x62}},
		{"datetime(6)", ts, col("datetime", "datetime(6)"), []byte("2021-01-01 00:00:00.123456")},
		{"datetime(0)", dt0, col("datetime", "datetime"), []byte("2022-06-15 12:30:45")},
		{"datetime(3)", ts, col("datetime", "datetime(3)"), []byte("2021-01-01 00:00:00.123")},
		{"date", d, col("date", "date"), []byte("2021-03-04")},
		{"timestamp(0)", dt0, col("timestamp", "timestamp"), []byte("2022-06-15 12:30:45")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderCell(tc.v, tc.col)
			if tc.want == nil {
				if got != nil {
					t.Errorf("got %q, want NULL (nil)", got)
				}
				return
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestIsZeroDateSentinel_TemporalOnly is the fix for a user-reported false
// MISMATCH: a baseline shows NULL for a zero-date value (internal/baseline's
// WriteRow maps it there unconditionally), while an event-touched row's
// image still carries the literal '0000-00-00...' sentinel text. Must
// recognize DATE/DATETIME/TIMESTAMP's zero-date family, in all three forms,
// and must NOT fire for TIME (where '00:00:00' is legal midnight, not a
// pseudo-NULL) or for a column that merely happens to hold that text as
// ordinary data.
func TestIsZeroDateSentinel_TemporalOnly(t *testing.T) {
	cases := []struct {
		dataType string
		value    string
		want     bool
	}{
		{"date", "0000-00-00", true},
		{"datetime", "0000-00-00 00:00:00", true},
		{"timestamp", "0000-00-00 00:00:00.000000", true},
		{"DATETIME", "0000-00-00 00:00:00", true}, // case-insensitive DataType
		{"datetime", "  0000-00-00 00:00:00  ", true},
		{"datetime", "2026-06-15 12:30:45", false}, // a real date, same column type
		{"time", "00:00:00", false},                // legal midnight, not a pseudo-NULL
		{"varchar", "0000-00-00 00:00:00", false},  // ordinary text data, not a temporal column
	}
	for _, tc := range cases {
		got := isZeroDateSentinel([]byte(tc.value), col(tc.dataType, tc.dataType))
		if got != tc.want {
			t.Errorf("isZeroDateSentinel(%q, dataType=%q) = %v, want %v", tc.value, tc.dataType, got, tc.want)
		}
	}
}

func TestRenderCellBaselineAnchored_ZeroDateNormalizesToNull(t *testing.T) {
	got := renderCellNormalized("0000-00-00 00:00:00", col("datetime", "datetime"))
	if got != nil {
		t.Errorf("renderCellNormalized(zero-date, datetime col) = %q, want nil (NULL)", got)
	}
	// A real value for the same column type must render normally, untouched.
	got2 := renderCellNormalized("2026-06-15 12:30:45", col("datetime", "datetime"))
	if string(got2) != "2026-06-15 12:30:45" {
		t.Errorf("renderCellNormalized(real date) = %q, want the value unchanged", got2)
	}
}

func TestRenderCell_JSONContainerCompletes(t *testing.T) {
	// A JSON column changed by an event decodes to map[string]any; renderCell
	// must produce deterministic bytes (so the digest completes) rather than
	// erroring. Two equal maps render identically.
	a := renderCell(map[string]any{"b": 2, "a": 1}, col("json", "json"))
	b := renderCell(map[string]any{"a": 1, "b": 2}, col("json", "json"))
	if a == nil || !bytes.Equal(a, b) {
		t.Errorf("JSON container rendering not deterministic: %q vs %q", a, b)
	}
}

// TestCanonicalizeJSONContainer_KeyOrderOnly is the core fix: two JSON objects
// carrying the same key/value pairs in a different order canonicalize to the
// SAME bytes, and a genuinely different value does NOT.
func TestCanonicalizeJSONContainer_KeyOrderOnly(t *testing.T) {
	a, aOK := canonicalizeJSONContainer([]byte(`{"a":1,"b":2}`))
	b, bOK := canonicalizeJSONContainer([]byte(`{"b":2,"a":1}`))
	if !aOK || !bOK {
		t.Fatalf("expected both to canonicalize: aOK=%v bOK=%v", aOK, bOK)
	}
	if string(a) != string(b) {
		t.Errorf("key-order-only difference should canonicalize identically, got %q vs %q", a, b)
	}
	c, cOK := canonicalizeJSONContainer([]byte(`{"a":1,"b":3}`))
	if !cOK {
		t.Fatal("expected c to canonicalize")
	}
	if string(a) == string(c) {
		t.Error("genuinely different values must NOT canonicalize to the same bytes")
	}
}

// TestCanonicalizeJSONContainer_ScalarsAndNonJSONUntouched: scalars (a bare
// number/bool/null/quoted string, even though each is independently valid
// JSON) and non-JSON text are left exactly as-is — canonicalization is scoped
// to object/array containers only.
func TestCanonicalizeJSONContainer_ScalarsAndNonJSONUntouched(t *testing.T) {
	for _, in := range []string{"42", "true", "false", "null", `"hello"`, "not json at all", ""} {
		got, ok := canonicalizeJSONContainer([]byte(in))
		if ok {
			t.Errorf("canonicalizeJSONContainer(%q): ok=true, want false (not a container)", in)
		}
		if string(got) != in {
			t.Errorf("canonicalizeJSONContainer(%q) = %q, want input returned unchanged", in, got)
		}
	}
}

// TestCanonicalizeJSONContainer_LargeNumberPrecisionPreserved guards the
// #496-class risk: decoding through UseNumber and re-marshaling must not
// round an integer that overflows float64's exact range, or reformat a
// decimal literal.
func TestCanonicalizeJSONContainer_LargeNumberPrecisionPreserved(t *testing.T) {
	for _, in := range []string{
		`{"n":9223372036854775807}`,  // max int64, loses precision through float64
		`{"n":18446744073709551615}`, // max uint64
		`{"n":1.50}`,                 // trailing zero a naive float64 round-trip drops
	} {
		got, ok := canonicalizeJSONContainer([]byte(in))
		if !ok {
			t.Fatalf("canonicalizeJSONContainer(%q): want ok=true", in)
		}
		if string(got) != in {
			t.Errorf("canonicalizeJSONContainer(%q) = %q, want the number literal preserved exactly", in, got)
		}
	}
}

// TestCanonicalizeJSONContainer_DuplicateKeysRefused is the fix for a review
// finding: decoding into map[string]any silently keeps only the LAST of a
// repeated object key. If canonicalizeJSONContainer collapsed
// {"a":1,"a":2} to {"a":2}, it would make that value indistinguishable from
// a baseline that never had the duplicate — masking a real divergence
// instead of merely reordering one. Must refuse (ok=false, raw bytes
// returned) whenever ANY object in the value repeats a key, at any nesting
// depth, inside an array or not — never silently collapse it.
func TestCanonicalizeJSONContainer_DuplicateKeysRefused(t *testing.T) {
	cases := []string{
		`{"a":1,"a":2}`,                   // top-level duplicate
		`{"a":1,"a":1}`,                   // duplicate with the SAME value — a "compare the final decoded map" approach would miss this; the structural walker must not
		`{"outer":{"a":1,"a":2}}`,         // duplicate in a nested object
		`[{"a":1},{"b":1,"b":2}]`,         // duplicate inside an array element
		`{"a":{"x":1},"b":{"x":1,"x":2}}`, // duplicate in one branch, not the sibling
		`{"a":1,"b":2,"a":3}`,             // duplicate not adjacent to its first occurrence
		`[[{"a":1,"a":2}]]`,               // duplicate nested inside array-in-array
		"{\"\\u0061\":1,\"a\":2}",         // duplicate via a differently-escaped but identical decoded key
	}
	for _, in := range cases {
		got, ok := canonicalizeJSONContainer([]byte(in))
		if ok {
			t.Errorf("canonicalizeJSONContainer(%q): ok=true, want false (must refuse a duplicate key)", in)
		}
		if string(got) != in {
			t.Errorf("canonicalizeJSONContainer(%q) = %q, want input returned unchanged on refusal", in, got)
		}
	}
	// A key repeated only as a VALUE (not a key) must NOT trip the guard.
	clean := `{"a":"a","b":"a"}`
	if _, ok := canonicalizeJSONContainer([]byte(clean)); !ok {
		t.Errorf("canonicalizeJSONContainer(%q): want ok=true (no key is actually duplicated)", clean)
	}
}

// TestCanonicalizeJSONContainer_InvalidUTF8Refused is the fix for a review
// finding: encoding/json replaces invalid UTF-8 bytes with U+FFFD on
// decode/re-encode, so two DIFFERENT invalid byte sequences could
// canonicalize to the SAME output — silently erasing a real difference.
// Must refuse whenever the input is not valid UTF-8.
func TestCanonicalizeJSONContainer_InvalidUTF8Refused(t *testing.T) {
	bad := []byte{'{', '"', 's', '"', ':', '"', 0xff, 0xfe, '"', '}'}
	got, ok := canonicalizeJSONContainer(bad)
	if ok {
		t.Errorf("canonicalizeJSONContainer(invalid UTF-8): ok=true, want false")
	}
	if !bytes.Equal(got, bad) {
		t.Errorf("canonicalizeJSONContainer(invalid UTF-8) = %q, want input returned unchanged on refusal", got)
	}
}

// TestCanonicalizeJSONContainer_UnpairedSurrogateRefused is the fix for a
// second review finding, distinct from the raw-invalid-UTF-8 case above: a
// \uD800-\uDFFF escape is valid JSON syntax and valid RAW UTF-8 (it's just
// ASCII characters before unescaping), so it passes json.Valid AND
// utf8.Valid(t) — the invalidity only appears once json.Decode unescapes the
// string, where Go substitutes U+FFFD, same as for raw invalid bytes. Two
// DIFFERENT unpaired surrogates would otherwise canonicalize to the
// identical U+FFFD and compare equal.
func TestCanonicalizeJSONContainer_UnpairedSurrogateRefused(t *testing.T) {
	one := []byte(`{"s":"\uD800"}`)
	other := []byte(`{"s":"\uDEAD"}`)
	if !utf8.Valid(one) || !utf8.Valid(other) {
		t.Fatal("precondition: the raw bytes (escape sequences, not yet decoded) must be valid UTF-8")
	}
	gotOne, okOne := canonicalizeJSONContainer(one)
	gotOther, okOther := canonicalizeJSONContainer(other)
	if okOne || okOther {
		t.Errorf("canonicalizeJSONContainer(unpaired surrogate): okOne=%v okOther=%v, want both false", okOne, okOther)
	}
	if !bytes.Equal(gotOne, one) || !bytes.Equal(gotOther, other) {
		t.Errorf("got %q / %q, want both inputs returned unchanged on refusal", gotOne, gotOther)
	}
}

func TestRenderCellCanonicalJSON_NullPassesThrough(t *testing.T) {
	if got := renderCellNormalized(nil, col("json", "json")); got != nil {
		t.Errorf("renderCellNormalized(nil, ...) = %q, want nil (SQL NULL)", got)
	}
}

func TestTemporalPrecision(t *testing.T) {
	cases := map[string]int{
		"datetime":      0,
		"datetime(6)":   6,
		"timestamp(3)":  3,
		"datetime(0)":   0,
		"int":           0,
		"decimal(10,2)": 0, // multi-arg paren isn't a single precision int → 0 (only temporal types call this)
	}
	for ct, want := range cases {
		if got := temporalPrecision(ct); got != want {
			t.Errorf("temporalPrecision(%q) = %d, want %d", ct, got, want)
		}
	}
}

// TestNormalizeRenderedBytes_TimeFractionCanonical is the #794 fix: go-mysql's
// timeFormat renders an integer-second TIME without any fractional suffix
// regardless of the declared fsp, while MySQL's text protocol and mydumper pad
// the fraction to the fsp ("09:00:00.000" for TIME(3)) — a conclusive false
// MISMATCH in both verify modes. Every rendering of the same value must
// normalize to the same minimal form, without needing the fsp (the live-scan
// hook carries DATA_TYPE only), and value-bearing fraction digits must
// survive so two DIFFERENT values never collapse.
func TestNormalizeRenderedBytes_TimeFractionCanonical(t *testing.T) {
	cases := []struct {
		name     string
		dataType string
		in       string
		want     string
	}{
		{"mysql/mydumper padded fsp3", "time", "09:00:00.000", "09:00:00"},
		{"go-mysql integer-second", "time", "09:00:00", "09:00:00"},
		{"padded nonzero fraction", "time", "09:00:00.120", "09:00:00.12"},
		{"minimal nonzero fraction", "time", "09:00:00.12", "09:00:00.12"},
		{"value-bearing trailing digit", "time", "09:00:00.001", "09:00:00.001"},
		{"fsp6 all-zero fraction", "time", "-838:59:59.000000", "-838:59:59"},
		{"seconds end in zero, no dot", "time", "10:00:20", "10:00:20"}, // TrimRight guard: must NOT become "10:00:2"
		{"seconds end in zero, zero fraction", "time", "10:00:20.000", "10:00:20"},
		{"case-insensitive DataType", "TIME", "09:00:00.000", "09:00:00"},
		{"non-time dataType untouched", "varchar", "09:00:00.000", "09:00:00.000"},
		{"decimal untouched", "decimal", "1.500", "1.500"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeRenderedBytes([]byte(tc.in), tc.dataType)
			if !bytes.Equal(got, []byte(tc.want)) {
				t.Errorf("normalizeRenderedBytes(%q, %q) = %q, want %q", tc.in, tc.dataType, got, tc.want)
			}
		})
	}

	// Anti-masking pin: two genuinely different TIME values stay different.
	a := normalizeRenderedBytes([]byte("09:00:00.100"), "time")
	b := normalizeRenderedBytes([]byte("09:00:00.010"), "time")
	if bytes.Equal(a, b) {
		t.Errorf("normalization collapsed distinct TIME values: %q == %q", a, b)
	}
}

// TestRenderCellNormalized_TimeIntegerSecond_BothSidesAgree pins the #794
// scenario end-to-end through the recon renderer: the event image's go-mysql
// text and the baseline's mydumper text of the same TIME(3) value must render
// identically.
func TestRenderCellNormalized_TimeIntegerSecond_BothSidesAgree(t *testing.T) {
	c := col("time", "time(3)")
	event := renderCellNormalized("09:00:00", c)        // go-mysql event-image text
	baseline := renderCellNormalized("09:00:00.000", c) // mydumper/baseline text
	if !bytes.Equal(event, baseline) {
		t.Errorf("event render %q != baseline render %q for the same TIME value", event, baseline)
	}
}

// TestNormalizeRenderedBytes_FloatDoubleCanonical is the #795 fix: MySQL's
// my_gcvt and Go's strconv render the same stored FLOAT/DOUBLE with different
// exponent forms and thresholds ("1e16" vs "1e+16"), a permanent conclusive
// false MISMATCH in live mode. Both texts must canonicalize to one rendering
// at the column's own width; anything that isn't clean float text stays
// untouched, and distinct values stay distinct (floats remain OUT of the
// deferred set — this normalizes representation, it must never mask a value
// divergence).
func TestNormalizeRenderedBytes_FloatDoubleCanonical(t *testing.T) {
	cases := []struct {
		name     string
		dataType string
		in       string
		want     string
	}{
		{"double mysql exponent", "double", "1e16", "1e+16"},
		{"double go exponent stable", "double", "1e+16", "1e+16"},
		{"double decimal-vs-exponent threshold", "double", "0.00001", "1e-05"},
		{"double plain stable", "double", "2.25", "2.25"},
		{"double integer-valued", "double", "12345", "12345"},
		{"double negative zero", "double", "-0", "-0"},
		{"float parsed at 32 bits", "float", "0.30000001192092896", "0.3"},
		{"float plain stable", "float", "1.5", "1.5"},
		// #795 FLOAT gap: MySQL's bare `SELECT f` truncates FLOAT text to ~6
		// significant digits (my_gcvt's FLT_DIG), losing the 7th digit of a
		// value like the float32 1.234567 entirely — no reformat of the
		// already-truncated "1.23457" can recover it. The fix lives in
		// internal/consistency's selectExpr (promoteFloat, `f+0e0`), which
		// widens the SELECT to DOUBLE arithmetic so my_gcvt renders the full
		// double-precision expansion; THAT text is what reaches this function.
		// This case pins that once the source hands over enough digits,
		// canonicalFloatText folds it to the same shortest float32 rendering
		// the Go-side recon renderer (renderCell's float32 case) produces.
		{"float promoted-source text folds to shortest float32 form", "float", "1.2345670461654663", "1.234567"},
		{"case-insensitive DataType", "DOUBLE", "1e16", "1e+16"},
		{"unparsable unchanged", "double", "not-a-float", "not-a-float"},
		{"whitespace unchanged", "double", " 1e16", " 1e16"},
		{"non-float dataType untouched", "varchar", "1e16", "1e16"},
		{"decimal untouched", "decimal", "1.50", "1.50"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeRenderedBytes([]byte(tc.in), tc.dataType)
			if !bytes.Equal(got, []byte(tc.want)) {
				t.Errorf("normalizeRenderedBytes(%q, %q) = %q, want %q", tc.in, tc.dataType, got, tc.want)
			}
		})
	}

	// Anti-masking pin: two distinct float64 values keep distinct canonical
	// forms — a real corruption still surfaces as a mismatch.
	a := normalizeRenderedBytes([]byte("1e16"), "double")
	b := normalizeRenderedBytes([]byte("1.0000000000000002e16"), "double")
	if bytes.Equal(a, b) {
		t.Errorf("normalization collapsed distinct DOUBLE values: %q == %q", a, b)
	}
}

// TestRenderCellNormalized_DoubleEventAndBaselineAgree pins that all three
// origins of the same DOUBLE value converge: a baseline float64 (DuckDB), an
// event image's json.Number literal, and MySQL's my_gcvt raw text (through
// normalizeRenderedBytes, as ConsistentTableChecksumNormalized applies it).
func TestRenderCellNormalized_DoubleEventAndBaselineAgree(t *testing.T) {
	c := col("double", "double")
	fromBaseline := renderCellNormalized(float64(1e16), c)
	fromEvent := renderCellNormalized(json.Number("1e+16"), c)
	fromMySQL := normalizeRenderedBytes([]byte("1e16"), "double")
	if !bytes.Equal(fromBaseline, fromEvent) || !bytes.Equal(fromEvent, fromMySQL) {
		t.Errorf("renderings diverge: baseline=%q event=%q mysql=%q", fromBaseline, fromEvent, fromMySQL)
	}
}

// TestRenderCellNormalized_FloatEventAndPromotedMySQLTextAgree is the FLOAT
// sibling of TestRenderCellNormalized_DoubleEventAndBaselineAgree, pinning
// the #795 fix end-to-end: a recon-side float32 (whether from a baseline or
// an event image) must converge with MySQL's live-source text ONLY once that
// text carries the extra precision internal/consistency's selectExpr
// (promoteFloat, `f+0e0`) now supplies — a bare, un-promoted `SELECT f`
// truncated to 6 significant digits never converges, which is the false
// MISMATCH #795 was filed to eliminate.
func TestRenderCellNormalized_FloatEventAndPromotedMySQLTextAgree(t *testing.T) {
	c := col("float", "float")
	fromBaseline := renderCellNormalized(float32(1.234567), c)
	fromEvent := renderCellNormalized(json.Number("1.234567"), c)
	// The text selectExpr(_, promoteFloat=true) now produces for this value
	// (`f+0e0` widens the read to DOUBLE arithmetic).
	fromPromotedMySQL := normalizeRenderedBytes([]byte("1.2345670461654663"), "float")
	if !bytes.Equal(fromBaseline, fromEvent) || !bytes.Equal(fromEvent, fromPromotedMySQL) {
		t.Errorf("renderings diverge: baseline=%q event=%q mysql(promoted)=%q", fromBaseline, fromEvent, fromPromotedMySQL)
	}

	// Anti-regression: the OLD un-promoted 6-sig-fig text must NOT match —
	// pinning that this test would have failed before the selectExpr fix,
	// so a future revert of promoteFloat is caught here too.
	fromUnpromotedMySQL := normalizeRenderedBytes([]byte("1.23457"), "float")
	if bytes.Equal(fromEvent, fromUnpromotedMySQL) {
		t.Errorf("unpromoted 6-sig-fig MySQL text %q unexpectedly matched recon %q — this would hide a regression of selectExpr's promoteFloat", fromUnpromotedMySQL, fromEvent)
	}
}
