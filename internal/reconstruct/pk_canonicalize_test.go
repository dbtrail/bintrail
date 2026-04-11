package reconstruct

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/metadata"
)

// colMeta is a small helper to build a metadata.ColumnMeta for test tables.
func colMeta(name, dataType, columnType string) metadata.ColumnMeta {
	return metadata.ColumnMeta{
		Name:       name,
		IsPK:       true,
		DataType:   dataType,
		ColumnType: columnType,
	}
}

// ─── canonicalizeDatetime precision coverage ─────────────────────────────────

// TestCanonicalizeDatetime_precisionAware is the #212 regression centerpiece:
// the canonicalizer must match go-mysql's formatDatetime output at every
// declared fractional precision (0..6), not just 0 and 6. Before this fix,
// the Nanosecond()==0 heuristic silently broke DATETIME(6) with whole-second
// values and DATETIME(N) with fractional values whose precision wasn't 0 or 6.
func TestCanonicalizeDatetime_precisionAware(t *testing.T) {
	cases := []struct {
		name       string
		in         time.Time
		columnType string
		want       string
	}{
		// DATETIME(0): indexer stores "YYYY-MM-DD HH:MM:SS" with no fraction.
		{
			name:       "datetime(0) whole second",
			in:         time.Date(2026, 4, 11, 14, 30, 45, 0, time.UTC),
			columnType: "datetime",
			want:       "2026-04-11 14:30:45",
		},
		// DATETIME(6) whole second: indexer stores ".000000" tail,
		// canonicalizer must match.
		{
			name:       "datetime(6) whole second",
			in:         time.Date(2026, 4, 11, 14, 30, 45, 0, time.UTC),
			columnType: "datetime(6)",
			want:       "2026-04-11 14:30:45.000000",
		},
		// DATETIME(6) microsecond precision.
		{
			name:       "datetime(6) microsecond",
			in:         time.Date(2026, 4, 11, 14, 30, 45, 123456000, time.UTC),
			columnType: "datetime(6)",
			want:       "2026-04-11 14:30:45.123456",
		},
		// DATETIME(3) millisecond precision — go-mysql truncates to 3 digits.
		{
			name:       "datetime(3) millisecond",
			in:         time.Date(2026, 4, 11, 14, 30, 45, 123000000, time.UTC),
			columnType: "datetime(3)",
			want:       "2026-04-11 14:30:45.123",
		},
		// DATETIME(3) whole second: indexer stores ".000".
		{
			name:       "datetime(3) whole second",
			in:         time.Date(2026, 4, 11, 14, 30, 45, 0, time.UTC),
			columnType: "datetime(3)",
			want:       "2026-04-11 14:30:45.000",
		},
		// DATETIME(1) with .1 second.
		{
			name:       "datetime(1) tenth second",
			in:         time.Date(2026, 4, 11, 14, 30, 45, 100000000, time.UTC),
			columnType: "datetime(1)",
			want:       "2026-04-11 14:30:45.1",
		},
		// TIMESTAMP is treated identically to DATETIME.
		{
			name:       "timestamp(6) microsecond",
			in:         time.Date(2020, 1, 1, 0, 0, 0, 500000000, time.UTC),
			columnType: "timestamp(6)",
			want:       "2020-01-01 00:00:00.500000",
		},
		// Non-UTC input normalised to UTC before formatting.
		{
			name:       "non-UTC normalised",
			in:         time.Date(2026, 4, 11, 10, 30, 45, 0, mustLoadLocation("America/New_York")),
			columnType: "datetime",
			want:       "2026-04-11 14:30:45",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := colMeta("ts", strings.TrimSuffix(strings.SplitN(tc.columnType, "(", 2)[0], ")"), tc.columnType)
			got, err := canonicalizePKValue(tc.in, col)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCanonicalizeDatetime_fallbackHeuristic covers the pre-#212 snapshot
// case where ColumnType is empty: the canonicalizer can still produce a
// correct result for DATETIME(0) (the common case) via the Nanosecond()==0
// heuristic. DATETIME(N>0) whole-second values are knowingly unreliable
// under this fallback — document the limitation in a separate test.
func TestCanonicalizeDatetime_fallbackHeuristic(t *testing.T) {
	col := colMeta("ts", "datetime", "") // empty ColumnType → pre-#212 snapshot

	// Nanosecond=0: fallback emits no fraction (correct for DATETIME(0))
	got, err := canonicalizePKValue(time.Date(2026, 4, 11, 14, 30, 45, 0, time.UTC), col)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "2026-04-11 14:30:45" {
		t.Errorf("fallback zero-nanosecond: got %q", got)
	}

	// Nanosecond!=0: fallback emits microsecond tail (correct for DATETIME(6))
	got2, err2 := canonicalizePKValue(time.Date(2026, 4, 11, 14, 30, 45, 123456000, time.UTC), col)
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	if got2 != "2026-04-11 14:30:45.123456" {
		t.Errorf("fallback sub-second: got %q", got2)
	}
}

// TestCanonicalizeDatetime_stringPassThrough covers the branch where DuckDB
// returns a DATETIME column as a pre-formatted string (some driver modes).
func TestCanonicalizeDatetime_stringPassThrough(t *testing.T) {
	col := colMeta("ts", "datetime", "datetime(6)")
	got, err := canonicalizePKValue("2026-04-11 14:30:45.123456", col)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "2026-04-11 14:30:45.123456" {
		t.Errorf("got %q, want pass-through", got)
	}
}

// TestCanonicalizeDatetime_unknownTypeErrors covers the defense-in-depth
// check: a DATETIME column whose scanned Go value is neither time.Time nor
// string must error instead of silently passing through %v.
func TestCanonicalizeDatetime_unknownTypeErrors(t *testing.T) {
	col := colMeta("ts", "datetime", "datetime")
	_, err := canonicalizePKValue(int64(1712847045), col)
	if err == nil {
		t.Fatal("expected error for non-time.Time DATETIME value, got nil")
	}
	if !strings.Contains(err.Error(), "time.Time") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─── canonicalizeDate ────────────────────────────────────────────────────────

func TestCanonicalizeDate(t *testing.T) {
	col := colMeta("d", "date", "date")

	got, err := canonicalizePKValue(time.Date(2026, 4, 11, 23, 59, 59, 0, time.UTC), col)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "2026-04-11" {
		t.Errorf("time.Time: got %q", got)
	}

	// String pass-through.
	got2, err := canonicalizePKValue("2026-04-11", col)
	if err != nil || got2 != "2026-04-11" {
		t.Errorf("string pass-through: got %q err=%v", got2, err)
	}

	// Non-time-or-string error.
	_, err = canonicalizePKValue(int64(12345), col)
	if err == nil {
		t.Fatal("expected error for non-time/string DATE value")
	}
}

// ─── parseDatetimePrecision ──────────────────────────────────────────────────

func TestParseDatetimePrecision(t *testing.T) {
	cases := []struct {
		in    string
		dec   int
		known bool
	}{
		{"datetime", 0, true},
		{"datetime(0)", 0, true},
		{"datetime(3)", 3, true},
		{"datetime(6)", 6, true},
		{"timestamp", 0, true},
		{"timestamp(6)", 6, true},
		{"TIMESTAMP(3)", 3, true},
		{"  datetime(6)  ", 6, true},
		{"", 0, false}, // pre-#212 snapshot
		{"datetime(7)", 0, false}, // out of range
		{"datetime(abc)", 0, false}, // unparseable
		{"varchar(64)", 0, false}, // wrong prefix
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			dec, known := parseDatetimePrecision(c.in)
			if dec != c.dec || known != c.known {
				t.Errorf("parse(%q) = (%d,%v), want (%d,%v)", c.in, dec, known, c.dec, c.known)
			}
		})
	}
}

// ─── integer and string pass-through ────────────────────────────────────────

func TestCanonicalizePKValue_intPassthrough(t *testing.T) {
	cases := []struct {
		dataType string
		value    any
	}{
		{"int", int32(42)},
		{"bigint", int64(9876543210)},
		{"smallint", int16(100)},
		{"tinyint", int8(5)},
		{"mediumint", int32(16777215)},
	}
	for _, tc := range cases {
		t.Run(tc.dataType, func(t *testing.T) {
			col := colMeta("id", tc.dataType, tc.dataType)
			got, err := canonicalizePKValue(tc.value, col)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.value {
				t.Errorf("%s: got %v, want %v", tc.dataType, got, tc.value)
			}
		})
	}
}

func TestCanonicalizePKValue_stringPassthrough(t *testing.T) {
	for _, dt := range []string{"varchar", "char", "text", "enum", "set"} {
		col := colMeta("id", dt, dt)
		got, err := canonicalizePKValue("some-id", col)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", dt, err)
			continue
		}
		if got != "some-id" {
			t.Errorf("%s: got %q, want some-id", dt, got)
		}
	}
}

// Non-string scanned into a VARCHAR PK must error — the canonicalizer has
// no way to know what to do with it.
func TestCanonicalizePKValue_stringTypeMismatchErrors(t *testing.T) {
	col := colMeta("name", "varchar", "varchar(64)")
	_, err := canonicalizePKValue(int64(42), col)
	if err == nil {
		t.Fatal("expected error for non-string varchar PK")
	}
	if !strings.Contains(err.Error(), "expected string") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─── hard-fail paths ─────────────────────────────────────────────────────────

func TestCanonicalizePKValue_nilErrors(t *testing.T) {
	col := colMeta("id", "int", "int")
	_, err := canonicalizePKValue(nil, col)
	if err == nil {
		t.Fatal("expected error for nil PK value")
	}
	if !strings.Contains(err.Error(), "nil PK value") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCanonicalizePKValue_unsupportedTypeErrors(t *testing.T) {
	for _, dt := range []string{"decimal", "binary", "blob", "bit", "json"} {
		col := colMeta("x", dt, dt)
		_, err := canonicalizePKValue("whatever", col)
		if err == nil {
			t.Errorf("%s: expected error for unsupported PK type, got nil", dt)
			continue
		}
		if !strings.Contains(err.Error(), "unsupported") {
			t.Errorf("%s: unexpected error: %v", dt, err)
		}
	}
}

// ─── canonicalizePKMap ───────────────────────────────────────────────────────

func TestCanonicalizePKMap_onlyTouchesPKColumns(t *testing.T) {
	pkCols := []metadata.ColumnMeta{
		colMeta("created_at", "datetime", "datetime"),
	}
	created := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	row := map[string]any{
		"created_at":    created,
		"status":        "shipped",
		"extra_payload": []byte{0xde, 0xad},
	}
	out, err := canonicalizePKMap(row, pkCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if out["created_at"] != "2026-04-11 12:00:00" {
		t.Errorf("PK not canonicalised: got %v", out["created_at"])
	}
	if out["status"] != "shipped" {
		t.Errorf("non-PK string mangled: got %v", out["status"])
	}
	if _, ok := out["extra_payload"].([]byte); !ok {
		t.Errorf("non-PK []byte changed type: got %T", out["extra_payload"])
	}

	// Rigorous immutability check: input map must still hold the
	// ORIGINAL time.Time value at the "created_at" key.
	if gotIn, ok := row["created_at"].(time.Time); !ok {
		t.Errorf("source map 'created_at' type changed; canonicalizePKMap must not mutate input")
	} else if !gotIn.Equal(created) {
		t.Errorf("source map 'created_at' value changed")
	}
}

func TestCanonicalizePKMap_missingColumnErrors(t *testing.T) {
	pkCols := []metadata.ColumnMeta{
		colMeta("id", "int", "int"),
	}
	row := map[string]any{
		// "id" is missing
		"payload": "x",
	}
	_, err := canonicalizePKMap(row, pkCols)
	if err == nil {
		t.Fatal("expected error for missing PK column, got nil")
	}
	// Sentinel check via errors.Is — existing callers rely on this.
	if !errors.Is(err, ErrPKColumnMissing) {
		t.Errorf("expected errors.Is(err, ErrPKColumnMissing), got: %v", err)
	}
	// Typed check via errors.As — callers that want the column name.
	var missing *MissingPKColumnError
	if !errors.As(err, &missing) {
		t.Fatalf("expected errors.As to unwrap *MissingPKColumnError, got: %T", err)
	}
	if missing.Column != "id" {
		t.Errorf("Column = %q, want %q", missing.Column, "id")
	}
}

func TestCanonicalizePKMap_compositePK(t *testing.T) {
	pkCols := []metadata.ColumnMeta{
		colMeta("tenant", "varchar", "varchar(64)"),
		colMeta("created_at", "datetime", "datetime(6)"),
		colMeta("seq", "int", "int"),
	}
	row := map[string]any{
		"tenant":     "acme",
		"created_at": time.Date(2026, 4, 11, 9, 0, 0, 500000000, time.UTC),
		"seq":        int64(1),
	}
	out, err := canonicalizePKMap(row, pkCols)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out["tenant"] != "acme" {
		t.Errorf("tenant: got %v", out["tenant"])
	}
	if out["created_at"] != "2026-04-11 09:00:00.500000" {
		t.Errorf("created_at: got %v", out["created_at"])
	}
	if out["seq"] != int64(1) {
		t.Errorf("seq: got %v", out["seq"])
	}
}

// ─── supportedPKType ─────────────────────────────────────────────────────────

func TestSupportedPKType(t *testing.T) {
	supported := []string{
		"int", "bigint", "smallint", "tinyint", "mediumint",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"enum", "set",
		"datetime", "timestamp", "date", "year",
	}
	for _, dt := range supported {
		if !supportedPKType(dt) {
			t.Errorf("expected %q to be supported", dt)
		}
	}
	unsupported := []string{
		"decimal", "numeric", "float", "double",
		"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob",
		"bit", "json",
		"", "unknown-type",
	}
	for _, dt := range unsupported {
		if supportedPKType(dt) {
			t.Errorf("expected %q NOT to be supported", dt)
		}
	}
}

func TestSupportedPKType_caseInsensitive(t *testing.T) {
	if !supportedPKType("DATETIME") {
		t.Error("uppercase DATETIME should be supported")
	}
	if !supportedPKType("  VarChar  ") {
		t.Error("mixed-case + padded VarChar should be supported")
	}
}

// mustLoadLocation is a test helper that panics on failure — the test
// matrix depends on a known zone being present.
func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic("mustLoadLocation: " + err.Error())
	}
	return loc
}
