package reconstruct

import (
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/metadata"
)

// TestCanonicalizePKValue_datetimeTime covers the core bug #212 was filed
// for: DuckDB returns TIMESTAMP columns as time.Time, but the indexer stores
// them as go-mysql-formatted strings. The canonicalizer must bridge the gap.
func TestCanonicalizePKValue_datetimeTime(t *testing.T) {
	cases := []struct {
		name     string
		in       time.Time
		dataType string
		want     string
	}{
		{
			name:     "datetime(0) — no subsecond",
			in:       time.Date(2026, 4, 11, 14, 30, 45, 0, time.UTC),
			dataType: "datetime",
			want:     "2026-04-11 14:30:45",
		},
		{
			name:     "datetime(6) — microsecond precision",
			in:       time.Date(2026, 4, 11, 14, 30, 45, 123456000, time.UTC),
			dataType: "datetime",
			want:     "2026-04-11 14:30:45.123456",
		},
		{
			name:     "timestamp treated same as datetime",
			in:       time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			dataType: "timestamp",
			want:     "2020-01-01 00:00:00",
		},
		{
			name:     "non-UTC normalised to UTC",
			in:       time.Date(2026, 4, 11, 10, 30, 45, 0, mustLoadLocation("America/New_York")),
			dataType: "datetime",
			want:     "2026-04-11 14:30:45",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := canonicalizePKValue(tc.in, tc.dataType)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCanonicalizePKValue_datetimeString covers the pass-through for
// TIMESTAMP columns that some DuckDB configurations return as strings.
func TestCanonicalizePKValue_datetimeString(t *testing.T) {
	got := canonicalizePKValue("2026-04-11 14:30:45", "datetime")
	if got != "2026-04-11 14:30:45" {
		t.Errorf("expected pass-through, got %v", got)
	}
}

// TestCanonicalizePKValue_dateTime verifies DATE columns are formatted
// without the time component.
func TestCanonicalizePKValue_dateTime(t *testing.T) {
	in := time.Date(2026, 4, 11, 23, 59, 59, 0, time.UTC)
	got := canonicalizePKValue(in, "date")
	if got != "2026-04-11" {
		t.Errorf("got %q, want 2026-04-11", got)
	}
}

// TestCanonicalizePKValue_intPassthrough confirms that integer types flow
// through unchanged — both the indexer and DuckDB return intN so no
// canonicalisation is needed.
func TestCanonicalizePKValue_intPassthrough(t *testing.T) {
	cases := []struct {
		dataType string
		value    any
	}{
		{"int", int32(42)},
		{"bigint", int64(9876543210)},
		{"smallint", int16(100)},
		{"int unsigned", uint32(4_000_000_000)},
	}
	for _, tc := range cases {
		t.Run(tc.dataType, func(t *testing.T) {
			got := canonicalizePKValue(tc.value, tc.dataType)
			if got != tc.value {
				t.Errorf("%s: got %v (%T), want %v (%T)",
					tc.dataType, got, got, tc.value, tc.value)
			}
		})
	}
}

// TestCanonicalizePKValue_stringPassthrough confirms VARCHAR/CHAR/TEXT PKs
// are returned unchanged (the indexer and DuckDB both deliver Go string).
func TestCanonicalizePKValue_stringPassthrough(t *testing.T) {
	for _, dt := range []string{"varchar", "char", "text", "enum", "set"} {
		got := canonicalizePKValue("some-id", dt)
		if got != "some-id" {
			t.Errorf("%s: got %q, want some-id", dt, got)
		}
	}
}

// TestCanonicalizePKValue_nilReturnsNil ensures nil values aren't coerced
// into something else.
func TestCanonicalizePKValue_nilReturnsNil(t *testing.T) {
	if got := canonicalizePKValue(nil, "int"); got != nil {
		t.Errorf("got %v, want nil", got)
	}
	if got := canonicalizePKValue(nil, "datetime"); got != nil {
		t.Errorf("datetime nil: got %v, want nil", got)
	}
}

// TestCanonicalizePKMap_onlyTouchesPKColumns verifies that non-PK columns
// in the input map are preserved untouched — they flow into the mydumper
// writer and must not be altered by the PK canonicalisation step.
func TestCanonicalizePKMap_onlyTouchesPKColumns(t *testing.T) {
	pkCols := []metadata.ColumnMeta{
		{Name: "created_at", OrdinalPosition: 1, IsPK: true, DataType: "datetime"},
	}
	created := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	row := map[string]any{
		"created_at":    created,
		"status":        "shipped",          // non-PK; must pass through
		"extra_payload": []byte{0xde, 0xad}, // non-PK; must pass through
	}
	out := canonicalizePKMap(row, pkCols)

	if out["created_at"] != "2026-04-11 12:00:00" {
		t.Errorf("PK not canonicalised: got %v", out["created_at"])
	}
	if out["status"] != "shipped" {
		t.Errorf("non-PK string mangled: got %v", out["status"])
	}
	// Non-PK []byte survives by-identity.
	if _, ok := out["extra_payload"].([]byte); !ok {
		t.Errorf("non-PK []byte changed type: got %T", out["extra_payload"])
	}
	// Source map is not mutated.
	if row["created_at"] != created {
		t.Error("source map was mutated; canonicalizePKMap must return a new map")
	}
}

// TestCanonicalizePKMap_compositePK handles a PK made of multiple columns
// with distinct types.
func TestCanonicalizePKMap_compositePK(t *testing.T) {
	pkCols := []metadata.ColumnMeta{
		{Name: "tenant", OrdinalPosition: 1, IsPK: true, DataType: "varchar"},
		{Name: "created_at", OrdinalPosition: 2, IsPK: true, DataType: "datetime"},
		{Name: "seq", OrdinalPosition: 3, IsPK: true, DataType: "int"},
	}
	row := map[string]any{
		"tenant":     "acme",
		"created_at": time.Date(2026, 4, 11, 9, 0, 0, 0, time.UTC),
		"seq":        int64(1),
	}
	out := canonicalizePKMap(row, pkCols)
	if out["tenant"] != "acme" {
		t.Errorf("tenant: got %v", out["tenant"])
	}
	if out["created_at"] != "2026-04-11 09:00:00" {
		t.Errorf("created_at: got %v", out["created_at"])
	}
	if out["seq"] != int64(1) {
		t.Errorf("seq: got %v", out["seq"])
	}
}

// TestSupportedPKType covers the Phase-1 warning guard.
func TestSupportedPKType(t *testing.T) {
	supported := []string{
		"int", "bigint", "smallint", "tinyint", "mediumint",
		"int unsigned", "bigint unsigned",
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

// TestSupportedPKType_caseInsensitive confirms we handle both "DATETIME" and
// "datetime" as the canonicalizer does.
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
