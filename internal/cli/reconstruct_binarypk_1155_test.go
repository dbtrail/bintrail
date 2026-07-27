package cli

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

func TestFixedBinaryWidth(t *testing.T) {
	cases := []struct {
		columnType string
		want       int
	}{
		{"binary(16)", 16},
		{"BINARY(255)", 255},
		{"binary", 0}, // no declared width to pad to
		{"varbinary(16)", 0},
		{"binary(0)", 0},
		{"binary(x)", 0},
		{"", 0},
	}
	for _, tc := range cases {
		if got := fixedBinaryWidth(tc.columnType); got != tc.want {
			t.Errorf("fixedBinaryWidth(%q) = %d, want %d", tc.columnType, got, tc.want)
		}
	}
}

// TestPadFixedBinaryFilter covers the retry that lets an operator paste a key
// straight out of binlog_events.pk_values: that spelling is the ROW image's,
// with trailing 0x00 stripped, while the baseline stores the padded value.
func TestPadFixedBinaryFilter(t *testing.T) {
	binary16 := metadata.ColumnMeta{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true}

	t.Run("stripped hex key is re-padded to the storage width", func(t *testing.T) {
		got, changed := padFixedBinaryFilter(map[string]string{"k": "0x11223344556677889900AABB"}, []metadata.ColumnMeta{binary16})
		if !changed {
			t.Fatal("padFixedBinaryFilter reported no change for a short BINARY(16) key")
		}
		if got["k"] != "0x11223344556677889900AABB00000000" {
			t.Errorf("padded key = %q, want %q", got["k"], "0x11223344556677889900AABB00000000")
		}
	})

	t.Run("plain-text key is re-padded as bytes", func(t *testing.T) {
		// pk_values stores a binary key VERBATIM when its bytes are valid
		// UTF-8 (formatPKValue is content-gated), so the retry must handle a
		// value with no 0x prefix too.
		got, changed := padFixedBinaryFilter(map[string]string{"k": "AB"}, []metadata.ColumnMeta{binary16})
		if !changed {
			t.Fatal("padFixedBinaryFilter reported no change for a short plain-text BINARY(16) key")
		}
		if got["k"] != "0x41420000000000000000000000000000" {
			t.Errorf("padded key = %q, want %q", got["k"], "0x41420000000000000000000000000000")
		}
	})

	t.Run("full-width key is left alone", func(t *testing.T) {
		full := "0xB2815CC3C200FF7C0102030405060780"
		_, changed := padFixedBinaryFilter(map[string]string{"k": full}, []metadata.ColumnMeta{binary16})
		if changed {
			t.Error("a full-width key must not be re-spelled — the exact lookup already covers it")
		}
	})

	t.Run("varbinary is never padded", func(t *testing.T) {
		vb := metadata.ColumnMeta{Name: "k", DataType: "varbinary", ColumnType: "varbinary(16)", IsPK: true}
		_, changed := padFixedBinaryFilter(map[string]string{"k": "0xAABB"}, []metadata.ColumnMeta{vb})
		if changed {
			t.Error("VARBINARY has no storage padding — padding it would look for a key that cannot exist")
		}
	})

	t.Run("unknown column width is left alone", func(t *testing.T) {
		// Pre-#212 snapshot: no COLUMN_TYPE, so the pad width is unknowable.
		noWidth := metadata.ColumnMeta{Name: "k", DataType: "binary", IsPK: true}
		_, changed := padFixedBinaryFilter(map[string]string{"k": "0xAABB"}, []metadata.ColumnMeta{noWidth})
		if changed {
			t.Error("padded a BINARY column with no declared width — the pad length would be a guess")
		}
	})

	t.Run("column name matching is case-insensitive", func(t *testing.T) {
		// --pk-columns is operator-typed; MySQL column names are
		// case-insensitive and so is the DuckDB lookup that already ran, so
		// the retry must not be the one link that cares about case.
		got, changed := padFixedBinaryFilter(map[string]string{"K": "0xAABB"}, []metadata.ColumnMeta{binary16})
		if !changed {
			t.Fatal("padFixedBinaryFilter skipped a differently-cased column name")
		}
		if got["K"] != "0xAABB"+strings.Repeat("00", 14) {
			t.Errorf("padded key = %q", got["K"])
		}
	})

	t.Run("no PK metadata is a no-op", func(t *testing.T) {
		_, changed := padFixedBinaryFilter(map[string]string{"k": "0xAABB"}, nil)
		if changed {
			t.Error("padFixedBinaryFilter must be inert without PK metadata")
		}
	})

	t.Run("non-PK filter columns are untouched", func(t *testing.T) {
		got, changed := padFixedBinaryFilter(map[string]string{"k": "0xAABB", "other": "7"}, []metadata.ColumnMeta{binary16})
		if !changed {
			t.Fatal("expected the BINARY column to be padded")
		}
		if got["other"] != "7" {
			t.Errorf("unrelated filter column was rewritten: %q", got["other"])
		}
	})
}

func TestDecodeHexPKValue(t *testing.T) {
	cases := []struct {
		in     string
		wantOK bool
	}{
		{"0xAABB", true},
		{"0xaabb", true},
		{"0xAAB", false}, // odd digit count is not a byte string
		{"0xZZ", false},  // not hex
		{"AABB", false},  // no prefix: raw key text
		{"0x", false},    // no payload
		{"", false},
	}
	for _, tc := range cases {
		if _, ok := decodeHexPKValue(tc.in); ok != tc.wantOK {
			t.Errorf("decodeHexPKValue(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
		}
	}
}

// TestUnsupportedPKType guards the #1155 misdiagnosis fix: the PK-changing-
// UPDATE explanation may only be reached when the lookup was capable of
// resolving the key in the first place.
func TestUnsupportedPKType(t *testing.T) {
	supported := []metadata.ColumnMeta{
		{Name: "k", DataType: "binary", ColumnType: "binary(16)"},
		{Name: "id", DataType: "int"},
	}
	if c := unsupportedPKType(supported); c != nil {
		t.Errorf("unsupportedPKType flagged %q (%s) — the binary family is supported since #1155", c.Name, c.DataType)
	}

	mixed := []metadata.ColumnMeta{
		{Name: "id", DataType: "int"},
		{Name: "flags", DataType: "bit", ColumnType: "bit(8)"},
	}
	c := unsupportedPKType(mixed)
	if c == nil {
		t.Fatal("unsupportedPKType did not flag a BIT primary-key column")
	}
	if c.Name != "flags" {
		t.Errorf("flagged column = %q, want %q", c.Name, "flags")
	}

	if c := unsupportedPKType(nil); c != nil {
		t.Errorf("unsupportedPKType(nil) = %v, want nil — no metadata means no verdict, not a bad verdict", c)
	}

	// A PostgreSQL snapshot leaves data_type AND column_type empty (#533), and
	// single-row reconstruct runs generically for a PG source. Flagging that as
	// "unsupported" would tell every PG operator their schema does not work
	// when it does — worse than the #782 misdiagnosis this branch replaces.
	pg := []metadata.ColumnMeta{{Name: "id", DataType: "", ColumnType: ""}}
	if c := unsupportedPKType(pg); c != nil {
		t.Errorf("unsupportedPKType flagged an empty DataType (%q) — that is the PostgreSQL snapshot signature, not an unsupported type", c.Name)
	}
}

// TestIndexPKSpelling covers the OTHER direction of the #1155 asymmetry: the
// event fetch matches binlog_events.pk_values, which holds the ROW image's
// spelling. An operator who produces the full-width key with
// `SELECT CONCAT('0x', HEX(k))` would otherwise resolve the baseline row and
// fetch ZERO events — rendering baseline-era state as the state at --at, with
// no error. That is a fail-loud-to-fail-silent regression, so it is pinned.
func TestIndexPKSpelling(t *testing.T) {
	binary16 := metadata.ColumnMeta{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true}

	cases := []struct {
		name  string
		in    string
		metas []metadata.ColumnMeta
		want  string
	}{
		{
			name:  "full-width key is trimmed to the stored spelling",
			in:    "0x11223344556677889900AABB00000000",
			metas: []metadata.ColumnMeta{binary16},
			want:  "0x11223344556677889900AABB",
		},
		{
			name:  "lowercase hex is uppercased to match pk_values",
			in:    "0xb2815cc3c200ff7c0102030405060780",
			metas: []metadata.ColumnMeta{binary16},
			want:  "0xB2815CC3C200FF7C0102030405060780",
		},
		{
			// formatPKValue is content-gated: once trimmed, printable ASCII is
			// stored VERBATIM with no 0x prefix. Re-spelling it as hex would
			// miss every event.
			name:  "printable-ASCII payload becomes the verbatim spelling",
			in:    "0x41420000000000000000000000000000",
			metas: []metadata.ColumnMeta{binary16},
			want:  "AB",
		},
		{
			name:  "already-stored spelling is untouched",
			in:    "0x11223344556677889900AABB",
			metas: []metadata.ColumnMeta{binary16},
			want:  "0x11223344556677889900AABB",
		},
		{
			name:  "varbinary is never trimmed",
			in:    "0xAABB0000",
			metas: []metadata.ColumnMeta{{Name: "k", DataType: "varbinary", ColumnType: "varbinary(16)"}},
			want:  "0xAABB0000",
		},
		{
			name:  "non-binary PK is untouched",
			in:    "12345",
			metas: []metadata.ColumnMeta{{Name: "id", DataType: "int"}},
			want:  "12345",
		},
		{
			name:  "no metadata is a no-op",
			in:    "0x11223344556677889900AABB00000000",
			metas: nil,
			want:  "0x11223344556677889900AABB00000000",
		},
		{
			// The composite component that is NOT binary must survive verbatim,
			// including a value that merely looks hex-ish.
			name:  "composite key re-spells only the binary component",
			in:    "77|0x11223344556677889900AABB00000000",
			metas: []metadata.ColumnMeta{{Name: "tenant", DataType: "int"}, binary16},
			want:  "77|0x11223344556677889900AABB",
		},
		{
			name:  "arity mismatch leaves the value alone",
			in:    "0x11223344556677889900AABB00000000",
			metas: []metadata.ColumnMeta{{Name: "tenant", DataType: "int"}, binary16},
			want:  "0x11223344556677889900AABB00000000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := indexPKSpelling(tc.in, tc.metas); got != tc.want {
				t.Errorf("indexPKSpelling(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
