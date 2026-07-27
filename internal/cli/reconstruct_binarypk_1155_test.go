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
}
