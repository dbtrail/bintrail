package reconstruct

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// These tests moved here from internal/cli with the reconcilers themselves
// (#1157): padFixedBinaryFilter and IndexPKSpelling now live beside
// ReadBaselineRow so every surface — CLI, console, MCP — shares one
// implementation.

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
		// Filter keys are operator-typed; MySQL column names are
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
			if got := IndexPKSpelling(tc.in, tc.metas); got != tc.want {
				t.Errorf("IndexPKSpelling(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestDecodeHexPKLiteral(t *testing.T) {
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
		if _, ok := decodeHexPKLiteral(tc.in); ok != tc.wantOK {
			t.Errorf("decodeHexPKLiteral(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
		}
	}
}

// TestReadBaselineRow_fixedBinaryPadRetry pins the #1157 fix at its new home:
// the pad-and-retry runs INSIDE ReadBaselineRow, so every caller that supplies
// PK metas — the CLI, the console's /api/reconstruct, the MCP reconstruct tool
// — resolves a fixed BINARY(n) key by its stripped pk_values spelling, and a
// caller that cannot supply them (nil metas) keeps the exact-match behavior.
func TestReadBaselineRow_fixedBinaryPadRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "v", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	path := filepath.Join(dir, "padded.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// The baseline holds the FULL storage width, padding included — what
	// mydumper --hex-blob dumps for a BINARY(16) column.
	rows := [][]string{
		{"0x11223344556677889900AABB00000000", "hit"},
		{"0x41420000000000000000000000000000", "ascii"},
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	metas := []metadata.ColumnMeta{{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true}}
	stripped := "0x11223344556677889900AABB" // the pk_values spelling: padding stripped

	t.Run("stripped spelling resolves with PK metas", func(t *testing.T) {
		row, err := ReadBaselineRow(ctx, path, map[string]string{"k": stripped}, metas)
		if err != nil {
			t.Fatalf("ReadBaselineRow: %v", err)
		}
		if row == nil {
			t.Fatal("no baseline row for the stripped BINARY(16) spelling — the pad-and-retry did not run (#1157)")
		}
		if row["v"] != "hit" {
			t.Errorf("matched the wrong row: v=%v", row["v"])
		}
	})

	t.Run("nil metas keep the exact-match-only behavior", func(t *testing.T) {
		// The structural exclusion (--baseline-only, the shim) must stay a
		// miss, not become a guess.
		row, err := ReadBaselineRow(ctx, path, map[string]string{"k": stripped}, nil)
		if err != nil {
			t.Fatalf("ReadBaselineRow: %v", err)
		}
		if row != nil {
			t.Errorf("stripped spelling resolved WITHOUT metas: %v — the retry must require a declared width", row)
		}
	})

	t.Run("verbatim ASCII spelling resolves with PK metas", func(t *testing.T) {
		// formatPKValue stores printable payloads verbatim, so "AB" is the
		// spelling an operator copies for the second row.
		row, err := ReadBaselineRow(ctx, path, map[string]string{"k": "AB"}, metas)
		if err != nil {
			t.Fatalf("ReadBaselineRow: %v", err)
		}
		if row == nil || row["v"] != "ascii" {
			t.Errorf("verbatim spelling did not resolve, got %v", row)
		}
	})

	t.Run("full-width spelling still resolves exactly", func(t *testing.T) {
		row, err := ReadBaselineRow(ctx, path, map[string]string{"k": "0x11223344556677889900AABB00000000"}, metas)
		if err != nil {
			t.Fatalf("ReadBaselineRow: %v", err)
		}
		if row == nil || row["v"] != "hit" {
			t.Errorf("full-width spelling regressed, got %v", row)
		}
	})

	t.Run("absent key still misses after the retry", func(t *testing.T) {
		row, err := ReadBaselineRow(ctx, path, map[string]string{"k": "0xDEADBEEF"}, metas)
		if err != nil {
			t.Fatalf("ReadBaselineRow: %v", err)
		}
		if row != nil {
			t.Errorf("absent key matched a row after padding: %v", row)
		}
	})
}
