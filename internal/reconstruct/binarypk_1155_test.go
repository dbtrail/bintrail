package reconstruct

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// The fixtures below are not invented. Every (source bytes → pk_values) pair
// was read off a real MySQL 8.0.46 ROW binlog through the production parser
// while preparing #1155:
//
//	source BINARY(16) 0B2815CC3C200FF7C010203040506000  → pk_values 0x0B2815CC3C200FF7C0102030405060
//	source BINARY(16) 11223344556677889900AABB00000000  → pk_values 0x11223344556677889900AABB
//	source BINARY(16) 41420000000000000000000000000000  → pk_values AB
//	source VARBINARY  AABB0000                          → pk_values 0xAABB0000
//
// They encode the three facts this fix turns on: a fixed BINARY(n) image is
// stripped of EVERY trailing 0x00 byte, VARBINARY is not stripped at all, and
// the spelling is chosen by CONTENT (row three is valid UTF-8, so it is stored
// as plain text with no 0x prefix). TestBinaryPKBaselineJoin_endToEnd re-derives
// all of it from a live server; these cases pin it without needing one.

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex fixture %q: %v", s, err)
	}
	return b
}

// TestCanonicalizePKValue_binaryFamilyMatchesIndexerSpelling is the core of
// #1155: a baseline row's binary PK, canonicalized and then run through the
// SAME event.BuildPKValues the indexer used at capture, must reproduce the
// stored binlog_events.pk_values byte for byte. Anything else and the change
// map silently misses every row.
func TestCanonicalizePKValue_binaryFamilyMatchesIndexerSpelling(t *testing.T) {
	cases := []struct {
		name string
		col  metadata.ColumnMeta
		// baselineBytes is what DuckDB scans out of the baseline Parquet:
		// the full stored column value, padding included.
		baselineBytes []byte
		wantPKValues  string
	}{
		{
			name:          "binary(16) with one trailing zero byte",
			col:           metadata.ColumnMeta{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true},
			baselineBytes: mustHex(t, "0B2815CC3C200FF7C010203040506000"),
			wantPKValues:  "0x0B2815CC3C200FF7C0102030405060",
		},
		{
			name:          "binary(16) with four trailing zero bytes",
			col:           metadata.ColumnMeta{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true},
			baselineBytes: mustHex(t, "11223344556677889900AABB00000000"),
			wantPKValues:  "0x11223344556677889900AABB",
		},
		{
			// Content-gated, not type-gated: the surviving bytes are valid
			// UTF-8, so formatPKValue stores the text and NOT a 0x literal.
			name:          "binary(16) whose payload is printable ASCII",
			col:           metadata.ColumnMeta{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true},
			baselineBytes: mustHex(t, "41420000000000000000000000000000"),
			wantPKValues:  "AB",
		},
		{
			name:          "binary(16) with no padding at all",
			col:           metadata.ColumnMeta{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true},
			baselineBytes: mustHex(t, "B2815CC3C200FF7C0102030405060780"),
			wantPKValues:  "0xB2815CC3C200FF7C0102030405060780",
		},
		{
			// The asymmetry that makes this a per-type decision: trailing
			// 0x00 in a VARBINARY is data, and the ROW image keeps it.
			name:          "varbinary keeps its trailing zero bytes",
			col:           metadata.ColumnMeta{Name: "k", DataType: "varbinary", ColumnType: "varbinary(16)", IsPK: true},
			baselineBytes: mustHex(t, "AABB0000"),
			wantPKValues:  "0xAABB0000",
		},
		{
			name:          "blob keeps its trailing zero bytes",
			col:           metadata.ColumnMeta{Name: "k", DataType: "tinyblob", ColumnType: "tinyblob", IsPK: true},
			baselineBytes: mustHex(t, "CC000000"),
			wantPKValues:  "0xCC000000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !supportedPKType(tc.col.DataType) {
				t.Fatalf("supportedPKType(%q) = false — verify would report this table inconclusive "+
					"and the merge would never reach the canonicalizer", tc.col.DataType)
			}
			pkCols := []metadata.ColumnMeta{tc.col}
			row := map[string]any{tc.col.Name: tc.baselineBytes, "val": "payload"}

			pkMap, err := canonicalizePKMap(row, pkCols)
			if err != nil {
				t.Fatalf("canonicalizePKMap: %v", err)
			}
			// This is the production join point (fulltable.go): the
			// canonicalized map goes straight into the indexer's own encoder.
			if got := event.BuildPKValues(pkCols, pkMap); got != tc.wantPKValues {
				t.Errorf("pk_values spelling = %q, want %q — the baseline row will not join its binlog events", got, tc.wantPKValues)
			}
			// The source map must survive untouched: fulltable emits rowMap,
			// not pkMap, and a mutated PK column would corrupt the output row.
			if got := row[tc.col.Name].([]byte); len(got) != len(tc.baselineBytes) {
				t.Errorf("canonicalizePKMap mutated the source row: PK is now %d bytes, want %d", len(got), len(tc.baselineBytes))
			}
		})
	}
}

// TestCanonicalizePKValue_binaryFamilyRejectsNonBytes pins the defense-in-depth
// arm: a binary PK column that scans back as something other than bytes means
// the schema snapshot and the Parquet disagree, and guessing would produce a
// key that silently matches nothing.
func TestCanonicalizePKValue_binaryFamilyRejectsNonBytes(t *testing.T) {
	for _, dt := range []string{"binary", "varbinary", "blob", "mediumblob", "longblob", "tinyblob"} {
		col := metadata.ColumnMeta{Name: "k", DataType: dt, IsPK: true}
		if _, err := canonicalizePKValue(int64(42), col); err == nil {
			t.Errorf("%s: canonicalizePKValue accepted an int64 for a binary-family PK column", dt)
		}
	}
}

// TestCanonicalizePKValue_binaryFamilyAcceptsString covers a value that reached
// us through a text round-trip rather than DuckDB's native BLOB scan.
func TestCanonicalizePKValue_binaryFamilyAcceptsString(t *testing.T) {
	col := metadata.ColumnMeta{Name: "k", DataType: "binary", ColumnType: "binary(4)", IsPK: true}
	got, err := canonicalizePKValue("AB\x00\x00", col)
	if err != nil {
		t.Fatalf("canonicalizePKValue: %v", err)
	}
	if string(got.([]byte)) != "AB" {
		t.Errorf("canonicalizePKValue = %q, want %q", got, "AB")
	}
}

// TestSupportedPKType_binaryFamily pins both halves of the scope decision:
// the binary family is in (#1155), and the types with unresolved upstream
// representation questions stay out.
func TestSupportedPKType_binaryFamily(t *testing.T) {
	for _, dt := range []string{"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"} {
		if !supportedPKType(dt) {
			t.Errorf("supportedPKType(%q) = false, want true (#1155)", dt)
		}
	}
	for _, dt := range []string{"bit", "json", "geometry", "point", "vector"} {
		if supportedPKType(dt) {
			t.Errorf("supportedPKType(%q) = true, want false — canonicalizePKValue has no branch for it, "+
				"so the merge would error out mid-table instead of reporting inconclusive up front", dt)
		}
	}
}

// TestTrimFixedBinaryPad covers the exported helper the CLI's inverse padding
// has to agree with.
func TestTrimFixedBinaryPad(t *testing.T) {
	cases := []struct{ in, want string }{
		{"AABB0000", "AABB"},
		{"AABB", "AABB"},
		{"00AABB", "00AABB"}, // leading zeros are data
		{"00000000", ""},     // an all-zero key trims to empty, faithfully
		{"", ""},
	}
	for _, tc := range cases {
		got := hex.EncodeToString(TrimFixedBinaryPad(mustHex(t, tc.in)))
		if !strings.EqualFold(got, tc.want) {
			t.Errorf("TrimFixedBinaryPad(%s) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// TestReadBaselineRow_binaryPKHexSpelling is the second half of #1155: the
// single-row lookup (`reconstruct --pk`, the console, the MCP reconstruct tool)
// binds the pk_values spelling as a query parameter. Before the fix the "0x…"
// form was bound as six literal characters against a BLOB column and matched
// nothing, so a BINARY(16) UUID row could never be reconstructed.
func TestReadBaselineRow_binaryPKHexSpelling(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "txt", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "v", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	path := filepath.Join(dir, "b_pk.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// mydumper --hex-blob renders a binary column as 0x<hex>; the writer
	// decodes it, so the Parquet holds the raw bytes.
	rows := [][]string{
		{"0xB2815CC3C200FF7C0102030405060780", "plain", "hit"},
		{"0x4142", "0x4142", "ascii"},
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The spelling an operator copies out of binlog_events.pk_values.
	row, err := ReadBaselineRow(ctx, path, map[string]string{"k": "0xB2815CC3C200FF7C0102030405060780"}, nil)
	if err != nil {
		t.Fatalf("ReadBaselineRow: %v", err)
	}
	if row == nil {
		t.Fatal("no baseline row for the 0x… PK spelling — this is #1155: the value was bound as text against a BLOB column")
	}
	if got := row["v"]; got != "hit" {
		t.Errorf("matched the wrong row: v=%v", got)
	}

	// Lowercase must resolve too — an operator pasting a key from another tool
	// should not silently get "row not found".
	row, err = ReadBaselineRow(ctx, path, map[string]string{"k": "0xb2815cc3c200ff7c0102030405060780"}, nil)
	if err != nil {
		t.Fatalf("ReadBaselineRow (lowercase): %v", err)
	}
	if row == nil {
		t.Error("lowercase 0x… spelling did not resolve")
	}

	// Control: a VARCHAR column whose VALUE reads "0x4142" must still be
	// matched as literal text. Decoding by value shape alone (rather than by
	// the column's actual Parquet type) would silently look for the two bytes
	// {0x41,0x42} here and return the wrong answer.
	row, err = ReadBaselineRow(ctx, path, map[string]string{"txt": "0x4142"}, nil)
	if err != nil {
		t.Fatalf("ReadBaselineRow (varchar control): %v", err)
	}
	if row == nil || row["v"] != "ascii" {
		t.Errorf("a VARCHAR value that merely looks like a hex literal must match as text, got %v", row)
	}

	// Control: a bytes-valued key that is NOT present must still miss. Guards
	// against a decode that accidentally widens the predicate.
	row, err = ReadBaselineRow(ctx, path, map[string]string{"k": "0xDEADBEEF"}, nil)
	if err != nil {
		t.Fatalf("ReadBaselineRow (absent): %v", err)
	}
	if row != nil {
		t.Errorf("absent binary PK matched a row: %v", row)
	}

	// Column names are case-insensitive everywhere else in the chain — DuckDB
	// resolves the quoted identifier regardless of case — so the BLOB probe
	// must be too. Keyed exactly, a differently-cased --pk-columns binds the
	// hex as text against a BLOB and silently misses: the one link that cares
	// about case, on the one PK type this change added.
	row, err = ReadBaselineRow(ctx, path, map[string]string{"K": "0xB2815CC3C200FF7C0102030405060780"}, nil)
	if err != nil {
		t.Fatalf("ReadBaselineRow (mixed-case column): %v", err)
	}
	if row == nil {
		t.Error("a differently-cased column name did not resolve a binary PK, while every other type resolves it")
	}
}

// TestReadBaselineRow_binaryHexTextSymmetry pins the property that makes the
// filter's type-gated decode safe, and it lives in ANOTHER package, which is
// why it needs a test rather than a comment.
//
// The decode looks asymmetric with the encoder: event.formatPKValue gates on
// CONTENT (valid UTF-8 → stored verbatim), the filter gates on the COLUMN
// TYPE. That reads as though a binary column whose bytes are the ASCII text
// "0x<even-hex>" would be stranded — stored as text, searched for as bytes.
//
// It is not, because internal/baseline's decodeBinaryLiteral decodes the same
// literal on the way IN. The baseline therefore cannot hold those characters
// as characters, and the ambiguity resolves the same way at both ends. If a
// future change made the writer preserve the text (say, by threading dump
// provenance through), this test fails and the filter needs the matching
// fallback — which is the signal it exists to give.
func TestReadBaselineRow_binaryHexTextSymmetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cols := []baseline.Column{
		{Name: "k", MySQLType: "varbinary", ParquetType: baseline.MysqlToParquetNode("varbinary")},
		{Name: "v", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	path := filepath.Join(dir, "vb.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"0xDEADBEEF", "hit"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rows, err := ReadBaselineRows(ctx, path, nil, 0)
	if err != nil {
		t.Fatalf("ReadBaselineRows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("wrote 1 row, read %d", len(rows))
	}
	stored, ok := rows[0]["k"].([]byte)
	if !ok {
		t.Fatalf("binary column scanned as %T, want []byte", rows[0]["k"])
	}
	if len(stored) != 4 {
		t.Fatalf("the writer stored %d bytes (%X) for the literal \"0xDEADBEEF\", want the 4 DECODED bytes — "+
			"if the writer now preserves the text, bindFilterArgs must gain a text-binding fallback or such a "+
			"key becomes unfindable", len(stored), stored)
	}

	// And the round trip closes: the same spelling an operator types resolves.
	row, err := ReadBaselineRow(ctx, path, map[string]string{"k": "0xDEADBEEF"}, nil)
	if err != nil {
		t.Fatalf("ReadBaselineRow: %v", err)
	}
	if row == nil || row["v"] != "hit" {
		t.Errorf("0x… spelling did not resolve the row the writer produced from that same literal: %v", row)
	}
}
