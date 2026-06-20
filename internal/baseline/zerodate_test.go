package baseline

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestWriteRowZeroDateCarveOut pins the #506 review carve-out: MySQL's all-zero
// date pseudo-NULL (`0000-00-00 00:00:00`, common in legacy tables with that
// DEFAULT) must COMPLETE the baseline as a deliberate NULL plus a once-per-column
// warning — NOT abort the run like a genuinely unrepresentable value would. The
// pre-carve-out blanket fail-loud killed the snapshot of any such legacy table.
func TestWriteRowZeroDateCarveOut(t *testing.T) {
	// Capture slog so we can assert the warn fired and named the column.
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	cols := []Column{
		{Name: "created_dt", MySQLType: "datetime", ParquetType: mysqlToParquetNode("datetime", false)},
		{Name: "created_on", MySQLType: "date", ParquetType: mysqlToParquetNode("date", false)},
		{Name: "created_ts", MySQLType: "timestamp", ParquetType: mysqlToParquetNode("timestamp", false)},
	}

	dir := t.TempDir()
	outPath := filepath.Join(dir, "zerodate.parquet")
	w, err := NewWriter(outPath, cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Two rows, both all-zero across the three forms (datetime full,
	// datetime-with-fraction, date, timestamp). The second row exercises the
	// once-per-column dedup: a per-row warn would flood here.
	rows := [][]string{
		{"0000-00-00 00:00:00", "0000-00-00", "0000-00-00 00:00:00"},
		{"0000-00-00 00:00:00.000000", "0000-00-00", "0000-00-00 00:00:00.000000"},
	}
	for i, r := range rows {
		if err := w.WriteRow(r, []bool{false, false, false}); err != nil {
			t.Fatalf("WriteRow(row %d) returned error, want nil (zero date must NOT abort): %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// (b) Read back: every column of every row must be NULL.
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rf.Close()
	info, err := rf.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	reader := parquet.NewReader(pf)
	defer reader.Close()
	got := make([]parquet.Row, 2)
	n, _ := reader.ReadRows(got)
	if n != 2 {
		t.Fatalf("ReadRows = %d, want 2", n)
	}
	for ri := range got[:n] {
		for ci, v := range got[ri] {
			if !v.IsNull() {
				t.Errorf("row %d col %d: got %v, want NULL", ri, ci, v)
			}
		}
	}

	// (c) The warn fired and named each column — exactly once each (dedup).
	logStr := logBuf.String()
	for _, name := range []string{"created_dt", "created_on", "created_ts"} {
		if c := countOccurrences(logStr, name); c != 1 {
			t.Errorf("column %q appears %d times in warn log, want exactly 1 (once-per-column dedup): %s", name, c, logStr)
		}
	}
}

// TestWriteRowZeroDateStillAbortsGarbage confirms the carve-out is surgical: a
// genuinely-unconvertible date (NOT the zero sentinel) still fails loud, naming
// the column and value. Only the legal pseudo-NULL is carved out.
func TestWriteRowZeroDateStillAbortsGarbage(t *testing.T) {
	cols := []Column{
		{Name: "d", MySQLType: "date", ParquetType: mysqlToParquetNode("date", false)},
	}
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "garbage.parquet"), cols, WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	err = w.WriteRow([]string{"not-a-date"}, []bool{false})
	if err == nil {
		t.Fatal("WriteRow with unconvertible date: got nil, want loud failure (carve-out must not swallow garbage)")
	}
	for _, want := range []string{"d", "not-a-date"} {
		if !contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

// TestWriteRowPartialZeroDateAborts pins that the carve-out is keyed on the
// ALL-zero `0000-00-00` prefix only: a PARTIAL zero date (`2020-00-00`, a real
// year with a zero month, or `2020-05-00 00:00:00`, a zero day) is NOT MySQL's
// pseudo-NULL sentinel — Go's parser rejects it as out-of-range, and that must
// still fail loud naming the column, not get silently carved to NULL.
func TestWriteRowPartialZeroDateAborts(t *testing.T) {
	cases := []struct {
		name      string
		mysqlType string
		raw       string
	}{
		{"date_zero_month", "date", "2020-00-00"},
		{"date_zero_day", "date", "2020-05-00"},
		{"datetime_zero_month", "datetime", "2020-00-00 00:00:00"},
		{"datetime_zero_day", "datetime", "2020-05-00 00:00:00"},
		{"timestamp_zero_day", "timestamp", "2020-05-00 00:00:00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols := []Column{
				{Name: "d", MySQLType: tc.mysqlType, ParquetType: mysqlToParquetNode(tc.mysqlType, false)},
			}
			dir := t.TempDir()
			w, err := NewWriter(filepath.Join(dir, "partial.parquet"), cols, WriterConfig{Compression: "none", RowGroupSize: 100})
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			defer w.Close()

			err = w.WriteRow([]string{tc.raw}, []bool{false})
			if err == nil {
				t.Fatalf("WriteRow(%q) with partial-zero date: got nil, want loud failure (only all-zero is carved out)", tc.raw)
			}
			for _, want := range []string{"d", tc.raw} {
				if !contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err.Error(), want)
				}
			}
		})
	}
}

// TestParseSchemaUnsignedUppercase hardens the colRe case-insensitive group: an
// uppercase UNSIGNED (hand-rolled schema; mydumper emits lowercase) must still
// populate the attribute, not silently fall through to signed.
func TestParseSchemaUnsignedUppercase(t *testing.T) {
	const schema = "CREATE TABLE `t` (\n" +
		"  `a` INT UNSIGNED NOT NULL,\n" +
		"  `b` BIGINT UNSIGNED NOT NULL,\n" +
		"  `c` Int(10) Unsigned NOT NULL,\n" +
		"  PRIMARY KEY (`a`)\n" +
		") ENGINE=InnoDB;\n"

	dir := t.TempDir()
	path := filepath.Join(dir, "shop.t-schema.sql")
	if err := os.WriteFile(path, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("got %d columns, want 3: %+v", len(cols), cols)
	}
	for i, c := range cols {
		if !c.Unsigned {
			t.Errorf("col[%d] (%s) Unsigned = false, want true (uppercase UNSIGNED must match)", i, c.Name)
		}
	}
}

func countOccurrences(s, sub string) int {
	n := 0
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			n++
		}
	}
	return n
}
