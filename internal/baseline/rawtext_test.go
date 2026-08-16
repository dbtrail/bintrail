package baseline

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestWriter_RawTextRoundTrip exercises the Column.RawText path used by the
// PostgreSQL baseline producer (#593): values must round-trip byte-identically
// as optional Parquet strings (no MySQL type conversion whatsoever) and NULL
// must stay distinguishable from the empty string.
func TestWriter_RawTextRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.parquet")

	// Deliberately hostile names/values: "id" would parse as int under the
	// MySQL mapping (RawText must bypass it), and the payload column carries
	// tabs, newlines, backslashes, and multibyte UTF-8.
	cols := []Column{
		{Name: "id", RawText: true},
		{Name: "payload", RawText: true},
	}
	w, err := NewWriter(path, cols, WriterConfig{Compression: "zstd", RowGroupSize: 10})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	rows := []struct {
		values []string
		nulls  []bool
	}{
		{[]string{"1", "plain"}, []bool{false, false}},
		{[]string{"2", "tab\tnewline\nback\\slash"}, []bool{false, false}},
		{[]string{"3", ""}, []bool{false, false}}, // empty string, NOT NULL
		{[]string{"4", ""}, []bool{false, true}},  // NULL
		{[]string{"5", "café 日本語 🎉"}, []bool{false, false}},
		{[]string{"00042", "not-an-int-and-that-is-fine"}, []bool{false, false}}, // leading zeros preserved verbatim
	}
	for i, r := range rows {
		if err := w.WriteRow(r.values, r.nulls); err != nil {
			t.Fatalf("WriteRow %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	// Schema: both columns must be OPTIONAL strings regardless of name.
	for _, field := range pf.Schema().Fields() {
		if !field.Optional() {
			t.Errorf("column %q is not optional", field.Name())
		}
		if lt := field.Type().LogicalType(); lt == nil || lt.UTF8 == nil {
			t.Errorf("column %q is not a UTF8 string (logical type %v)", field.Name(), lt)
		}
	}

	reader := parquet.NewReader(pf)
	defer reader.Close()
	out := make([]parquet.Row, len(rows))
	n, _ := reader.ReadRows(out)
	if n != len(rows) {
		t.Fatalf("read %d rows, want %d", n, len(rows))
	}

	// Parquet column order is alphabetical: id(0), payload(1).
	byID := map[string]parquet.Value{}
	for _, r := range out[:n] {
		byID[string(r[0].ByteArray())] = r[1]
	}
	check := func(id, want string) {
		t.Helper()
		v, ok := byID[id]
		if !ok {
			t.Fatalf("row id=%q not found", id)
		}
		if v.IsNull() {
			t.Fatalf("row id=%q payload is NULL, want %q", id, want)
		}
		if got := string(v.ByteArray()); got != want {
			t.Errorf("row id=%q payload = %q, want %q", id, got, want)
		}
	}
	check("1", "plain")
	check("2", "tab\tnewline\nback\\slash")
	check("3", "") // empty string round-trips as a non-NULL empty string
	check("5", "café 日本語 🎉")
	check("00042", "not-an-int-and-that-is-fine")
	if v := byID["4"]; !v.IsNull() {
		t.Errorf("row id=4 payload = %q, want NULL", string(v.ByteArray()))
	}
}
