package reconstruct

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/baseline"
)

// TestMydumperWriter_singleChunk covers the happy path: a few rows that all
// fit in one chunk file, plus the schema file, plus the Files() accounting.
func TestMydumperWriter_singleChunk(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "mydb", "orders", []string{"id", "status"}, 0 /* default 256MB */)
	if err != nil {
		t.Fatalf("NewMydumperWriter: %v", err)
	}

	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(32),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	if err := w.WriteSchema(createSQL); err != nil {
		t.Fatalf("WriteSchema: %v", err)
	}

	rows := [][]any{
		{int64(1), "new"},
		{int64(2), "paid"},
		{int64(3), "shipped"},
	}
	for _, r := range rows {
		if err := w.WriteRow(r); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Expect exactly 2 files: schema + one chunk.
	files := w.Files()
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d: %v", len(files), files)
	}
	if files[0] != "mydb.orders-schema.sql" {
		t.Errorf("first file = %q, want mydb.orders-schema.sql", files[0])
	}
	if files[1] != "mydb.orders.00000.sql" {
		t.Errorf("second file = %q, want mydb.orders.00000.sql", files[1])
	}

	// Schema file contents must be byte-identical to what was passed in.
	schemaBytes, err := os.ReadFile(filepath.Join(dir, files[0]))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if string(schemaBytes) != createSQL {
		t.Errorf("schema file differs from input")
	}

	// Chunk file must contain a single multi-row INSERT.
	chunkBytes, err := os.ReadFile(filepath.Join(dir, files[1]))
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	chunk := string(chunkBytes)
	if !strings.HasPrefix(chunk, "INSERT INTO `mydb`.`orders` (`id`, `status`) VALUES\n") {
		t.Errorf("chunk missing INSERT prefix, got:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(1, 'new')") {
		t.Errorf("chunk missing first tuple, got:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(2, 'paid')") {
		t.Errorf("chunk missing second tuple, got:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(3, 'shipped')") {
		t.Errorf("chunk missing third tuple, got:\n%s", chunk)
	}
	if !strings.HasSuffix(chunk, ";\n") {
		t.Errorf("chunk missing terminating semicolon, got tail:\n%q", chunk[len(chunk)-10:])
	}

	// The comma separator between tuples must land between them, not at
	// the start or end.
	if strings.Count(chunk, "),\n(") != 2 {
		t.Errorf("expected 2 tuple separators, got chunk:\n%s", chunk)
	}
}

// TestMydumperWriter_rotateByChunkSize forces chunking with a tiny chunkSize
// to verify rotation happens at the right boundary, new chunk files are
// named with incrementing indexes, and each chunk terminates cleanly.
func TestMydumperWriter_rotateByChunkSize(t *testing.T) {
	dir := t.TempDir()
	// 1-byte chunkSize forces a rotation after every tuple.
	w, err := NewMydumperWriter(dir, "mydb", "users", []string{"id"}, 1)
	if err != nil {
		t.Fatalf("NewMydumperWriter: %v", err)
	}
	_ = w.WriteSchema("-- minimal")

	for i := int64(1); i <= 3; i++ {
		if err := w.WriteRow([]any{i}); err != nil {
			t.Fatalf("WriteRow %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files := w.Files()
	// schema + 3 chunks.
	if len(files) != 4 {
		t.Fatalf("expected 4 files (schema + 3 chunks), got %d: %v", len(files), files)
	}
	wantChunks := []string{"mydb.users.00000.sql", "mydb.users.00001.sql", "mydb.users.00002.sql"}
	for i, want := range wantChunks {
		if files[i+1] != want {
			t.Errorf("chunk[%d] = %q, want %q", i, files[i+1], want)
		}
	}

	// Each chunk must be a complete, terminated INSERT with one tuple.
	for i, name := range wantChunks {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		content := string(b)
		if !strings.HasSuffix(content, ";\n") {
			t.Errorf("chunk %d missing ;\\n terminator: %q", i, content)
		}
		tupleStart := strings.Index(content, "VALUES\n(")
		if tupleStart < 0 {
			t.Errorf("chunk %d missing tuple, content:\n%s", i, content)
		}
	}
}

// TestMydumperWriter_valueTypeMatrix exercises the full set of value types
// that full-table reconstruct will feed through the writer: strings with
// special characters, nil, int64, float64, time.Time, []byte, map[string]any
// (JSON column).
func TestMydumperWriter_valueTypeMatrix(t *testing.T) {
	dir := t.TempDir()
	cols := []string{"c_int", "c_text", "c_null", "c_double", "c_time", "c_blob", "c_json"}
	w, err := NewMydumperWriter(dir, "mydb", "mixed", cols, 0)
	if err != nil {
		t.Fatalf("NewMydumperWriter: %v", err)
	}

	ts := time.Date(2026, 4, 11, 14, 30, 45, 500000000, time.UTC)
	row := []any{
		int64(42),
		"O'Brien\\path",
		nil,
		float64(3.14),
		ts,
		[]byte{0xde, 0xad, 0xbe, 0xef},
		map[string]any{"k": "v"},
	}
	if err := w.WriteRow(row); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "mydb.mixed.00000.sql"))
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	content := string(b)

	wants := []string{
		"42",                             // int64
		`'O\'Brien\\path'`,               // string with escapes
		"NULL",                           // nil
		"3.14",                           // float64
		"'2026-04-11 14:30:45.500000'",   // time.Time
		"X'deadbeef'",                    // []byte
		`'{"k":"v"}'`,                    // map[string]any (JSON column)
	}
	for _, want := range wants {
		if !strings.Contains(content, want) {
			t.Errorf("chunk missing %q, content:\n%s", want, content)
		}
	}
}

// TestMydumperWriter_columnCountMismatch verifies the defensive guard in
// WriteRow that rejects an argument slice whose length does not match the
// declared column count.
func TestMydumperWriter_columnCountMismatch(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "d", "t", []string{"a", "b"}, 0)
	if err != nil {
		t.Fatalf("NewMydumperWriter: %v", err)
	}
	err = w.WriteRow([]any{1})
	if err == nil {
		t.Fatal("expected error for mismatched column count, got nil")
	}
	if !strings.Contains(err.Error(), "columns") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestMydumperWriter_emptyCols rejects a construction with zero columns —
// that would produce an unparseable INSERT statement.
func TestMydumperWriter_emptyCols(t *testing.T) {
	_, err := NewMydumperWriter(t.TempDir(), "d", "t", nil, 0)
	if err == nil {
		t.Fatal("expected error for empty column list, got nil")
	}
}

// TestWriteMetadataFile_roundTrips asserts that the metadata file we write
// can be parsed back by baseline.ParseMetadata (the same parser used for
// mydumper-produced metadata files). This proves our format is compatible
// with the rest of the bintrail toolchain.
func TestWriteMetadataFile_roundTrips(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC)
	if err := WriteMetadataFile(dir, at,
		"3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100",
		"binlog.000042",
		12345,
	); err != nil {
		t.Fatalf("WriteMetadataFile: %v", err)
	}

	m, err := baseline.ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata round-trip: %v", err)
	}
	if m.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want binlog.000042", m.BinlogFile)
	}
	if m.BinlogPos != 12345 {
		t.Errorf("BinlogPos = %d, want 12345", m.BinlogPos)
	}
	if m.GTIDSet != "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100" {
		t.Errorf("GTIDSet = %q", m.GTIDSet)
	}
}
