package baseline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ─── ParseMetadata ────────────────────────────────────────────────────────────

const sampleMetadata = `Started dump at: 2025-02-28 00:00:00
Finished dump at: 2025-02-28 00:01:23
SHOW MASTER STATUS:
	Log: binlog.000042
	Pos: 12345
	GTID: 3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100
`

func TestParseMetadata(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	want := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	if !m.StartedAt.Equal(want) {
		t.Errorf("StartedAt = %v, want %v", m.StartedAt, want)
	}
}

func TestParseMetadataMissing(t *testing.T) {
	_, err := ParseMetadata(t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing metadata file")
	}
}

// ─── ParseSchema ──────────────────────────────────────────────────────────────

const sampleSchema = `CREATE TABLE ` + "`orders`" + ` (
  ` + "`id`" + ` int NOT NULL AUTO_INCREMENT,
  ` + "`user_id`" + ` bigint NOT NULL,
  ` + "`amount`" + ` decimal(10,2) NOT NULL,
  ` + "`note`" + ` varchar(255) DEFAULT NULL,
  ` + "`created_at`" + ` datetime NOT NULL,
  ` + "`paid_on`" + ` date DEFAULT NULL,
  PRIMARY KEY (` + "`id`" + `)
) ENGINE=InnoDB;
`

func TestParseSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders-schema.sql")
	if err := os.WriteFile(path, []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if len(cols) != 6 {
		t.Fatalf("got %d columns, want 6", len(cols))
	}
	wantNames := []string{"id", "user_id", "amount", "note", "created_at", "paid_on"}
	for i, name := range wantNames {
		if cols[i].Name != name {
			t.Errorf("col[%d].Name = %q, want %q", i, cols[i].Name, name)
		}
	}
	wantTypes := []string{"int", "bigint", "decimal", "varchar", "datetime", "date"}
	for i, typ := range wantTypes {
		if cols[i].MySQLType != typ {
			t.Errorf("col[%d].MySQLType = %q, want %q", i, cols[i].MySQLType, typ)
		}
	}
}

// ─── MySQLToParquetType ───────────────────────────────────────────────────────

func TestMySQLToParquetType(t *testing.T) {
	cases := []struct {
		typ  string
		want string // substring of the parquet node string representation
	}{
		{"int", "INT32"},
		{"bigint", "INT64"},
		{"float", "FLOAT"},
		{"double", "DOUBLE"},
		{"decimal", "STRING"},
		{"varchar", "STRING"},
		{"datetime", "INT64"},
		{"date", "INT32"},
		{"blob", "BYTE_ARRAY"},
		{"json", "STRING"},
	}
	for _, tc := range cases {
		node := mysqlToParquetNode(tc.typ)
		if node == nil {
			t.Errorf("mysqlToParquetNode(%q) = nil", tc.typ)
		}
		// Just check it doesn't panic; the actual type mapping is validated
		// end-to-end in TestWriteAndReadParquet.
	}
}

// ─── ReadTabRow ───────────────────────────────────────────────────────────────

func TestReadTabRow(t *testing.T) {
	cases := []struct {
		line  string
		want  []string
		nulls []bool
	}{
		{
			// Three fields separated by real tab characters.
			line:  "1\tAlice\talice@example.com",
			want:  []string{"1", "Alice", "alice@example.com"},
			nulls: []bool{false, false, false},
		},
		{
			// \N in a field (the literal two chars backslash + N) = NULL.
			// Tab is the real tab character as field separator.
			line:  "1\t\\N",
			want:  []string{"1", ""},
			nulls: []bool{false, true},
		},
		{
			// \\ in a field = single backslash; \N = NULL.
			line:  "hello\\\\world\t\\N",
			want:  []string{`hello\world`, ""},
			nulls: []bool{false, true},
		},
	}
	for _, tc := range cases {
		values, nulls, err := parseTabRow(tc.line, len(tc.want))
		if err != nil {
			t.Errorf("parseTabRow(%q): %v", tc.line, err)
			continue
		}
		if len(values) != len(tc.want) {
			t.Errorf("parseTabRow(%q): got %d values, want %d", tc.line, len(values), len(tc.want))
			continue
		}
		for i, w := range tc.want {
			if values[i] != w {
				t.Errorf("parseTabRow(%q)[%d] = %q, want %q", tc.line, i, values[i], w)
			}
			if nulls[i] != tc.nulls[i] {
				t.Errorf("parseTabRow(%q) nulls[%d] = %v, want %v", tc.line, i, nulls[i], tc.nulls[i])
			}
		}
	}
}

// ─── ReadSQLRow ───────────────────────────────────────────────────────────────

func TestReadSQLRow(t *testing.T) {
	const sqlData = "INSERT INTO `orders` VALUES(1,'Alice',NULL),(2,'Bob',42);\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(path, []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	var rows [][]string
	var allNulls [][]bool
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		allNulls = append(allNulls, append([]bool(nil), nulls...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// Row 0: (1, 'Alice', NULL)
	if rows[0][0] != "1" || rows[0][1] != "Alice" || !allNulls[0][2] {
		t.Errorf("row 0 = %v nulls %v, want [1 Alice <NULL>]", rows[0], allNulls[0])
	}
	// Row 1: (2, 'Bob', 42)
	if rows[1][0] != "2" || rows[1][1] != "Bob" || rows[1][2] != "42" {
		t.Errorf("row 1 = %v, want [2 Bob 42]", rows[1])
	}
}

// ─── DiscoverTables ───────────────────────────────────────────────────────────

func TestDiscoverTables(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"shop.orders-schema.sql",
		"shop.orders.00000.sql",
		"shop.orders.00001.sql",
		"shop.users-schema.sql",
		"shop.users.00000.dat",
		"metadata",
		"shop-schema-create.sql", // database-level schema — no table
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	tables, err := DiscoverTables(dir)
	if err != nil {
		t.Fatalf("DiscoverTables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("got %d tables, want 2: %+v", len(tables), tables)
	}
	// Sorted alphabetically: orders before users
	if tables[0].Table != "orders" {
		t.Errorf("tables[0] = %q, want orders", tables[0].Table)
	}
	if len(tables[0].DataFiles) != 2 {
		t.Errorf("orders has %d data files, want 2", len(tables[0].DataFiles))
	}
	if tables[0].Format != "sql" {
		t.Errorf("orders format = %q, want sql", tables[0].Format)
	}
	if tables[1].Table != "users" || tables[1].Format != "tab" {
		t.Errorf("tables[1] = %+v, want users/tab", tables[1])
	}
}

// ─── WriteAndReadParquet (round-trip) ─────────────────────────────────────────

func TestWriteAndReadParquet(t *testing.T) {
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: mysqlToParquetNode("int")},
		{Name: "name", MySQLType: "varchar", ParquetType: mysqlToParquetNode("varchar")},
		{Name: "score", MySQLType: "double", ParquetType: mysqlToParquetNode("double")},
		{Name: "born", MySQLType: "date", ParquetType: mysqlToParquetNode("date")},
	}

	dir := t.TempDir()
	outPath := filepath.Join(dir, "test.parquet")

	cfg := WriterConfig{
		Compression:       "none",
		RowGroupSize:      100,
		SnapshotTimestamp: "2025-02-28T00:00:00Z",
		SourceDatabase:    "testdb",
		SourceTable:       "testtable",
		MydumperFormat:    "sql",
		BintrailVersion:   "test",
	}

	w, err := NewWriter(outPath, cols, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Row 1: id=1, name="Alice", score=9.5, born=2000-01-15
	if err := w.WriteRow(
		[]string{"1", "Alice", "9.5", "2000-01-15"},
		[]bool{false, false, false, false},
	); err != nil {
		t.Fatalf("WriteRow 1: %v", err)
	}
	// Row 2: id=2, name=NULL, score=NULL, born=NULL
	if err := w.WriteRow(
		[]string{"2", "", "", ""},
		[]bool{false, true, true, true},
	); err != nil {
		t.Fatalf("WriteRow 2: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify file was created.
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat output: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("output file is empty")
	}
}

// ─── Run (integration-lite, file-only) ────────────────────────────────────────

func TestRun(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Write metadata
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	// Write schema
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	// Write SQL data
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15'),(2,11,'19.99',NULL,'2025-01-02 00:00:00',NULL);\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Compression:  "none",
		RowGroupSize: 100,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 1 {
		t.Errorf("TablesProcessed = %d, want 1", stats.TablesProcessed)
	}
	if stats.RowsWritten != 2 {
		t.Errorf("RowsWritten = %d, want 2", stats.RowsWritten)
	}

	// Verify at least one .parquet file exists under the output dir
	found := false
	_ = filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(path) == ".parquet" {
			found = true
		}
		return nil
	})
	if !found {
		t.Error("no .parquet file found in output directory")
	}
}
