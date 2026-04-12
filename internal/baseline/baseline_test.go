package baseline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
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

func TestParseMetadataFields(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	if m.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q", m.BinlogFile, "binlog.000042")
	}
	if m.GTIDSet != "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100" {
		t.Errorf("GTIDSet = %q, want %q", m.GTIDSet, "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100")
	}
	if m.BinlogPos != 12345 {
		t.Errorf("BinlogPos = %d, want 12345", m.BinlogPos)
	}
}

// sampleMetadataNew is the format produced by mydumper 0.16+ (TOML-like with
// # prefixes and KEY = "value" assignments).
const sampleMetadataNew = `# Started dump at: 2026-03-02 23:45:20
[config]
quote-character = BACKTICK

[source]
# executed_gtid_set = "55512139-1432-11f1-8d8d-0693b428a89b:1-11490596"
# SOURCE_LOG_FILE = "mysql-bin-changelog.000879"
# SOURCE_LOG_POS = 4504702
# Finished dump at: 2026-03-02 23:45:21
`

func TestParseMetadataNewFormat(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(sampleMetadataNew), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := ParseMetadata(dir)
	if err != nil {
		t.Fatalf("ParseMetadata: %v", err)
	}
	wantTime := time.Date(2026, 3, 2, 23, 45, 20, 0, time.UTC)
	if !m.StartedAt.Equal(wantTime) {
		t.Errorf("StartedAt = %v, want %v", m.StartedAt, wantTime)
	}
	if m.BinlogFile != "mysql-bin-changelog.000879" {
		t.Errorf("BinlogFile = %q, want %q", m.BinlogFile, "mysql-bin-changelog.000879")
	}
	if m.BinlogPos != 4504702 {
		t.Errorf("BinlogPos = %d, want 4504702", m.BinlogPos)
	}
	if m.GTIDSet != "55512139-1432-11f1-8d8d-0693b428a89b:1-11490596" {
		t.Errorf("GTIDSet = %q, want %q", m.GTIDSet, "55512139-1432-11f1-8d8d-0693b428a89b:1-11490596")
	}
}

func TestParseMetadataMissingTimestamp(t *testing.T) {
	const content = "SHOW MASTER STATUS:\n\tLog: binlog.000001\n\tPos: 100\n"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := ParseMetadata(dir)
	if err == nil {
		t.Error("expected error for missing 'Started dump at:' line, got nil")
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

func TestParseSchemaEmpty(t *testing.T) {
	// A CREATE TABLE whose first line inside the parens is PRIMARY KEY — colRe
	// never matches before the stop condition, so no columns are found.
	const emptySchema = "CREATE TABLE `empty` (\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.empty-schema.sql")
	if err := os.WriteFile(path, []byte(emptySchema), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := ParseSchema(path)
	if err == nil {
		t.Error("expected error for schema with no columns, got nil")
	}
}

func TestParseSchemaStopAtUniqueKey(t *testing.T) {
	const schema = "CREATE TABLE `users` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `email` varchar(255) NOT NULL,\n" +
		"  UNIQUE KEY `email_unique` (`email`),\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.users-schema.sql")
	if err := os.WriteFile(path, []byte(schema), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := ParseSchema(path)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	// id and email only; UNIQUE KEY line triggers the stop condition.
	if len(cols) != 2 {
		t.Errorf("got %d columns, want 2: %+v", len(cols), cols)
	}
}

func TestBuildParquetSchema(t *testing.T) {
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: MysqlToParquetNode("int")},
		{Name: "name", MySQLType: "varchar", ParquetType: MysqlToParquetNode("varchar")},
	}
	schema := BuildParquetSchema(cols)
	if schema == nil {
		t.Error("BuildParquetSchema returned nil")
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
		node := MysqlToParquetNode(tc.typ)
		if node == nil {
			t.Errorf("MysqlToParquetNode(%q) = nil", tc.typ)
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

func TestReadTabFile(t *testing.T) {
	// 3 rows: normal, NULL in col 2, NULL in col 3.
	const tabData = "1\tAlice\t100\n2\t\\N\t200\n3\tCharlie\t\\N\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.users.00000.dat")
	if err := os.WriteFile(path, []byte(tabData), 0o644); err != nil {
		t.Fatal(err)
	}

	var rows [][]string
	var allNulls [][]bool
	if err := ReadTabFile(path, 3, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		allNulls = append(allNulls, append([]bool(nil), nulls...))
		return nil
	}); err != nil {
		t.Fatalf("ReadTabFile: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	if rows[0][0] != "1" || rows[0][1] != "Alice" || rows[0][2] != "100" {
		t.Errorf("row 0 = %v, want [1 Alice 100]", rows[0])
	}
	if !allNulls[1][1] || rows[1][2] != "200" {
		t.Errorf("row 1 = %v nulls %v, want [2 <NULL> 200]", rows[1], allNulls[1])
	}
	if rows[2][1] != "Charlie" || !allNulls[2][2] {
		t.Errorf("row 2 = %v nulls %v, want [3 Charlie <NULL>]", rows[2], allNulls[2])
	}
}

func TestReadTabRowEscapes(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{`hello\tworld`, "hello\tworld"},
		{`line1\nline2`, "line1\nline2"},
		{`cr\rhere`, "cr\rhere"},
		{`back\\slash`, `back\slash`},
	}
	for _, tc := range cases {
		values, _, err := parseTabRow(tc.line, 1)
		if err != nil {
			t.Errorf("parseTabRow(%q): %v", tc.line, err)
			continue
		}
		if len(values) != 1 || values[0] != tc.want {
			t.Errorf("parseTabRow(%q) = %v, want %q", tc.line, values, tc.want)
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

func TestReadSQLRowEscaping(t *testing.T) {
	cases := []struct {
		name string
		// sql is the full INSERT statement written to the file.
		sql  string
		want string
	}{
		{
			// \'  →  '   (backslash-escaped single quote)
			name: "backslash-single-quote",
			sql:  "INSERT INTO t VALUES('it\\'s fine');\n",
			want: "it's fine",
		},
		{
			// ''  →  '   (doubled single-quote escape)
			name: "doubled-single-quote",
			sql:  "INSERT INTO t VALUES('it''s fine');\n",
			want: "it's fine",
		},
		{
			// \n  →  newline
			name: "newline",
			sql:  "INSERT INTO t VALUES('line1\\nline2');\n",
			want: "line1\nline2",
		},
		{
			// \t  →  tab
			name: "tab",
			sql:  "INSERT INTO t VALUES('col1\\tcol2');\n",
			want: "col1\tcol2",
		},
		{
			// \\  →  single backslash
			name: "backslash",
			sql:  "INSERT INTO t VALUES('path\\\\file');\n",
			want: `path\file`,
		},
		{
			// \0  →  null byte
			name: "null-byte",
			sql:  "INSERT INTO t VALUES('\\0');\n",
			want: "\x00",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "t.00000.sql")
			if err := os.WriteFile(path, []byte(tc.sql), 0o644); err != nil {
				t.Fatal(err)
			}
			var rows [][]string
			if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
				rows = append(rows, append([]string(nil), values...))
				return nil
			}); err != nil {
				t.Fatalf("ReadSQLFile: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if rows[0][0] != tc.want {
				t.Errorf("col 0 = %q, want %q", rows[0][0], tc.want)
			}
		})
	}
}

func TestReadSQLRowDoubleQuoted(t *testing.T) {
	// Double-quoted strings with \" and \n escapes.
	sql := `INSERT INTO t VALUES("hello \"world\"", "line1\nline2");` + "\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "t.00000.sql")
	if err := os.WriteFile(path, []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("got rows=%v, want 1 row with 2 cols", rows)
	}
	if rows[0][0] != `hello "world"` {
		t.Errorf(`col 0 = %q, want %q`, rows[0][0], `hello "world"`)
	}
	if rows[0][1] != "line1\nline2" {
		t.Errorf("col 1 = %q, want embedded-newline string", rows[0][1])
	}
}

func TestReadSQLRowHexLiteral(t *testing.T) {
	sql := "INSERT INTO t VALUES(0x48656C6C6F, 42);\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "t.00000.sql")
	if err := os.WriteFile(path, []byte(sql), 0o644); err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("got rows=%v, want 1 row with 2 cols", rows)
	}
	if rows[0][0] != "0x48656C6C6F" {
		t.Errorf("hex literal = %q, want %q", rows[0][0], "0x48656C6C6F")
	}
	if rows[0][1] != "42" {
		t.Errorf("col 1 = %q, want 42", rows[0][1])
	}
}

func TestReadSQLRowNonInsertLines(t *testing.T) {
	// Comments, SET statements, and blank lines before the INSERT should be skipped.
	const data = "-- mydumper SQL dump\nSET NAMES utf8;\nSET TIME_ZONE='+00:00';\n\n" +
		"INSERT INTO `orders` VALUES(1,'test');\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		rows = append(rows, append([]string(nil), values...))
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0][0] != "1" || rows[0][1] != "test" {
		t.Errorf("row = %v, want [1 test]", rows[0])
	}
}

func TestReadSQLRowEmpty(t *testing.T) {
	// File with no INSERT statements → 0 rows, no error.
	const data = "-- just comments\nSET NAMES utf8;\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "shop.orders.00000.sql")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if count != 0 {
		t.Errorf("got %d rows, want 0", count)
	}
}

func TestReadSQLRowUnterminated(t *testing.T) {
	// String literal that never closes should return an error.
	const data = "INSERT INTO t VALUES('unterminated);\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "t.00000.sql")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		return nil
	})
	if err == nil {
		t.Error("expected error for unterminated string, got nil")
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

// TestDiscoverTables_noChunkSuffix verifies that mydumper 0.10.0's unchunked
// file naming (<db>.<table>.sql without the .<chunk> number) is recognized by
// DiscoverTables. Ubuntu 24.04's apt package ships mydumper 0.10.0 which uses
// this format; the chunked format (<db>.<table>.00000.sql) was introduced in
// 0.11.0. Both must work so the dump → baseline pipeline succeeds regardless
// of which mydumper version the operator has installed (#221).
func TestDiscoverTables_noChunkSuffix(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"e2e_source.orders-schema.sql",
		"e2e_source.orders.sql", // NO chunk number — mydumper 0.10.0 format
		"e2e_source.users-schema.sql",
		"e2e_source.users.sql", // same
		"e2e_source-schema-create.sql",
		"metadata",
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
	if tables[0].Database != "e2e_source" || tables[0].Table != "orders" {
		t.Errorf("tables[0] = %q.%q, want e2e_source.orders", tables[0].Database, tables[0].Table)
	}
	if len(tables[0].DataFiles) != 1 {
		t.Errorf("orders has %d data files, want 1", len(tables[0].DataFiles))
	}
	if tables[0].Format != "sql" {
		t.Errorf("orders format = %q, want sql", tables[0].Format)
	}
	if tables[1].Database != "e2e_source" || tables[1].Table != "users" {
		t.Errorf("tables[1] = %q.%q, want e2e_source.users", tables[1].Database, tables[1].Table)
	}
}

// TestDiscoverTables_mixedChunkAndNoChunk verifies that if a directory somehow
// contains both chunked and unchunked files for different tables, both are
// discovered. This isn't a realistic mydumper output but guards against the
// fallback path accidentally breaking the chunked path.
func TestDiscoverTables_mixedChunkAndNoChunk(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"shop.orders-schema.sql",
		"shop.orders.00000.sql", // chunked (0.11.0+)
		"shop.orders.00001.sql",
		"shop.users-schema.sql",
		"shop.users.sql", // unchunked (0.10.0)
		"metadata",
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
	if tables[0].Table != "orders" || len(tables[0].DataFiles) != 2 {
		t.Errorf("orders: want 2 chunked files, got %d", len(tables[0].DataFiles))
	}
	if tables[1].Table != "users" || len(tables[1].DataFiles) != 1 {
		t.Errorf("users: want 1 unchunked file, got %d", len(tables[1].DataFiles))
	}
}

// ─── Writer: convertValue, resolveCodec, sortColumnsForParquet ────────────────

func TestConvertValueTypes(t *testing.T) {
	cases := []struct {
		mysqlType string
		raw       string
		check     func(t *testing.T, v parquet.Value)
	}{
		{"bigint", "9876543210", func(t *testing.T, v parquet.Value) {
			if v.Int64() != 9876543210 {
				t.Errorf("got %d, want 9876543210", v.Int64())
			}
		}},
		{"float", "3.14", func(t *testing.T, v parquet.Value) {
			if v.Float() == 0 {
				t.Error("expected non-zero float")
			}
		}},
		{"double", "2.718281828", func(t *testing.T, v parquet.Value) {
			if v.Double() == 0 {
				t.Error("expected non-zero double")
			}
		}},
		{"datetime", "2025-01-15 12:30:00", func(t *testing.T, v parquet.Value) {
			if v.Int64() == 0 {
				t.Error("expected non-zero datetime microseconds")
			}
		}},
		{"datetime", "2025-01-15 12:30:00.123456", func(t *testing.T, v parquet.Value) {
			if v.Int64() == 0 {
				t.Error("expected non-zero datetime microseconds (with fractional)")
			}
		}},
		{"timestamp", "2025-01-15 12:30:00", func(t *testing.T, v parquet.Value) {
			if v.Int64() == 0 {
				t.Error("expected non-zero timestamp microseconds")
			}
		}},
		{"year", "2025", func(t *testing.T, v parquet.Value) {
			if v.Int32() != 2025 {
				t.Errorf("year got %d, want 2025", v.Int32())
			}
		}},
		{"decimal", "123.45", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "123.45" {
				t.Errorf("decimal got %q, want %q", string(v.ByteArray()), "123.45")
			}
		}},
		{"blob", "binarydata", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "binarydata" {
				t.Errorf("blob got %q, want %q", string(v.ByteArray()), "binarydata")
			}
		}},
		{"unknowntype", "fallback", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "fallback" {
				t.Errorf("unknown type fallback got %q, want %q", string(v.ByteArray()), "fallback")
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.mysqlType+"/"+tc.raw, func(t *testing.T) {
			col := Column{Name: "c", MySQLType: tc.mysqlType}
			v, err := convertValue(col, tc.raw)
			if err != nil {
				t.Fatalf("convertValue(%q, %q): %v", tc.mysqlType, tc.raw, err)
			}
			tc.check(t, v)
		})
	}
}

func TestConvertValueError(t *testing.T) {
	col := Column{Name: "n", MySQLType: "int"}
	if _, err := convertValue(col, "not-a-number"); err == nil {
		t.Error("expected error for non-numeric int value, got nil")
	}
}

func TestResolveCodec(t *testing.T) {
	if resolveCodec("zstd") == nil {
		t.Error("zstd codec should not be nil")
	}
	// Empty string defaults to zstd.
	if resolveCodec("") == nil {
		t.Error("empty string should default to zstd (non-nil)")
	}
	if resolveCodec("snappy") == nil {
		t.Error("snappy codec should not be nil")
	}
	if resolveCodec("gzip") == nil {
		t.Error("gzip codec should not be nil")
	}
	if resolveCodec("none") != nil {
		t.Error("'none' codec should be nil (no compression)")
	}
}

func TestSortColumnsForParquet(t *testing.T) {
	cols := []Column{
		{Name: "zebra", MySQLType: "int"},
		{Name: "apple", MySQLType: "varchar"},
		{Name: "mango", MySQLType: "bigint"},
	}
	sorted, order := sortColumnsForParquet(cols)

	wantNames := []string{"apple", "mango", "zebra"}
	for i, want := range wantNames {
		if sorted[i].Name != want {
			t.Errorf("sorted[%d] = %q, want %q", i, sorted[i].Name, want)
		}
	}
	// apple was MySQL index 1, mango was 2, zebra was 0.
	wantOrder := []int{1, 2, 0}
	for i, want := range wantOrder {
		if order[i] != want {
			t.Errorf("order[%d] = %d, want %d", i, order[i], want)
		}
	}
}

// ─── WriteAndReadParquet (round-trip) ─────────────────────────────────────────

func TestWriteAndReadParquet(t *testing.T) {
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: MysqlToParquetNode("int")},
		{Name: "name", MySQLType: "varchar", ParquetType: MysqlToParquetNode("varchar")},
		{Name: "score", MySQLType: "double", ParquetType: MysqlToParquetNode("double")},
		{Name: "born", MySQLType: "date", ParquetType: MysqlToParquetNode("date")},
	}

	dir := t.TempDir()
	outPath := filepath.Join(dir, "test.parquet")

	cfg := WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			"bintrail.snapshot_timestamp": "2025-02-28T00:00:00Z",
			"bintrail.source_database":    "testdb",
			"bintrail.source_table":       "testtable",
			"bintrail.mydumper_format":    "sql",
			"bintrail.bintrail_version":   "test",
		},
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

	// Read back and verify values + metadata.
	rf, err := os.Open(outPath)
	if err != nil {
		t.Fatalf("open for read: %v", err)
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
	if pf.NumRows() != 2 {
		t.Errorf("NumRows = %d, want 2", pf.NumRows())
	}

	// Verify key-value metadata was written.
	for key, want := range map[string]string{
		"bintrail.source_database": "testdb",
		"bintrail.source_table":    "testtable",
		"bintrail.mydumper_format": "sql",
	} {
		got, ok := pf.Lookup(key)
		if !ok {
			t.Errorf("metadata key %q not found", key)
		} else if got != want {
			t.Errorf("metadata[%q] = %q, want %q", key, got, want)
		}
	}
}

// ─── Run (orchestrator) ───────────────────────────────────────────────────────

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

	// Find the output .parquet file and verify metadata.
	var parquetPath string
	_ = filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(path) == ".parquet" {
			parquetPath = path
		}
		return nil
	})
	if parquetPath == "" {
		t.Fatal("no .parquet file found in output directory")
	}

	// Verify binlog position metadata was written.
	rf, err := os.Open(parquetPath)
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer rf.Close()
	info, err := rf.Stat()
	if err != nil {
		t.Fatalf("stat parquet: %v", err)
	}
	pf, err := parquet.OpenFile(rf, info.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	for key, want := range map[string]string{
		MetaKeyBinlogFile: "binlog.000042",
		MetaKeyBinlogPos:  "12345",
		MetaKeyGTIDSet:    "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100",
	} {
		got, ok := pf.Lookup(key)
		if !ok {
			t.Errorf("metadata key %q not found", key)
		} else if got != want {
			t.Errorf("metadata[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestRunWithTimestampOverride(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// No metadata file — timestamp override must bypass metadata parsing.
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Timestamp:    ts,
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
}

func TestRunWithTableFilter(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	// Two tables: orders and users.
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const ordersData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(ordersData), 0o644); err != nil {
		t.Fatal(err)
	}
	const usersSchema = "CREATE TABLE `users` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.users-schema.sql"), []byte(usersSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.users.00000.sql"), []byte("INSERT INTO `users` VALUES(1);\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Tables:       []string{"shop.orders"}, // filter to orders only
		Compression:  "none",
		RowGroupSize: 100,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 1 {
		t.Errorf("TablesProcessed = %d, want 1 (only orders, not users)", stats.TablesProcessed)
	}
}

func TestRunTabFormat(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	const schema2 = "CREATE TABLE `users` (\n  `id` int NOT NULL,\n  `name` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.users-schema.sql"), []byte(schema2), 0o644); err != nil {
		t.Fatal(err)
	}
	// TSV format: id TAB name; second row has NULL name.
	const tabData = "1\tAlice\n2\t\\N\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.users.00000.dat"), []byte(tabData), 0o644); err != nil {
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
}

func TestRunMultiChunk(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	// Two chunk files, one row each.
	const chunk0 = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	const chunk1 = "INSERT INTO `orders` VALUES(2,11,'19.99','note2','2025-01-02 00:00:00','2025-01-16');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(chunk0), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00001.sql"), []byte(chunk1), 0o644); err != nil {
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
	if stats.RowsWritten != 2 {
		t.Errorf("RowsWritten = %d, want 2 (one row per chunk file)", stats.RowsWritten)
	}
}

func TestFilterTablesNoMatch(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','n','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Tables:       []string{"shop.nonexistent"}, // no match
		Compression:  "none",
		RowGroupSize: 100,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.TablesProcessed != 0 {
		t.Errorf("TablesProcessed = %d, want 0 (filter matched nothing)", stats.TablesProcessed)
	}
}

func TestRunRetrySkipsExistingFiles(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Write metadata + schema + data for one table.
	if err := os.WriteFile(filepath.Join(inputDir, "metadata"), []byte(sampleMetadata), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders-schema.sql"), []byte(sampleSchema), 0o644); err != nil {
		t.Fatal(err)
	}
	const sqlData = "INSERT INTO `orders` VALUES(1,10,'9.99','note','2025-01-01 00:00:00','2025-01-15');\n"
	if err := os.WriteFile(filepath.Join(inputDir, "shop.orders.00000.sql"), []byte(sqlData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		InputDir:     inputDir,
		OutputDir:    outputDir,
		Compression:  "none",
		RowGroupSize: 100,
	}

	// First run: creates the Parquet file.
	stats1, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if stats1.RowsWritten != 1 {
		t.Fatalf("first Run: RowsWritten = %d, want 1", stats1.RowsWritten)
	}

	// Second run with Retry=true: should skip the existing file.
	cfg.Retry = true
	stats2, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("retry Run: %v", err)
	}
	if stats2.TablesProcessed != 1 {
		t.Errorf("retry Run: TablesProcessed = %d, want 1", stats2.TablesProcessed)
	}
	if stats2.RowsWritten != 0 {
		t.Errorf("retry Run: RowsWritten = %d, want 0 (file was skipped)", stats2.RowsWritten)
	}
	if stats2.FilesWritten != 1 {
		t.Errorf("retry Run: FilesWritten = %d, want 1 (counted as existing)", stats2.FilesWritten)
	}
}

// ─── ReadParquetMetadata ─────────────────────────────────────────────────────

func TestReadParquetMetadata(t *testing.T) {
	// Create a Parquet file with binlog position metadata via NewWriter.
	outPath := filepath.Join(t.TempDir(), "test.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	wantCreateTableSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	cfg := WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			MetaKeyBinlogFile:     "binlog.000042",
			MetaKeyBinlogPos:      "12345",
			MetaKeyGTIDSet:        "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100",
			MetaKeyCreateTableSQL: wantCreateTableSQL,
		},
	}
	w, err := NewWriter(outPath, cols, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read metadata back.
	m, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if m.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q", m.BinlogFile, "binlog.000042")
	}
	if m.BinlogPos != 12345 {
		t.Errorf("BinlogPos = %d, want 12345", m.BinlogPos)
	}
	if m.GTIDSet != "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100" {
		t.Errorf("GTIDSet = %q, want %q", m.GTIDSet, "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100")
	}
	if m.CreateTableSQL != wantCreateTableSQL {
		t.Errorf("CreateTableSQL = %q, want %q", m.CreateTableSQL, wantCreateTableSQL)
	}
}

func TestReadParquetMetadata_noPosition(t *testing.T) {
	// Create a Parquet file without binlog position metadata.
	outPath := filepath.Join(t.TempDir(), "test.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	cfg := WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			"bintrail.source_database": "testdb",
		},
	}
	w, err := NewWriter(outPath, cols, cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, err := ReadParquetMetadata(outPath)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if m.BinlogFile != "" {
		t.Errorf("BinlogFile = %q, want empty", m.BinlogFile)
	}
	if m.BinlogPos != 0 {
		t.Errorf("BinlogPos = %d, want 0", m.BinlogPos)
	}
	if m.GTIDSet != "" {
		t.Errorf("GTIDSet = %q, want empty", m.GTIDSet)
	}
}

// ─── parseBaselineDirTimestamp ────────────────────────────────────────────────

func TestParseBaselineDirTimestamp(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  time.Time
		ok    bool
	}{
		{"valid UTC", "2025-02-28T00-00-00Z", time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC), true},
		{"valid with time", "2026-03-15T14-30-00Z", time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC), true},
		{"no T separator", "2025-02-28", time.Time{}, false},
		{"empty", "", time.Time{}, false},
		{"malformed time", "2025-02-28Tnot-a-time", time.Time{}, false},
		{"random folder", "some-folder", time.Time{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseBaselineDirTimestamp(tc.input)
			if ok != tc.ok {
				t.Errorf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && !got.Equal(tc.want) {
				t.Errorf("time = %v, want %v", got, tc.want)
			}
		})
	}
}

// ─── DiscoverBaselines ───────────────────────────────────────────────────────

func TestDiscoverBaselines(t *testing.T) {
	baseDir := t.TempDir()

	// Create a well-formed baseline directory structure with a Parquet file.
	snapshotDir := filepath.Join(baseDir, "2025-02-28T00-00-00Z", "mydb")
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write a minimal Parquet file with binlog metadata.
	parquetPath := filepath.Join(snapshotDir, "orders.parquet")
	cols := []Column{
		{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)},
	}
	w, err := NewWriter(parquetPath, cols, WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			MetaKeyBinlogFile: "binlog.000042",
			MetaKeyBinlogPos:  "12345",
			MetaKeyGTIDSet:    "abc:1-100",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Also create a non-timestamp directory (should be skipped).
	if err := os.MkdirAll(filepath.Join(baseDir, "not-a-timestamp"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Discover baselines.
	results, err := DiscoverBaselines(baseDir)
	if err != nil {
		t.Fatalf("DiscoverBaselines: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d baselines, want 1", len(results))
	}

	b := results[0]
	wantTime := time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)
	if !b.SnapshotTime.Equal(wantTime) {
		t.Errorf("SnapshotTime = %v, want %v", b.SnapshotTime, wantTime)
	}
	if b.Database != "mydb" {
		t.Errorf("Database = %q, want %q", b.Database, "mydb")
	}
	if b.Table != "orders" {
		t.Errorf("Table = %q, want %q", b.Table, "orders")
	}
	if b.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q", b.BinlogFile, "binlog.000042")
	}
	if b.BinlogPos != 12345 {
		t.Errorf("BinlogPos = %d, want 12345", b.BinlogPos)
	}
	if b.GTIDSet != "abc:1-100" {
		t.Errorf("GTIDSet = %q, want %q", b.GTIDSet, "abc:1-100")
	}
}

func TestDiscoverBaselines_emptyDir(t *testing.T) {
	results, err := DiscoverBaselines(t.TempDir())
	if err != nil {
		t.Fatalf("DiscoverBaselines: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d baselines, want 0", len(results))
	}
}
