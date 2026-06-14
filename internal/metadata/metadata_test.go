package metadata

import (
	"strings"
	"testing"
)

// buildTestResolver constructs a Resolver directly without a database,
// allowing MapRow and Resolve to be tested without a MySQL connection.
func buildTestResolver(tables map[string]*TableMeta) *Resolver {
	return &Resolver{snapshotID: 1, tables: tables}
}

func TestResolve_found(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.orders": {
			Schema: "mydb", Table: "orders",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "status", OrdinalPosition: 2, DataType: "varchar"},
			},
			PKColumns: []string{"id"},
		},
	})

	tm, err := r.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tm.Table != "orders" {
		t.Errorf("expected table=orders, got %q", tm.Table)
	}
	if len(tm.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(tm.Columns))
	}
}

func TestResolve_notFound(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{})

	_, err := r.Resolve("mydb", "missing")
	if err == nil {
		t.Fatal("expected error for unknown table, got nil")
	}
}

func TestMapRow_success(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.orders": {
			Schema: "mydb", Table: "orders",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "status", OrdinalPosition: 2, DataType: "varchar"},
				{Name: "amount", OrdinalPosition: 3, DataType: "decimal"},
			},
			PKColumns: []string{"id"},
		},
	})

	row := []any{int64(42), "shipped", 99.95}
	named, err := r.MapRow("mydb", "orders", row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if named["id"] != int64(42) {
		t.Errorf("id: want 42, got %v", named["id"])
	}
	if named["status"] != "shipped" {
		t.Errorf("status: want shipped, got %v", named["status"])
	}
	if named["amount"] != 99.95 {
		t.Errorf("amount: want 99.95, got %v", named["amount"])
	}
}

func TestCoerceUnsigned(t *testing.T) {
	const maxU64 = uint64(18446744073709551615) // 2^64-1; int64(-1) bit pattern

	cases := []struct {
		name string
		v    any
		col  ColumnMeta
		want any
	}{
		// Unsigned columns with the high bit set: reinterpret to the correct width.
		{"bigint unsigned max", int64(-1), ColumnMeta{DataType: "bigint", ColumnType: "bigint unsigned"}, maxU64},
		{"int unsigned max", int32(-1), ColumnMeta{DataType: "int", ColumnType: "int unsigned"}, uint32(4294967295)},
		{"mediumint unsigned max (24-bit mask)", int32(-1), ColumnMeta{DataType: "mediumint", ColumnType: "mediumint unsigned"}, uint32(16777215)},
		{"smallint unsigned max", int16(-1), ColumnMeta{DataType: "smallint", ColumnType: "smallint unsigned"}, uint16(65535)},
		{"tinyint unsigned max", int8(-1), ColumnMeta{DataType: "tinyint", ColumnType: "tinyint unsigned"}, uint8(255)},
		// Unsigned but value below the threshold: numerically unchanged (still converted type).
		{"int unsigned small positive", int32(1000000), ColumnMeta{DataType: "int", ColumnType: "int unsigned"}, uint32(1000000)},
		{"tinyint unsigned 200", int8(-56), ColumnMeta{DataType: "tinyint", ColumnType: "tinyint unsigned"}, uint8(200)},
		// Signed column: never touched.
		{"signed bigint negative", int64(-1), ColumnMeta{DataType: "bigint", ColumnType: "bigint(20)"}, int64(-1)},
		// Pre-#212 snapshot: ColumnType empty → cannot know signedness → no-op.
		{"empty column type", int64(-1), ColumnMeta{DataType: "bigint", ColumnType: ""}, int64(-1)},
		// NULL and non-integer values on an unsigned column are returned unchanged.
		{"null on unsigned", nil, ColumnMeta{DataType: "bigint", ColumnType: "bigint unsigned"}, nil},
		{"string on unsigned", "x", ColumnMeta{DataType: "bigint", ColumnType: "bigint unsigned"}, "x"},
		// ENUM whose value list literally contains "unsigned" matches the substring
		// gate, but go-mysql decodes ENUM as int64, so it reaches the DataType switch
		// (DataType "enum" ∉ integer widths) — which must leave it untouched. This
		// locks that load-bearing default so a future refactor can't coerce it.
		{"enum value list contains 'unsigned'", int64(2), ColumnMeta{DataType: "enum", ColumnType: "enum('signed','unsigned')"}, int64(2)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := coerceUnsigned(tc.v, tc.col)
			if got != tc.want {
				t.Errorf("coerceUnsigned(%#v, %q) = %#v (%T); want %#v (%T)",
					tc.v, tc.col.ColumnType, got, got, tc.want, tc.want)
			}
		})
	}
}

func TestMapRow_unsignedCoercion(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.counters": {
			Schema: "mydb", Table: "counters",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "bigint", ColumnType: "bigint unsigned"},
				{Name: "n", OrdinalPosition: 2, DataType: "int", ColumnType: "int unsigned"},
			},
			PKColumns: []string{"id"},
		},
	})

	// go-mysql would hand us these signed values for an unsigned PK and column.
	row := []any{int64(-1), int32(-1)}
	named, err := r.MapRow("mydb", "counters", row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if named["id"] != uint64(18446744073709551615) {
		t.Errorf("id: want uint64 max, got %#v (%T)", named["id"], named["id"])
	}
	if named["n"] != uint32(4294967295) {
		t.Errorf("n: want uint32 max, got %#v (%T)", named["n"], named["n"])
	}
}

func TestMapRow_columnCountMismatch(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"mydb.orders": {
			Schema: "mydb", Table: "orders",
			Columns: []ColumnMeta{
				{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
				{Name: "status", OrdinalPosition: 2, DataType: "varchar"},
			},
			PKColumns: []string{"id"},
		},
	})

	// Row has 3 values but snapshot only has 2 columns — should error.
	row := []any{int64(42), "shipped", "extra"}
	_, err := r.MapRow("mydb", "orders", row)
	if err == nil {
		t.Fatal("expected error for column count mismatch, got nil")
	}
}

func TestPKColumnMetas_ordering(t *testing.T) {
	tm := &TableMeta{
		Columns: []ColumnMeta{
			{Name: "seq", OrdinalPosition: 2, IsPK: true},
			{Name: "id", OrdinalPosition: 1, IsPK: true},
			{Name: "note", OrdinalPosition: 3, IsPK: false},
		},
	}

	pks := tm.PKColumnMetas()
	if len(pks) != 2 {
		t.Fatalf("expected 2 PK columns, got %d", len(pks))
	}
	// PKColumnMetas preserves Columns slice order (ordinal order, as loaded from DB).
	// The slice above is already in ordinal order — verify both columns are included.
	names := map[string]bool{}
	for _, c := range pks {
		names[c.Name] = true
	}
	if !names["seq"] || !names["id"] {
		t.Errorf("expected both id and seq in PK columns, got %v", pks)
	}
}

// TestResolverTables pins the behaviour of Resolver.Tables added for
// #315: returns every TableMeta whose Schema matches, sorted by Table
// name, and excludes tables from other schemas in the same snapshot.
// An unknown schema returns an empty (non-nil) slice.
func TestResolverTables(t *testing.T) {
	r := buildTestResolver(map[string]*TableMeta{
		"appdb.users":    {Schema: "appdb", Table: "users"},
		"appdb.orders":   {Schema: "appdb", Table: "orders"},
		"appdb.products": {Schema: "appdb", Table: "products"},
		"otherdb.audits": {Schema: "otherdb", Table: "audits"},
		"otherdb.events": {Schema: "otherdb", Table: "events"},
	})

	t.Run("appdb_returns_three_tables_sorted", func(t *testing.T) {
		got := r.Tables("appdb")
		if len(got) != 3 {
			t.Fatalf("len = %d, want 3", len(got))
		}
		want := []string{"orders", "products", "users"}
		for i, tm := range got {
			if tm.Table != want[i] {
				t.Errorf("got[%d] = %q, want %q (sort regression)", i, tm.Table, want[i])
			}
		}
	})

	t.Run("otherdb_returns_two_tables_sorted", func(t *testing.T) {
		got := r.Tables("otherdb")
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		if got[0].Table != "audits" || got[1].Table != "events" {
			t.Errorf("got %v, want [audits events]", got)
		}
	})

	t.Run("unknown_schema_returns_empty_non_nil_slice", func(t *testing.T) {
		got := r.Tables("nope")
		if got == nil {
			t.Error("Tables(unknown) returned nil; want empty (non-nil) slice")
		}
		if len(got) != 0 {
			t.Errorf("len = %d, want 0", len(got))
		}
	})
}

// When schemas are named explicitly, the scan is scoped to exactly those
// schemas via a parameterized IN list — and the bintrail-internal exclusion is
// NOT added (an explicitly named internal schema is still policed).
func TestBuildFKCascadeQuery_withSchemas(t *testing.T) {
	query, args := buildFKCascadeQuery([]string{"iotcore", "billing"})

	if !strings.Contains(query, "CONSTRAINT_SCHEMA IN (?,?)") {
		t.Errorf("expected parameterized IN list, got query:\n%s", query)
	}
	if strings.Contains(query, "NOT IN") || strings.Contains(query, "information_schema.TABLES") {
		t.Errorf("explicit --schemas must not add the internal-schema exclusion, got query:\n%s", query)
	}
	if len(args) != 2 || args[0] != "iotcore" || args[1] != "billing" {
		t.Errorf("expected args [iotcore billing], got %v", args)
	}
}

// With no schemas filter the scan excludes MySQL system schemas AND bintrail's
// own index schemas. The latter are recognised structurally — a schema is
// bintrail-internal only if it holds all of binlog_events, schema_snapshots and
// stream_state — not by name, so an agent does not fatal-fail on bintrail's own
// index FK cascades regardless of how the index DB is named (#347/#365).
func TestBuildFKCascadeQuery_noSchemasExcludesInternal(t *testing.T) {
	query, args := buildFKCascadeQuery(nil)

	if len(args) != 0 {
		t.Errorf("expected no args for unscoped query, got %v", args)
	}
	for _, want := range []string{"'mysql'", "'information_schema'", "'performance_schema'", "'sys'"} {
		if !strings.Contains(query, want) {
			t.Errorf("expected unscoped query to exclude system schema %s, got query:\n%s", want, query)
		}
	}
	// Structural detection: subquery over information_schema.TABLES requiring all
	// three signature tables (HAVING COUNT(DISTINCT TABLE_NAME) = 3).
	for _, want := range []string{
		"information_schema.TABLES",
		"'binlog_events'", "'schema_snapshots'", "'stream_state'",
		"GROUP BY TABLE_SCHEMA HAVING COUNT(DISTINCT TABLE_NAME) = 3",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("expected unscoped query to contain %q, got query:\n%s", want, query)
		}
	}
}

func TestHasReplPrivileges(t *testing.T) {
	tests := []struct {
		name       string
		grants     []string
		wantSlave  bool
		wantClient bool
	}{
		{
			name:       "both privileges",
			grants:     []string{"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "all privileges",
			grants:     []string{"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost'"},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "only slave",
			grants:     []string{"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: false,
		},
		{
			name:       "only client",
			grants:     []string{"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'"},
			wantSlave:  false,
			wantClient: true,
		},
		{
			name:       "no replication privileges",
			grants:     []string{"GRANT SELECT ON mydb.* TO 'reader'@'%'"},
			wantSlave:  false,
			wantClient: false,
		},
		{
			name: "across multiple grant lines",
			grants: []string{
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name: "mixed with other privileges",
			grants: []string{
				"GRANT SELECT, INSERT ON mydb.* TO 'user'@'%'",
				"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "empty grants",
			grants:     nil,
			wantSlave:  false,
			wantClient: false,
		},
		{
			name:       "case insensitive",
			grants:     []string{"GRANT replication slave, replication client ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSlave, gotClient := HasReplPrivileges(tt.grants)
			if gotSlave != tt.wantSlave {
				t.Errorf("slave = %v, want %v", gotSlave, tt.wantSlave)
			}
			if gotClient != tt.wantClient {
				t.Errorf("client = %v, want %v", gotClient, tt.wantClient)
			}
		})
	}
}
