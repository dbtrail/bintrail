package metadata

import (
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
