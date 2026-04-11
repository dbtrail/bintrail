//go:build integration

package metadata

import (
	"strings"
	"testing"

	"github.com/dbtrail/bintrail/internal/testutil"
)

func TestTakeSnapshot_nonInnoDB(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		status VARCHAR(20)
	) ENGINE=MyISAM`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error for non-InnoDB table, got nil")
	}
	if !strings.Contains(err.Error(), "not using InnoDB") {
		t.Errorf("expected 'not using InnoDB' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), sourceName+".orders") {
		t.Errorf("expected table name in error, got: %v", err)
	}
}

func TestTakeSnapshot_noPK(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE events (
		name VARCHAR(100),
		value INT
	) ENGINE=InnoDB`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error for table without primary key, got nil")
	}
	if !strings.Contains(err.Error(), "without a primary key") {
		t.Errorf("expected 'without a primary key' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), sourceName+".events") {
		t.Errorf("expected table name in error, got: %v", err)
	}
}

func TestTakeSnapshot_bothViolations(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE myisam_tbl (id INT PRIMARY KEY) ENGINE=MyISAM`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE nopk_tbl (name VARCHAR(100)) ENGINE=InnoDB`)

	_, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "not using InnoDB") {
		t.Errorf("expected 'not using InnoDB' in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "without a primary key") {
		t.Errorf("expected 'without a primary key' in error, got: %v", err)
	}
}

func TestTakeSnapshot_basic(t *testing.T) {
	// Create two databases: source (with a real table) and index (for snapshot storage).
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)

	// Create index tables.
	testutil.InitIndexTables(t, indexDB)

	// Create a source table.
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		customer VARCHAR(100) NOT NULL,
		status VARCHAR(20) NOT NULL,
		amount DECIMAL(10,2) NOT NULL
	)`)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}
	if stats.SnapshotID < 1 {
		t.Errorf("expected SnapshotID >= 1, got %d", stats.SnapshotID)
	}
	if stats.TableCount < 1 {
		t.Errorf("expected at least 1 table, got %d", stats.TableCount)
	}
	if stats.ColumnCount < 4 {
		t.Errorf("expected at least 4 columns (orders has 4), got %d", stats.ColumnCount)
	}

	// Verify rows exist in schema_snapshots.
	var count int
	indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE snapshot_id = ?", stats.SnapshotID).Scan(&count)
	if count < 4 {
		t.Errorf("expected at least 4 snapshot rows, got %d", count)
	}
}

func TestTakeSnapshot_filteredSchemas(t *testing.T) {
	// Two source DBs but only snapshot one.
	sourceDB1, name1 := testutil.CreateTestDB(t)
	sourceDB2, name2 := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB1, "CREATE TABLE tbl1 (id INT PRIMARY KEY)")
	testutil.MustExec(t, sourceDB2, "CREATE TABLE tbl2 (id INT PRIMARY KEY)")

	// Only snapshot name1.
	stats, err := TakeSnapshot(sourceDB1, indexDB, []string{name1})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Verify name2's table is not in the snapshot.
	var count int
	indexDB.QueryRow("SELECT COUNT(*) FROM schema_snapshots WHERE snapshot_id = ? AND schema_name = ?",
		stats.SnapshotID, name2).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows for filtered schema %q, got %d", name2, count)
	}
}

func TestNewResolver_latestSnapshot(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Insert two snapshots manually.
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "status", 3, "", "varchar", "NO")

	// NewResolver(db, 0) should load the latest (snapshot 2 with 3 columns).
	resolver, err := NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver(db, 0) failed: %v", err)
	}
	if resolver.SnapshotID() != 2 {
		t.Errorf("expected snapshot_id=2, got %d", resolver.SnapshotID())
	}

	tm, err := resolver.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 3 {
		t.Errorf("expected 3 columns in snapshot 2, got %d", len(tm.Columns))
	}
}

func TestNewResolver_specificSnapshot(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-18 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, indexDB, 2, "2026-02-19 10:00:00", "mydb", "orders", "status", 3, "", "varchar", "NO")

	// Load specific snapshot 1 (2 columns only).
	resolver, err := NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver(db, 1) failed: %v", err)
	}
	if resolver.SnapshotID() != 1 {
		t.Errorf("expected snapshot_id=1, got %d", resolver.SnapshotID())
	}

	tm, err := resolver.Resolve("mydb", "orders")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(tm.Columns) != 2 {
		t.Errorf("expected 2 columns in snapshot 1, got %d", len(tm.Columns))
	}
}

func TestNewResolver_emptyTable(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// No snapshots inserted — should error.
	_, err := NewResolver(indexDB, 0)
	if err == nil {
		t.Error("expected error for empty schema_snapshots, got nil")
	}
}

func TestResolver_pkColumns(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Composite PK: (order_id, item_id).
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "order_id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "item_id", 2, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, indexDB, 1, "2026-02-19 10:00:00", "mydb", "order_items", "quantity", 3, "", "int", "NO")

	resolver, err := NewResolver(indexDB, 1)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	tm, err := resolver.Resolve("mydb", "order_items")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	pkCols := tm.PKColumnMetas()
	if len(pkCols) != 2 {
		t.Fatalf("expected 2 PK columns, got %d", len(pkCols))
	}
	if pkCols[0].Name != "order_id" || pkCols[1].Name != "item_id" {
		t.Errorf("expected PK columns [order_id, item_id], got [%s, %s]", pkCols[0].Name, pkCols[1].Name)
	}
}

// TestTakeSnapshot_columnType is the #212 regression test for the schema-
// snapshot side of the precision-aware PK canonicalizer. TakeSnapshot must
// read `information_schema.COLUMNS.COLUMN_TYPE` and store the full type
// (e.g. "datetime(6)") in `schema_snapshots.column_type`, so the reconstruct
// canonicalizer can parse the declared fractional precision.
//
// Before this fix, schema_snapshots only had `data_type` (base type like
// "datetime"), and the canonicalizer had no way to tell DATETIME(0) from
// DATETIME(6). This test proves TakeSnapshot actually captures the precision.
func TestTakeSnapshot_columnType(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE events (
		id INT PRIMARY KEY,
		created_at DATETIME(6) NOT NULL,
		amount DECIMAL(12,4) NOT NULL,
		slug VARCHAR(64) NOT NULL
	)`)

	stats, err := TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Pull column_type back out for each column and check it matches.
	cases := []struct {
		column   string
		wantType string
	}{
		{"id", "int"},
		{"created_at", "datetime(6)"},
		{"amount", "decimal(12,4)"},
		{"slug", "varchar(64)"},
	}
	for _, c := range cases {
		var got string
		err := indexDB.QueryRow(
			`SELECT column_type FROM schema_snapshots
			 WHERE snapshot_id = ? AND table_name = 'events' AND column_name = ?`,
			stats.SnapshotID, c.column,
		).Scan(&got)
		if err != nil {
			t.Errorf("query column_type for %s: %v", c.column, err)
			continue
		}
		if got != c.wantType {
			t.Errorf("column %s: got column_type=%q, want %q", c.column, got, c.wantType)
		}
	}
}
