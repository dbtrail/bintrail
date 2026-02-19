//go:build integration

package metadata

import (
	"testing"

	"github.com/bintrail/bintrail/internal/testutil"
)

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
