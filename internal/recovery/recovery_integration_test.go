//go:build integration

package recovery

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/query"
	"github.com/bintrail/bintrail/internal/testutil"
)

func TestGenerateSQL_deleteToInsert(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, "2026-02-19 14:00:00", nil,
		"mydb", "orders", 3, "1",
		nil,
		[]byte(`{"id":1,"customer":"Alice","status":"active"}`),
		nil,
	)

	g := New(db, nil)
	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "mydb", Table: "orders", Limit: 100,
	}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 statement, got %d", n)
	}

	out := buf.String()
	assertContains(t, out, "INSERT INTO")
	assertContains(t, out, "`mydb`")
	assertContains(t, out, "`orders`")
	assertContains(t, out, "'Alice'")
	assertContains(t, out, "BEGIN;")
	assertContains(t, out, "COMMIT;")
}

func TestGenerateSQL_updateReverse(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, "2026-02-19 14:00:00", nil,
		"mydb", "orders", 2, "1",
		[]byte(`["status"]`),
		[]byte(`{"id":1,"status":"pending"}`),
		[]byte(`{"id":1,"status":"shipped"}`),
	)

	g := New(db, nil)
	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "mydb", Table: "orders", Limit: 100,
	}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 statement, got %d", n)
	}

	out := buf.String()
	assertContains(t, out, "UPDATE")
	assertContains(t, out, "SET")
	// SET should restore to before-image values.
	assertContains(t, out, "'pending'")
	// WHERE should use after-image values (current DB state).
	assertContains(t, out, "'shipped'")
}

func TestGenerateSQL_insertToDelete(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, "2026-02-19 14:00:00", nil,
		"mydb", "orders", 1, "1",
		nil, nil,
		[]byte(`{"id":1,"customer":"Bob"}`),
	)

	g := New(db, nil)
	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "mydb", Table: "orders", Limit: 100,
	}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 statement, got %d", n)
	}

	out := buf.String()
	assertContains(t, out, "DELETE FROM")
	assertContains(t, out, "'Bob'")
}

func TestGenerateSQL_reverseOrder(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Two events: first INSERT, then UPDATE — reversal should process UPDATE first.
	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, "2026-02-19 14:00:00", nil,
		"mydb", "orders", 1, "1",
		nil, nil,
		[]byte(`{"id":1,"status":"new"}`),
	)
	testutil.InsertEvent(t, db,
		"binlog.000001", 200, 300, "2026-02-19 14:01:00", nil,
		"mydb", "orders", 2, "1",
		[]byte(`["status"]`),
		[]byte(`{"id":1,"status":"new"}`),
		[]byte(`{"id":1,"status":"done"}`),
	)

	g := New(db, nil)
	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "mydb", Table: "orders", Limit: 100,
	}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 statements, got %d", n)
	}

	out := buf.String()
	// The UPDATE (reverse of later event) should appear before the DELETE (reverse of earlier INSERT).
	updateIdx := strings.Index(out, "UPDATE")
	deleteIdx := strings.Index(out, "DELETE FROM")
	if updateIdx < 0 || deleteIdx < 0 {
		t.Fatalf("expected both UPDATE and DELETE in output:\n%s", out)
	}
	if updateIdx > deleteIdx {
		t.Errorf("expected UPDATE before DELETE (reverse chronological order)")
	}
}

func TestGenerateSQL_noEvents(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	g := New(db, nil)
	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "nonexistent", Table: "tbl", Limit: 100,
	}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 statements, got %d", n)
	}
	assertContains(t, buf.String(), "No events matched")
}

func TestGenerateSQL_withResolver(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Insert a snapshot so the resolver can identify PK columns.
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 10:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 10:00:00", "mydb", "orders", "customer", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 10:00:00", "mydb", "orders", "status", 3, "", "varchar", "NO")

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	// INSERT event — reversal is a DELETE using only PK in WHERE.
	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, "2026-02-19 14:00:00", nil,
		"mydb", "orders", 1, "1",
		nil, nil,
		[]byte(`{"id":1,"customer":"Alice","status":"new"}`),
	)

	g := New(db, resolver)
	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "mydb", Table: "orders", Limit: 100,
	}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 statement, got %d", n)
	}

	out := buf.String()
	assertContains(t, out, "DELETE FROM")
	// With resolver, WHERE should use only PK column `id`, not all columns.
	assertContains(t, out, "`id` = 1")
	// Should NOT have customer or status in WHERE clause.
	if strings.Contains(out, "WHERE") {
		whereIdx := strings.Index(out, "WHERE")
		wherePart := out[whereIdx:]
		endIdx := strings.Index(wherePart, ";")
		if endIdx > 0 {
			wherePart = wherePart[:endIdx]
		}
		if strings.Contains(wherePart, "customer") {
			t.Errorf("expected PK-only WHERE (no customer), got: %s", wherePart)
		}
		if strings.Contains(wherePart, "status") {
			t.Errorf("expected PK-only WHERE (no status), got: %s", wherePart)
		}
	}
}

func TestGenerateSQL_skipsGeneratedColumnsInInsert(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Insert snapshot with a generated column (line_total).
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 10:00:00", "shop", "order_items", "order_id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 10:00:00", "shop", "order_items", "quantity", 2, "", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 10:00:00", "shop", "order_items", "unit_price", 3, "", "decimal", "NO")
	// Mark line_total as generated.
	testutil.MustExec(t, db,
		`INSERT INTO schema_snapshots
		 (snapshot_id, snapshot_time, schema_name, table_name, column_name,
		  ordinal_position, column_key, data_type, is_nullable, is_generated)
		 VALUES (1, '2026-02-19 10:00:00', 'shop', 'order_items', 'line_total', 4, '', 'decimal', 'NO', 1)`)

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver failed: %v", err)
	}

	// DELETE event: row_before includes the generated column value from the binlog.
	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, "2026-02-19 14:00:00", nil,
		"shop", "order_items", 3, "5",
		nil,
		[]byte(`{"order_id":5,"quantity":3,"unit_price":68.81,"line_total":206.43}`),
		nil,
	)

	g := New(db, resolver)
	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "shop", Table: "order_items", Limit: 100,
	}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 statement, got %d", n)
	}

	out := buf.String()
	assertContains(t, out, "INSERT INTO")
	assertContains(t, out, "`order_id`")
	assertContains(t, out, "`quantity`")
	if strings.Contains(out, "line_total") {
		t.Errorf("generated column 'line_total' must not appear in INSERT:\n%s", out)
	}
}

func assertContains(t *testing.T, s, want string) {
	t.Helper()
	if !strings.Contains(s, want) {
		t.Errorf("expected %q in output:\n%s", want, s)
	}
}
