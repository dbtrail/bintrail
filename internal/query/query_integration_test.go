//go:build integration

package query

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/testutil"
)

func TestFetch_schemaFilter(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts, nil, "other", "stuff", 1, "1", nil, nil, []byte(`{"id":1}`))

	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{Schema: "mydb", Limit: 100})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row for schema=mydb, got %d", len(rows))
	}
	if len(rows) > 0 && rows[0].SchemaName != "mydb" {
		t.Errorf("expected schema=mydb, got %q", rows[0].SchemaName)
	}
}

func TestFetch_pkLookup(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts, nil, "mydb", "orders", 1, "99", nil, nil, []byte(`{"id":99}`))

	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{Schema: "mydb", Table: "orders", PKValues: "42", Limit: 100})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row for pk=42, got %d", len(rows))
	}
	if len(rows) > 0 && rows[0].PKValues != "42" {
		t.Errorf("expected pk_values=42, got %q", rows[0].PKValues)
	}
}

func TestFetch_eventTypeFilter(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts, nil, "mydb", "orders", 3, "2", nil, []byte(`{"id":2}`), nil)

	et := parser.EventDelete
	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{EventType: &et, Limit: 100})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 DELETE event, got %d", len(rows))
	}
	if len(rows) > 0 && rows[0].EventType != parser.EventDelete {
		t.Errorf("expected DELETE, got %v", rows[0].EventType)
	}
}

func TestFetch_gtidFilter(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, &gtid, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts, nil, "mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`))

	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{GTID: gtid, Limit: 100})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row for GTID, got %d", len(rows))
	}
	if len(rows) > 0 && (rows[0].GTID == nil || *rows[0].GTID != gtid) {
		t.Errorf("expected GTID=%q, got %v", gtid, rows[0].GTID)
	}
}

func TestFetch_timeRange(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, "2026-02-19 12:00:00", nil, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, "2026-02-19 14:00:00", nil, "mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, "2026-02-19 16:00:00", nil, "mydb", "orders", 1, "3", nil, nil, []byte(`{"id":3}`))

	since := time.Date(2026, 2, 19, 13, 0, 0, 0, time.UTC)
	until := time.Date(2026, 2, 19, 15, 0, 0, 0, time.UTC)
	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{Since: &since, Until: &until, Limit: 100})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row in time range, got %d", len(rows))
	}
}

func TestFetch_changedColumnFilter(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 2, "1",
		[]byte(`["status","amount"]`),
		[]byte(`{"id":1,"status":"new","amount":99.99}`),
		[]byte(`{"id":1,"status":"shipped","amount":109.99}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts, nil, "mydb", "orders", 2, "2",
		[]byte(`["name"]`),
		[]byte(`{"id":2,"name":"old"}`),
		[]byte(`{"id":2,"name":"new"}`))

	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{
		Schema: "mydb", Table: "orders",
		ChangedColumn: "status", Limit: 100,
	})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row for changed_column=status, got %d", len(rows))
	}
}

func TestFetch_columnEqFilter(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	// InitIndexTables creates the original schema; apply the later ALTERs so
	// the SELECT list (which includes connection_id) resolves.
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	ts := "2026-02-19 14:00:00"
	// Event 1: INSERT with status=active in row_after.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1",
		nil, nil, []byte(`{"id":1,"status":"active","order_id":7}`))
	// Event 2: DELETE with status=active only in row_before.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts, nil, "mydb", "orders", 3, "2",
		nil, []byte(`{"id":2,"status":"active","order_id":8}`), nil)
	// Event 3: INSERT with status=closed.
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts, nil, "mydb", "orders", 1, "3",
		nil, nil, []byte(`{"id":3,"status":"closed","order_id":7}`))
	// Event 4: UPDATE with nullable_col=null in row_after.
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, ts, nil, "mydb", "orders", 2, "4",
		[]byte(`["nullable_col"]`),
		[]byte(`{"id":4,"nullable_col":"was"}`),
		[]byte(`{"id":4,"nullable_col":null}`))

	e := New(db)

	// Plain match: status=active hits the INSERT (row_after) and DELETE (row_before).
	t.Run("insertAndDelete", func(t *testing.T) {
		rows, err := e.Fetch(context.Background(), Options{
			Schema: "mydb", Table: "orders",
			ColumnEq: []ColumnEq{{Column: "status", Value: "active"}},
			Limit:    100,
		})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows for status=active, got %d", len(rows))
		}
	})

	// AND composition: status=active AND order_id=7 → only the INSERT.
	t.Run("andComposition", func(t *testing.T) {
		rows, err := e.Fetch(context.Background(), Options{
			Schema: "mydb", Table: "orders",
			ColumnEq: []ColumnEq{
				{Column: "status", Value: "active"},
				{Column: "order_id", Value: "7"},
			},
			Limit: 100,
		})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row for status=active AND order_id=7, got %d", len(rows))
		}
		if rows[0].PKValues != "1" {
			t.Errorf("expected pk=1, got %q", rows[0].PKValues)
		}
	})

	// NULL sentinel: nullable_col=NULL hits the UPDATE where row_after has JSON null.
	t.Run("nullSentinel", func(t *testing.T) {
		rows, err := e.Fetch(context.Background(), Options{
			Schema: "mydb", Table: "orders",
			ColumnEq: []ColumnEq{{Column: "nullable_col", IsNull: true}},
			Limit:    100,
		})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row for nullable_col=NULL, got %d", len(rows))
		}
		if rows[0].PKValues != "4" {
			t.Errorf("expected pk=4, got %q", rows[0].PKValues)
		}
	})
}

func TestRun_jsonFormat(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1",
		nil, nil, []byte(`{"id":1,"name":"Alice"}`))

	e := New(db)
	var buf bytes.Buffer
	n, err := e.Run(context.Background(), Options{Limit: 100}, "json", &buf)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}

	var out []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 JSON element, got %d", len(out))
	}
	if out[0]["event_type"] != "INSERT" {
		t.Errorf("expected event_type=INSERT, got %v", out[0]["event_type"])
	}
}

func TestRun_csvFormat(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 3, "1",
		nil, []byte(`{"id":1}`), nil)

	e := New(db)
	var buf bytes.Buffer
	n, err := e.Run(context.Background(), Options{Limit: 100}, "csv", &buf)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Errorf("expected 2 lines (header + data), got %d", len(lines))
	}
	if !strings.Contains(lines[1], "DELETE") {
		t.Errorf("expected DELETE in CSV row, got: %s", lines[1])
	}
}

func TestRun_tableFormat(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1",
		nil, nil, []byte(`{"id":1}`))

	e := New(db)
	var buf bytes.Buffer
	n, err := e.Run(context.Background(), Options{Limit: 100}, "table", &buf)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
	if !strings.Contains(buf.String(), "INSERT") {
		t.Errorf("expected INSERT in table output, got: %s", buf.String())
	}
}
