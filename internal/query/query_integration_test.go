//go:build integration

package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
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

// TestFetch_nullBinlogFile reproduces the SaaS Explorer crash reported in
// dbtrail/bintrail#318 / nethalo/dbtrail#1484: rows with NULL binlog_file
// (from customer DBs that predate the NOT NULL migration, or from rows
// inserted by external pipelines) used to fail Scan with
// "converting NULL to string is unsupported".
func TestFetch_nullBinlogFile(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Relax the NOT NULL on binlog_file to simulate a drifted customer schema.
	if _, err := db.Exec(`ALTER TABLE binlog_events MODIFY binlog_file VARCHAR(255) NULL`); err != nil {
		t.Fatalf("ALTER TABLE failed: %v", err)
	}

	ts := "2026-02-19 14:00:00"
	// One row with binlog_file set, one with NULL.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1",
		nil, nil, []byte(`{"id":1}`))
	if _, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, gtid,
		 schema_name, table_name, event_type, pk_values,
		 changed_columns, row_before, row_after)
		VALUES (NULL, 0, 0, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?)`,
		ts, "mydb", "orders", 1, "2", []byte(`{"id":2}`),
	); err != nil {
		t.Fatalf("insert NULL-binlog_file row failed: %v", err)
	}

	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{Schema: "mydb", Table: "orders", Limit: 100})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Rows come back ordered by event_timestamp ASC, event_id ASC; the
	// non-NULL row was inserted first so it gets the lower event_id.
	if rows[0].BinlogFile != "binlog.000001" {
		t.Errorf("row 0: expected BinlogFile=binlog.000001, got %q", rows[0].BinlogFile)
	}
	if rows[1].BinlogFile != "" {
		t.Errorf("row 1: expected BinlogFile=\"\" for NULL column, got %q", rows[1].BinlogFile)
	}
}

// TestFetch_allNullableNotNullColumns is the dbtrail/bintrail#1484 deploy
// verification follow-up: byos-202 surfaced NULL not just in binlog_file
// (#318) but also in start_pos — proof that production indexes can carry
// NULL across MANY "NOT NULL" columns simultaneously, not just one. This
// test relaxes every defended-against column to NULL, inserts a row with
// NULL in all of them, and asserts Fetch returns it without crashing. A
// regression that re-introduces a bare-type scan for any one column will
// fail with the column-specific "converting NULL to X is unsupported".
//
// event_timestamp is excluded because it's part of the PRIMARY KEY in
// the partition definition — MySQL rejects NULL there regardless of the
// column's own constraint. event_id is excluded because AUTO_INCREMENT
// cannot return NULL on read. schema_version has DEFAULT 0 so MySQL fills
// 0 instead of NULL on omission, but is still defended at the scan layer.
func TestFetch_allNullableNotNullColumns(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Relax the NOT NULL on every column that the defensive scanner is
	// supposed to handle (matches the set of sql.Null* locals in scanRows).
	for _, alter := range []string{
		`ALTER TABLE binlog_events MODIFY binlog_file VARCHAR(255) NULL`,
		`ALTER TABLE binlog_events MODIFY start_pos BIGINT UNSIGNED NULL`,
		`ALTER TABLE binlog_events MODIFY end_pos BIGINT UNSIGNED NULL`,
		`ALTER TABLE binlog_events MODIFY schema_name VARCHAR(64) NULL`,
		`ALTER TABLE binlog_events MODIFY table_name VARCHAR(64) NULL`,
		`ALTER TABLE binlog_events MODIFY event_type TINYINT UNSIGNED NULL`,
		`ALTER TABLE binlog_events MODIFY pk_values VARCHAR(512) NULL`,
		`ALTER TABLE binlog_events MODIFY schema_version INT UNSIGNED NULL`,
	} {
		if _, err := db.Exec(alter); err != nil {
			t.Fatalf("ALTER TABLE failed (%q): %v", alter, err)
		}
	}

	// Insert one row populated for sorting + Fetch filter, and one row with
	// NULL in every defended column. event_timestamp must be set on the
	// drifted row because it's in the PK.
	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1",
		nil, nil, []byte(`{"id":1}`))
	if _, err := db.Exec(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, gtid,
		 schema_name, table_name, event_type, pk_values,
		 changed_columns, row_before, row_after, schema_version)
		VALUES (NULL, NULL, NULL, ?, NULL,
		        NULL, NULL, NULL, NULL,
		        NULL, NULL, NULL, NULL)`,
		ts,
	); err != nil {
		t.Fatalf("insert all-NULL row failed: %v", err)
	}

	// Fetch without schema/table filters since the drifted row has NULL
	// schema_name and table_name.
	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{Limit: 100})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// rows[0] is the non-NULL row (lower event_id), rows[1] is the all-NULL
	// drift row. NULL → zero-value mapping is the documented behavior.
	if rows[0].BinlogFile != "binlog.000001" || rows[0].SchemaName != "mydb" {
		t.Errorf("row 0: unexpected values %+v", rows[0])
	}
	got := rows[1]
	if got.BinlogFile != "" || got.StartPos != 0 || got.EndPos != 0 ||
		got.SchemaName != "" || got.TableName != "" || got.PKValues != "" ||
		got.EventType != 0 || got.SchemaVersion != 0 {
		t.Errorf("row 1 (all-NULL drift): expected all zero values, got %+v", got)
	}
}

// TestFetch_orderDirectionDisjointRowSets pins the #1511 fix: with
// Order=ASC + Limit=N and Order=DESC + Limit=N against a dataset with > 2N
// rows, the returned event_ids are STRICTLY DISJOINT (the ASC page is the
// oldest N events; the DESC page is the newest N — no overlap). This is the
// single assertion the bug-filing user explicitly called out as "what would
// have caught the original bug" — the pre-#1511 code returned the same
// oldest N rows for both directions regardless of Order.
func TestFetch_orderDirectionDisjointRowSets(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Insert 10 events with strictly increasing timestamps for the same (schema, table, pk).
	// The event_id auto-increments in insertion order so the bug surfaces as
	// "DESC returns event_ids 1..5 just sorted descending" instead of 6..10.
	for i := 0; i < 10; i++ {
		ts := time.Date(2026, 5, 24, 14, 0, i, 0, time.UTC).Format("2006-01-02 15:04:05")
		testutil.InsertEvent(t, db, "binlog.000001", uint64(100+i*100), uint64(200+i*100), ts, nil,
			"mydb", "orders", 1, fmt.Sprintf("%d", i+1), nil, nil, []byte(fmt.Sprintf(`{"id":%d}`, i+1)))
	}

	e := New(db)
	ascRows, err := e.Fetch(context.Background(), Options{
		Schema: "mydb", Table: "orders", Limit: 5, Order: "ASC",
	})
	if err != nil {
		t.Fatalf("Fetch ASC failed: %v", err)
	}
	descRows, err := e.Fetch(context.Background(), Options{
		Schema: "mydb", Table: "orders", Limit: 5, Order: "DESC",
	})
	if err != nil {
		t.Fatalf("Fetch DESC failed: %v", err)
	}

	if len(ascRows) != 5 {
		t.Fatalf("expected 5 ASC rows, got %d", len(ascRows))
	}
	if len(descRows) != 5 {
		t.Fatalf("expected 5 DESC rows, got %d", len(descRows))
	}

	ascIDs := make(map[uint64]bool, 5)
	for _, r := range ascRows {
		ascIDs[r.EventID] = true
	}
	// The disjoint-set assertion — the bug-filing user's explicit ask.
	for _, r := range descRows {
		if ascIDs[r.EventID] {
			t.Errorf("event_id %d appears in BOTH ASC and DESC pages; this is the #1511 regression "+
				"(DESC LIMIT N returning the same oldest N rows as ASC LIMIT N). "+
				"ascIDs=%v descIDs=%v",
				r.EventID, ascRows, descRows)
		}
	}

	// Stronger property: the ASC page is the timestamp-earliest 5 events, the
	// DESC page is the timestamp-latest 5. Combined they cover all 10 inserted rows.
	combined := make(map[uint64]bool, 10)
	for _, r := range ascRows {
		combined[r.EventID] = true
	}
	for _, r := range descRows {
		combined[r.EventID] = true
	}
	if len(combined) != 10 {
		t.Errorf("ASC + DESC pages combined should cover all 10 inserted events; got %d unique event_ids", len(combined))
	}

	// Direction sanity: ASC's first event_timestamp must be ≤ DESC's last (sanity
	// against an inverted slice). Note we compare timestamps, not event_ids,
	// because the bug was about row identity, not display order.
	if !ascRows[0].EventTimestamp.Before(descRows[len(descRows)-1].EventTimestamp) &&
		!ascRows[0].EventTimestamp.Equal(descRows[len(descRows)-1].EventTimestamp) {
		t.Errorf("expected ASC[0] timestamp ≤ DESC[last] timestamp; got asc[0]=%v desc[last]=%v",
			ascRows[0].EventTimestamp, descRows[len(descRows)-1].EventTimestamp)
	}
}

// TestFetch_wideRowSurvivesSmallSortBuffer reproduces the ER_OUT_OF_SORTMEMORY
// (1038) class seen on a WordPress source: a single fat row image (wp_options
// autoload blob, ~750K in the field) is wider than the stock default
// sort_buffer_size (256K on MySQL 8.x), so a filesort that carries the wide JSON
// columns as addon fields overflows and the whole query dies — even on the
// Overview/Events list where only a handful of rows are oversized.
//
// The test pins the connection to the stock 256K buffer, proves with a control
// query that a naive wide-column sort genuinely 1038s at that size, then asserts
// Fetch (which sorts narrow keys only via late materialization) returns every
// row with the ordering intact. A regression that re-introduces a wide-column
// filesort fails here with 1038.
//
// Version note: this 1038 is MySQL-8.4-specific. The CI integration matrix
// (.github/workflows/ci.yml) runs the suite on BOTH 8.0 and 8.4 (port 13306):
// on the 8.4 cell this test actively reproduces the 1038 and asserts the fix;
// on the 8.0 cell it SKIPs (8.0 degrades a too-wide addon sort to sort-by-rowid
// instead of erroring — not a flake). The version-independent guards that run
// on every cell are the unit tests TestBuildQuery_wideColumnsNeverSorted (SQL
// shape) and TestSortResults_orderingAndTieBreak (Go-side order).
func TestFetch_wideRowSurvivesSmallSortBuffer(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Pin the whole pool to one connection at the stock MySQL default buffer so
	// the fat row below genuinely cannot fit a wide-column filesort.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("SET SESSION sort_buffer_size = 262144"); err != nil {
		t.Fatalf("pin sort_buffer_size: %v", err)
	}

	ts := "2026-02-19 14:00:00"
	for i := uint64(1); i <= 5; i++ {
		testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil,
			"wordpress", "dbt_options", 1, fmt.Sprint(i),
			nil, nil, []byte(fmt.Sprintf(`{"id":%d}`, i)))
	}
	// One fat image well over 256K — mimics a serialized wp_options autoload value.
	fat := `{"option_value":"` + strings.Repeat("a", 400*1024) + `"}`
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts, nil,
		"wordpress", "dbt_options", 1, "6", nil, nil, []byte(fat))

	// Control: a naive wide-column filesort overflows the 256K buffer, proving
	// the regression condition is real before we assert Fetch survives it.
	ctrlRows, ctrlErr := db.Query(
		"SELECT event_id, row_after FROM binlog_events ORDER BY event_timestamp DESC, event_id DESC LIMIT 200")
	if ctrlErr == nil {
		ctrlRows.Close()
		t.Skip("test MySQL did not honor the 256K sort buffer; cannot reproduce 1038 here")
	}
	if !strings.Contains(ctrlErr.Error(), "1038") {
		t.Fatalf("control query failed for an unexpected reason (wanted 1038): %v", ctrlErr)
	}

	// The real path must return all 6 rows at the same buffer that just broke
	// the naive sort.
	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{
		Schema: "wordpress", Table: "dbt_options", Limit: 200, Order: "DESC",
	})
	if err != nil {
		t.Fatalf("Fetch must survive a fat row at 256K sort buffer, got: %v", err)
	}
	if len(rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(rows))
	}
	// Ordering preserved by the Go-side sort: DESC ⇒ highest event_id first.
	if rows[0].EventID < rows[len(rows)-1].EventID {
		t.Errorf("expected DESC order by event_id, got first=%d last=%d",
			rows[0].EventID, rows[len(rows)-1].EventID)
	}
}
