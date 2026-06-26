//go:build integration

package shim

import (
	"database/sql"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestFullTableFlashback_DroppedColumn_Regression is the inverted #600 repro:
// a column DROPPED between the AS OF instant and now must still appear in the
// full-table resultset (its value is captured in the index), and the column
// set must NOT depend on whether a `WHERE pk=` is present.
//
// Scenario (mirrors the real lifecycle — install snapshot, DROP COLUMN,
// auto re-snapshot on DDL), all on hand-seeded snapshots for speed:
//
//	now+1m : snapshot 1 → schema (id, coupon_code, total)
//	now+5m : INSERT order id=1 → row_after {id, coupon_code:SAVE10, total}
//	now+8m : <-- AS OF target  (coupon_code STILL existed at this instant)
//	now+10m: DROP COLUMN coupon_code + re-snapshot → snapshot 2 (id, total)
//
// Pre-#600: full-table shaped columns from the LATEST snapshot (post-drop)
// and silently omitted coupon_code, while single-row showed it — the
// WHERE-clause asymmetry. After the fix both surface it.
func TestFullTableFlashback_DroppedColumn_Regression(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	snap1TS := now.Add(1 * time.Minute).Format("2006-01-02 15:04:05")
	snap2TS := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")

	insertCol := func(snapID int, snapTS, column string, ordinal int, key, dataType, columnType string) {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable)
			VALUES (?, ?, 'myapp', 'orders', ?, ?, ?, ?, ?, 'NO')`,
			snapID, snapTS, column, ordinal, key, dataType, columnType)
	}
	// Snapshot 1: the pre-drop schema — coupon_code is real and present.
	insertCol(1, snap1TS, "id", 1, "PRI", "int", "int")
	insertCol(1, snap1TS, "coupon_code", 2, "", "varchar", "varchar(32)")
	insertCol(1, snap1TS, "total", 3, "", "int", "int")
	// Snapshot 2: the post-drop schema — coupon_code is gone. This is the
	// LATEST snapshot, so NewResolver(db, 0) loads (id, total).
	insertCol(2, snap2TS, "id", 1, "PRI", "int", "int")
	insertCol(2, snap2TS, "total", 2, "", "int", "int")

	// One INSERT captured under the pre-drop schema: row_after carries
	// coupon_code, exactly as the binlog ROW image recorded it.
	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "orders", 1, "1", nil, nil,
		[]byte(`{"id":1,"coupon_code":"SAVE10","total":100}`))

	h := NewHandlerWithConfig(db, Config{
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	asOf := now.Add(8 * time.Minute) // coupon_code still existed at this instant

	assertHasColumnWithValue := func(t *testing.T, label string, q TimeTravelQuery) []string {
		t.Helper()
		res, err := h.runPointInTime(q)
		if err != nil {
			t.Fatalf("%s runPointInTime: %v", label, err)
		}
		fields := fieldNames(res.Resultset.Fields)
		cells := rowCells(t, res.Resultset)
		t.Logf("%s fields=%v rows=%v", label, fields, cells)
		if !slices.Contains(fields, "coupon_code") {
			t.Errorf("%s: fields must contain the since-dropped coupon_code, got %v", label, fields)
		}
		found := false
		for _, row := range cells {
			for _, cell := range row {
				if strings.Contains(cell, "SAVE10") {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("%s: captured value SAVE10 must be surfaced, rows=%v", label, cells)
		}
		return fields
	}

	// Full-table _flashback: SELECT * FROM _flashback.orders AS OF now+8m
	ftFields := assertHasColumnWithValue(t, "FULL-TABLE _flashback", TimeTravelQuery{
		Type:   TypeFlashback,
		Schema: "myapp",
		Table:  "orders",
		AsOf:   asOf,
	})

	// Single-row: ... WHERE id=1 AS OF now+8m
	srFields := assertHasColumnWithValue(t, "SINGLE-ROW _flashback", TimeTravelQuery{
		Type:     TypeFlashback,
		Schema:   "myapp",
		Table:    "orders",
		PKColumn: "id",
		PKValue:  "1",
		AsOf:     asOf,
	})

	// _snapshot full-table goes through the same imagesToResult (degrades to
	// the binlog-only path with no baseline configured) — it must match too.
	stFields := assertHasColumnWithValue(t, "FULL-TABLE _snapshot", TimeTravelQuery{
		Type:   TypeSnapshot,
		Schema: "myapp",
		Table:  "orders",
		AsOf:   asOf,
	})

	// Asymmetry gone: full-table and single-row return the SAME column set.
	if !sameStringSet(ftFields, srFields) {
		t.Errorf("WHERE-clause asymmetry: full-table=%v single-row=%v must be the same column set", ftFields, srFields)
	}
	if !sameStringSet(ftFields, stFields) {
		t.Errorf("_flashback vs _snapshot column set mismatch: %v vs %v", ftFields, stFields)
	}

	// An EXPLICIT full-table projection (#313) must stay verbatim through the
	// real runPointInTime → runFullTable → fullTableResult dispatch: a column
	// the user did NOT list (coupon_code) must not be appended back. This is
	// the end-to-end regression guard for the #600 union fix — making the
	// shared builder union unconditionally would have silently widened the
	// user's projection.
	projRes, err := h.runPointInTime(TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "orders",
		Columns: []string{"id", "total"},
		AsOf:    asOf,
	})
	if err != nil {
		t.Fatalf("explicit-projection runPointInTime: %v", err)
	}
	if projFields := fieldNames(projRes.Resultset.Fields); !slices.Equal(projFields, []string{"id", "total"}) {
		t.Errorf("explicit full-table projection fields = %v, want exactly [id total] (coupon_code must NOT be appended)", projFields)
	}
}

// TestFullTableFlashback_DroppedColumn_RealOracle runs the full lifecycle on
// a REAL source MySQL (real CREATE TABLE → metadata.TakeSnapshot → real
// SELECT * oracle → real DROP COLUMN → re-snapshot), satisfying #600's
// acceptance criteria that (a) the no-drift base goes through real
// TakeSnapshot, not hand-seeded schema_snapshots, and (b) the drop case is
// asserted against what SELECT * actually returned at T — not against the
// shim's own (formerly buggy) output.
func TestFullTableFlashback_DroppedColumn_RealOracle(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	sourceDB, sourceName := testutil.CreateTestDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		coupon_code VARCHAR(32) NOT NULL,
		total INT NOT NULL
	) ENGINE=InnoDB`)

	// Snapshot 1: the pre-drop schema is the latest at this point.
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot (pre-drop): %v", err)
	}

	// Real oracle: what SELECT * returns while coupon_code still exists.
	testutil.MustExec(t, sourceDB, `INSERT INTO orders (id, coupon_code, total) VALUES (1, 'SAVE10', 100)`)
	oracleCols := selectStarColumns(t, sourceDB, "orders")
	if !slices.Contains(oracleCols, "coupon_code") {
		t.Fatalf("oracle fixture broken: SELECT * should include coupon_code, got %v", oracleCols)
	}

	// The captured binlog event for that INSERT, seeded into the index with the
	// row image the parser would have produced.
	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, indexDB, now)
	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, indexDB, "mysql-bin.000001", 100, 200, eventTS, nil,
		sourceName, "orders", 1, "1", nil, nil,
		[]byte(`{"id":1,"coupon_code":"SAVE10","total":100}`))

	// Now drop the column and re-snapshot: the LATEST schema loses coupon_code.
	testutil.MustExec(t, sourceDB, `ALTER TABLE orders DROP COLUMN coupon_code`)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot (post-drop): %v", err)
	}

	h := NewHandlerWithConfig(indexDB, Config{
		NoArchive:   true,
		IndexDBName: indexName,
	}, slog.Default())
	asOf := now.Add(8 * time.Minute)

	ftRes, err := h.runPointInTime(TimeTravelQuery{
		Type:   TypeFlashback,
		Schema: sourceName,
		Table:  "orders",
		AsOf:   asOf,
	})
	if err != nil {
		t.Fatalf("full-table runPointInTime: %v", err)
	}
	ftFields := fieldNames(ftRes.Resultset.Fields)
	ftCells := rowCells(t, ftRes.Resultset)
	t.Logf("oracle SELECT * cols=%v ; FULL-TABLE fields=%v rows=%v", oracleCols, ftFields, ftCells)

	// Acceptance #2: the full-table column SET equals what SELECT * returned at
	// T (the dropped column is present), asserted against the real oracle.
	if !sameStringSet(ftFields, oracleCols) {
		t.Errorf("full-table columns %v must equal the real SELECT * oracle %v at T", ftFields, oracleCols)
	}
	if !rowsContain(ftCells, "SAVE10") {
		t.Errorf("full-table must surface the captured coupon_code value SAVE10, rows=%v", ftCells)
	}

	// Acceptance #3: single-row returns the same column set (asymmetry gone).
	srRes, err := h.runPointInTime(TimeTravelQuery{
		Type:     TypeFlashback,
		Schema:   sourceName,
		Table:    "orders",
		PKColumn: "id",
		PKValue:  "1",
		AsOf:     asOf,
	})
	if err != nil {
		t.Fatalf("single-row runPointInTime: %v", err)
	}
	srFields := fieldNames(srRes.Resultset.Fields)
	if !sameStringSet(ftFields, srFields) {
		t.Errorf("asymmetry: full-table=%v single-row=%v must be the same column set", ftFields, srFields)
	}
}

// TestFullTableFlashback_NoDrift_RealSnapshot_Equivalence pins acceptance #1:
// for a table with NO schema change between T and now, the full-table column
// list is byte-identical to the latest snapshot's order (== what SELECT *
// returns) — the fix appends nothing when no image carries an off-snapshot
// key. Goes through real metadata.TakeSnapshot, not hand-seeded snapshots.
func TestFullTableFlashback_NoDrift_RealSnapshot_Equivalence(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	sourceDB, sourceName := testutil.CreateTestDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE widgets (
		id INT PRIMARY KEY,
		name VARCHAR(64) NOT NULL,
		qty INT NOT NULL
	) ENGINE=InnoDB`)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	testutil.MustExec(t, sourceDB, `INSERT INTO widgets (id, name, qty) VALUES (1, 'a', 10), (2, 'b', 20)`)
	oracleCols := selectStarColumns(t, sourceDB, "widgets") // [id name qty], CREATE order

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, indexDB, now)
	tsA := now.Add(3 * time.Minute).Format("2006-01-02 15:04:05")
	tsB := now.Add(4 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, indexDB, "mysql-bin.000001", 100, 200, tsA, nil,
		sourceName, "widgets", 1, "1", nil, nil, []byte(`{"id":1,"name":"a","qty":10}`))
	testutil.InsertEvent(t, indexDB, "mysql-bin.000001", 200, 300, tsB, nil,
		sourceName, "widgets", 1, "2", nil, nil, []byte(`{"id":2,"name":"b","qty":20}`))

	h := NewHandlerWithConfig(indexDB, Config{
		NoArchive:   true,
		IndexDBName: indexName,
	}, slog.Default())

	res, err := h.runPointInTime(TimeTravelQuery{
		Type:   TypeFlashback,
		Schema: sourceName,
		Table:  "widgets",
		AsOf:   now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	gotCols := fieldNames(res.Resultset.Fields)
	// Byte-identical to the snapshot's ordinal order, == SELECT * order.
	if !slices.Equal(gotCols, oracleCols) {
		t.Errorf("no-drift full-table columns = %v, want byte-identical to SELECT * order %v", gotCols, oracleCols)
	}
	if n := len(rowCells(t, res.Resultset)); n != 2 {
		t.Errorf("expected 2 rows, got %d", n)
	}
}

// selectStarColumns returns the column names a real `SELECT *` yields for a
// table — the forensic oracle for what columns existed at that instant.
func selectStarColumns(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query("SELECT * FROM " + table)
	if err != nil {
		t.Fatalf("oracle SELECT * FROM %s: %v", table, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("oracle columns for %s: %v", table, err)
	}
	return cols
}

// sameStringSet reports whether a and b contain the same elements (order- and
// duplicate-insensitive) — used to assert the full-table/single-row column
// SETS converged (#600 asymmetry).
func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sa := append([]string{}, a...)
	sb := append([]string{}, b...)
	sort.Strings(sa)
	sort.Strings(sb)
	return slices.Equal(sa, sb)
}

func rowsContain(rows [][]string, want string) bool {
	for _, row := range rows {
		for _, cell := range row {
			if strings.Contains(cell, want) {
				return true
			}
		}
	}
	return false
}
