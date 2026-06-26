//go:build integration

package recovery

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
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

func TestGenerateSQL_largeUnsignedBigintExact(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// DELETE whose before-image holds a BIGINT UNSIGNED max value. Reversing it
	// emits an INSERT; the value must appear EXACTLY, not rounded through float64
	// — this completes the #490 unsigned fix end-to-end through recover (#496).
	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, "2026-02-19 14:00:00", nil,
		"mydb", "counters", 3, "1",
		nil,
		[]byte(`{"id":1,"big":18446744073709551615}`),
		nil,
	)

	g := New(db, nil)
	var buf bytes.Buffer
	if _, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "mydb", Table: "counters", Limit: 100,
	}, &buf); err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	out := buf.String()
	assertContains(t, out, "18446744073709551615")
	if strings.Contains(out, "18446744073709551616") {
		t.Errorf("BIGINT UNSIGNED max was rounded through float64 (got ...616):\n%s", out)
	}
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

func TestGenerateSQL_updateLargeUnsignedExact(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// UPDATE reversal with a BIGINT UNSIGNED max in both images. With a nil
	// resolver the value lands in BOTH the SET clause (from row_before) and the
	// all-columns WHERE clause (from row_after) — both via FormatSQLValue — so one
	// event locks the large value on both clauses (#496).
	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200, "2026-02-19 14:00:00", nil,
		"mydb", "counters", 2, "1",
		[]byte(`["n"]`),
		[]byte(`{"id":1,"big":18446744073709551615,"n":1}`),
		[]byte(`{"id":1,"big":18446744073709551615,"n":2}`),
	)

	g := New(db, nil)
	var buf bytes.Buffer
	if _, err := g.GenerateSQL(context.Background(), query.Options{
		Schema: "mydb", Table: "counters", Limit: 100,
	}, &buf); err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}
	out := buf.String()
	assertContains(t, out, "UPDATE")
	// Must appear exactly (SET and WHERE), never rounded to ...616.
	if strings.Count(out, "18446744073709551615") < 2 {
		t.Errorf("expected the exact BIGINT UNSIGNED value in SET and WHERE:\n%s", out)
	}
	if strings.Contains(out, "18446744073709551616") {
		t.Errorf("BIGINT UNSIGNED was rounded through float64:\n%s", out)
	}
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

// ─── schema drift after the event (#601), real-path through the index DB ─────────

// insertEvent601 inserts a binlog_events row with an explicit schema_version (the
// event-time snapshot id) — testutil.InsertEvent always defaults it to 0, which would
// make the event-time and latest resolvers identical and disable drift detection.
func insertEvent601(t *testing.T, db *sql.DB, schemaVersion uint32, ts, schema, table string, eventType uint8, pk string, rowBefore, rowAfter []byte) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, gtid,
		 schema_name, table_name, event_type, pk_values,
		 changed_columns, row_before, row_after, schema_version)
		VALUES ('binlog.000601', 100, 200, ?, NULL, ?, ?, ?, ?, NULL, ?, ?, ?)`,
		ts, schema, table, eventType, pk, rowBefore, rowAfter, schemaVersion)
}

// snapshot601 seeds an orders snapshot. withCoupon controls whether the (non-PK)
// coupon_code column is present, so a later snapshot can drop it.
func snapshot601(t *testing.T, db *sql.DB, id int, snapTime string, withCoupon bool) {
	t.Helper()
	testutil.InsertSnapshot(t, db, id, snapTime, "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, id, snapTime, "mydb", "orders", "customer", 2, "", "varchar", "YES")
	if withCoupon {
		testutil.InsertSnapshot(t, db, id, snapTime, "mydb", "orders", "coupon_code", 3, "", "varchar", "YES")
	}
}

// TestRecover601_droppedColumnRefused is the issue's repro: a DELETE captured while the
// table still had coupon_code reverses to an INSERT that references it, but coupon_code
// was dropped before now. The generator must refuse loudly and write nothing — emitting
// the INSERT would fail to apply against the current table.
func TestRecover601_droppedColumnRefused(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	snapshot601(t, db, 1, "2026-02-19 13:00:00", true)  // event-time: has coupon_code
	snapshot601(t, db, 2, "2026-02-19 15:00:00", false) // latest: coupon_code dropped

	insertEvent601(t, db, 1, "2026-02-19 14:00:00", "mydb", "orders", 3, "1",
		[]byte(`{"id":1,"customer":"Alice","coupon_code":"SAVE10"}`), nil)

	resolver, err := metadata.NewResolver(db, 0) // latest = snapshot 2
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	g := New(db, resolver)

	var buf bytes.Buffer
	_, err = g.GenerateSQL(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 100}, &buf)
	if err == nil {
		t.Fatalf("expected schema-drift refusal, got nil; output:\n%s", buf.String())
	}
	if !strings.Contains(err.Error(), "coupon_code") || !strings.Contains(err.Error(), "mydb.orders") {
		t.Errorf("refusal must name mydb.orders and coupon_code, got: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("no partial output expected on refusal, got:\n%s", buf.String())
	}
}

// TestRecover601_pkScopedDeleteNotRefused guards the false-positive boundary the advisor
// flagged: an INSERT reverses to a DELETE whose WHERE references only the PK (still
// present). The dropped non-PK column is never emitted, so this valid recovery must
// proceed — detection follows what is emitted, not every column in the image.
func TestRecover601_pkScopedDeleteNotRefused(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	snapshot601(t, db, 1, "2026-02-19 13:00:00", true)
	snapshot601(t, db, 2, "2026-02-19 15:00:00", false)

	insertEvent601(t, db, 1, "2026-02-19 14:00:00", "mydb", "orders", 1, "1",
		nil, []byte(`{"id":1,"customer":"Alice","coupon_code":"SAVE10"}`))

	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	g := New(db, resolver)

	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 100}, &buf)
	if err != nil {
		t.Fatalf("PK-scoped recovery must not be refused: %v", err)
	}
	out := buf.String()
	assertContains(t, out, "DELETE FROM")
	assertContains(t, out, "`id`")
	if strings.Contains(out, "coupon_code") {
		t.Errorf("dropped non-PK column must not appear in a PK-scoped DELETE:\n%s", out)
	}
	if n != 1 {
		t.Errorf("expected 1 statement, got %d", n)
	}
}

// TestRecover601_noDriftStillEmits confirms the detector does not fire when the column
// is still present in the latest snapshot — the reversal emits normally, including the
// non-PK column.
func TestRecover601_noDriftStillEmits(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	snapshot601(t, db, 1, "2026-02-19 13:00:00", true)
	snapshot601(t, db, 2, "2026-02-19 15:00:00", true) // latest STILL has coupon_code

	insertEvent601(t, db, 1, "2026-02-19 14:00:00", "mydb", "orders", 3, "1",
		[]byte(`{"id":1,"customer":"Alice","coupon_code":"SAVE10"}`), nil)

	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	g := New(db, resolver)

	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 100}, &buf)
	if err != nil {
		t.Fatalf("no-drift recovery must not be refused: %v", err)
	}
	assertContains(t, buf.String(), "INSERT INTO")
	assertContains(t, buf.String(), "coupon_code")
	if n != 1 {
		t.Errorf("expected 1 statement, got %d", n)
	}
}

// TestRecover601_updateSetDriftRefused covers the UPDATE-reversal SET path — the only
// builder that combines SET columns (row_before) with WHERE columns, and the most common
// event type. It also drifts TWO columns to exercise the per-table column accumulation in
// the refusal message. A reverse-UPDATE restores row_before, so a column dropped after
// the event lands in the SET clause and must trigger the refusal.
func TestRecover601_updateSetDriftRefused(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// event-time snapshot (1): orders has coupon_code AND discount
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 13:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 13:00:00", "mydb", "orders", "customer", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 13:00:00", "mydb", "orders", "coupon_code", 3, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 13:00:00", "mydb", "orders", "discount", 4, "", "int", "YES")
	// latest snapshot (2): both coupon_code and discount dropped
	testutil.InsertSnapshot(t, db, 2, "2026-02-19 15:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 2, "2026-02-19 15:00:00", "mydb", "orders", "customer", 2, "", "varchar", "YES")

	// UPDATE event under snapshot 1: row_before (restored by SET) carries the dropped
	// columns; row_after carries the PK used by the WHERE.
	insertEvent601(t, db, 1, "2026-02-19 14:00:00", "mydb", "orders", 2, "1",
		[]byte(`{"id":1,"customer":"Alice","coupon_code":"SAVE10","discount":5}`),
		[]byte(`{"id":1,"customer":"Bob","coupon_code":"NONE","discount":0}`))

	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	g := New(db, resolver)

	var buf bytes.Buffer
	_, err = g.GenerateSQL(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 100}, &buf)
	if err == nil {
		t.Fatalf("expected schema-drift refusal for UPDATE SET clause, got nil; output:\n%s", buf.String())
	}
	if !strings.Contains(err.Error(), "coupon_code") || !strings.Contains(err.Error(), "discount") {
		t.Errorf("refusal must name BOTH drifted SET columns, got: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("no partial output expected on refusal, got:\n%s", buf.String())
	}
}

// TestRecover601_tableDroppedFromLatestDegradesWithWarning covers the ambiguous case the
// silent-failure review flagged: the table is absent from the latest snapshot (dropped
// after the event, OR simply outside a scoped --schemas/--tables snapshot). Detection
// cannot run, so the generator must DEGRADE (emit, not refuse — refusing would break
// legitimate recovery of a scoped-out table) but must WARN rather than go silent.
func TestRecover601_tableDroppedFromLatestDegradesWithWarning(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// event-time snapshot (1): orders exists.
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 13:00:00", "mydb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-02-19 13:00:00", "mydb", "orders", "customer", 2, "", "varchar", "YES")
	// latest snapshot (2): orders absent — only another table was captured.
	testutil.InsertSnapshot(t, db, 2, "2026-02-19 15:00:00", "mydb", "customers", "id", 1, "PRI", "int", "NO")

	insertEvent601(t, db, 1, "2026-02-19 14:00:00", "mydb", "orders", 3, "1",
		[]byte(`{"id":1,"customer":"Alice"}`), nil)

	// Capture slog to assert the detector announces it could not check (the fix: warn,
	// don't go dark). SetDefault is restored on return.
	var logbuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	resolver, err := metadata.NewResolver(db, 0) // latest = snapshot 2 (no orders)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	g := New(db, resolver)

	var buf bytes.Buffer
	n, err := g.GenerateSQL(context.Background(), query.Options{Schema: "mydb", Table: "orders", Limit: 100}, &buf)
	if err != nil {
		t.Fatalf("table absent from latest snapshot is ambiguous; must degrade, not refuse: %v", err)
	}
	assertContains(t, buf.String(), "INSERT INTO")
	if !strings.Contains(logbuf.String(), "drift check") {
		t.Errorf("detector must WARN when it cannot check drift, got logs:\n%s", logbuf.String())
	}
	if n != 1 {
		t.Errorf("expected 1 statement, got %d", n)
	}
}
