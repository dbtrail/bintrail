//go:build integration

package shim

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunPointInTime_PlannerCoversCurrentHour pins issue #259: planner
// must not classify the current hour as a coverage gap. The sqlmock
// counterpart in handler_test.go can't catch information_schema.PARTITIONS
// shape drift.
func TestRunPointInTime_PlannerCoversCurrentHour(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	// Seed previous-hour archive so buildPlan doesn't short-circuit to
	// nil (runPointInTime passes only Until). Without this, the
	// regression hides — engine.Fetch answers from binlog_events
	// regardless of the planner's DBName.
	prev := now.Add(-time.Hour).Format("p_2006010215")
	testutil.MustExec(t, db,
		"INSERT INTO archive_state (partition_name, archived_at) VALUES (?, NOW())",
		prev)

	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   false,
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	result, err := h.runPointInTime(TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "users",
		PKValue: "1",
		AsOf:    now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	if result == nil || result.Resultset == nil {
		t.Fatal("expected non-nil resultset")
	}
	// Server-built resultsets populate RowDatas, not Values; RowNumber()
	// reads len(Values) and is always 0.
	if got := len(result.Resultset.RowDatas); got != 1 {
		t.Errorf("expected 1 row, got %d", got)
	}
	// Field shape distinguishes a real row from emptyResult's
	// `[_flashback]` sentinel. imageToResult sorts JSON keys, so the
	// row we inserted ({"id","name"}) yields exactly these two fields.
	gotFields := fieldNames(result.Resultset.Fields)
	if want := []string{"id", "name"}; !slices.Equal(gotFields, want) {
		t.Errorf("fields = %v, want %v", gotFields, want)
	}
}

// TestRunDiff_PlannerCoversCurrentHour pins #259 for runDiff, which
// fails loud (*query.GapError) where runPointInTime fails silent —
// distinct guards for distinct shapes.
func TestRunDiff_PlannerCoversCurrentHour(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   false,
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	// Keep window inside the partitioned hour: planner expands rangeEnd
	// to until.Truncate(Hour)+1h, so until=hour+30m won't hit the next hour.
	result, err := h.runDiff(TimeTravelQuery{
		Type:    TypeDiff,
		Schema:  "myapp",
		Table:   "users",
		PKValue: "1",
		Since:   now.Add(time.Minute),
		Until:   now.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runDiff: %v", err)
	}
	if result == nil || result.Resultset == nil {
		t.Fatal("expected non-nil resultset")
	}
	if got := len(result.Resultset.RowDatas); got != 1 {
		t.Errorf("expected 1 row, got %d", got)
	}
	// runDiff hardcodes its 6-column shape; verifying the field set
	// guards against the empty-result fallback being silently returned.
	gotFields := fieldNames(result.Resultset.Fields)
	want := []string{"event_id", "event_timestamp", "event_type", "gtid", "row_before", "row_after"}
	if !slices.Equal(gotFields, want) {
		t.Errorf("fields = %v, want %v", gotFields, want)
	}
}

// TestRunPointInTime_DeleteReturnsEmpty mirrors issue #287's
// reproduction end-to-end: seed an INSERT then a DELETE for the same
// PK, then query AS OF *after* the DELETE. The wire response must be
// empty (no row resurrected from the DELETE's row_before).
//
// The same fixture also pins the docs claim ("Time-travel query
// returns empty" in docs/time-travel-sql.md) that _diff still
// surfaces the DELETE — the operator-facing distinction between
// "row never existed" and "row deleted". If _diff ever started
// filtering DELETEs, the disambiguation path the PR's docs sell
// would silently break and the count assertion below would fail.
// (The downstream marshalling of row_before is exercised
// indirectly: runDiff at handler.go:686 unconditionally passes
// r.RowBefore through marshalImageOrdered, so any future change
// that drops that field would also drop the row from the count.)
func TestRunPointInTime_DeleteReturnsEmpty(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	insertTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	deleteTS := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	rowBefore := []byte(`{"id":2,"sku":"SKU-B","qty":2}`)
	rowAfter := []byte(`{"id":2,"sku":"SKU-B","qty":2}`)

	// INSERT then DELETE for pk=2.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, insertTS, nil,
		"myapp", "orders", 1, "2", nil, nil, rowAfter)
	testutil.InsertEvent(t, db, "mysql-bin.000001", 300, 400, deleteTS, nil,
		"myapp", "orders", 3, "2", nil, rowBefore, nil)

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   false,
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	// Claim 1: _flashback AS OF after the DELETE → empty resultset.
	flashbackResult, err := h.runPointInTime(TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "2",
		AsOf:    now.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	if flashbackResult == nil || flashbackResult.Resultset == nil {
		t.Fatal("runPointInTime returned nil resultset")
	}
	if got := len(flashbackResult.Resultset.RowDatas); got != 0 {
		t.Errorf("_flashback AS OF after DELETE: expected 0 rows, got %d", got)
	}
	// emptyResult emits a single-column "_flashback" sentinel so the
	// client gets a well-formed reply rather than a torn one. If the
	// fields list looks like the orders table's columns (id, sku, qty)
	// instead, the DELETE short-circuit regressed.
	gotFields := fieldNames(flashbackResult.Resultset.Fields)
	if want := []string{"_flashback"}; !slices.Equal(gotFields, want) {
		t.Errorf("_flashback empty result fields = %v, want %v (looks like the table columns leaked — DELETE short-circuit regressed)", gotFields, want)
	}

	// Claim 2: _diff over the same PK and window returns both events
	// (INSERT + DELETE), proving DELETEs remain visible via _diff —
	// the docs claim disambiguates "row deleted" from "row never
	// existed" by hitting _diff and counting rows.
	diffResult, err := h.runDiff(TimeTravelQuery{
		Type:    TypeDiff,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "2",
		Since:   now.Add(time.Minute),
		Until:   now.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runDiff: %v", err)
	}
	if got := len(diffResult.Resultset.RowDatas); got != 2 {
		t.Errorf("_diff: expected 2 rows (INSERT + DELETE), got %d", got)
	}
}

// addHourlyPartition reorganizes p_future into one named hourly partition
// + fresh p_future, matching the layout `bintrail init` produces.
// Format args come from time.Format so injection is impossible.
func addHourlyPartition(t *testing.T, db *sql.DB, h time.Time) {
	t.Helper()
	pName := h.Format("p_2006010215")
	upper := h.Add(time.Hour).Format("2006-01-02 15:04:05")
	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE binlog_events REORGANIZE PARTITION p_future INTO ("+
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s')), "+
			"PARTITION p_future VALUES LESS THAN MAXVALUE)",
		pName, upper,
	))
}

func fieldNames(fields []*mysql.Field) []string {
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = string(f.Name)
	}
	return out
}

// TestRunPointInTime_EnumSetOrdinalsMapToLabels pins #472 end-to-end
// against a real index DB: binlog ROW images store ENUMs as 1-based
// ordinals and SETs as bitmasks, and the shim must map them back to
// labels via the snapshot's column_type so a time-travel row renders
// the way a live SELECT does. Also covers runDiff (the audit JSON must
// carry the same representation) and the 0-ordinal / 0-mask sentinels.
func TestRunPointInTime_EnumSetOrdinalsMapToLabels(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	// Snapshot rows with column_type populated (testutil.InsertSnapshot
	// predates the column, so raw SQL keeps the shared helper untouched).
	snapTS := now.Format("2006-01-02 15:04:05")
	insertTyped := func(column string, ordinal int, key, dataType, columnType string) {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable)
			VALUES (1, ?, 'myapp', 'orders', ?, ?, ?, ?, ?, 'NO')`,
			snapTS, column, ordinal, key, dataType, columnType)
	}
	insertTyped("id", 1, "PRI", "int", "int unsigned")
	insertTyped("status", 2, "", "enum", "enum('pending','processing','shipped')")
	insertTyped("tags", 3, "", "set", "set('red','blue')")

	// UPDATE event: pending/no-tags → shipped/red,blue — all stored as
	// ordinals/bitmasks, exactly as the binlog ROW image records them.
	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "orders", 2, "1", []byte(`["status","tags"]`),
		[]byte(`{"id":1,"status":1,"tags":0}`),
		[]byte(`{"id":1,"status":3,"tags":3}`))

	h := NewHandlerWithConfig(db, Config{
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	result, err := h.runPointInTime(TimeTravelQuery{
		Type:     TypeFlashback,
		Schema:   "myapp",
		Table:    "orders",
		PKColumn: "id",
		PKValue:  "1",
		AsOf:     now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	gotFields := fieldNames(result.Resultset.Fields)
	if want := []string{"id", "status", "tags"}; !slices.Equal(gotFields, want) {
		t.Fatalf("fields = %v, want %v", gotFields, want)
	}
	cells := rowCells(t, result.Resultset)
	if len(cells) != 1 {
		t.Fatalf("expected 1 row, got %d", len(cells))
	}
	if got, want := cells[0][1], "shipped"; got != want {
		t.Errorf("status = %q, want %q (enum ordinal not mapped to label)", got, want)
	}
	if got, want := cells[0][2], "red,blue"; got != want {
		t.Errorf("tags = %q, want %q (set bitmask not mapped to members)", got, want)
	}

	// _diff must show the same representation in both image JSONs,
	// including the 0-ordinal ("") and 0-mask ("") sentinels in row_before.
	diffResult, err := h.runDiff(TimeTravelQuery{
		Type:    TypeDiff,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "1",
		Since:   now.Add(time.Minute),
		Until:   now.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runDiff: %v", err)
	}
	diffCells := rowCells(t, diffResult.Resultset)
	if len(diffCells) != 1 {
		t.Fatalf("_diff: expected 1 row, got %d", len(diffCells))
	}
	rowBefore, rowAfter := diffCells[0][4], diffCells[0][5]
	if !strings.Contains(rowBefore, `"status":"pending"`) || !strings.Contains(rowBefore, `"tags":""`) {
		t.Errorf("row_before = %s, want status mapped to \"pending\" and tags to \"\"", rowBefore)
	}
	if !strings.Contains(rowAfter, `"status":"shipped"`) || !strings.Contains(rowAfter, `"tags":"red,blue"`) {
		t.Errorf("row_after = %s, want status \"shipped\" and tags \"red,blue\"", rowAfter)
	}

	// _flashback full-table (no WHERE → runFullTable): its mapping loop
	// is wired differently from the single-row path (explicit hoist),
	// so the point-lookup assertions above don't cover it.
	ftRes, err := h.runPointInTime(TimeTravelQuery{
		Type:   TypeFlashback,
		Schema: "myapp",
		Table:  "orders",
		AsOf:   now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("full-table _flashback: %v", err)
	}
	ftCells := rowCells(t, ftRes.Resultset)
	if len(ftCells) != 1 {
		t.Fatalf("full-table: expected 1 row, got %d", len(ftCells))
	}
	if got, want := ftCells[0][1], "shipped"; got != want {
		t.Errorf("full-table status = %q, want %q", got, want)
	}
	if got, want := ftCells[0][2], "red,blue"; got != want {
		t.Errorf("full-table tags = %q, want %q", got, want)
	}
}

// TestRunPointInTime_BlobTextDecoded pins #661 end-to-end against a real index
// DB: BLOB/TEXT columns are stored base64-encoded (marshalRow base64-encodes the
// []byte go-mysql delivers), so _flashback must decode them before emission or
// the client gets the base64 text instead of the real value. Covers the single-
// row path, the full-table path (with a NULL row to exercise []byte + nil column
// type-consistency), and _diff (TEXT renders as the real string in the audit JSON).
func TestRunPointInTime_BlobTextDecoded(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)
	snapTS := now.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "docs", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "docs", "body", 2, "", "text", "YES")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "docs", "payload", 3, "", "blob", "YES")

	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	rawBlob := "\x00\xff\x7f\x80" // arbitrary non-UTF-8 bytes must survive the wire
	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	// id=1: real text + binary blob, both stored base64.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "docs", 1 /*insert*/, "1", nil, nil,
		[]byte(fmt.Sprintf(`{"id":1,"body":%q,"payload":%q}`, b64("hello world"), b64(rawBlob))))
	// id=2: NULL body, so the full-table BLOB/TEXT columns mix decoded values
	// with NULL — the one case that could trip BuildSimpleTextResultset.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, eventTS, nil,
		"myapp", "docs", 1 /*insert*/, "2", nil, nil,
		[]byte(fmt.Sprintf(`{"id":2,"body":null,"payload":%q}`, b64("second"))))

	h := NewHandlerWithConfig(db, Config{NoArchive: true, IndexDBName: dbName}, slog.Default())

	// Single-row _flashback id=1.
	res, err := h.runPointInTime(TimeTravelQuery{
		Type: TypeFlashback, Schema: "myapp", Table: "docs",
		PKColumn: "id", PKValue: "1", AsOf: now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 {
		t.Fatalf("expected 1 row, got %d", len(cells))
	}
	if got, want := cells[0][1], "hello world"; got != want {
		t.Errorf("body = %q, want %q (TEXT not decoded from base64)", got, want)
	}
	if got, want := cells[0][2], rawBlob; got != want {
		t.Errorf("payload = %q, want %q (BLOB not decoded from base64)", got, want)
	}

	// Full-table _flashback: id=1 decoded, id=2 body NULL + payload decoded.
	ftRes, err := h.runFullTable(TimeTravelQuery{
		Type: TypeFlashback, Schema: "myapp", Table: "docs", AsOf: now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("full-table _flashback: %v", err)
	}
	byID := make(map[string][]string)
	for _, c := range rowCells(t, ftRes.Resultset) {
		byID[c[0]] = c
	}
	if len(byID) != 2 {
		t.Fatalf("full-table: expected 2 rows, got %d (%v)", len(byID), byID)
	}
	if got, want := byID["1"][1], "hello world"; got != want {
		t.Errorf("full-table id=1 body = %q, want %q", got, want)
	}
	if got, want := byID["2"][1], "NULL"; got != want {
		t.Errorf("full-table id=2 body = %q, want %q (NULL must survive alongside decoded rows)", got, want)
	}
	if got, want := byID["2"][2], "second"; got != want {
		t.Errorf("full-table id=2 payload = %q, want %q", got, want)
	}

	// _diff id=1: the audit JSON must carry the decoded TEXT, not the base64.
	diffRes, err := h.runDiff(TimeTravelQuery{
		Type: TypeDiff, Schema: "myapp", Table: "docs", PKValue: "1",
		Since: now.Add(time.Minute), Until: now.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runDiff: %v", err)
	}
	diffCells := rowCells(t, diffRes.Resultset)
	if len(diffCells) != 1 {
		t.Fatalf("_diff: expected 1 row, got %d", len(diffCells))
	}
	if rowAfter := diffCells[0][5]; !strings.Contains(rowAfter, `"body":"hello world"`) {
		t.Errorf("_diff row_after = %s, want body decoded to \"hello world\"", rowAfter)
	}
}

// TestEnumLabels_FullChainFromRealSnapshot covers the wiring no other
// test exercises end-to-end: real CREATE TABLE → metadata.TakeSnapshot
// (capturing an ENUM declaration well past #212's old VARCHAR(128)
// limit, with a backslash-escaped member) → resolver load → shim label
// mapping on the wire. Before column_type was widened to TEXT, this
// test failed at the TakeSnapshot step with a 1406 that aborted the
// whole snapshot transaction (#472 review finding).
func TestEnumLabels_FullChainFromRealSnapshot(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	sourceDB, sourceName := testutil.CreateTestDB(t)
	// The status declaration renders a 189-char COLUMN_TYPE; path's first
	// member is `a\b`, which information_schema renders as 'a\\b'.
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY,
		status ENUM('pending_payment','payment_confirmed','awaiting_fulfillment','partially_shipped','shipped','out_for_delivery','delivered','return_requested','refund_processed','cancelled_by_customer') NOT NULL,
		path ENUM('a\\b','plain') NOT NULL
	) ENGINE=InnoDB`)

	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot must survive a long ENUM declaration: %v", err)
	}
	var capturedLen int
	if err := indexDB.QueryRow(
		`SELECT CHAR_LENGTH(column_type) FROM schema_snapshots WHERE table_name = 'orders' AND column_name = 'status'`,
	).Scan(&capturedLen); err != nil {
		t.Fatalf("read captured column_type length: %v", err)
	}
	if capturedLen <= 128 {
		t.Fatalf("fixture regression: status COLUMN_TYPE is %d chars, must exceed the old 128 limit to pin the widening", capturedLen)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, indexDB, now)
	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	// status ordinal 5 = 'shipped'; path ordinal 1 = the escaped member a\b.
	testutil.InsertEvent(t, indexDB, "mysql-bin.000001", 100, 200, eventTS, nil,
		sourceName, "orders", 1, "1", nil, nil,
		[]byte(`{"id":1,"status":5,"path":1}`))

	h := NewHandlerWithConfig(indexDB, Config{
		NoArchive:   true,
		IndexDBName: indexName,
	}, slog.Default())
	res, err := h.runPointInTime(TimeTravelQuery{
		Type:     TypeFlashback,
		Schema:   sourceName,
		Table:    "orders",
		PKColumn: "id",
		PKValue:  "1",
		AsOf:     now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 {
		t.Fatalf("expected 1 row, got %d", len(cells))
	}
	if got, want := cells[0][1], "shipped"; got != want {
		t.Errorf("status = %q, want %q (ordinal 5 of the real captured declaration)", got, want)
	}
	if got, want := cells[0][2], "a\\b"; got != want {
		t.Errorf("path = %q, want %q (backslash member must roundtrip byte-exact)", got, want)
	}
}

// TestEnumLabels_EpochAwareDecoding pins #475: an enum REORDERED between
// two snapshots must decode each event with the definition in effect at
// the event's timestamp. Ordinal 3 means 'shipped' under epoch 1 but
// 'pending' under epoch 2 — a latest-snapshot-only decode (the pre-#475
// behavior) would confidently mislabel the older event. Also pins the
// clamp: an event predating the first snapshot decodes with the first
// epoch.
func TestEnumLabels_EpochAwareDecoding(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	insertTyped := func(snapID int, snapTS, column string, ordinal int, key, dataType, columnType string) {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable)
			VALUES (?, ?, 'myapp', 'orders', ?, ?, ?, ?, ?, 'NO')`,
			snapID, snapTS, column, ordinal, key, dataType, columnType)
	}
	snap1TS := now.Add(1 * time.Minute).Format("2006-01-02 15:04:05")
	snap2TS := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	insertTyped(1, snap1TS, "id", 1, "PRI", "int", "int")
	insertTyped(1, snap1TS, "status", 2, "", "enum", "enum('pending','processing','shipped')")
	insertTyped(2, snap2TS, "id", 1, "PRI", "int", "int")
	insertTyped(2, snap2TS, "status", 2, "", "enum", "enum('shipped','processing','pending')")

	insertEventAt := func(pos uint64, ts time.Time, pk string, rowAfter string) {
		testutil.InsertEvent(t, db, "mysql-bin.000001", pos, pos+100,
			ts.Format("2006-01-02 15:04:05"), nil,
			"myapp", "orders", 1, pk, nil, nil, []byte(rowAfter))
	}
	// Event C predates the first snapshot → clamps to epoch 1.
	insertEventAt(100, now.Add(30*time.Second), "3", `{"id":3,"status":3}`)
	// Event A inside epoch 1: ordinal 3 = 'shipped' under v1.
	insertEventAt(200, now.Add(5*time.Minute), "1", `{"id":1,"status":3}`)
	// Event B inside epoch 2: ordinal 1 = 'shipped' under v2.
	insertEventAt(300, now.Add(15*time.Minute), "2", `{"id":2,"status":1}`)

	h := NewHandlerWithConfig(db, Config{
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	asOf := now.Add(20 * time.Minute)
	for _, tc := range []struct{ pk, want, why string }{
		{"1", "shipped", "epoch-1 event must decode with epoch-1 labels (latest-only would say 'pending')"},
		{"2", "shipped", "epoch-2 event must decode with epoch-2 labels"},
		{"3", "shipped", "pre-first-snapshot event must clamp to the first epoch"},
	} {
		res, err := h.runPointInTime(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "id", PKValue: tc.pk, AsOf: asOf,
		})
		if err != nil {
			t.Fatalf("pk=%s: %v", tc.pk, err)
		}
		cells := rowCells(t, res.Resultset)
		if len(cells) != 1 {
			t.Fatalf("pk=%s: expected 1 row, got %d", tc.pk, len(cells))
		}
		if got := cells[0][1]; got != tc.want {
			t.Errorf("pk=%s: status = %q, want %q — %s", tc.pk, got, tc.want, tc.why)
		}
	}
}

// TestRunPointInTime_BlobTextEpochAware is the load-bearing proof that the #661
// base64 decode is resolved at each event's epoch, not from the latest snapshot.
// A column widened VARCHAR→TEXT across the flashback window is the trap: an old
// VARCHAR value reached go-mysql as a Go string, so marshalRow stored it as a
// PLAIN JSON string (never base64). If the decode used the latest (TEXT) snapshot
// for the old event, a plain value that happens to be valid base64 ("test")
// would be silently mangled to garbage bytes. The epoch-aware lookup types the
// old event's column as VARCHAR → leaves it untouched, while the new TEXT event
// is still decoded.
func TestRunPointInTime_BlobTextEpochAware(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	insertTyped := func(snapID int, snapTS, column string, ordinal int, key, dataType string) {
		testutil.InsertSnapshot(t, db, snapID, snapTS, "myapp", "docs", column, ordinal, key, dataType, "YES")
	}
	snap1TS := now.Add(1 * time.Minute).Format("2006-01-02 15:04:05")
	snap2TS := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// Epoch 1: notes is VARCHAR (stored plain). Epoch 2: notes widened to TEXT.
	insertTyped(1, snap1TS, "id", 1, "PRI", "int")
	insertTyped(1, snap1TS, "notes", 2, "", "varchar")
	insertTyped(2, snap2TS, "id", 1, "PRI", "int")
	insertTyped(2, snap2TS, "notes", 2, "", "text")

	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	// Event A in epoch 1: VARCHAR value stored as a PLAIN string. "test" is valid
	// base64, so an epoch-blind decode would corrupt it.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200,
		now.Add(5*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"myapp", "docs", 1 /*insert*/, "1", nil, nil, []byte(`{"id":1,"notes":"test"}`))
	// Event B in epoch 2: TEXT value stored base64-encoded.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 300, 400,
		now.Add(15*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"myapp", "docs", 1 /*insert*/, "2", nil, nil,
		[]byte(fmt.Sprintf(`{"id":2,"notes":%q}`, b64("hi there"))))

	h := NewHandlerWithConfig(db, Config{NoArchive: true, IndexDBName: dbName}, slog.Default())
	asOf := now.Add(20 * time.Minute)
	for _, tc := range []struct{ pk, want, why string }{
		{"1", "test", "epoch-1 VARCHAR value was stored plain, NOT base64 — decoding it (latest=TEXT) corrupts it"},
		{"2", "hi there", "epoch-2 TEXT value is base64-stored and must be decoded"},
	} {
		res, err := h.runPointInTime(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "docs",
			PKColumn: "id", PKValue: tc.pk, AsOf: asOf,
		})
		if err != nil {
			t.Fatalf("pk=%s: %v", tc.pk, err)
		}
		cells := rowCells(t, res.Resultset)
		if len(cells) != 1 {
			t.Fatalf("pk=%s: expected 1 row, got %d", tc.pk, len(cells))
		}
		if got := cells[0][1]; got != tc.want {
			t.Errorf("pk=%s: notes = %q, want %q — %s", tc.pk, got, tc.want, tc.why)
		}
	}
}
