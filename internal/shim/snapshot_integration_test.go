//go:build integration

package shim

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/testutil"
)

// writeBaselineSnapshot writes a baseline Parquet file into the
// FindBaseline directory layout: <root>/<RFC3339-name>/<schema>/<table>.parquet.
// cols is the ordered column set; rows are string-encoded cell values
// matching baseline.Writer's WriteRow contract. Returns the root dir to
// pass as Config.BaselineDir.
func writeBaselineSnapshot(t *testing.T, snapTime time.Time, schema, table string, cols []baseline.Column, rows [][]string) string {
	t.Helper()
	root := t.TempDir()
	// Dir name is RFC3339 with the time portion's colons replaced by
	// hyphens (parseDirTimestamp restores them), matching what
	// `bintrail baseline` writes on disk.
	dirName := snapTime.UTC().Format("2006-01-02T15-04-05") + "Z"
	tableDir := filepath.Join(root, dirName, schema)
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline layout: %v", err)
	}
	path := filepath.Join(tableDir, table+".parquet")

	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	nulls := make([]bool, len(cols))
	for _, r := range rows {
		if err := w.WriteRow(r, nulls); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("baseline writer close: %v", err)
	}
	return root
}

// usersBaselineCols is the (id INT, name VARCHAR) column set the snapshot
// tests reuse.
func usersBaselineCols() []baseline.Column {
	return []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
}

// seedUsersSnapshot records a schema_snapshots row set for myapp.users so
// the resolver can answer the PK-type guard and column ordering.
func seedUsersSnapshot(t *testing.T, db *sql.DB, snapTime time.Time) {
	t.Helper()
	ts := snapTime.UTC().Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "users", "name", 2, "", "varchar", "YES")
}

// rowCells parses every RowData in a server-built resultset into string
// cells so tests can assert the actual values (BuildSimpleTextResultset
// populates RowDatas, not Values, so GetString* can't be used directly).
func rowCells(t *testing.T, rs *mysql.Resultset) [][]string {
	t.Helper()
	out := make([][]string, 0, len(rs.RowDatas))
	for _, rd := range rs.RowDatas {
		fvs, err := rd.Parse(rs.Fields, false, nil)
		if err != nil {
			t.Fatalf("parse row data: %v", err)
		}
		cells := make([]string, len(fvs))
		for i := range fvs {
			// ParseText yields typed FieldValues; AsString() is empty for
			// numerics, so go through Value() and stringify per type.
			switch v := fvs[i].Value().(type) {
			case nil:
				cells[i] = "NULL"
			case []byte:
				cells[i] = string(v)
			default:
				cells[i] = fmt.Sprintf("%v", v)
			}
		}
		out = append(out, cells)
	}
	return out
}

// TestSnapshotBaseline_DivergenceAndMerge is the core proof for #355: with
// a baseline configured, _snapshot resolves a row that exists in the
// baseline but was never touched in the binlog window, while _flashback
// (binlog-only) returns nothing for the same query. It also covers the
// three merge cases: UPDATE-after-baseline (event wins), DELETE-after-
// baseline (row absent), and INSERT-after-baseline (row not in baseline
// appears).
func TestSnapshotBaseline_DivergenceAndMerge(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)

	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	seedUsersSnapshot(t, db, snapTime)

	// Baseline at snapTime: four rows. id=1 (alice) is never touched
	// afterwards; id=2 (bob) is updated; id=3 (carol) is deleted; id=4 is
	// absent (inserted later).
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "users", usersBaselineCols(), [][]string{
		{"1", "alice"},
		{"2", "bob"},
		{"3", "carol"},
	})

	// Post-baseline binlog events: UPDATE id=2, DELETE id=3, INSERT id=4.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 2 /*update*/, "2", nil,
		[]byte(`{"id":2,"name":"bob"}`), []byte(`{"id":2,"name":"bob2"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, eventTS, nil,
		"myapp", "users", 3 /*delete*/, "3", nil,
		[]byte(`{"id":3,"name":"carol"}`), nil)
	testutil.InsertEvent(t, db, "mysql-bin.000001", 300, 400, eventTS, nil,
		"myapp", "users", 1 /*insert*/, "4", nil, nil,
		[]byte(`{"id":4,"name":"dave"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())

	snapshotQ := func(pk string) TimeTravelQuery {
		return TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", PKColumn: "id", PKValue: pk, AsOf: asOf}
	}

	// ── id=1: never touched → present under _snapshot, absent under _flashback ──
	snapRes, err := h.runSnapshot(snapshotQ("1"))
	if err != nil {
		t.Fatalf("_snapshot id=1: %v", err)
	}
	cells := rowCells(t, snapRes.Resultset)
	if len(cells) != 1 {
		t.Fatalf("_snapshot id=1: expected 1 row, got %d", len(cells))
	}
	if want := []string{"1", "alice"}; !slices.Equal(cells[0], want) {
		t.Errorf("_snapshot id=1 row = %v, want %v", cells[0], want)
	}
	if want := []string{"id", "name"}; !slices.Equal(fieldNames(snapRes.Resultset.Fields), want) {
		t.Errorf("_snapshot id=1 fields = %v, want %v", fieldNames(snapRes.Resultset.Fields), want)
	}

	flashRes, err := h.runPointInTime(TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "users", PKColumn: "id", PKValue: "1", AsOf: asOf})
	if err != nil {
		t.Fatalf("_flashback id=1: %v", err)
	}
	if got := len(flashRes.Resultset.RowDatas); got != 0 {
		t.Errorf("_flashback id=1 (never touched): expected 0 rows, got %d — divergence with _snapshot lost", got)
	}

	// ── id=2: updated after baseline → event image wins over baseline ──
	res2, err := h.runSnapshot(snapshotQ("2"))
	if err != nil {
		t.Fatalf("_snapshot id=2: %v", err)
	}
	cells = rowCells(t, res2.Resultset)
	if len(cells) != 1 || !slices.Equal(cells[0], []string{"2", "bob2"}) {
		t.Errorf("_snapshot id=2 = %v, want [[2 bob2]] (post-baseline UPDATE must win)", cells)
	}

	// ── id=3: deleted after baseline → absent ──
	res3, err := h.runSnapshot(snapshotQ("3"))
	if err != nil {
		t.Fatalf("_snapshot id=3: %v", err)
	}
	if got := len(res3.Resultset.RowDatas); got != 0 {
		t.Errorf("_snapshot id=3 (deleted after baseline): expected 0 rows, got %d", got)
	}

	// ── id=4: inserted after baseline (not in baseline) → present ──
	res4, err := h.runSnapshot(snapshotQ("4"))
	if err != nil {
		t.Fatalf("_snapshot id=4: %v", err)
	}
	cells = rowCells(t, res4.Resultset)
	if len(cells) != 1 || !slices.Equal(cells[0], []string{"4", "dave"}) {
		t.Errorf("_snapshot id=4 = %v, want [[4 dave]] (post-baseline INSERT must appear)", cells)
	}
}

// TestSnapshotBaseline_HandleQueryWiring drives a real query string
// through HandleQuery → Parse → dispatch, proving the TypeSnapshot switch
// actually reaches the baseline path (and TypeFlashback does not). The
// other tests call runSnapshot/runPointInTime directly, so without this
// a wrong switch arm (TypeSnapshot routed back to runPointInTime) would
// leave every assertion green while the feature was dead.
func TestSnapshotBaseline_HandleQueryWiring(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	asOfLit := asOf.Format("2006-01-02 15:04:05")

	seedUsersSnapshot(t, db, snapTime)
	// id=1 exists in the baseline and is never touched in binlog.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "users", usersBaselineCols(), [][]string{
		{"1", "alice"},
	})

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())
	if err := h.UseDB("myapp"); err != nil {
		t.Fatalf("UseDB: %v", err)
	}

	// _snapshot must reach the baseline path and surface the untouched row.
	snapRes, err := h.HandleQuery("SELECT * FROM _snapshot.users AS OF '" + asOfLit + "' WHERE id = 1")
	if err != nil {
		t.Fatalf("HandleQuery _snapshot: %v", err)
	}
	cells := rowCells(t, snapRes.Resultset)
	if len(cells) != 1 || !slices.Equal(cells[0], []string{"1", "alice"}) {
		t.Errorf("HandleQuery _snapshot id=1 = %v, want [[1 alice]] — TypeSnapshot did not reach runSnapshot", cells)
	}

	// _flashback (binlog-only) over the same query must stay empty.
	flashRes, err := h.HandleQuery("SELECT * FROM _flashback.users AS OF '" + asOfLit + "' WHERE id = 1")
	if err != nil {
		t.Fatalf("HandleQuery _flashback: %v", err)
	}
	if got := len(flashRes.Resultset.RowDatas); got != 0 {
		t.Errorf("HandleQuery _flashback id=1: expected 0 rows, got %d", got)
	}
}

// TestSnapshotBaseline_DecimalPK proves the PK-type guard's promise for a
// non-integer supported type: a DECIMAL PK is stored as a Parquet STRING,
// so ReadBaselineRow's `col = ?` string-param match round-trips and a
// never-touched row resolves from the baseline. This keeps int from being
// the only proven type behind baselinePKStringMatchable.
func TestSnapshotBaseline_DecimalPK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	ts := snapTime.UTC().Format("2006-01-02 15:04:05")

	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "prices", "amt", 1, "PRI", "decimal", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "prices", "label", 2, "", "varchar", "YES")

	cols := []baseline.Column{
		{Name: "amt", MySQLType: "decimal", ParquetType: baseline.MysqlToParquetNode("decimal")},
		{Name: "label", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	pkVal := "9.99"
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "prices", cols, [][]string{
		{pkVal, "cheap"},
	})

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{
		Type: TypeSnapshot, Schema: "myapp", Table: "prices",
		PKColumn: "amt", PKValue: pkVal, AsOf: asOf,
	})
	if err != nil {
		t.Fatalf("_snapshot decimal PK: %v", err)
	}
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 || cells[0][1] != "cheap" {
		t.Errorf("_snapshot decimal PK = %v, want a row with label=cheap (decimal baseline match missed)", cells)
	}
}

// TestSnapshotBaseline_DatetimePKFallsBack pins the negative half of the
// guard: a DATETIME PK is stored as a Parquet TIMESTAMP, where DuckDB's
// `col = '<string>'` match does not reliably hit, so baselinePKStringMatchable
// excludes it and runSnapshot falls back to the binlog-only path instead of
// risking a false "row never existed". The fallback must still resolve a row
// that has binlog events. (A datetime-PK row that was never touched is a
// known limitation — recoverable only via the offline `bintrail reconstruct`.)
func TestSnapshotBaseline_DatetimePKFallsBack(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	ts := snapTime.UTC().Format("2006-01-02 15:04:05")
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "logins", "at", 1, "PRI", "datetime", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "logins", "who", 2, "", "varchar", "YES")

	cols := []baseline.Column{
		{Name: "at", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "who", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	pkVal := "2020-01-01 00:00:00"
	// Baseline holds a different `who` ("stale"); if the guard wrongly used
	// the (missing) baseline match the result could diverge. With the guard
	// excluding datetime, only the binlog event below is consulted.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "logins", cols, [][]string{
		{pkVal, "stale"},
	})

	// A binlog INSERT for the datetime-PK row. pk_values for a datetime PK is
	// the go-mysql string form, matching what the parser hands runSnapshot.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "logins", 1 /*insert*/, pkVal, nil, nil,
		[]byte(`{"at":"2020-01-01 00:00:00","who":"fresh"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{
		Type: TypeSnapshot, Schema: "myapp", Table: "logins",
		PKColumn: "at", PKValue: pkVal, AsOf: asOf,
	})
	if err != nil {
		t.Fatalf("_snapshot datetime PK (fallback): %v", err)
	}
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 || cells[0][1] != "fresh" {
		t.Errorf("_snapshot datetime PK fallback = %v, want a row with who=fresh from the binlog-only path", cells)
	}
}

// TestSnapshotBaseline_EmptyStringPK is a regression test for the empty-PK
// filter bypass: `WHERE name = ''` against a NOT-NULL string PK is a
// documented legitimate shape, but Options.PKValues=="" disables the
// pk_values filter in buildQuery. If runSnapshotPointInTime routed the
// empty value through PKValues (not PKValuesIn), the fetch would return
// every event for the table and ApplyAt would fold an unrelated PK's
// latest image onto the baseline row — a silent wrong answer. This pins
// that the empty-PK query returns exactly the empty-PK baseline row,
// untouched by other PKs' events.
func TestSnapshotBaseline_EmptyStringPK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	ts := snapTime.UTC().Format("2006-01-02 15:04:05")
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	// PK is the `name` VARCHAR column.
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "people", "name", 1, "PRI", "varchar", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "people", "note", 2, "", "varchar", "YES")

	cols := []baseline.Column{
		{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "note", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	// Baseline holds the legitimate empty-PK row, never touched in binlog.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "people", cols, [][]string{
		{"", "the-empty-pk-row"},
	})

	// Unrelated PKs with later events. If the filter were bypassed, ApplyAt
	// would fold "carol" (last in commit order) onto the empty-PK baseline.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "people", 1 /*insert*/, "alice", nil, nil,
		[]byte(`{"name":"alice","note":"a"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, eventTS, nil,
		"myapp", "people", 1 /*insert*/, "carol", nil, nil,
		[]byte(`{"name":"carol","note":"c"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{
		Type: TypeSnapshot, Schema: "myapp", Table: "people",
		PKColumn: "name", PKValue: "", AsOf: asOf,
	})
	if err != nil {
		t.Fatalf("_snapshot empty PK: %v", err)
	}
	// The discriminating assertion is the `note` column: it must be the
	// empty-PK baseline row, NOT "a"/"c" from alice/carol. If the pk_values
	// filter were bypassed, ApplyAt would fold carol (last in commit order)
	// onto the baseline and note would be "c". (The empty-string PK value
	// itself may round-trip through Parquet/DuckDB as NULL — a benign
	// representation detail orthogonal to the filter bug under test.)
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 {
		t.Fatalf("_snapshot WHERE name='' returned %d rows, want 1: %v", len(cells), cells)
	}
	if cells[0][1] != "the-empty-pk-row" {
		t.Errorf("_snapshot WHERE name='' note = %q, want \"the-empty-pk-row\" — empty-PK filter bypass leaked another PK's event", cells[0][1])
	}
}

// TestSnapshotBaseline_NoBaselineFallsBackToBinlogOnly proves that when no
// baseline source is configured, _snapshot behaves exactly like the
// binlog-only _flashback path: a row with events resolves, a row that was
// never touched does not (no baseline to fall back on).
func TestSnapshotBaseline_NoBaselineFallsBackToBinlogOnly(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	seedUsersSnapshot(t, db, snapTime)

	// Only id=7 has a binlog event; id=1 is never present anywhere.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1 /*insert*/, "7", nil, nil,
		[]byte(`{"id":7,"name":"grace"}`))

	// No BaselineDir / BaselineS3 → _snapshot must degrade to binlog-only.
	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	// Row with an event resolves (binlog-only path).
	res7, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", PKColumn: "id", PKValue: "7", AsOf: asOf})
	if err != nil {
		t.Fatalf("_snapshot id=7: %v", err)
	}
	cells := rowCells(t, res7.Resultset)
	if len(cells) != 1 || !slices.Equal(cells[0], []string{"7", "grace"}) {
		t.Errorf("_snapshot id=7 (no baseline) = %v, want [[7 grace]]", cells)
	}

	// Never-touched row stays absent: with no baseline there is nothing to
	// recover it from — same as _flashback.
	res1, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", PKColumn: "id", PKValue: "1", AsOf: asOf})
	if err != nil {
		t.Fatalf("_snapshot id=1: %v", err)
	}
	if got := len(res1.Resultset.RowDatas); got != 0 {
		t.Errorf("_snapshot id=1 (no baseline, never touched): expected 0 rows, got %d", got)
	}
}
