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

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
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

// TestSnapshotBaseline_DatetimePKResolvesFromBaseline proves the positive half
// of the guard after #359: a DATETIME PK is stored as a Parquet TIMESTAMP, and
// with ReadBaselineRow now pinning the DuckDB session to UTC the
// `col = '<string>'` match resolves deterministically — so baselinePKStringMatchable
// admits datetime and a never-touched datetime-PK row resolves straight from the
// baseline (previously this fell back to binlog-only and returned nothing). It
// also covers the merge case: a datetime-PK row updated after the baseline has
// the binlog event win over the baseline image.
func TestSnapshotBaseline_DatetimePKResolvesFromBaseline(t *testing.T) {
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
	untouchedPK := "2020-01-01 00:00:00"
	updatedPK := "2020-02-02 00:00:00"
	// Baseline holds two datetime-PK rows: one never touched in binlog, one
	// updated afterwards.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "logins", cols, [][]string{
		{untouchedPK, "alice"},
		{updatedPK, "bob"},
	})

	// Post-baseline UPDATE on the second row. pk_values for a datetime PK is
	// the go-mysql string form, matching what the parser hands runSnapshot.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "logins", 2 /*update*/, updatedPK, nil,
		[]byte(`{"at":"2020-02-02 00:00:00","who":"bob"}`),
		[]byte(`{"at":"2020-02-02 00:00:00","who":"bob2"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())

	snapshotQ := func(pk string) TimeTravelQuery {
		return TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "logins", PKColumn: "at", PKValue: pk, AsOf: asOf}
	}

	// ── never-touched datetime-PK row resolves from the baseline ──
	res, err := h.runSnapshot(snapshotQ(untouchedPK))
	if err != nil {
		t.Fatalf("_snapshot datetime PK (untouched): %v", err)
	}
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 || cells[0][1] != "alice" {
		t.Errorf("_snapshot datetime PK %q = %v, want a row with who=alice from the baseline (pre-#359 this fell back to binlog-only and returned nothing)", untouchedPK, cells)
	}

	// Cross-check the divergence: _flashback (binlog-only) sees nothing for the
	// never-touched row, exactly the gap the baseline lookup closes.
	flashRes, err := h.runPointInTime(TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "logins", PKColumn: "at", PKValue: untouchedPK, AsOf: asOf})
	if err != nil {
		t.Fatalf("_flashback datetime PK (untouched): %v", err)
	}
	if got := len(flashRes.Resultset.RowDatas); got != 0 {
		t.Errorf("_flashback datetime PK %q (never touched): expected 0 rows, got %d — divergence with _snapshot lost", untouchedPK, got)
	}

	// ── updated datetime-PK row: binlog event wins over the baseline image ──
	res2, err := h.runSnapshot(snapshotQ(updatedPK))
	if err != nil {
		t.Fatalf("_snapshot datetime PK (updated): %v", err)
	}
	cells = rowCells(t, res2.Resultset)
	if len(cells) != 1 || cells[0][1] != "bob2" {
		t.Errorf("_snapshot datetime PK %q = %v, want a row with who=bob2 (post-baseline UPDATE must win)", updatedPK, cells)
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

// rowsByKey indexes a resultset's rows by their first column (the PK), mapping
// it to the second column, so full-table assertions are order-independent
// (baseline scan order vs. appended-insert order is an implementation detail).
func rowsByKey(t *testing.T, rs *mysql.Resultset) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, cells := range rowCells(t, rs) {
		if len(cells) < 2 {
			t.Fatalf("rowsByKey: expected >=2 columns, got %v", cells)
		}
		out[cells[0]] = cells[1]
	}
	return out
}

// TestSnapshotBaseline_FullTableMerge is the core proof for #362: a no-WHERE
// full-table _snapshot reconstructs the whole table at AS OF by merging the
// baseline with post-snapshot binlog deltas — a never-touched row appears, an
// updated row takes its latest image, a deleted row drops out, and a row
// inserted after the baseline appears. The binlog-only full-table _flashback
// over the same instant omits the never-touched row, which is exactly the gap
// the baseline merge closes.
func TestSnapshotBaseline_FullTableMerge(t *testing.T) {
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

	// Baseline: id=1 alice (never touched), id=2 bob (updated), id=3 carol
	// (deleted). id=4 is inserted only in binlog.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "users", usersBaselineCols(), [][]string{
		{"1", "alice"},
		{"2", "bob"},
		{"3", "carol"},
	})
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

	fullTableQ := TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf}

	res, err := h.runSnapshot(fullTableQ)
	if err != nil {
		t.Fatalf("full-table _snapshot: %v", err)
	}
	got := rowsByKey(t, res.Resultset)
	want := map[string]string{"1": "alice", "2": "bob2", "4": "dave"}
	if len(got) != len(want) {
		t.Fatalf("full-table _snapshot returned %d rows %v, want %d %v", len(got), got, len(want), want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("full-table _snapshot row id=%s = %q, want %q (full map: %v)", k, got[k], v, got)
		}
	}
	if _, deleted := got["3"]; deleted {
		t.Errorf("full-table _snapshot included id=3, which was deleted after the baseline: %v", got)
	}
	if want, ok := got["1"]; !ok || want != "alice" {
		t.Errorf("full-table _snapshot missing never-touched baseline row id=1 (alice): %v", got)
	}
	if cols := fieldNames(res.Resultset.Fields); !slices.Equal(cols, []string{"id", "name"}) {
		t.Errorf("full-table _snapshot columns = %v, want [id name]", cols)
	}

	// Divergence: binlog-only full-table _flashback omits the never-touched row.
	flashRes, err := h.runFullTable(TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "users", AsOf: asOf})
	if err != nil {
		t.Fatalf("full-table _flashback: %v", err)
	}
	flash := rowsByKey(t, flashRes.Resultset)
	if _, present := flash["1"]; present {
		t.Errorf("full-table _flashback included never-touched id=1; divergence with _snapshot lost: %v", flash)
	}
	if flash["2"] != "bob2" || flash["4"] != "dave" {
		t.Errorf("full-table _flashback = %v, want id=2 bob2 and id=4 dave (rows with binlog activity)", flash)
	}
}

// TestSnapshotBaseline_FullTableMerge_DroppedColumn pins #600 acceptance
// criterion 4 on the path the other tests miss: the _snapshot BASELINE-MERGE
// path (snapshot.go:211 → fullTableResult), not the degraded no-baseline path
// that is byte-identical to _flashback. A column dropped between the baseline
// instant and now lives only in the pre-drop baseline Parquet; the merge must
// surface it on the never-touched baseline row even though the latest snapshot
// (which drives columnOrderFor) no longer lists it.
func TestSnapshotBaseline_FullTableMerge_DroppedColumn(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute) // pre-drop baseline + snapshot 1
	dropTime := hourTop.Add(8 * time.Minute) // re-snapshot after DROP COLUMN
	asOf := hourTop.Add(10 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	// Snapshot 1 (pre-drop): orders(id, coupon_code, total). Snapshot 2 (post-
	// drop, LATEST) drops coupon_code → columnOrderFor returns [id total].
	snap1 := snapTime.UTC().Format("2006-01-02 15:04:05")
	snap2 := dropTime.UTC().Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snap1, "myapp", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snap1, "myapp", "orders", "coupon_code", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, snap1, "myapp", "orders", "total", 3, "", "int", "NO")
	testutil.InsertSnapshot(t, db, 2, snap2, "myapp", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 2, snap2, "myapp", "orders", "total", 2, "", "int", "NO")

	// Pre-drop baseline carrying the since-dropped column. id=1 never touched
	// (its coupon_code can come only from the baseline); id=2 updated after.
	ordersCols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "coupon_code", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "total", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "orders", ordersCols, [][]string{
		{"1", "SAVE10", "100"},
		{"2", "OLD50", "200"},
	})
	// id=2 updated after the column was dropped: the event image no longer
	// carries coupon_code, so id=2's coupon_code resolves to NULL on the wire.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "orders", 2 /*update*/, "2", nil,
		[]byte(`{"id":2,"total":200}`), []byte(`{"id":2,"total":250}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "orders", AsOf: asOf})
	if err != nil {
		t.Fatalf("full-table _snapshot: %v", err)
	}
	fields := fieldNames(res.Resultset.Fields)
	cells := rowCells(t, res.Resultset)
	t.Logf("baseline-merge _snapshot fields=%v rows=%v", fields, cells)

	if !slices.Contains(fields, "coupon_code") {
		t.Fatalf("baseline-merge _snapshot must surface the since-dropped coupon_code, got fields %v", fields)
	}
	ccIdx := slices.Index(fields, "coupon_code")
	idIdx := slices.Index(fields, "id")
	var sawRow1 bool
	for _, row := range cells {
		if row[idIdx] == "1" {
			sawRow1 = true
			if row[ccIdx] != "SAVE10" {
				t.Errorf("never-touched baseline row id=1 coupon_code = %q, want SAVE10 (rows=%v)", row[ccIdx], cells)
			}
		}
	}
	if !sawRow1 {
		t.Errorf("baseline-merge _snapshot dropped the never-touched baseline row id=1: %v", cells)
	}
}

// TestSnapshotBaseline_FullTableRowCap proves the cost guardrail still bites on
// the merged path: with the cap below the reconstructed row count, full-table
// _snapshot returns ER_TOO_BIG_SELECT rather than buffering an unbounded
// resultset.
func TestSnapshotBaseline_FullTableRowCap(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)

	seedUsersSnapshot(t, db, snapTime)
	// Three never-touched baseline rows; cap of 1 must trip.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "users", usersBaselineCols(), [][]string{
		{"1", "alice"},
		{"2", "bob"},
		{"3", "carol"},
	})

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:       true,
		NoArchive:       true,
		IndexDBName:     dbName,
		BaselineDir:     baselineDir,
		FullTableRowCap: 1,
	}, slog.Default())

	_, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf})
	if err == nil {
		t.Fatalf("full-table _snapshot with cap=1 over 3 rows: expected ER_TOO_BIG_SELECT, got nil")
	}
	if me, ok := err.(*mysql.MyError); !ok || me.Code != mysql.ER_TOO_BIG_SELECT {
		t.Errorf("full-table _snapshot cap error = %v, want ER_TOO_BIG_SELECT (1104)", err)
	}
}

// TestSnapshotBaseline_FullTableNoBaselineFallsBack proves the "_snapshot
// degrades to _flashback" contract for the full-table shape: with no baseline
// source configured, full-table _snapshot returns exactly the binlog-only set
// (rows with activity), never erroring where _flashback would succeed.
func TestSnapshotBaseline_FullTableNoBaselineFallsBack(t *testing.T) {
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
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1 /*insert*/, "9", nil, nil,
		[]byte(`{"id":9,"name":"heidi"}`))

	// No baseline source.
	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf})
	if err != nil {
		t.Fatalf("full-table _snapshot (no baseline): %v", err)
	}
	got := rowsByKey(t, res.Resultset)
	if len(got) != 1 || got["9"] != "heidi" {
		t.Errorf("full-table _snapshot (no baseline) = %v, want only id=9 heidi (binlog-only fallback)", got)
	}
}

// TestSnapshotBaseline_FullTableHandleQueryWiring drives a real no-WHERE query
// string through HandleQuery → Parse → dispatch, proving the full-table
// TypeSnapshot path actually reaches runSnapshotFullTable (the other full-table
// tests call runSnapshot directly, so a wrong dispatch arm would leave them
// green while the feature was dead — the same gap TestSnapshotBaseline_HandleQueryWiring
// guards for the single-row shape). It also exercises a never-touched row with a
// DATETIME non-PK column, proving fullTableTextCell's UTC formatting end-to-end.
func TestSnapshotBaseline_FullTableHandleQueryWiring(t *testing.T) {
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
	ts := snapTime.UTC().Format("2006-01-02 15:04:05")
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	// events(id INT PK, name VARCHAR, created DATETIME, amount DECIMAL). The
	// DECIMAL column proves a never-touched baseline row's decimal value (stored
	// as a Parquet string → []byte) renders correctly through fullTableTextCell
	// rather than hitting the %v last resort.
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "events", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "events", "name", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "events", "created", 3, "", "datetime", "YES")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "events", "amount", 4, "", "decimal", "YES")

	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "created", MySQLType: "datetime", ParquetType: baseline.MysqlToParquetNode("datetime")},
		{Name: "amount", MySQLType: "decimal", ParquetType: baseline.MysqlToParquetNode("decimal")},
	}
	// id=1 never touched (proves baseline pass-through + datetime UTC format +
	// decimal rendering); id=2 updated after baseline.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "events", cols, [][]string{
		{"1", "alice", "2020-01-01 00:00:00", "12.34"},
		{"2", "bob", "2020-02-02 00:00:00", "99.99"},
	})
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "events", 2 /*update*/, "2", nil,
		[]byte(`{"id":2,"name":"bob","created":"2020-02-02 00:00:00","amount":"99.99"}`),
		[]byte(`{"id":2,"name":"bob2","created":"2020-02-02 00:00:00","amount":"99.99"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())
	if err := h.UseDB("myapp"); err != nil {
		t.Fatalf("UseDB: %v", err)
	}

	// Full-table _snapshot must reach the baseline merge and surface the
	// never-touched row with its datetime column formatted UTC.
	snapRes, err := h.HandleQuery("SELECT * FROM _snapshot.events AS OF '" + asOfLit + "'")
	if err != nil {
		t.Fatalf("HandleQuery full-table _snapshot: %v", err)
	}
	var row1 []string
	for _, cells := range rowCells(t, snapRes.Resultset) {
		if len(cells) > 0 && cells[0] == "1" {
			row1 = cells
		}
	}
	if row1 == nil {
		t.Fatalf("full-table _snapshot did not reach runSnapshotFullTable: never-touched id=1 absent in %v", rowCells(t, snapRes.Resultset))
	}
	if want := []string{"1", "alice", "2020-01-01 00:00:00", "12.34"}; !slices.Equal(row1, want) {
		t.Errorf("full-table _snapshot id=1 = %v, want %v (datetime must format UTC; decimal must render as its string)", row1, want)
	}

	// Full-table _flashback (binlog-only) must omit the never-touched row.
	flashRes, err := h.HandleQuery("SELECT * FROM _flashback.events AS OF '" + asOfLit + "'")
	if err != nil {
		t.Fatalf("HandleQuery full-table _flashback: %v", err)
	}
	for _, cells := range rowCells(t, flashRes.Resultset) {
		if len(cells) > 0 && cells[0] == "1" {
			t.Errorf("full-table _flashback included never-touched id=1; dispatch/divergence wrong: %v", cells)
		}
	}
}

// TestSnapshotBaseline_FullTableDoubleMerge is the end-to-end proof for #496/#505:
// a DOUBLE column whose rows mix a baseline-origin (DuckDB float64) integral value
// and event-origin (json.Number) fractional values must (a) NOT trip
// BuildSimpleTextResultset's "row types aren't consistent" (the crash the review
// caught), and (b) render baseline-origin and event-origin cells of the same
// value byte-identically (both via FormatTextValue).
func TestSnapshotBaseline_FullTableDoubleMerge(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOfLit := hourTop.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	ts := snapTime.UTC().Format("2006-01-02 15:04:05")
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "metrics", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "metrics", "score", 2, "", "double", "YES")

	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "score", MySQLType: "double", ParquetType: baseline.MysqlToParquetNode("double")},
	}
	// id=1 never touched (baseline-origin, integral double → "100"); id=2 updated
	// after baseline (event-origin, fractional); id=3 never touched (baseline-origin
	// fractional → must equal the event-origin 100.5).
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "metrics", cols, [][]string{
		{"1", "100"},
		{"2", "999"},
		{"3", "100.5"},
	})
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "metrics", 2 /*update*/, "2", nil,
		[]byte(`{"id":2,"score":999}`),
		[]byte(`{"id":2,"score":100.5}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir,
	}, slog.Default())
	if err := h.UseDB("myapp"); err != nil {
		t.Fatalf("UseDB: %v", err)
	}

	// Must NOT crash — the score column mixes integral ("100") and fractional
	// ("100.5") rendered cells.
	res, err := h.HandleQuery("SELECT * FROM _snapshot.metrics AS OF '" + asOfLit + "'")
	if err != nil {
		t.Fatalf("HandleQuery full-table _snapshot DOUBLE merge: %v", err)
	}
	score := map[string]string{}
	for _, cells := range rowCells(t, res.Resultset) {
		if len(cells) == 2 {
			score[cells[0]] = cells[1]
		}
	}
	if score["1"] != "100" {
		t.Errorf("baseline-origin id=1 score = %q, want \"100\"", score["1"])
	}
	if score["2"] != "100.5" {
		t.Errorf("event-origin id=2 score = %q, want \"100.5\"", score["2"])
	}
	// Baseline-origin (id=3) and event-origin (id=2) cells of 100.5 must match.
	if score["3"] != score["2"] {
		t.Errorf("baseline-origin id=3 (%q) and event-origin id=2 (%q) must render 100.5 identically", score["3"], score["2"])
	}
}

// The three tests below close the coverage gap on the configured-but-degraded
// full-table fallback branches of runSnapshotFullTable. Each configures a
// baseline source (so we are past the no-source Debug branch) and asserts the
// result equals the binlog-only set — i.e. the query degrades to runFullTable
// rather than erroring or silently mis-merging. A never-touched baseline row
// (present only in the baseline) being ABSENT is the discriminator that proves
// the merge did NOT run.

// TestSnapshotBaseline_FullTableUnsupportedPKDegrades: an unsupported PK type
// (FLOAT) is rejected by reconstruct.SupportedPKType, so full-table _snapshot
// must fall through to binlog-only even though a baseline with a never-touched
// row exists.
func TestSnapshotBaseline_FullTableUnsupportedPKDegrades(t *testing.T) {
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

	// FLOAT PK — unsupported by the baseline canonicalizer.
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "readings", "k", 1, "PRI", "float", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "readings", "v", 2, "", "varchar", "YES")
	cols := []baseline.Column{
		{Name: "k", MySQLType: "float", ParquetType: baseline.MysqlToParquetNode("float")},
		{Name: "v", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	// k=1.5 never touched (would appear only if the merge ran); k=2.5 has an event.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "readings", cols, [][]string{
		{"1.5", "untouched"},
	})
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "readings", 1 /*insert*/, "2.5", nil, nil,
		[]byte(`{"k":2.5,"v":"fresh"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "readings", AsOf: asOf})
	if err != nil {
		t.Fatalf("full-table _snapshot (unsupported PK) must degrade, not error: %v", err)
	}
	got := rowsByKey(t, res.Resultset)
	if _, merged := got["1.5"]; merged {
		t.Errorf("unsupported-PK full-table _snapshot merged the baseline (never-touched k=1.5 present); must degrade to binlog-only: %v", got)
	}
	if got["2.5"] != "fresh" {
		t.Errorf("unsupported-PK full-table _snapshot = %v, want only the binlog row k=2.5 fresh", got)
	}
}

// TestSnapshotBaseline_FullTableUnresolvablePKDegrades: when the table is not
// in the schema snapshot, pkColumnMetas can't determine the PK, so full-table
// _snapshot must fall through to binlog-only rather than merging against an
// empty/wrong PK set.
func TestSnapshotBaseline_FullTableUnresolvablePKDegrades(t *testing.T) {
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

	// No schema_snapshots row for myapp.users → pkColumnMetas returns false.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "users", usersBaselineCols(), [][]string{
		{"1", "untouched"},
	})
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1 /*insert*/, "2", nil, nil,
		[]byte(`{"id":2,"name":"fresh"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf})
	if err != nil {
		t.Fatalf("full-table _snapshot (unresolvable PK) must degrade, not error: %v", err)
	}
	got := rowsByKey(t, res.Resultset)
	if _, merged := got["1"]; merged {
		t.Errorf("unresolvable-PK full-table _snapshot merged the baseline (never-touched id=1 present); must degrade to binlog-only: %v", got)
	}
	if got["2"] != "fresh" {
		t.Errorf("unresolvable-PK full-table _snapshot = %v, want only the binlog row id=2 fresh", got)
	}
}

// TestSnapshotBaseline_FullTableNoBaselineAtOrBeforeAsOfDegrades: a baseline
// source IS configured, the table IS in the snapshot, but the only baseline
// snapshot is dated AFTER AsOf — so FindBaseline returns ErrNoBaseline and
// full-table _snapshot must degrade to binlog-only (distinct from the
// no-source-configured case in TestSnapshotBaseline_FullTableNoBaselineFallsBack).
func TestSnapshotBaseline_FullTableNoBaselineAtOrBeforeAsOfDegrades(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	asOf := hourTop.Add(10 * time.Minute)
	// Snapshot row at a time at-or-before AsOf so pkColumnMetas resolves the PK...
	snapMeta := hourTop.Add(1 * time.Minute)
	seedUsersSnapshot(t, db, snapMeta)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	// ...but the only baseline PARQUET is dated AFTER AsOf, so FindBaseline finds
	// nothing at-or-before AsOf → ErrNoBaseline.
	futureBaseline := asOf.Add(30 * time.Minute)
	baselineDir := writeBaselineSnapshot(t, futureBaseline, "myapp", "users", usersBaselineCols(), [][]string{
		{"1", "untouched"},
	})
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1 /*insert*/, "2", nil, nil,
		[]byte(`{"id":2,"name":"fresh"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir,
	}, slog.Default())

	res, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: asOf})
	if err != nil {
		t.Fatalf("full-table _snapshot (no baseline at-or-before AsOf) must degrade, not error: %v", err)
	}
	got := rowsByKey(t, res.Resultset)
	if _, merged := got["1"]; merged {
		t.Errorf("no-baseline-at-or-before-AsOf full-table _snapshot merged a future baseline (id=1 present); must degrade to binlog-only: %v", got)
	}
	if got["2"] != "fresh" {
		t.Errorf("no-baseline-at-or-before-AsOf full-table _snapshot = %v, want only the binlog row id=2 fresh", got)
	}
}

// TestSnapshotBaseline_EnumLabelsAcrossBaselineAndDeltas pins #472 on
// both _snapshot paths. A baseline row carries the ENUM value as a label
// string (the mydumper dump shape) and must pass through the mapper
// untouched; a post-baseline delta carries the binlog ordinal and must
// map. Single-row exercises the post-ApplyAt mapping; full-table
// exercises the ordering constraint that mapImage runs BEFORE
// fullTableTextCell — moving it after would coerce the delta's float64
// ordinal into the text "3" (which the mapper correctly refuses) and
// silently revert touched rows to ordinals while baseline rows keep
// labels: the exact mixed-representation bug class #472 fixes.
func TestSnapshotBaseline_EnumLabelsAcrossBaselineAndDeltas(t *testing.T) {
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

	// Typed snapshot rows: testutil.InsertSnapshot predates column_type,
	// so insert with raw SQL (same pattern as the handler enum test).
	snapTS := snapTime.UTC().Format("2006-01-02 15:04:05")
	insertTyped := func(column string, ordinal int, key, dataType, columnType string) {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable)
			VALUES (1, ?, 'myapp', 'orders', ?, ?, ?, ?, ?, 'NO')`,
			snapTS, column, ordinal, key, dataType, columnType)
	}
	insertTyped("id", 1, "PRI", "int", "int")
	insertTyped("status", 2, "", "enum", "enum('pending','processing','shipped')")

	// Baseline at snapTime: labels as strings. id=1 never touched after;
	// id=2 updated post-baseline (delta arrives as ordinal).
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "orders",
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "status", MySQLType: "enum", ParquetType: baseline.MysqlToParquetNode("enum")},
		},
		[][]string{
			{"1", "pending"},
			{"2", "processing"},
		})

	// Post-baseline UPDATE: id=2 → status ordinal 3 ('shipped').
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "orders", 2, "2", []byte(`["status"]`),
		[]byte(`{"id":2,"status":2}`), []byte(`{"id":2,"status":3}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())

	// ── single-row, untouched: baseline label passes through ──
	res1, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "orders", PKColumn: "id", PKValue: "1", AsOf: asOf})
	if err != nil {
		t.Fatalf("_snapshot id=1: %v", err)
	}
	cells := rowCells(t, res1.Resultset)
	if len(cells) != 1 || !slices.Equal(cells[0], []string{"1", "pending"}) {
		t.Errorf("_snapshot id=1 = %v, want [[1 pending]] (baseline label must pass through)", cells)
	}

	// ── single-row, touched: delta ordinal maps to label ──
	res2, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "orders", PKColumn: "id", PKValue: "2", AsOf: asOf})
	if err != nil {
		t.Fatalf("_snapshot id=2: %v", err)
	}
	cells = rowCells(t, res2.Resultset)
	if len(cells) != 1 || !slices.Equal(cells[0], []string{"2", "shipped"}) {
		t.Errorf("_snapshot id=2 = %v, want [[2 shipped]] (delta ordinal 3 must map post-ApplyAt)", cells)
	}

	// ── full-table: baseline row AND delta row both carry labels ──
	resFT, err := h.runSnapshot(TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "orders", AsOf: asOf})
	if err != nil {
		t.Fatalf("_snapshot full-table: %v", err)
	}
	ftCells := rowCells(t, resFT.Resultset)
	if len(ftCells) != 2 {
		t.Fatalf("_snapshot full-table: expected 2 rows, got %d (%v)", len(ftCells), ftCells)
	}
	got := map[string]string{}
	for _, row := range ftCells {
		got[row[0]] = row[1]
	}
	if got["1"] != "pending" || got["2"] != "shipped" {
		t.Errorf("_snapshot full-table = %v, want id=1 'pending' (baseline) and id=2 'shipped' (mapped delta)", got)
	}
}
