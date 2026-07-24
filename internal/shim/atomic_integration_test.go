//go:build integration

package shim

import (
	"log/slog"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunPointInTime_TransactionAtomicCut is the proof for #988: the single-row
// _flashback path cuts at the TRANSACTION boundary, not the row. A
// multi-statement transaction whose first statement touches the queried row
// BEFORE AsOf but whose later statement (on another row) commits AFTER AsOf must
// be excluded WHOLE — the row resolves to its pre-transaction state, never the
// half-applied image that never existed at AsOf.
//
// Layout for id=1:
//
//	E0  INSERT id=1 name=v0        @ t0   GTID g0   (committed before AsOf)
//	E1  UPDATE id=1 v0→v1          @ t1   GTID g1   (straddling txn, 1st stmt, before AsOf)
//	E2  UPDATE id=2 (other row)    @ t2   GTID g1   (same txn, 2nd stmt, AFTER AsOf)
//
// A naive LimitPerPK=1 cut returns E1 (v1). The atomic cut sees g1 continues
// past AsOf (E2) and drops the whole g1 group from id=1's fetch → v0.
func TestRunPointInTime_TransactionAtomicCut(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	seedUsersSnapshot(t, db, snapTime)

	fmtTS := func(x time.Time) string { return x.Format("2006-01-02 15:04:05") }
	t0 := hourTop.Add(2 * time.Minute)
	t1 := hourTop.Add(5 * time.Minute)
	asOf := hourTop.Add(6 * time.Minute) // between t1 and t2
	t2 := hourTop.Add(8 * time.Minute)   // second statement commits after AsOf

	g0 := "11111111-1111-1111-1111-111111111111:1"
	g1 := "11111111-1111-1111-1111-111111111111:2"

	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, fmtTS(t0), &g0,
		"myapp", "users", 1 /*insert*/, "1", nil, nil,
		[]byte(`{"id":1,"name":"v0"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, fmtTS(t1), &g1,
		"myapp", "users", 2 /*update*/, "1", nil,
		[]byte(`{"id":1,"name":"v0"}`), []byte(`{"id":1,"name":"v1"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 300, 400, fmtTS(t2), &g1,
		"myapp", "users", 2 /*update*/, "2", nil,
		[]byte(`{"id":2,"name":"x"}`), []byte(`{"id":2,"name":"y"}`))

	h := NewHandlerWithConfig(db, Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName}, slog.Default())

	t.Run("straddling_txn_excluded_whole", func(t *testing.T) {
		res, err := h.runPointInTime(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "users",
			AsOf: asOf, PKColumn: "id", PKValue: "1",
		})
		if err != nil {
			t.Fatalf("runPointInTime: %v", err)
		}
		cells := rowCells(t, res.Resultset)
		if len(cells) != 1 {
			t.Fatalf("got %d rows, want 1: %v", len(cells), cells)
		}
		if cells[0][0] != "1" || cells[0][1] != "v0" {
			t.Errorf("id=1 AS OF between the straddling txn's statements = %v, want [1 v0] "+
				"(v1 would be the half-applied transaction that never existed at AsOf)", cells[0])
		}
	})

	t.Run("after_txn_commit_includes_it", func(t *testing.T) {
		// AsOf after t2: the whole g1 transaction is committed, so the row is v1.
		afterAll := t2.Add(1 * time.Minute)
		res, err := h.runPointInTime(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "users",
			AsOf: afterAll, PKColumn: "id", PKValue: "1",
		})
		if err != nil {
			t.Fatalf("runPointInTime: %v", err)
		}
		cells := rowCells(t, res.Resultset)
		if len(cells) != 1 || cells[0][1] != "v1" {
			t.Errorf("id=1 AS OF after the txn committed = %v, want [1 v1]", cells)
		}
	})
}

// TestRunPointInTime_EmptyStringPK pins the #988 empty-PKValue split: a legit
// `WHERE k = ''` against a NOT-NULL string PK must stay row-scoped via
// PKValuesIn. A regression that let Options.PKValues == "" DISABLE the PK filter
// (buildQuery's behaviour) would fold every event for the whole table onto one
// state — here that would return the OTHER row's latest image instead of the
// empty-key row's own.
func TestRunPointInTime_EmptyStringPK(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	ts := snapTime.UTC().Format("2006-01-02 15:04:05")
	// myapp.kv with a single-column string PK 'k'.
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "kv", "k", 1, "PRI", "varchar", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "kv", "v", 2, "", "varchar", "YES")

	fmtTS := func(x time.Time) string { return x.Format("2006-01-02 15:04:05") }
	t0 := hourTop.Add(2 * time.Minute)
	t1 := hourTop.Add(5 * time.Minute)
	asOf := hourTop.Add(9 * time.Minute)

	// pk_values is the single-column value verbatim: "" for k='', "other" for k='other'.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, fmtTS(t0), nil,
		"myapp", "kv", 1 /*insert*/, "", nil, nil, []byte(`{"k":"","v":"empty"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, fmtTS(t0), nil,
		"myapp", "kv", 1 /*insert*/, "other", nil, nil, []byte(`{"k":"other","v":"x"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 300, 400, fmtTS(t1), nil,
		"myapp", "kv", 2 /*update*/, "other", nil,
		[]byte(`{"k":"other","v":"x"}`), []byte(`{"k":"other","v":"x2"}`))

	h := NewHandlerWithConfig(db, Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName}, slog.Default())
	res, err := h.runPointInTime(TimeTravelQuery{
		Type: TypeFlashback, Schema: "myapp", Table: "kv", AsOf: asOf, PKColumn: "k", PKValue: "",
	})
	if err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 {
		t.Fatalf("got %d rows, want exactly 1 (the empty-key row): %v", len(cells), cells)
	}
	// v is the discriminator: the empty-key row is "empty"; a disabled filter
	// would fold the whole table and surface the 'other' row's latest ("x2").
	if cells[0][1] != "empty" {
		t.Errorf("WHERE k = '' returned %v, want v=\"empty\" — a disabled PK filter would return the 'other' row (\"x2\")", cells[0])
	}
}
