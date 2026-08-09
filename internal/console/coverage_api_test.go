package console

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// coverageMockDB wires the CollectCoverageSummary query sequence: floor
// partitions → archive MIN/MAX → walk partitions → one per-partition MAX
// probe → stream_state (no row = file-mode). archErr != nil makes the floor
// unknown.
func coverageMockDB(t *testing.T, part string, latest time.Time, archErr error) *sql.DB {
	return coverageMockDBArchives(t, part, latest, archErr, nil, nil, 0)
}

// coverageMockDBArchives is coverageMockDB with an archive_state row: archMin/
// archMax name the archived range and sources is COUNT(DISTINCT bintrail_id),
// so a caller can build the multi-source (unattributable) floor.
func coverageMockDBArchives(t *testing.T, part string, latest time.Time, archErr error, archMin, archMax any, sources int) *sql.DB {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(part))
	if archErr != nil {
		mock.ExpectQuery(`MIN\(partition_name\)`).WillReturnError(archErr)
	} else {
		mock.ExpectQuery(`MIN\(partition_name\)`).
			WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(archMin, archMax, sources))
	}
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(part))
	mock.ExpectQuery(`MAX\(event_timestamp\) FROM binlog_events PARTITION`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(latest))
	mock.ExpectQuery("FROM stream_state").WillReturnError(sql.ErrNoRows)
	return db
}

func coverageGet(t *testing.T, srv *Server) coverageResponse {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/coverage", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got coverageResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestCoverageAPI pins the live-RPO statement (#1194): the delta window from
// the strict floor, the full-table window from each table's EARLIEST usable
// anchor reduced by max across tables (#1294),
// broken tables named, and the degraded states each keeping their identity.
func TestCoverageAPI(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	tsDir := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }
	anchorNew := now.Add(-time.Hour)
	anchorOld := now.Add(-10 * time.Hour)

	dir := t.TempDir()
	// orders: usable newest anchor (-1h, plus a superseded -200h). users:
	// usable at -10h. legacy: newest -150h predates the floor → broken.
	writeBaselineFixture(t, dir, tsDir(time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, tsDir(200*time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, tsDir(10*time.Hour), "shop", "users.parquet")
	writeBaselineFixture(t, dir, tsDir(150*time.Hour), "shop", "legacy.parquet")

	// #1219: the archives reach back 300h but belong to two sources, so the
	// floor collapses to the live partitions (-100h) and the -150h `legacy`
	// anchor becomes UNATTRIBUTABLE. Grading it against the bare floor hour
	// would name a table whose archives are intact in broken_tables — the
	// false alarm the narrowed floor exists to avoid — and letting it define
	// the window would assert restorability it cannot prove. Neither: the
	// full-table half reports "unknown" and claims no anchor.
	t.Run("unattributable floor: no broken claim and no window claim", func(t *testing.T) {
		srv := newBaselineServer(t, dir, true)
		srv.cm.boot.db = coverageMockDBArchives(t, part, latest, nil,
			now.Add(-300*time.Hour).Format("p_2006010215"),
			now.Add(-101*time.Hour).Format("p_2006010215"), 2)
		srv.cm.boot.dbName = "binlog_index"
		got := coverageGet(t, srv)
		if len(got.BrokenTables) != 0 {
			t.Fatalf("unattributable anchors must not be named broken: %+v", got.BrokenTables)
		}
		if got.FullTableStatus != "unknown" || got.FullTableFrom != "" {
			t.Fatalf("want unknown with no window, got status=%q from=%q", got.FullTableStatus, got.FullTableFrom)
		}
		// The delta half still states the window every source provably has.
		if got.DeltaFrom == "" {
			t.Fatal("the live floor is still a real window and must be reported")
		}
	})

	t.Run("window, latest usable anchor, broken named", func(t *testing.T) {
		srv := newBaselineServer(t, dir, true)
		srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
		srv.cm.boot.dbName = "binlog_index"
		got := coverageGet(t, srv)
		if got.DeltaFrom == "" || got.DeltaTo != latest.Format(consoleTSFormat) {
			t.Fatalf("delta window = [%q, %q]", got.DeltaFrom, got.DeltaTo)
		}
		if got.Continuity != "none" || got.LagSeconds != nil {
			t.Fatalf("file-mode index: continuity=%q lag=%v", got.Continuity, got.LagSeconds)
		}
		// Freshness is WIRED (#1227): an unpopulated field would be "", not
		// "none". A file-mode index makes no liveness claim, exactly as it makes
		// no continuity claim — and with no checkpoint to age, the age must be
		// OMITTED rather than serialized as a confident 0 ("just now").
		if got.Freshness != "none" {
			t.Fatalf("file-mode index: freshness=%q, want \"none\"", got.Freshness)
		}
		if got.CheckpointAgeSeconds != nil {
			t.Fatalf("checkpoint_age_seconds = %v, want omitted with no checkpoint", *got.CheckpointAgeSeconds)
		}
		if got.FullTableStatus != "ok" {
			t.Fatalf("full_table_status = %q", got.FullTableStatus)
		}
		// Max ACROSS tables: users' -10h anchor must not widen the
		// all-tables claim past orders, whose only usable anchor is -1h (its
		// -200h one is below the floor). Within a table the earliest usable
		// anchor wins, which #1294 covers separately.
		if got.FullTableFrom != anchorNew.Format(consoleTSFormat) {
			t.Fatalf("full_table_from = %q, want %q (not %q)", got.FullTableFrom,
				anchorNew.Format(consoleTSFormat), anchorOld.Format(consoleTSFormat))
		}
		if len(got.BrokenTables) != 1 || got.BrokenTables[0] != "shop.legacy" {
			t.Fatalf("broken_tables = %v", got.BrokenTables)
		}
	})

	t.Run("all tables broken: no window claim, all named", func(t *testing.T) {
		dir2 := t.TempDir()
		writeBaselineFixture(t, dir2, tsDir(150*time.Hour), "shop", "legacy.parquet")
		writeBaselineFixture(t, dir2, tsDir(200*time.Hour), "shop", "carts.parquet")
		srv := newBaselineServer(t, dir2, true)
		srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
		srv.cm.boot.dbName = "binlog_index"
		got := coverageGet(t, srv)
		if got.FullTableStatus != "ok" || got.FullTableFrom != "" {
			t.Fatalf("all-broken must claim NO window: %+v", got)
		}
		if len(got.BrokenTables) != 2 || got.BrokenTables[0] != "shop.carts" || got.BrokenTables[1] != "shop.legacy" {
			t.Fatalf("broken_tables = %v", got.BrokenTables)
		}
	})

	t.Run("unknown floor suppresses the full-table half as unknown", func(t *testing.T) {
		srv := newBaselineServer(t, dir, true)
		srv.cm.boot.db = coverageMockDB(t, part, latest, &mysql.MySQLError{Number: 1045, Message: "access denied"})
		srv.cm.boot.dbName = "binlog_index"
		got := coverageGet(t, srv)
		if got.DeltaFrom != "" || got.DeltaTo == "" {
			t.Fatalf("floor must be unknown, edge present: %+v", got)
		}
		if got.FullTableStatus != "unknown" || got.FullTableFrom != "" || len(got.BrokenTables) != 0 {
			t.Fatalf("unknown floor must be 'unknown', never a silently-empty ok: %+v", got)
		}
	})

	t.Run("listing failure is unknown, not silently-empty", func(t *testing.T) {
		srv := newBaselineServer(t, dir+"/does-not-exist", true)
		srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
		srv.cm.boot.dbName = "binlog_index"
		got := coverageGet(t, srv)
		if got.DeltaTo == "" {
			t.Fatalf("delta half must survive a listing failure: %+v", got)
		}
		if got.FullTableStatus != "unknown" || got.FullTableFrom != "" || len(got.BrokenTables) != 0 {
			t.Fatalf("a failed listing must not render as 'nothing broken': %+v", got)
		}
	})

	t.Run("nil db degrades to unavailable with no window", func(t *testing.T) {
		srv := newBaselineServer(t, dir, true)
		got := coverageGet(t, srv)
		if got.Continuity != "unavailable" || got.DeltaTo != "" || got.FullTableFrom != "" || got.FullTableStatus != "" {
			t.Fatalf("nil db must degrade to unavailable with no window: %+v", got)
		}
	})
}

// A table can have SEVERAL baselines inside coverage, and reconstruct serves an
// instant from the newest one AT OR BEFORE it — so the table is restorable from
// its EARLIEST usable anchor, not its newest. Reducing to the newest understated
// the window by however long ago the last baseline ran, and got worse with good
// hygiene: every new snapshot pushed the reported floor forward (#1294).
//
// The pre-existing fixture could not catch this — its one multi-anchor table has
// a single USABLE anchor, so both rules agree there.
func TestCoverageFullTableWindowUsesEarliestUsableAnchor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215") // floor: -100h
	tsDir := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }

	dir := t.TempDir()
	// orders: THREE anchors, two of them usable (-1h, -50h) and one below the
	// floor (-200h). users: one usable anchor at -10h.
	writeBaselineFixture(t, dir, tsDir(time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, tsDir(50*time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, tsDir(200*time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, tsDir(10*time.Hour), "shop", "users.parquet")

	srv := newBaselineServer(t, dir, true)
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if got.FullTableStatus != "ok" {
		t.Fatalf("full_table_status = %q, want ok", got.FullTableStatus)
	}
	if len(got.BrokenTables) != 0 {
		t.Fatalf("broken_tables = %v; every table here has a usable baseline", got.BrokenTables)
	}
	// orders starts at -50h (its earliest usable anchor, NOT its newest -1h
	// one); users starts at -10h. "every table" therefore holds from -10h.
	want := now.Add(-10 * time.Hour).Format(consoleTSFormat)
	if got.FullTableFrom != want {
		t.Errorf("full_table_from = %q, want %q\n"+
			"reducing to the NEWEST anchor per table would give %q — a window %v narrower than the truth",
			got.FullTableFrom, want, now.Add(-time.Hour).Format(consoleTSFormat), 9*time.Hour)
	}
}

// A baseline BELOW the floor must not widen the window even though older
// anchors now participate: its deltas are gone, so it restores nothing.
func TestCoverageFullTableWindowIgnoresBelowFloorAnchors(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	tsDir := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }

	dir := t.TempDir()
	writeBaselineFixture(t, dir, tsDir(5*time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, tsDir(300*time.Hour), "shop", "orders.parquet") // below the floor

	srv := newBaselineServer(t, dir, true)
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if want := now.Add(-5 * time.Hour).Format(consoleTSFormat); got.FullTableFrom != want {
		t.Errorf("full_table_from = %q, want %q — the -300h anchor is below the floor and restores nothing", got.FullTableFrom, want)
	}
	if len(got.BrokenTables) != 0 {
		t.Errorf("broken_tables = %v; the table has a usable newer baseline, so it is not broken", got.BrokenTables)
	}
}
