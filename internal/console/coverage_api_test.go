package console

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestCoverageAPI pins the live-RPO statement (#1194): the delta window from
// the strict floor, the full-table window from the latest usable baseline
// anchor, broken tables named, and the nil-db degrade to "unavailable".
func TestCoverageAPI(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)
	anchor := now.Add(-time.Hour)
	// orders: usable newest anchor. legacy: newest predates the floor → broken.
	writeBaselineFixture(t, dir, anchor.Format("2006-01-02T15-04-05Z"), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, now.Add(-150*time.Hour).Format("2006-01-02T15-04-05Z"), "shop", "legacy.parquet")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	latest := now.Add(-30 * time.Second)
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(now.Add(-100 * time.Hour).Format("p_2006010215")))
	mock.ExpectQuery(`MIN\(partition_name\)`).
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
	mock.ExpectQuery(`MAX\(event_timestamp\)`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(latest))
	mock.ExpectQuery("FROM stream_state").WillReturnError(sql.ErrNoRows)

	srv := newBaselineServer(t, dir, true)
	srv.cm.boot.db = db
	srv.cm.boot.dbName = "binlog_index"
	rec, body := doServersReq(t, srv, "GET", "/api/coverage", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got coverageResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.DeltaFrom == "" || got.DeltaTo != latest.Format(consoleTSFormat) {
		t.Fatalf("delta window = [%q, %q]", got.DeltaFrom, got.DeltaTo)
	}
	if got.Continuity != "none" || got.LagSeconds != nil {
		t.Fatalf("file-mode index: continuity=%q lag=%v", got.Continuity, got.LagSeconds)
	}
	if !got.BaselineConfigured || got.FullTableFrom != anchor.Format(consoleTSFormat) {
		t.Fatalf("full_table_from = %q, want %q", got.FullTableFrom, anchor.Format(consoleTSFormat))
	}
	if len(got.BrokenTables) != 1 || got.BrokenTables[0] != "shop.legacy" {
		t.Fatalf("broken_tables = %v", got.BrokenTables)
	}

	// No index connection: an explicit "unavailable", never a window.
	srvNil := newBaselineServer(t, dir, true)
	rec, body = doServersReq(t, srvNil, "GET", "/api/coverage", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	got = coverageResponse{}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Continuity != "unavailable" || got.DeltaTo != "" || got.FullTableFrom != "" {
		t.Fatalf("nil db must degrade to unavailable with no window: %+v", got)
	}
}
