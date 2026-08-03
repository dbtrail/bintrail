package console

import (
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestBaselinesAPI_staleness pins the console's staleness surface: per-row
// verdicts, the newest-per-table headline (a superseded broken snapshot must
// not drive it), and the shared rollup with the status package.
func TestBaselinesAPI_staleness(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	ts := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }
	// Floor = 100h ago. orders: newest 1h ago (ok) + superseded 200h ago
	// (broken, routine). legacy: newest 150h ago (broken — drives headline).
	writeBaselineFixture(t, dir, ts(time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, ts(200*time.Hour), "shop", "orders.parquet")
	writeBaselineFixture(t, dir, ts(150*time.Hour), "shop", "legacy.parquet")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(now.Add(-100 * time.Hour).Format("p_2006010215")))
	mock.ExpectQuery(`MIN\(partition_name\)`).
		WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))

	srv := newBaselineServer(t, dir, true)
	srv.cm.boot.db = db
	srv.cm.boot.dbName = "binlog_index"
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Staleness != "broken" {
		t.Fatalf("headline = %q, want broken (legacy's newest predates coverage)", got.Staleness)
	}
	if len(got.Snapshots) != 3 {
		t.Fatalf("snapshots = %+v, want 3", got.Snapshots)
	}
	// Newest first: orders(1h)=ok, legacy(150h)=broken, orders(200h)=broken.
	if got.Snapshots[0].Staleness != "ok" || got.Snapshots[1].Staleness != "broken" || got.Snapshots[2].Staleness != "broken" {
		t.Fatalf("per-row verdicts wrong: %+v", got.Snapshots)
	}

	// No index connection: every verdict — including the headline — is the
	// explicit "unknown", never "ok".
	srvNil := newBaselineServer(t, dir, true)
	rec, body = doServersReq(t, srvNil, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	got = baselinesResponse{}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Staleness != "unknown" || got.Snapshots[0].Staleness != "unknown" {
		t.Fatalf("nil db must yield explicit unknown everywhere: headline %q, rows %+v", got.Staleness, got.Snapshots)
	}
}
