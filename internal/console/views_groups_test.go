package console

import (
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// expectGroupedArchiveSource queues the two archive_state reads buildViewsInput
// performs, with a registry that HAS recorded column sets — the state after
// `archive reconcile --repair`, or after rotation has written archives on this
// build.
func expectGroupedArchiveSource(mock sqlmock.Sqlmock, id string) {
	key := func(hour string) string {
		return "events/bintrail_id=" + id + "/event_date=2026-05-01/event_hour=" + hour + "/e.parquet"
	}
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow(id, nil, "bkt", key("03")))
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "local_path", "s3_key", "column_set"}).
			AddRow(id, nil, key("03"), "event_id,query_text").
			AddRow(id, nil, key("04"), "event_id"))
}

// The console serves BOTH consumers of this SQL from one builder: the SQL panel
// binds these views in-process on every statement, and the download binds them
// in the operator's DuckDB. So the grouping has to reach the console builder,
// not only the CLI one.
func TestViewsAPI_groupsTheArchiveLayout(t *testing.T) {
	const id = "aaaa"
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectGroupedArchiveSource(mock, id)

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if strings.Contains(sql, "union_by_name = true") {
		t.Errorf("the console's events view still unifies every footer at bind time:\n%s", sql)
	}
	if !strings.Contains(sql, "union_by_name = false") {
		t.Errorf("the console's events view is not grouped at all:\n%s", sql)
	}
	if !strings.Contains(sql, "UNION ALL BY NAME") {
		t.Errorf("the two column sets were not joined:\n%s", sql)
	}
	// The narrow group must pad the column it lacks rather than name it: naming
	// it is a binder error that defines no view, which on the SQL panel means
	// the whole page fails rather than one column reading NULL.
	if !strings.Contains(sql, `NULL AS "query_text"`) {
		t.Errorf("the group without query_text does not pad it:\n%s", sql)
	}
	if !strings.Contains(sql, "event_hour=04") {
		t.Errorf("a registered partition is missing from the file lists:\n%s", sql)
	}
}

// The all-or-nothing rule, on the console side. The file list comes from the
// registry, so grouping a PARTIAL one would leave the unrecorded partitions out
// of the view — a wrong answer instead of a slow one. This case is what makes
// the gate testable at all: with every partition unrecorded the grouped and the
// ungrouped paths both produce no groups and look identical.
func TestViewsAPI_partialRegistryKeepsTheGlob(t *testing.T) {
	const id = "aaaa"
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	key := func(hour string) string {
		return "events/bintrail_id=" + id + "/event_date=2026-05-01/event_hour=" + hour + "/e.parquet"
	}
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow(id, nil, "bkt", key("03")))
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "local_path", "s3_key", "column_set"}).
			AddRow(id, nil, key("03"), "event_id,query_text").
			AddRow(id, nil, key("04"), nil))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if strings.Contains(sql, "union_by_name = false") {
		t.Errorf("the console grouped a registry that cannot account for every partition, "+
			"so event_hour=04 is not in the view at all:\n%s", sql)
	}
	if !strings.Contains(sql, "no recorded column set") {
		t.Errorf("the file does not explain why it still binds every footer:\n%s", sql)
	}
}
