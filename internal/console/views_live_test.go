package console

import (
	"net/http/httptest"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/ext"
)

// The index the download describes. The password is here for one reason: to
// be looked for in the artifact.
const (
	liveTestDSN      = "reader:hunter2@tcp(db.internal:3307)/idx"
	liveTestPassword = "hunter2"
)

// newLiveViewsServer builds a console whose boot bundle points at a mocked
// index, so the download's live half runs the REAL resolution path (the DSN
// parse, the column probe, the attribution query) rather than a hand-built
// views.LiveIndex.
func newLiveViewsServer(t *testing.T, dsn string) (*Server, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db
	srv.cm.boot.dsn = dsn
	return srv, mock
}

// expectArchiveSource queues the archive_state read buildViewsInput does, so
// there is a layout to describe.
func expectArchiveSource(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow(id, nil, "bkt", "events/bintrail_id="+id+"/f.parquet"))
}

// TestViewsAPI_includeLiveAddsTheHotLeg: the download can now carry the leg
// over the live index (#1480), which the console used to be structurally
// unable to offer (LiveLegUnavailable was hardcoded). Asking for it must
// produce the ATTACH, the two-leg events view, and the location facts a reader
// on another machine needs.
func TestViewsAPI_includeLiveAddsTheHotLeg(t *testing.T) {
	const id = "aaaa"
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, id)
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME"}).
			AddRow("event_id").AddRow("event_timestamp").AddRow("schema_name").AddRow("table_name"))
	mock.ExpectQuery("FROM bintrail_servers").WillReturnRows(
		sqlmock.NewRows([]string{"n", "id"}).AddRow(1, id))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_live=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	for _, want := range []string{
		"ATTACH ''",          // the live catalog is opened
		"bintrail_live",      // and the two-leg view reads it
		"HOST 'db.internal'", // resolved from the console's own DSN
		"PORT 3307",
		"DATABASE 'idx'",
		"USER 'reader'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("the requested live leg is missing %q from the file:\n%s", want, sql)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The password is the one connection fact the file must never carry: it is
// meant to be shared, and its header says it holds no credentials. The slot
// is emitted empty for the operator to fill in their own session.
func TestViewsAPI_includeLiveNeverCarriesThePassword(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("event_id"))
	mock.ExpectQuery("FROM bintrail_servers").WillReturnRows(
		sqlmock.NewRows([]string{"n", "id"}).AddRow(1, "aaaa"))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_live=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if strings.Contains(sql, liveTestPassword) {
		t.Errorf("the downloaded file carries the index password:\n%s", sql)
	}
	if !strings.Contains(sql, "PASSWORD ''") {
		t.Errorf("the password slot is not empty for the reader to fill:\n%s", sql)
	}
}

// TestViewsAPI_liveLegIsOptIn: the leg names the index by host and port in a
// shareable file, and a query against the two-leg view reads the live capture
// index. Neither may happen because someone clicked download.
func TestViewsAPI_liveLegIsOptIn(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if strings.Contains(sql, "bintrail_live") || strings.Contains(sql, "ATTACH ''") {
		t.Errorf("the live leg is in a file nobody asked it for:\n%s", sql)
	}
	// And the archives-only note names the control THIS reader has. The CLI
	// flag is not something a browser can pass.
	if !strings.Contains(sql, `ticking "Include the live index"`) {
		t.Errorf("the archives-only note does not name the console's own route:\n%s", sql)
	}
	if strings.Contains(sql, "--include-live") {
		t.Errorf("the note sends a console reader to a command-line flag:\n%s", sql)
	}
	// The probe queries must not have run either: this download asked the
	// index nothing.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestViewsAPI_includeLiveStillRefusesAProfiledSession: the parameter is new
// surface on an existing gate. A data profile refuses this file because it maps
// straight onto the unredacted Parquet, and asking for one more leg cannot be
// the way around that.
func TestViewsAPI_includeLiveStillRefusesAProfiledSession(t *testing.T) {
	srv, _ := newLiveViewsServer(t, liveTestDSN)

	for _, path := range []string{"/api/views.sql", "/api/views.sql?include_live=1"} {
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090"+path, nil)
		req.Host = "127.0.0.1:8090"
		req.Header.Set("Authorization", "Bearer t")
		req = req.WithContext(withPolicy(&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != 403 {
			t.Errorf("%s: code = %d, body = %s; want 403 for a profiled session", path, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "db.internal") {
			t.Errorf("%s: the refusal describes the index anyway: %s", path, rec.Body.String())
		}
	}
}

// A server this console reaches over a unix socket cannot carry the leg
// however it is asked for: the file locates the index by host and port so it
// can run elsewhere. Refused with that reason, never a file quietly missing
// the half the reader ticked a box for.
func TestViewsAPI_includeLiveRefusesASocketIndex(t *testing.T) {
	srv, mock := newLiveViewsServer(t, "reader:hunter2@unix(/tmp/mysql.sock)/idx")
	// One per request below: the layout is resolved before the live half.
	expectArchiveSource(mock, "aaaa")
	expectArchiveSource(mock, "aaaa")

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_live=1", "")
	if rec.Code != 422 {
		t.Fatalf("code = %d, body = %s; want 422", rec.Code, body)
	}
	if !strings.Contains(string(body), "unix socket") {
		t.Errorf("the refusal does not say why: %s", body)
	}
	if strings.Contains(string(body), liveTestPassword) {
		t.Errorf("the refusal echoes the index password: %s", body)
	}
	// The archives-only download for that same server names no route at all,
	// rather than pointing at a box that would refuse.
	rec, body = doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("plain download: code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), `ticking "Include the live index"`) {
		t.Errorf("the file offers a route this server cannot take:\n%s", body)
	}
	if !strings.Contains(string(body), "no way to reach the index") {
		t.Errorf("the file does not say the live leg is unavailable here:\n%s", body)
	}
}

// An index with no binlog_events table cannot back the view the leg IS. The
// generated file would otherwise be a binder error that defines nothing.
func TestViewsAPI_includeLiveRefusesAnIndexWithoutBinlogEvents(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_live=1", "")
	if rec.Code != 422 {
		t.Fatalf("code = %d, body = %s; want 422", rec.Code, body)
	}
	if !strings.Contains(string(body), "binlog_events") {
		t.Errorf("the refusal does not name what is missing: %s", body)
	}
}

// The hot leg names only columns the index HAS: it has no union_by_name, so a
// column named that is not there makes DuckDB refuse the whole file. An index
// the console never migrated is exactly that case.
func TestViewsAPI_includeLiveNamesOnlyObservedColumns(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")
	// A legacy index: no connection_id, no query_text, no commit_ts_us.
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME"}).
			AddRow("event_id").AddRow("event_timestamp").AddRow("schema_name"))
	mock.ExpectQuery("FROM bintrail_servers").WillReturnRows(
		sqlmock.NewRows([]string{"n", "id"}).AddRow(1, "aaaa"))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_live=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "not a column of this index's binlog_events") {
		t.Errorf("the hot leg does not mark the columns this index lacks:\n%s", body)
	}
}
