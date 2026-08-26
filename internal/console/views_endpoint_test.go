package console

import (
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestViewsAPI_namesTheS3Endpoint wires the producer to the artifact (#1453).
// The generator is tested with a populated Input; this pins that the console
// FILLS it. views.sql is the one artifact that leaves the machine, and a file
// missing ENDPOINT hands the recipient an s3:// path naming their own bucket
// and sends their DuckDB to AWS, with no error anywhere.
func TestViewsAPI_namesTheS3Endpoint(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "http://minio:9000")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// An S3 archive source: a local-only layout emits no S3 preamble at all,
	// so it could not show the endpoint either way.
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))

	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if !strings.Contains(sql, "ENDPOINT 'minio:9000'") {
		t.Errorf("the downloaded file does not name the store it reads:\n%s", sql)
	}
	if !strings.Contains(sql, "URL_STYLE 'path'") || !strings.Contains(sql, "USE_SSL false") {
		t.Errorf("the file names the store but not how to address it:\n%s", sql)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// An invalid endpoint is an upstream fault the file must not paper over, on a
// layout that actually reads S3: a 502, not a file that quietly describes AWS.
func TestViewsAPI_invalidEndpointIs502(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "minio:9000") // no scheme

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))

	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 502 {
		t.Fatalf("code = %d, body = %s; want 502", rec.Code, body)
	}
	if strings.Contains(string(body), "minio:9000") {
		t.Errorf("the 502 body echoes the rejected value: %s", body)
	}
}

// The same broken variable on a server whose data is entirely local is not
// this page's problem: nothing in the rendered file reads through httpfs, so
// refusing would break a working download over a setting it never consults.
func TestViewsAPI_localOnlyLayoutIgnoresEndpointTypo(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "minio:9000") // the same rejected value

	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	srv := newViewsServer(t, dir, false)

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s; want 200", rec.Code, body)
	}
	// Positive evidence that the endpoint was genuinely not needed, rather
	// than needed and silently dropped.
	if strings.Contains(string(body), "s3://") || strings.Contains(string(body), "httpfs") {
		t.Errorf("a layout treated as local reads S3 after all:\n%s", body)
	}
}
