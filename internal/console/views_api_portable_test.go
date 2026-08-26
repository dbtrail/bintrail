package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestViewsAPI_prefersS3OverLocalCopy (#1456): the download is meant to run on
// ANOTHER machine, so an archive registered both on this host and in S3 must be
// described by its S3 location. The local copy is real and holds data here,
// which is exactly the shape that made the console's own reads (rightly) pick
// it, and the downloaded file (wrongly) unusable off-host.
func TestViewsAPI_prefersS3OverLocalCopy(t *testing.T) {
	dir := t.TempDir()
	localBase := filepath.Join(dir, "bintrail_id=aaaa")
	if err := os.MkdirAll(filepath.Join(localBase, "event_date=2026-06-05", "event_hour=10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(localBase, "event_date=2026-06-05", "event_hour=10", "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cols := []string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}
	mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows(cols).
		AddRow("aaaa", filepath.Join(localBase, "events.parquet"), "bkt", "events/bintrail_id=aaaa/f.parquet"))

	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if !strings.Contains(sql, "'s3://bkt/events/bintrail_id=aaaa/event_date=*/event_hour=*/*.parquet'") {
		t.Errorf("events view does not read the S3 copy:\n%s", sql)
	}
	if strings.Contains(sql, localBase) {
		t.Errorf("the generating host's local path leaked into a file meant to run elsewhere:\n%s", sql)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
