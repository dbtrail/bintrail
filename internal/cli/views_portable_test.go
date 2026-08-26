package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestDiscoverArchiveSourcesFrom_prefersS3 pins the CLI half of #1456: with a
// local copy that holds data AND an S3 registration, `bintrail views` must
// name the S3 root, because the file is written to run somewhere else. A
// revert to the local-first resolver fails here.
func TestDiscoverArchiveSourcesFrom_prefersS3(t *testing.T) {
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

	got, err := discoverArchiveSourcesFrom(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "s3://bkt/events/bintrail_id=aaaa" {
		t.Fatalf("sources = %v, want the S3 root only", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
