package query

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// TestPortableArchiveSources pins the routing an artifact that LEAVES the host
// needs (#1456): the S3 location whenever one is registered, even when a local
// copy with real data sits right beside it, because the reader may be on
// another machine where that directory does not exist.
func TestPortableArchiveSources(t *testing.T) {
	dir := t.TempDir()

	// Local copy WITH data: ResolveArchiveSources would pick it; the portable
	// variant must not.
	dataBase := filepath.Join(dir, "bintrail_id=with-data")
	if err := os.MkdirAll(filepath.Join(dataBase, "event_date=2026-06-05", "event_hour=10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataBase, "event_date=2026-06-05", "event_hour=10", "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	localOnly := filepath.Join(dir, "bintrail_id=local-only")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cols := []string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}
	mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows(cols).
		// (1) local data present AND S3 registered: S3, the location that
		// resolves from anywhere.
		AddRow("with-data", filepath.Join(dataBase, "events.parquet"), "bkt", "events/bintrail_id=with-data/f.parquet").
		// (2) local only: the local base is all there is; never omit a source.
		AddRow("local-only", filepath.Join(localOnly, "events.parquet"), nil, nil).
		// (3) S3 only (local pruned and the row's local_path NULL): S3.
		AddRow("s3-only", nil, "bkt", "events/bintrail_id=s3-only/f.parquet").
		// (4) neither parseable: contributes nothing, as in ResolveArchiveSources.
		AddRow("empty", nil, nil, nil).
		// (5) S3 columns present but the key has no bintrail_id= segment (an
		// `upload --source` pointed below that directory): no S3 root can be
		// built, so the local base is listed, with a warning, never omitted.
		AddRow("odd-key", filepath.Join(dir, "bintrail_id=odd-key", "events.parquet"), "bkt", "events/no-id-segment/f.parquet"))

	got, rerr := PortableArchiveSources(context.Background(), db)
	if rerr != nil {
		t.Fatalf("unexpected resolver error: %v", rerr)
	}
	want := []string{
		"s3://bkt/events/bintrail_id=with-data",
		localOnly,
		"s3://bkt/events/bintrail_id=s3-only",
		filepath.Join(dir, "bintrail_id=odd-key"),
	}
	if len(got) != len(want) {
		t.Fatalf("got %d sources %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sources[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// The portable variant shares the registry read, so it must share its error
// contract too: a pre-archive index is (nil, nil), anything else propagates.
func TestPortableArchiveSourcesErrors(t *testing.T) {
	t.Run("1146 table-not-found stays swallowed", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("FROM archive_state").WillReturnError(
			&mysql.MySQLError{Number: 1146, Message: "Table 'idx.archive_state' doesn't exist"})
		got, rerr := PortableArchiveSources(context.Background(), db)
		if rerr != nil || got != nil {
			t.Fatalf("pre-archive index must resolve to (nil, nil), got (%v, %v)", got, rerr)
		}
	})

	t.Run("any other query error propagates", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		forced := &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"}
		mock.ExpectQuery("FROM archive_state").WillReturnError(forced)
		_, rerr := PortableArchiveSources(context.Background(), db)
		if !errors.Is(rerr, forced) {
			t.Fatalf("expected wrapped registry error, got %v", rerr)
		}
	})

	t.Run("nil db stays (nil, nil)", func(t *testing.T) {
		got, rerr := PortableArchiveSources(context.Background(), nil)
		if got != nil || rerr != nil {
			t.Fatalf("nil db must resolve to (nil, nil), got (%v, %v)", got, rerr)
		}
	})
}
