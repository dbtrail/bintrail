package reconstruct

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGapInWindow(t *testing.T) {
	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		gapAt time.Time
		want  bool
	}{
		{"before window = out of scope", since.Add(-time.Hour), false},
		{"exactly at since (exclusive) = out of scope", since, false},
		{"inside window = in scope", since.Add(24 * time.Hour), true},
		{"exactly at until (inclusive) = in scope", until, true},
		{"after window = out of scope", until.Add(time.Hour), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := gapInWindow(c.gapAt, since, until); got != c.want {
				t.Errorf("gapInWindow(%v, %v, %v) = %v, want %v", c.gapAt, since, until, got, c.want)
			}
		})
	}
}

// streamStateRows builds the sqlmock row set loadStreamStateCore expects,
// with gap_lost_at/gap_lost_detail set to the given values (nil = SQL NULL).
func streamStateRows(gapLostAt any, gapLostDetail any) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"mode", "binlog_file", "binlog_position", "gtid_set",
		"events_indexed", "last_event_time", "last_checkpoint",
		"server_id", "bintrail_id", "gap_lost_at", "gap_lost_detail",
	}).AddRow(
		"position", "binlog.000001", int64(100), "",
		int64(0), nil, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		uint32(1), "bintrail-1", gapLostAt, gapLostDetail,
	)
}

func TestCheckCaptureGap_gapInsideWindow_strictRefuses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	gapAt := since.Add(24 * time.Hour)

	mock.ExpectQuery("SELECT mode, binlog_file").
		WillReturnRows(streamStateRows(gapAt, "binlogs purged before stream caught up"))
	mock.ExpectQuery("SELECT source_health").
		WillReturnError(sql.ErrNoRows)

	err = CheckCaptureGap(context.Background(), db, "mydb", "orders", since, until, false)
	if err == nil {
		t.Fatal("expected an error under strict mode when gap_lost_at is inside the window, got nil")
	}
	if !strings.Contains(err.Error(), "capture gap") {
		t.Errorf("expected error to mention the capture gap, got: %v", err)
	}
	if !strings.Contains(err.Error(), "allow-gaps") {
		t.Errorf("expected error to mention --allow-gaps as the way to proceed, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestCheckCaptureGap_gapInsideWindow_allowGapsWarnsOnly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	gapAt := since.Add(24 * time.Hour)

	mock.ExpectQuery("SELECT mode, binlog_file").
		WillReturnRows(streamStateRows(gapAt, "binlogs purged before stream caught up"))
	mock.ExpectQuery("SELECT source_health").
		WillReturnError(sql.ErrNoRows)

	if err := CheckCaptureGap(context.Background(), db, "mydb", "orders", since, until, true); err != nil {
		t.Fatalf("expected nil error under --allow-gaps, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestCheckCaptureGap_gapOutsideWindow_noError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	// Recorded before the reconstruction window (already covered by the
	// baseline) — must not be flagged.
	gapAt := since.Add(-time.Hour)

	mock.ExpectQuery("SELECT mode, binlog_file").
		WillReturnRows(streamStateRows(gapAt, "old gap"))
	mock.ExpectQuery("SELECT source_health").
		WillReturnError(sql.ErrNoRows)

	if err := CheckCaptureGap(context.Background(), db, "mydb", "orders", since, until, false); err != nil {
		t.Fatalf("expected nil error when gap_lost_at is outside the window, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestCheckCaptureGap_noGapRecorded_noError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery("SELECT mode, binlog_file").
		WillReturnRows(streamStateRows(nil, nil))
	mock.ExpectQuery("SELECT source_health").
		WillReturnError(sql.ErrNoRows)

	if err := CheckCaptureGap(context.Background(), db, "mydb", "orders", since, until, false); err != nil {
		t.Fatalf("expected nil error when gap_lost_at is NULL, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
