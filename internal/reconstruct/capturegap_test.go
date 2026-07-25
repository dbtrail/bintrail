package reconstruct

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
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

// legacyStreamStateRows builds the row set loadStreamStateBase expects — the
// column list of an index whose stream_state predates gap_lost_* (#765).
func legacyStreamStateRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"mode", "binlog_file", "binlog_position", "gtid_set",
		"events_indexed", "last_event_time", "last_checkpoint",
		"server_id", "bintrail_id",
	}).AddRow(
		"position", "binlog.000001", int64(100), "",
		int64(0), nil, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		uint32(1), "bintrail-1",
	)
}

// expectLegacyStreamState primes the fallback LoadStreamState performs on an
// index missing the gap_lost_* columns: the core SELECT fails with MySQL 1054
// and the base SELECT answers instead, leaving GapColumnsPresent false.
func expectLegacyStreamState(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT mode, binlog_file").
		WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'gap_lost_at' in 'field list'"})
	mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(legacyStreamStateRows())
	mock.ExpectQuery("SELECT source_health").WillReturnError(sql.ErrNoRows)
}

// TestCheckCaptureGap_legacyIndexUnevaluable_strictRefuses is the #765 guard's
// blind spot: on an index that never had the gap_lost_* columns, gap_lost_at is
// invalid because the question was never ASKED, not because the answer was no.
// Reading it as "no gap" makes the check silently inert on exactly the
// un-migrated indexes the console serves (it never migrates registry servers),
// so an unevaluable verdict must refuse under strict mode like a real gap does.
func TestCheckCaptureGap_legacyIndexUnevaluable_strictRefuses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	expectLegacyStreamState(mock)

	err = CheckCaptureGap(context.Background(), db, "mydb", "orders", since, until, false)
	if err == nil {
		t.Fatal("expected a refusal on an index whose gap state is not evaluable, got nil")
	}
	if !strings.Contains(err.Error(), "NOT EVALUABLE") {
		t.Errorf("expected the refusal to say the gap state is not evaluable, got: %v", err)
	}
	if !strings.Contains(err.Error(), "allow-gaps") {
		t.Errorf("expected the refusal to name the override, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestCaptureGapStatus_legacyIndexUnevaluable pins the structured verdict the
// MCP surface reads: Unevaluable, with no stamped timestamp to report.
func TestCaptureGapStatus_legacyIndexUnevaluable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	expectLegacyStreamState(mock)

	gap, err := CaptureGapStatus(context.Background(), db, since, until)
	if err != nil {
		t.Fatalf("CaptureGapStatus: %v", err)
	}
	if gap == nil {
		t.Fatal("expected a non-nil verdict on a legacy index, got nil (the check would be inert)")
	}
	if !gap.Unevaluable {
		t.Errorf("expected Unevaluable, got %+v", gap)
	}
	if !gap.At.IsZero() {
		t.Errorf("an unevaluable verdict must carry no stamped time, got %v", gap.At)
	}
	if !strings.Contains(gap.Reason(), "NOT EVALUABLE") || strings.Contains(gap.Reason(), "--") {
		t.Errorf("Reason must describe the finding without naming a CLI flag, got: %s", gap.Reason())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestCaptureGapStatus_legacyIndexUnevaluable_allowGapsProceeds keeps the
// override working: an unevaluable verdict is a refusal, not a hard wall.
func TestCaptureGapStatus_legacyIndexUnevaluable_allowGapsProceeds(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	expectLegacyStreamState(mock)

	if err := CheckCaptureGap(context.Background(), db, "mydb", "orders", since, until, true); err != nil {
		t.Fatalf("expected nil error under --allow-gaps, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestCaptureGapStatus_emptyStreamState_noVerdict draws the line the
// unevaluable arm must not cross: an index that only ever ran file-based
// `bintrail index` has an EMPTY stream_state. No capture ran, so there is no
// continuity to have broken — that is a real "no gap", not an unknown, and
// treating it as unevaluable would refuse every file-mode reconstruction.
func TestCaptureGapStatus_emptyStreamState_noVerdict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT mode, binlog_file").WillReturnError(sql.ErrNoRows)

	gap, err := CaptureGapStatus(context.Background(), db, since, until)
	if err != nil {
		t.Fatalf("CaptureGapStatus: %v", err)
	}
	if gap != nil {
		t.Errorf("an empty stream_state must not be reported as a gap, got %+v", gap)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
