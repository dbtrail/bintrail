package status

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestContinuityStatus(t *testing.T) {
	if got := ContinuityStatus(nil, errors.New("boom")); got != "unavailable" {
		t.Fatalf("read failure = %q, want unavailable", got)
	}
	if got := ContinuityStatus(nil, nil); got != "none" {
		t.Fatalf("no stream row = %q, want none", got)
	}
	if got := ContinuityStatus(&StreamStateInfo{GapColumnsPresent: false}, nil); got != "unknown" {
		t.Fatalf("legacy index = %q, want unknown", got)
	}
	gapped := &StreamStateInfo{GapColumnsPresent: true, GapLostAt: sql.NullTime{Time: time.Now(), Valid: true}}
	if got := ContinuityStatus(gapped, nil); got != "gap_lost" {
		t.Fatalf("stamped gap = %q, want gap_lost", got)
	}
	if got := ContinuityStatus(&StreamStateInfo{GapColumnsPresent: true}, nil); got != "ok" {
		t.Fatalf("clean stream = %q, want ok", got)
	}
}

func TestCollectCoverageSummary(t *testing.T) {
	partsQ := regexp.QuoteMeta("SELECT PARTITION_NAME FROM information_schema.PARTITIONS")
	archQ := regexp.QuoteMeta("SELECT MIN(partition_name), MAX(partition_name) FROM archive_state")
	maxQ := regexp.QuoteMeta("SELECT MAX(event_timestamp) FROM binlog_events")
	streamQ := "FROM stream_state"
	streamCols := []string{"mode", "binlog_file", "binlog_position", "gtid_set",
		"events_indexed", "last_event_time", "last_checkpoint",
		"server_id", "bintrail_id", "gap_lost_at", "gap_lost_detail"}
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db, mock
	}
	latest := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	now := latest.Add(42 * time.Second)

	t.Run("streaming index: window, lag, ok", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
		mock.ExpectQuery(maxQ).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(latest))
		mock.ExpectQuery(streamQ).WillReturnRows(sqlmock.NewRows(streamCols).
			AddRow("gtid", "", 0, "uuid:1-9", 100, latest, latest, 7, "id-1", nil, nil))
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil {
			t.Fatal(err)
		}
		if !sum.DeltaFrom.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) || !sum.DeltaTo.Equal(latest) {
			t.Fatalf("window = [%v, %v]", sum.DeltaFrom, sum.DeltaTo)
		}
		if sum.Continuity != "ok" || sum.LagSeconds == nil || *sum.LagSeconds != 42 {
			t.Fatalf("continuity=%q lag=%v", sum.Continuity, sum.LagSeconds)
		}
	})

	t.Run("file-mode index: no stream row, no lag claim", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
		mock.ExpectQuery(maxQ).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(latest))
		mock.ExpectQuery(streamQ).WillReturnError(sql.ErrNoRows)
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil {
			t.Fatal(err)
		}
		if sum.Continuity != "none" || sum.LagSeconds != nil {
			t.Fatalf("continuity=%q lag=%v — file mode must claim neither", sum.Continuity, sum.LagSeconds)
		}
	})

	t.Run("stamped gap surfaces as gap_lost", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
		mock.ExpectQuery(maxQ).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(latest))
		mock.ExpectQuery(streamQ).WillReturnRows(sqlmock.NewRows(streamCols).
			AddRow("gtid", "", 0, "uuid:1-9", 100, latest, latest, 7, "id-1", latest.Add(-time.Hour), "auto-advance"))
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil || sum.Continuity != "gap_lost" {
			t.Fatalf("continuity=%q err=%v", sum.Continuity, err)
		}
	})

	t.Run("floor failure degrades from to unknown, not the endpoint", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnError(&mysql.MySQLError{Number: 1045, Message: "access denied"})
		mock.ExpectQuery(maxQ).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(latest))
		mock.ExpectQuery(streamQ).WillReturnError(sql.ErrNoRows)
		sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now)
		if err != nil {
			t.Fatal(err)
		}
		if !sum.DeltaFrom.IsZero() || !sum.DeltaTo.Equal(latest) {
			t.Fatalf("unknown floor must stay zero: %+v", sum)
		}
	})

	t.Run("newest-event failure is fatal", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max"}).AddRow(nil, nil))
		mock.ExpectQuery(maxQ).WillReturnError(errors.New("boom"))
		if _, err := CollectCoverageSummary(context.Background(), db, "binlog_index", now); err == nil {
			t.Fatal("a window without an upper edge must be an error")
		}
	})
}
