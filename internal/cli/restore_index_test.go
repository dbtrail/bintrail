package cli

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestBuildRestorePartitionSQL(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	hours := map[time.Time]bool{
		time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC): true,
		time.Date(2026, 8, 3, 8, 0, 0, 0, time.UTC):  true,
		// Overlaps the horizon — must not duplicate.
		now: true,
	}
	sqlStr, count := buildRestorePartitionSQL("idx", hours, now, 2)
	if count != 4 { // 08, 10, 12, 13
		t.Fatalf("partition count = %d, want 4", count)
	}
	for _, want := range []string{
		"ALTER TABLE `idx`.`binlog_events` PARTITION BY RANGE (TO_SECONDS(event_timestamp))",
		"PARTITION p_2026080308", "PARTITION p_2026080310", "PARTITION p_2026080312", "PARTITION p_2026080313",
	} {
		if !strings.Contains(sqlStr, want) {
			t.Fatalf("missing %q in:\n%s", want, sqlStr)
		}
	}
	// Ordered ascending, p_future last — RANGE partitioning requires it.
	if !strings.HasSuffix(strings.TrimSpace(sqlStr), "PARTITION p_future VALUES LESS THAN MAXVALUE\n)") {
		t.Fatalf("p_future must close the list:\n%s", sqlStr)
	}
	if strings.Index(sqlStr, "p_2026080308") > strings.Index(sqlStr, "p_2026080310") {
		t.Fatalf("partitions must be ascending:\n%s", sqlStr)
	}
}

func TestRestoreIndexTargetEmpty(t *testing.T) {
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db, mock
	}
	// Table absent: fine (fresh database).
	db, mock := newDB(t)
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	if err := restoreIndexTargetEmpty(context.Background(), db, "idx"); err != nil {
		t.Fatalf("absent table must pass: %v", err)
	}
	// Table exists, empty: fine.
	db, mock = newDB(t)
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	mock.ExpectQuery("SELECT 1 FROM binlog_events").WillReturnError(sql.ErrNoRows)
	if err := restoreIndexTargetEmpty(context.Background(), db, "idx"); err != nil {
		t.Fatalf("empty table must pass: %v", err)
	}
	// Table holds events: refused.
	db, mock = newDB(t)
	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1))
	mock.ExpectQuery("SELECT 1 FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	if err := restoreIndexTargetEmpty(context.Background(), db, "idx"); err == nil ||
		!strings.Contains(err.Error(), "already holds events") {
		t.Fatalf("populated index must be refused: %v", err)
	}
}

func TestRestoreIndexReportExitError(t *testing.T) {
	r := &restoreIndexReport{FilesLoaded: 3}
	if err := r.ExitError(); err != nil {
		t.Fatalf("clean load must exit 0: %v", err)
	}
	r.FailedFiles = []string{"p_2026080310: boom"}
	if err := r.ExitError(); err == nil || !strings.Contains(err.Error(), "p_2026080310") {
		t.Fatalf("failed file must be named: %v", err)
	}
}
