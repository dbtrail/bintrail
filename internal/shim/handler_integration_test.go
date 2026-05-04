//go:build integration

package shim

import (
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/testutil"
)

// TestRunPointInTime_PlannerCoversCurrentHour pins issue #259: planner
// must not classify the current hour as a coverage gap. The sqlmock
// counterpart in handler_test.go can't catch information_schema.PARTITIONS
// shape drift.
func TestRunPointInTime_PlannerCoversCurrentHour(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	// Seed previous-hour archive so buildPlan doesn't short-circuit to
	// nil (runPointInTime passes only Until). Without this, the
	// regression hides — engine.Fetch answers from binlog_events
	// regardless of the planner's DBName.
	prev := now.Add(-time.Hour).Format("p_2006010215")
	testutil.MustExec(t, db,
		"INSERT INTO archive_state (partition_name, archived_at) VALUES (?, NOW())",
		prev)

	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   false,
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	result, err := h.runPointInTime(TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "users",
		PKValue: "1",
		AsOf:    now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	if result == nil || result.Resultset == nil {
		t.Fatal("expected non-nil resultset")
	}
	// Server-built resultsets populate RowDatas, not Values; RowNumber()
	// reads len(Values) and is always 0.
	if got := len(result.Resultset.RowDatas); got != 1 {
		t.Errorf("expected 1 row, got %d", got)
	}
	// Field shape distinguishes a real row from emptyResult's
	// `[_flashback]` sentinel. imageToResult sorts JSON keys, so the
	// row we inserted ({"id","name"}) yields exactly these two fields.
	gotFields := fieldNames(result.Resultset.Fields)
	if want := []string{"id", "name"}; !slices.Equal(gotFields, want) {
		t.Errorf("fields = %v, want %v", gotFields, want)
	}
}

// TestRunDiff_PlannerCoversCurrentHour pins #259 for runDiff, which
// fails loud (*query.GapError) where runPointInTime fails silent —
// distinct guards for distinct shapes.
func TestRunDiff_PlannerCoversCurrentHour(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, now)

	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   false,
		NoArchive:   true,
		IndexDBName: dbName,
	}, slog.Default())

	// Keep window inside the partitioned hour: planner expands rangeEnd
	// to until.Truncate(Hour)+1h, so until=hour+30m won't hit the next hour.
	result, err := h.runDiff(TimeTravelQuery{
		Type:    TypeDiff,
		Schema:  "myapp",
		Table:   "users",
		PKValue: "1",
		Since:   now.Add(time.Minute),
		Until:   now.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("runDiff: %v", err)
	}
	if result == nil || result.Resultset == nil {
		t.Fatal("expected non-nil resultset")
	}
	if got := len(result.Resultset.RowDatas); got != 1 {
		t.Errorf("expected 1 row, got %d", got)
	}
	// runDiff hardcodes its 6-column shape; verifying the field set
	// guards against the empty-result fallback being silently returned.
	gotFields := fieldNames(result.Resultset.Fields)
	want := []string{"event_id", "event_timestamp", "event_type", "gtid", "row_before", "row_after"}
	if !slices.Equal(gotFields, want) {
		t.Errorf("fields = %v, want %v", gotFields, want)
	}
}

// addHourlyPartition reorganizes p_future into one named hourly partition
// + fresh p_future, matching the layout `bintrail init` produces.
// Format args come from time.Format so injection is impossible.
func addHourlyPartition(t *testing.T, db *sql.DB, h time.Time) {
	t.Helper()
	pName := h.Format("p_2006010215")
	upper := h.Add(time.Hour).Format("2006-01-02 15:04:05")
	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE binlog_events REORGANIZE PARTITION p_future INTO ("+
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s')), "+
			"PARTITION p_future VALUES LESS THAN MAXVALUE)",
		pName, upper,
	))
}

func fieldNames(fields []*mysql.Field) []string {
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = string(f.Name)
	}
	return out
}
