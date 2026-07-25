//go:build integration

package verify

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestVerifyRecoverInputs_StampedCaptureGapIsInconclusiveNotMismatch is the
// end-to-end form of this mode's sharpest failure mode.
//
// The chain below has a HOLE, not corruption: event 1 leaves qty=1, event 2
// arrives with row_before qty=99, and the events that moved it were NEVER
// CAPTURED. Every stored image is intact — the exact shape of the real incident
// on record for this project (a 10-hour capture gap that lost 301 deletes and
// 37 inserts with perfectly intact images).
//
// The upstream coverage guard cannot see it: query.buildPlan classifies an hour
// as covered iff a p_YYYYMMDDHH partition EXISTS, and both hours here are
// partitioned and live. So the walk reaches a break and — before the fix — the
// table came back a conclusive MISMATCH.
//
// Phase 1 pins that false mismatch (it is what a chain break looks like with no
// continuity record to consult). Phase 2 stamps stream_state.gap_lost_at inside
// the window and pins that the SAME index, SAME events, now reports
// INCONCLUSIVE. Phase 3 moves the stamp outside the window to prove the gate is
// scoped and not a blanket disable.
func TestVerifyRecoverInputs_StampedCaptureGapIsInconclusiveNotMismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	for _, c := range []struct {
		name             string
		ord              int
		key, dt, colType string
	}{
		{"id", 1, "PRI", "int", "int"},
		{"name", 2, "", "varchar", "varchar(64)"},
		{"qty", 3, "", "int", "int"},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'NO', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1, h2 := curHour.Add(-time.Hour), curHour
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	const tsFmt = "2006-01-02 15:04:05"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		now.Add(-3*time.Minute).Format(tsFmt), nil, dbName, "orders", 1 /*INSERT*/, "7", nil,
		nil, []byte(`{"id":7,"name":"widget","qty":1}`))
	// The updates that took qty 1 → 99 were never captured. Both images below
	// are exactly what the source wrote.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300,
		now.Add(-2*time.Minute).Format(tsFmt), nil, dbName, "orders", 2 /*UPDATE*/, "7", nil,
		[]byte(`{"id":7,"name":"widget","qty":99}`), []byte(`{"id":7,"name":"widget","qty":100}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := RecoverInputsConfig{
		IndexDB:     db,
		Resolver:    resolver,
		IndexDBName: dbName,
		NoArchive:   true,
		Since:       h1,
		Until:       now,
	}
	run := func(t *testing.T) TableResult {
		t.Helper()
		res, err := VerifyRecoverInputs(context.Background(), cfg, dbName, "orders")
		if err != nil {
			t.Fatalf("VerifyRecoverInputs: %v", err)
		}
		return res
	}

	// ── Phase 1: no continuity record at all ──────────────────────────────────
	res := run(t)
	if res.Status != StatusMismatch {
		t.Fatalf("premise: a chain break with no gap record should still be reported, got %s (%s)", res.Status, res.Detail)
	}
	// Even here the finding must not assert corruption as the cause — the
	// missing events ARE the cause in this fixture.
	if !strings.Contains(res.Detail, "never captured") {
		t.Errorf("mismatch detail must name the missing-events explanation, got: %s", res.Detail)
	}

	// ── Phase 2: the loss is stamped inside the window ────────────────────────
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, last_checkpoint, server_id, gap_lost_at, gap_lost_detail)
		VALUES (1, 'gtid', UTC_TIMESTAMP(), 1, ?, ?)`,
		now.Add(-10*time.Minute).Format(tsFmt), "unfillable binlog gap; auto-advanced past the missing range")

	res = run(t)
	if res.Status != StatusInconclusive {
		t.Fatalf("a stamped permanent loss inside the window must degrade the table to inconclusive, got %s (%s)", res.Status, res.Detail)
	}
	if !strings.Contains(res.Detail, "permanently lost") || !strings.Contains(res.Detail, "auto-advanced") {
		t.Errorf("detail should name the stamped loss and carry its recorded reason, got: %s", res.Detail)
	}

	// ── Phase 3: the same stamp, outside the window ───────────────────────────
	testutil.MustExec(t, db, `UPDATE stream_state SET gap_lost_at = ? WHERE id = 1`,
		now.Add(-2*time.Hour).Format(tsFmt))

	if res = run(t); res.Status != StatusMismatch {
		t.Fatalf("a loss stamped OUTSIDE the window must not suppress the finding, got %s (%s)", res.Status, res.Detail)
	}

	// ── Phase 4: the record cannot be READ ────────────────────────────────────
	// A failed read says nothing about continuity, so it must surface as a hard
	// error (the CLI renders it as this table's StatusError and keeps going) —
	// NOT as an inconclusive verdict about data that was never consulted, which
	// would be a second door to the all-inconclusive non-zero exit.
	testutil.MustExec(t, db, "DROP TABLE stream_state")
	if _, err := VerifyRecoverInputs(context.Background(), cfg, dbName, "orders"); err == nil {
		t.Fatal("an unreadable capture-continuity record must surface as an error, not a verdict")
	} else if !strings.Contains(err.Error(), "capture-continuity record") {
		t.Errorf("error should name what could not be read, got: %v", err)
	}
}
