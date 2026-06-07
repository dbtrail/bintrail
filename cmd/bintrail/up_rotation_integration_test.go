//go:build integration

package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/testutil"
)

// TestRotateOneIndex_UpgradeGuard verifies the implicit-default upgrade
// guard: an index holding history far beyond the default window (the
// signature of a pre-existing deployment upgrading into built-in rotation)
// must NOT be dropped until the operator sets --rotate-retain explicitly.
func TestRotateOneIndex_UpgradeGuard(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// 100 days of pre-existing history — far beyond 2× the 30d default —
	// plus the current-hour partition (a live deployment always has recent
	// partitions; add-future top-up appends after the LATEST named one, so a
	// lone ancient partition would make top-up land uselessly in the past).
	old := time.Now().UTC().Add(-100 * 24 * time.Hour).Truncate(time.Hour)
	current := time.Now().UTC().Truncate(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{old, current})

	savedVars := saveRotateVars()
	t.Cleanup(func() { restoreRotateVars(savedVars) })
	logs := captureSlog(t)

	// Built-in rotation profile (what startUpRotation fans out) — with
	// add-future headroom so the guarded cycle's top-up promise is asserted.
	rotArchiveDir = ""
	rotArchiveS3 = ""
	rotBintrailID = ""
	rotFormat = "json"
	rotRetry = false
	rotNoReplace = false
	rotAddFuture = 2
	rotProtectUnarchived = true
	rotRetain = "30d"

	dsn := testutil.IntegrationDSN(dbName)
	s := upRotationSettings{
		enabled: true, retain: 30 * 24 * time.Hour, retainRaw: "30d",
		interval: time.Hour, addFuture: 2, explicit: false,
	}

	// Implicit default: the guard must refuse the drop and say so loudly.
	if _, err := rotateOneIndex(context.Background(), dsn, s); err != nil {
		t.Fatalf("rotateOneIndex (implicit): %v", err)
	}
	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	found, future := false, 0
	nowHour := time.Now().UTC().Truncate(time.Hour)
	for _, p := range partitions {
		if p.Name == partitionName(old) {
			found = true
		}
		if d, ok := partitionDate(p.Name); ok && d.After(nowHour) {
			future++
		}
	}
	if !found {
		t.Fatalf("partition %s was dropped under the IMPLICIT default — the upgrade guard failed", partitionName(old))
	}
	if !logs.has(slog.LevelError, "refusing to drop it without an explicit choice") {
		t.Error("upgrade guard did not log its Error explaining the refusal")
	}
	// The guarded cycle must still top up future partitions (retain=0 skips
	// only the drop branch) — otherwise refusing drops would also starve
	// p_future headroom while the operator decides.
	if future < 2 {
		t.Errorf("guarded cycle added %d future partitions, want >= 2 (top-up must survive the guard)", future)
	}

	// Explicit choice: the same retention now drops the old history.
	s.explicit = true
	if _, err := rotateOneIndex(context.Background(), dsn, s); err != nil {
		t.Fatalf("rotateOneIndex (explicit): %v", err)
	}
	partitions, err = listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	for _, p := range partitions {
		if p.Name == partitionName(old) {
			t.Errorf("partition %s should have been dropped once retention was explicit", partitionName(old))
		}
	}
}

// TestStartUpRotation_escalatesOnPersistentDeferral runs the real loop
// against a real index where the protect-unarchived guard defers the same
// partition every cycle (archiving history exists, partition unarchived) and
// asserts the loop escalates to Error after upRotationEscalateAfter cycles —
// the "archiving flow stalled, index growing unbounded" detection.
func TestStartUpRotation_escalatesOnPersistentDeferral(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// One partition past retention (30h > 24h) but NOT archived...
	h1 := time.Now().UTC().Add(-30 * time.Hour).Truncate(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	// ...while archive_state shows archiving history for a different
	// partition → protect active, h1 deferred every cycle.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count)
		VALUES ('p_2020010100', 'cron-uuid', '/archives/old.parquet', 1)`)

	savedVars := saveRotateVars()
	t.Cleanup(func() { restoreRotateVars(savedVars) })
	logs := captureSlog(t)

	prevN := upRotationEscalateAfter
	upRotationEscalateAfter = 2
	t.Cleanup(func() { upRotationEscalateAfter = prevN })

	s := upRotationSettings{
		enabled: true, retain: 24 * time.Hour, retainRaw: "24h",
		interval: 25 * time.Millisecond, addFuture: 0,
		explicit: true, // h1 is only 30h old; guard wouldn't trip, but be unambiguous
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startUpRotation(ctx, s, func() []string {
		return []string{testutil.IntegrationDSN(dbName)}
	})

	deadline := time.After(30 * time.Second)
	for !logs.has(slog.LevelError, "made no progress for consecutive cycles") {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("loop never escalated to Error after consecutive all-deferred cycles")
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-done

	// The deferred partition must still exist — detection, not destruction.
	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	found := false
	for _, p := range partitions {
		if p.Name == partitionName(h1) {
			found = true
		}
	}
	if !found {
		t.Error("deferred partition was dropped — the guard must never destroy unarchived data")
	}
}
