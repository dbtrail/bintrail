//go:build integration

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/testutil"
)

// TestCheckIndexCapacity_skipsWithoutHistory: a freshly-initialized index
// (p_future only, zero events) cannot support a write-rate measurement — the
// check must SKIP, never guess.
func TestCheckIndexCapacity_skipsWithoutHistory(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	r := checkIndexCapacity(context.Background(), testutil.IntegrationDSN(dbName), dbName, 30*24*time.Hour)
	if r.Status != statusSkip {
		t.Fatalf("status = %s, want skip on an empty index (detail: %s)", r.Status, r.Detail)
	}
	if !strings.Contains(r.Detail, "not enough recent history") {
		t.Errorf("detail should explain the missing history, got: %s", r.Detail)
	}
}

// TestCheckIndexCapacity_measuresAndProjects seeds three completed hourly
// partitions with rows and verifies the check produces a real projection
// (shrunken measurement floors — information_schema row counts are estimates
// and the fixture is small).
func TestCheckIndexCapacity_measuresAndProjects(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	prevHours, prevRows := capMinSampleHours, capMinSampleRows
	capMinSampleHours, capMinSampleRows = 2, 5
	t.Cleanup(func() { capMinSampleHours, capMinSampleRows = prevHours, prevRows })

	// Three completed hours of history (current hour excluded by design).
	h3 := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	h2 := h3.Add(time.Hour)
	h1 := h2.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h3, h2, h1})
	pos := uint64(100)
	for _, h := range []time.Time{h3, h2, h1} {
		for i := range 10 {
			ts := h.Add(time.Duration(i*5) * time.Minute).Format("2006-01-02 15:04:05")
			testutil.InsertEvent(t, db, "binlog.000001", pos, pos+50, ts, nil,
				"testdb", "users", 1, fmt.Sprintf("%d", pos), nil, nil, []byte(`{"id":1}`))
			pos += 100
		}
	}
	// ANALYZE refreshes the information_schema partition statistics the
	// check reads (they are dictionary-cached estimates otherwise).
	testutil.MustExec(t, db, fmt.Sprintf("ANALYZE TABLE `%s`.`binlog_events`", dbName))

	r := checkIndexCapacity(context.Background(), testutil.IntegrationDSN(dbName), dbName, 30*24*time.Hour)
	if r.Status == statusSkip || r.Status == statusFail {
		t.Fatalf("status = %s, want a real measurement (detail: %s)", r.Status, r.Detail)
	}
	if !strings.Contains(r.Detail, "projected steady-state") {
		t.Errorf("detail should carry the projection, got: %s", r.Detail)
	}
}

// TestBuildDoctorReport_includesCapacityCheck pins the battery wiring: the
// capacity check must appear in buildDoctorReport's output with the retain
// parameter flowing through. Without this, a refactor dropping the
// report.add(checkIndexCapacity(...)) line would leave every direct
// checkIndexCapacity test green while doctor silently stopped covering
// capacity. (The index test server doubles as the source — the source block
// only early-returns on a failed CONNECT, and individual source check
// failures don't stop the battery.)
func TestBuildDoctorReport_includesCapacityCheck(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	prevHours, prevRows := capMinSampleHours, capMinSampleRows
	capMinSampleHours, capMinSampleRows = 2, 5
	t.Cleanup(func() { capMinSampleHours, capMinSampleRows = prevHours, prevRows })

	h3 := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	h2 := h3.Add(time.Hour)
	h1 := h2.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h3, h2, h1})
	pos := uint64(100)
	for _, h := range []time.Time{h3, h2, h1} {
		for i := range 10 {
			ts := h.Add(time.Duration(i*5) * time.Minute).Format("2006-01-02 15:04:05")
			testutil.InsertEvent(t, db, "binlog.000001", pos, pos+50, ts, nil,
				"testdb", "users", 1, fmt.Sprintf("%d", pos), nil, nil, []byte(`{"id":1}`))
			pos += 100
		}
	}
	testutil.MustExec(t, db, fmt.Sprintf("ANALYZE TABLE `%s`.`binlog_events`", dbName))

	dsn := testutil.IntegrationDSN(dbName)
	capacityIn := func(retain time.Duration) checkResult {
		t.Helper()
		report := buildDoctorReport(context.Background(), dsn, dsn, "", retain)
		for _, c := range report.Checks {
			if c.Name == capacityCheckName {
				return c
			}
		}
		t.Fatalf("%q missing from the doctor battery (retain=%v)", capacityCheckName, retain)
		return checkResult{}
	}

	if c := capacityIn(30 * 24 * time.Hour); !strings.Contains(c.Detail, "projected steady-state") {
		t.Errorf("retain=30d: detail should carry the projection, got: %s", c.Detail)
	}
	if c := capacityIn(0); !strings.Contains(c.Detail, "unbounded") {
		t.Errorf("retain=0: detail should name unbounded growth, got: %s", c.Detail)
	}
}

// TestCheckIndexCapacity_queryErrorFails: a real query error must FAIL with
// remediation like every sibling check — a SKIP would let the operator
// believe capacity was covered when it wasn't. A cancelled context turns the
// partition-statistics query into an error (the connect itself is lazy).
func TestCheckIndexCapacity_queryErrorFails(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := checkIndexCapacity(ctx, testutil.IntegrationDSN(dbName), dbName, 30*24*time.Hour)
	if r.Status != statusFail {
		t.Fatalf("status = %s, want fail on a query error (detail: %s)", r.Status, r.Detail)
	}
	if r.Remediation == "" {
		t.Error("query-error FAIL must carry remediation")
	}
}

// TestCheckIndexCapacity_uninitializedIndexExplainsItself: zero visible
// partitions must disambiguate "not initialized / not visible" from "fresh
// but initialized" — never tell the operator to wait for a table that will
// never appear.
func TestCheckIndexCapacity_uninitializedIndexExplainsItself(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	_ = db // database exists, but binlog_events was never created

	r := checkIndexCapacity(context.Background(), testutil.IntegrationDSN(dbName), dbName, 30*24*time.Hour)
	if r.Status != statusSkip {
		t.Fatalf("status = %s, want skip (detail: %s)", r.Status, r.Detail)
	}
	if !strings.Contains(r.Detail, "not initialized") {
		t.Errorf("detail should say the index is not initialized, got: %s", r.Detail)
	}
	if strings.Contains(r.Detail, "re-run after") {
		t.Errorf("detail must NOT advise waiting for a table that will never appear, got: %s", r.Detail)
	}
}

// TestCheckIndexCapacity_noRetentionWarnsUnbounded: same fixture, retain=0 —
// the check must WARN that the index grows without bound.
func TestCheckIndexCapacity_noRetentionWarnsUnbounded(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	prevHours, prevRows := capMinSampleHours, capMinSampleRows
	capMinSampleHours, capMinSampleRows = 2, 5
	t.Cleanup(func() { capMinSampleHours, capMinSampleRows = prevHours, prevRows })

	h3 := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	h2 := h3.Add(time.Hour)
	h1 := h2.Add(time.Hour)
	setupPartitionedTable(t, db, dbName, []time.Time{h3, h2, h1})
	pos := uint64(100)
	for _, h := range []time.Time{h3, h2, h1} {
		for i := range 10 {
			ts := h.Add(time.Duration(i*5) * time.Minute).Format("2006-01-02 15:04:05")
			testutil.InsertEvent(t, db, "binlog.000001", pos, pos+50, ts, nil,
				"testdb", "users", 1, fmt.Sprintf("%d", pos), nil, nil, []byte(`{"id":1}`))
			pos += 100
		}
	}
	testutil.MustExec(t, db, fmt.Sprintf("ANALYZE TABLE `%s`.`binlog_events`", dbName))

	r := checkIndexCapacity(context.Background(), testutil.IntegrationDSN(dbName), dbName, 0)
	if r.Status != statusWarn {
		t.Fatalf("status = %s, want warn for retain=0 (detail: %s)", r.Status, r.Detail)
	}
	if !strings.Contains(r.Detail, "unbounded") {
		t.Errorf("detail should name the unbounded growth, got: %s", r.Detail)
	}
}
