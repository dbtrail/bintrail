//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/console"
	"github.com/dbtrail/bintrail/internal/testutil"
)

// TestIntegrationMonitorSupervisor exercises the control plane end to end
// against real MySQL: provision a per-source index DB, take the advisory
// lock, stream real binlog events into it, refuse a second daemon, and stop
// cleanly releasing the lock.
func TestIntegrationMonitorSupervisor(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The daemon's boot index: any database on the test server (per-source
	// DBs are derived siblings on the same server).
	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	// A dedicated source schema with a table that exists BEFORE the stream
	// starts (so the startup snapshot covers it — see #396 for the
	// DDL-after-start race).
	srcDB, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	srcSchema := fmt.Sprintf("cp_src_%d", time.Now().UnixNano()%1e9)
	mustExec := func(q string) {
		t.Helper()
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec("CREATE DATABASE " + srcSchema)
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS " + srcSchema) })
	mustExec("CREATE TABLE " + srcSchema + ".items (id INT PRIMARY KEY, qty INT)")

	sup := newMonitorSupervisor(ctx, bootDSN)
	entry := console.ServerEntry{
		ID:        fmt.Sprintf("itest%d", time.Now().UnixNano()%1e9),
		Name:      "integration",
		SourceDSN: testutil.BaseDSN() + "/",
		Schemas:   srcSchema,
	}

	// Derive + provision + start.
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(derived, "bintrail_idx_"+entry.ID) {
		t.Fatalf("derived DSN missing per-source db: %q", derived)
	}
	entry.DSN = derived
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID) })

	// Doctor against the real source must not fail (warnings allowed —
	// retention on the test container is a warn).
	report, err := sup.Doctor(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed > 0 {
		t.Fatalf("doctor failed on the test source: %+v", report.Checks)
	}

	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })

	waitState := func(want string, timeout time.Duration) console.MonitorStatus {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			st := sup.Status(entry.ID)
			if st.State == want {
				return st
			}
			time.Sleep(200 * time.Millisecond)
		}
		st := sup.Status(entry.ID)
		t.Fatalf("state = %+v, want %s", st, want)
		return st
	}
	waitState("running", 30*time.Second)

	// The advisory lock: a second supervisor (second daemon) must refuse.
	sup2 := newMonitorSupervisor(ctx, bootDSN)
	if err := sup2.Start(ctx, entry); err == nil || !strings.Contains(err.Error(), "already monitoring") {
		t.Fatalf("second daemon Start: err=%v, want advisory-lock refusal", err)
	}

	// Real events flow into the per-source index.
	mustExec("INSERT INTO " + srcSchema + ".items VALUES (1, 10), (2, 5)")
	mustExec("UPDATE " + srcSchema + ".items SET qty = 99 WHERE id = 1")

	idxDB, err := sql.Open("mysql", derived)
	if err != nil {
		t.Fatal(err)
	}
	defer idxDB.Close()
	var count int
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		_ = idxDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ?", srcSchema).Scan(&count)
		if count >= 3 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if count < 3 {
		t.Fatalf("expected >=3 events in the per-source index, got %d", count)
	}

	// Stop: drains the stream and releases the advisory lock.
	if err := sup.Stop(ctx, entry.ID); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st := sup.Status(entry.ID); st.State != "stopped" {
		t.Errorf("after Stop: state=%+v, want stopped", st)
	}
	var got int
	if err := idxDB.QueryRow("SELECT GET_LOCK(?, 0)", "bintrail_monitor_"+entry.ID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Error("advisory lock must be free after Stop")
	}
	_, _ = idxDB.Exec("SELECT RELEASE_LOCK(?)", "bintrail_monitor_"+entry.ID)
}

// TestIntegrationDDLThenImmediateInserts is the #396 regression: a
// `CREATE TABLE …; INSERT …; UPDATE …; DELETE …;` burst against a LIVE stream
// must index every trailing row event. Before the synchronous parser-side DDL
// hook, the consumer-side auto-snapshot raced the parser and the rows that
// followed the DDL in the binlog were silently skipped ("table not in
// snapshot") and permanently lost.
func TestIntegrationDDLThenImmediateInserts(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	srcDB, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	srcSchema := fmt.Sprintf("ddlrace_%d", time.Now().UnixNano()%1e9)
	if _, err := srcDB.Exec("CREATE DATABASE " + srcSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS " + srcSchema) })
	// A pre-existing table so the STARTUP snapshot has something to capture —
	// the bug under test is about a table created LATER, mid-stream.
	if _, err := srcDB.Exec("CREATE TABLE " + srcSchema + ".seed (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	sup := newMonitorSupervisor(ctx, bootDSN)
	entry := console.ServerEntry{
		ID:        fmt.Sprintf("ddlrace%d", time.Now().UnixNano()%1e9),
		Name:      "ddl-race",
		SourceDSN: testutil.BaseDSN() + "/",
		Schemas:   srcSchema,
	}
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.DSN = derived
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID) })

	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && sup.Status(entry.ID).State != "running" {
		time.Sleep(200 * time.Millisecond)
	}
	if st := sup.Status(entry.ID); st.State != "running" {
		t.Fatalf("stream not running: %+v", st)
	}

	// THE repro: the table is created WHILE the stream is live, with the row
	// events immediately behind the DDL in the binlog — one multi-statement
	// burst, no pause for any snapshot to win a race.
	if _, err := srcDB.Exec("CREATE TABLE " + srcSchema + ".burst (id INT PRIMARY KEY, v VARCHAR(20))"); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.Exec("INSERT INTO " + srcSchema + ".burst VALUES (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.Exec("UPDATE " + srcSchema + ".burst SET v='z' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.Exec("DELETE FROM " + srcSchema + ".burst WHERE id=2"); err != nil {
		t.Fatal(err)
	}

	idxDB, err := sql.Open("mysql", derived)
	if err != nil {
		t.Fatal(err)
	}
	defer idxDB.Close()

	// 3 row events (INSERT batch counts once per row? InsertEvent granularity:
	// the indexer writes one row per affected row — INSERT(2 rows) + UPDATE(1)
	// + DELETE(1) = 4) must ALL land; before the fix this was 0.
	var count int
	deadline = time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		_ = idxDB.QueryRow(
			"SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND table_name = 'burst'",
			srcSchema).Scan(&count)
		if count >= 4 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if count < 4 {
		t.Fatalf("rows immediately after the DDL were lost: indexed %d of 4 (#396)", count)
	}

	// And the DDL itself was recorded with a fresh snapshot.
	var changes int
	if err := idxDB.QueryRow(
		"SELECT COUNT(*) FROM schema_changes WHERE table_name = 'burst'").Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if changes < 1 {
		t.Errorf("schema_changes must record the CREATE TABLE, got %d rows", changes)
	}
}
