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
