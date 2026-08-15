//go:build integration

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/telemetry/telemetrytest"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationFlashbackDaemonWiringEmitsBeacon is the #1362 wiring guard
// for `bintrail-pg flashback`, the PostgreSQL-wire time-travel daemon: it
// drives the REAL runFlashback against the integration MySQL index (the
// daemon pings and migrates the index before it listens, so this cannot be a
// unit test; no live PostgreSQL is involved — the engine is index-only) and
// passes only once a real daemon_beacon carrying "flashback" is delivered end
// to end. Delete its `go tel.Client().RunDaemon(...)` line and this fails.
func TestIntegrationFlashbackDaemonWiringEmitsBeacon(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	c, bodies := telemetrytest.CollectingClient(t)
	defer tel.SetClientForTest(c)()

	cfgPath := filepath.Join(t.TempDir(), "shim.yaml")
	shimYAML := "tenants:\n  - mysql_user: app\n    mysql_password: secret\n"
	if err := os.WriteFile(cfgPath, []byte(shimYAML), 0o600); err != nil {
		t.Fatalf("write shim.yaml: %v", err)
	}

	origListen, origIndexDSN, origConfig := fbListen, fbIndexDSN, fbShimConfig
	defer func() { fbListen, fbIndexDSN, fbShimConfig = origListen, origIndexDSN, origConfig }()
	fbListen = "127.0.0.1:0"
	fbIndexDSN = testutil.IntegrationDSN(dbName)
	fbShimConfig = cfgPath

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	flashbackCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runFlashback(flashbackCmd, nil) }()

	telemetrytest.WaitForBeacon(t, bodies, "flashback")

	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runFlashback did not return after cancel — daemon shutdown would hang")
	}
}
