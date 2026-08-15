//go:build integration

package consoleapp

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/telemetry/telemetrytest"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The #1362 wiring guards for BOTH `bintrail-console watch` run paths. Each
// drives the real run function against the integration MySQL index and passes
// only once a real daemon_beacon carrying "watch" is delivered end to end —
// delete either path's `go tel.Client().RunDaemon(...)` line and the matching
// test fails. Both paths connect to the index BEFORE the telemetry launch, so
// these cannot be unit tests.

// watchWiringSetup prepares the shared pieces: a real index DB, cleared env,
// the collecting client injected into the hook, and saved/restored watch
// flag globals.
func watchWiringSetup(t *testing.T) (bodies func() []string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	c, collected := telemetrytest.CollectingClient(t)
	restoreClient := tel.SetClientForTest(c)
	t.Cleanup(restoreClient)
	clearConsoleEnv(t)

	origIndex, origSource := upIndexDSN, upSourceDSN
	origListen, origToken, origServers := upConsoleListen, upConsoleToken, upConsoleServersFile
	origRotation := upRotationCfg
	t.Cleanup(func() {
		upIndexDSN, upSourceDSN = origIndex, origSource
		upConsoleListen, upConsoleToken, upConsoleServersFile = origListen, origToken, origServers
		upRotationCfg = origRotation
	})

	upIndexDSN = testutil.IntegrationDSN(dbName)
	upSourceDSN = ""
	upConsoleListen = "127.0.0.1:0"
	upConsoleToken = "wiring-test-token"
	upConsoleServersFile = filepath.Join(t.TempDir(), "servers.yaml")

	// runWatch normally parses this before dispatching to the run path under
	// test; mirror it with the flag defaults.
	var err error
	upRotationCfg, err = rotation.ParseSettings(upRotateRetain, upRotateInterval, upRotateAddFuture, false)
	if err != nil {
		t.Fatalf("parse default rotation settings: %v", err)
	}
	return collected
}

func TestIntegrationWatchConsoleOnlyDaemonWiringEmitsBeacon(t *testing.T) {
	bodies := watchWiringSetup(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runUpConsoleOnly(watchCmd) }()

	telemetrytest.WaitForBeacon(t, bodies, "watch")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("runUpConsoleOnly returned an error on clean shutdown: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("runUpConsoleOnly did not return after cancel — daemon shutdown would hang")
	}
}

// TestIntegrationWatchStreamDaemonWiringEmitsBeacon covers the source-ful
// path. The source DSN points at a hanging TCP endpoint so the main stream
// blocks in its source dial — the daemon stays alive past the shortened tick
// regardless of the test host's binlog configuration, and severing the
// endpoint lets the stream fail out and the daemon drain.
func TestIntegrationWatchStreamDaemonWiringEmitsBeacon(t *testing.T) {
	bodies := watchWiringSetup(t)

	addr, sever := telemetrytest.HangingTCPAddr(t)
	upSourceDSN = "root:x@tcp(" + addr + ")/ignored"
	origServerID := upServerID
	t.Cleanup(func() { upServerID = origServerID })
	upServerID = 54321

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runUpStreamWithConsole(watchCmd, nil) }()

	telemetrytest.WaitForBeacon(t, bodies, "watch")

	cancel()
	sever()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("runUpStreamWithConsole did not return after cancel — daemon shutdown would hang")
	}
}
