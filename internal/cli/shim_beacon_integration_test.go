//go:build integration

package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/telemetry/telemetrytest"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationShimDaemonWiringEmitsBeacon is the #1362 wiring guard for
// `bintrail shim`: it drives the REAL runShim against the integration MySQL
// index (the shim pings and migrates the index before it ever listens, so
// this cannot be a unit test) and passes only once a real daemon_beacon
// carrying "shim" is delivered end to end. Delete shim's
// `go processClient.RunDaemon(...)` line and this fails — the loop itself is
// pinned by internal/telemetry's own daemon tests.
//
// The client is injected through the same package seam production uses: the
// binary's TelemetryHook publishes its resolved client via SetClientForTest /
// Start, and runShim picks it up from processClient.
func TestIntegrationShimDaemonWiringEmitsBeacon(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	c, bodies := telemetrytest.CollectingClient(t)
	defer (&TelemetryHook{}).SetClientForTest(c)()

	cfgPath := filepath.Join(t.TempDir(), "shim.yaml")
	shimYAML := "tenants:\n  - mysql_user: app\n    mysql_password: secret\n"
	if err := os.WriteFile(cfgPath, []byte(shimYAML), 0o600); err != nil {
		t.Fatalf("write shim.yaml: %v", err)
	}

	origListen, origIndexDSN := shListen, shIndexDSN
	origConfig, origNoArchive := shShimConfig, shNoArchive
	defer func() {
		shListen, shIndexDSN = origListen, origIndexDSN
		shShimConfig, shNoArchive = origConfig, origNoArchive
	}()
	shListen = "127.0.0.1:0"
	shIndexDSN = testutil.IntegrationDSN(dbName)
	shShimConfig = cfgPath
	shNoArchive = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shimCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runShim(shimCmd, nil) }()

	telemetrytest.WaitForBeacon(t, bodies, "shim")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("runShim returned an error on clean shutdown: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("runShim did not return after cancel — daemon shutdown would hang")
	}
}
