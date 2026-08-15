package cliapp

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/telemetry/telemetrytest"
)

// These are the WIRING guards for #1362: each drives the REAL daemon run
// function and passes only once a real daemon_beacon carrying that daemon's
// command name is delivered end to end. Delete the daemon's
// `go tel.Client().RunDaemon(ctx, cmd.Name())` line and the matching test
// fails — the loop itself is pinned by internal/telemetry's own daemon tests.

// TestStreamDaemonWiringEmitsBeacon holds `bintrail stream` alive in its very
// first index dial (a TCP endpoint that accepts and never answers, so the
// driver's handshake read blocks) while the shortened daemon tick fires.
func TestStreamDaemonWiringEmitsBeacon(t *testing.T) {
	c, bodies := telemetrytest.CollectingClient(t)
	defer tel.SetClientForTest(c)()

	addr, sever := telemetrytest.HangingTCPAddr(t)
	origIndex, origSource := strmIndexDSN, strmSourceDSN
	defer func() { strmIndexDSN, strmSourceDSN = origIndex, origSource }()
	strmIndexDSN = "root:x@tcp(" + addr + ")/bintrail_index"
	strmSourceDSN = "root:x@tcp(" + addr + ")/ignored"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runStream(streamCmd, nil) }()

	telemetrytest.WaitForBeacon(t, bodies, "stream")

	cancel()
	sever()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runStream did not return after cancel — daemon shutdown would hang")
	}
}

// TestUpDelegationNamesBeaconUp pins the docs' "up beacons" claim, which
// rides on runUpStream delegating to runStream WITH the up command itself:
// the shared call site names the beacon via cmd.Name(), so invoked as `up` it
// must say "up", not "stream". Hardcode the name in runStream — or break the
// delegation's cmd pass-through — and this fails.
func TestUpDelegationNamesBeaconUp(t *testing.T) {
	c, bodies := telemetrytest.CollectingClient(t)
	defer tel.SetClientForTest(c)()

	addr, sever := telemetrytest.HangingTCPAddr(t)
	origIndex, origSource := strmIndexDSN, strmSourceDSN
	defer func() { strmIndexDSN, strmSourceDSN = origIndex, origSource }()
	// As in production `up`, the strm* globals carry the DSNs by the time
	// runStream runs (populateStreamFlags has copied them).
	strmIndexDSN = "root:x@tcp(" + addr + ")/bintrail_index"
	strmSourceDSN = "root:x@tcp(" + addr + ")/ignored"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runStream(upCmd, nil) }()

	telemetrytest.WaitForBeacon(t, bodies, "up")

	cancel()
	sever()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runStream did not return after cancel — daemon shutdown would hang")
	}
}

// TestAgentDaemonWiringEmitsBeacon keeps `bintrail agent` alive through its
// channel reconnect backoff (a connection-refused endpoint, a few attempts)
// while the shortened daemon tick fires.
func TestAgentDaemonWiringEmitsBeacon(t *testing.T) {
	c, bodies := telemetrytest.CollectingClient(t)
	defer tel.SetClientForTest(c)()

	origEndpoint, origAPIKey := agtEndpoint, agtAPIKey
	origIndex, origSource := agtIndexDSN, agtSourceDSN
	origArchiveDir := agtArchiveDir
	origAttempts := agtMaxReconnectAttempts
	defer func() {
		agtEndpoint, agtAPIKey = origEndpoint, origAPIKey
		agtIndexDSN, agtSourceDSN = origIndex, origSource
		agtArchiveDir = origArchiveDir
		agtMaxReconnectAttempts = origAttempts
	}()
	// Port 1 refuses instantly; the reconnect backoff between attempts keeps
	// the daemon alive well past the shortened tick, and the bounded attempt
	// count means the run function returns on its own even without a signal.
	agtEndpoint = "ws://127.0.0.1:1/v1/agent"
	agtAPIKey = "wiring-test"
	agtIndexDSN, agtSourceDSN = "", ""
	// An archive dir satisfies the at-least-one-data-source validation with
	// no pre-launch connection (the flag is recorded, not scanned, here).
	agtArchiveDir = t.TempDir()
	agtMaxReconnectAttempts = 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agentCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runAgent(agentCmd, nil) }()

	telemetrytest.WaitForBeacon(t, bodies, "agent")

	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runAgent did not return after cancel — daemon shutdown would hang")
	}
}
