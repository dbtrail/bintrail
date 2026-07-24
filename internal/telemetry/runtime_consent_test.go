package telemetry

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

// TestSetRuntimeConsentTogglesLiveDecision pins the console opt-out: a running
// daemon must stop (and be able to resume) beaconing without a restart, and can
// never be forced ON where Init suppressed it.
func TestSetRuntimeConsentTogglesLiveDecision(t *testing.T) {
	clearEnv(t)
	c := Init(Config{Dir: t.TempDir(), Endpoint: "http://127.0.0.1:1", Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	if !c.Enabled() {
		t.Fatal("expected enabled at init")
	}
	c.SetRuntimeConsent(false)
	if c.Enabled() {
		t.Error("still enabled after runtime opt-out — a live daemon would keep beaconing")
	}
	// The recorded Decision must track the toggle, not stay frozen at Init —
	// otherwise `telemetry status` and the console button read the stale value.
	if d := c.Decision(); d.Enabled || d.Source != SourceConfig {
		t.Errorf("decision stale after opt-out: %+v", d)
	}
	c.SetRuntimeConsent(true)
	if !c.Enabled() {
		t.Error("not re-enabled after opting back in")
	}
	if d := c.Decision(); !d.Enabled || d.Source != SourceConfig {
		t.Errorf("decision stale after opt-in: %+v", d)
	}

	// An inert build (no endpoint) can never be forced ON at runtime.
	inert := Init(Config{Dir: t.TempDir(), Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	inert.SetRuntimeConsent(true)
	if inert.Enabled() {
		t.Error("runtime consent forced an endpoint-less build ON")
	}
}

// TestSetRuntimeConsentRaceSafe runs the toggle concurrently with the reads the
// beacon/record paths make — this is the reason enabled is atomic. `go test
// -race` (CI) is what gives it teeth.
func TestSetRuntimeConsentRaceSafe(t *testing.T) {
	clearEnv(t)
	c := Init(Config{Dir: t.TempDir(), Endpoint: "http://127.0.0.1:1", Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); for j := 0; j < 1000; j++ { _ = c.Enabled() } }()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); for j := 0; j < 1000; j++ { c.SetRuntimeConsent(j%2 == 0) } }()
	}
	wg.Wait()

	var nc *Client
	nc.SetRuntimeConsent(true) // nil receiver must not panic
}

// TestRunDaemonResumesAfterRuntimeOptIn pins the fix for the daemon loop: a
// daemon booted consent-off (but on a build that CAN report) must keep its loop
// alive so a later console opt-in resumes beaconing without a restart.
func TestRunDaemonResumesAfterRuntimeOptIn(t *testing.T) {
	clearEnv(t)
	withDaemonTick(t, 30*time.Millisecond)

	url, count := deliveryCounter(t)
	c := Init(Config{Dir: t.TempDir(), Endpoint: url, Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	c.SetRuntimeConsent(false) // opt out at boot; endpoint still set → loop stays alive
	if c.Enabled() {
		t.Fatal("expected disabled after opt-out")
	}
	startDaemon(t, c)

	// Several ticks while off: nothing may be delivered.
	time.Sleep(200 * time.Millisecond)
	if n := count(); n != 0 {
		t.Fatalf("beaconed %d times while opted out", n)
	}

	// Opt back in: the loop is still running and must resume beaconing.
	c.SetRuntimeConsent(true)
	deadline := time.Now().Add(10 * time.Second)
	for count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if count() == 0 {
		t.Fatal("runtime opt-in did not resume beaconing — the loop had exited at boot")
	}
}
