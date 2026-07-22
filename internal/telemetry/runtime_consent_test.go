package telemetry

import (
	"bytes"
	"sync"
	"testing"
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
	c.SetRuntimeConsent(true)
	if !c.Enabled() {
		t.Error("not re-enabled after opting back in")
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
