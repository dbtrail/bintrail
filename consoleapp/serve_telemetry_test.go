package consoleapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/telemetry/telemetrytest"
)

// TestServeTelemetryOptOutStopsBeacons pins the consent surface serve's
// beacon rides on (#1362 review), through the REAL runServe: serve wires the
// consent-only adapter (serveTelemetry) into the console, so the UI opt-out
// must (a) flip the LIVE client — the very next tick delivers nothing, not
// just the next start — and (b) never let GET /api/telemetry claim reporting
// is off while the client is enabled and beaconing (the old nil-controller
// fallback did exactly that). Delete runServe's `Telemetry:` wiring or break
// the adapter's delegation and this fails.
func TestServeTelemetryOptOutStopsBeacons(t *testing.T) {
	c, bodies := telemetrytest.CollectingClient(t)
	defer tel.SetClientForTest(c)()
	// HOME→temp (inside): the opt-out handler persists the machine-wide
	// choice and purges the default spool under the config dir.
	clearConsoleEnv(t)

	serversPath := filepath.Join(t.TempDir(), "servers.yaml")
	reg, err := console.LoadRegistry(serversPath)
	if err != nil {
		t.Fatalf("load empty registry: %v", err)
	}
	if _, err := reg.Add(console.ServerEntry{
		Name: "consent-test",
		DSN:  "root:x@tcp(127.0.0.1:9)/bintrail_index", // lazy — never dialed
	}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	// Reserve a port so the test can talk to the daemon runServe starts
	// (":0" would leave the bound port unknowable from out here).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	listen := l.Addr().String()
	l.Close()
	baseURL := "http://" + listen

	origIndex, origListen, origToken := conIndexDSN, conListen, conToken
	origServers, origProfile := conServersFile, conProfile
	origBaselineDir, origBaselineS3 := conBaselineDir, conBaselineS3
	defer func() {
		conIndexDSN, conListen, conToken = origIndex, origListen, origToken
		conServersFile, conProfile = origServers, origProfile
		conBaselineDir, conBaselineS3 = origBaselineDir, origBaselineS3
	}()
	conIndexDSN = ""
	conListen = listen
	conToken = "consent-test-token"
	conServersFile = serversPath
	conProfile, conBaselineDir, conBaselineS3 = "", "", ""

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runServe(serveCmd, nil) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("runServe did not return after cancel")
		}
	}()

	do := func(method, body string) (*http.Response, error) {
		var buf *bytes.Buffer
		if body == "" {
			buf = bytes.NewBuffer(nil)
		} else {
			buf = bytes.NewBufferString(body)
		}
		req, err := http.NewRequest(method, baseURL+"/api/telemetry", buf)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer consent-test-token")
		return http.DefaultClient.Do(req)
	}

	type stateDTO struct {
		Reporting bool `json:"reporting"`
		Consent   bool `json:"consent"`
	}
	getState := func() stateDTO {
		t.Helper()
		resp, err := do(http.MethodGet, "")
		if err != nil {
			t.Fatalf("GET /api/telemetry: %v", err)
		}
		defer resp.Body.Close()
		var st stateDTO
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			t.Fatalf("decode state: %v", err)
		}
		return st
	}

	// Wait for the daemon to serve.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if resp, err := do(http.MethodGet, ""); err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("serve never started answering /api/telemetry")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// While the live client is enabled, the state endpoint must say so — the
	// nil-controller fallback used to report reporting:false while the loop
	// delivered beacons.
	if st := getState(); !st.Reporting {
		t.Fatalf("state claims reporting:false while the live client is enabled and beaconing: %+v", st)
	}

	// Prove the loop is live, then opt out through the console.
	telemetrytest.WaitForBeacon(t, bodies, "serve")

	resp, err := do(http.MethodPost, `{"enabled":false}`)
	if err != nil {
		t.Fatalf("POST /api/telemetry: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/telemetry: status %d, want 200", resp.StatusCode)
	}
	if c.Enabled() {
		t.Fatal("opt-out did not reach the live client — serve would keep beaconing after the operator said no")
	}

	// The next ticks must deliver nothing new. Contention can only mean FEWER
	// ticks fire, so this cannot false-fail.
	delivered := len(bodies())
	time.Sleep(300 * time.Millisecond) // ~12 shortened ticks
	if n := len(bodies()); n != delivered {
		t.Fatalf("beacons kept flowing after opt-out: %d deliveries grew to %d", delivered, n)
	}

	if st := getState(); st.Reporting || st.Consent {
		t.Fatalf("state does not reflect the opt-out: %+v", st)
	}
}
