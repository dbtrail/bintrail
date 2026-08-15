// Package telemetrytest provides shared helpers for the per-daemon telemetry
// WIRING tests (#1362) — the guards that drive a REAL daemon run function
// (runStream, runServe, runShim, …) and fail when that daemon's
// `go client.RunDaemon(ctx, name)` line is deleted.
//
// The telemetry package's own tests already pin the loop itself (RunDaemon
// beacons once per UTC day and drains what it spooled); what rots is the
// one-line launch inside each daemon. These helpers make that launch cheap to
// observe: an enabled client delivering to an in-process endpoint, a shortened
// daemon tick, and a poll that only succeeds once a real `daemon_beacon`
// carrying the daemon's command name has been delivered end to end.
package telemetrytest

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// ClearReportingEnv neutralizes every environment variable that could
// suppress consent — DO_NOT_TRACK, BINTRAIL_TELEMETRY, and the CI markers —
// so a wiring test behaves identically on a laptop and in CI. (These vars can
// only ever turn telemetry OFF, so clearing them enables nothing beyond the
// test client's own temp-dir consent default.)
func ClearReportingEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv(telemetry.EnvVar, "")
	for _, v := range telemetry.CIEnvVars() {
		t.Setenv(v, "")
	}
}

// CollectingClient returns an ENABLED telemetry client that spools to a temp
// dir and delivers to an in-process endpoint, plus an accessor for every
// NDJSON body delivered so far. It also shortens the daemon tick (restored on
// cleanup) so a wired daemon's first beacon arrives within milliseconds of
// RunDaemon starting instead of after an hour.
func CollectingClient(t *testing.T) (c *telemetry.Client, bodies func() []string) {
	t.Helper()
	ClearReportingEnv(t)
	t.Cleanup(telemetry.ShortenDaemonTickForTest(25 * time.Millisecond))

	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	interactive := false
	c = telemetry.Init(telemetry.Config{
		Dir:         t.TempDir(),
		Endpoint:    srv.URL,
		Stderr:      io.Discard,
		Interactive: &interactive,
	})
	if !c.Enabled() {
		t.Fatal("test telemetry client is not enabled — the wiring test could never observe a beacon")
	}
	c.WaitStartupDrain()
	return c, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
}

// WaitForBeacon polls until a delivered payload carries a `daemon_beacon`
// event for the given sanitized command name, failing the test after a
// generous deadline. It can only pass if the daemon's run path started
// telemetry.Client.RunDaemon with that name — delete the wiring line and this
// fails.
func WaitForBeacon(t *testing.T, bodies func() []string, command string) {
	t.Helper()
	wantType := `"event_type":"daemon_beacon"`
	wantCmd := `"command":"` + command + `"`
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, b := range bodies() {
			for line := range strings.SplitSeq(b, "\n") {
				if strings.Contains(line, wantType) && strings.Contains(line, wantCmd) {
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no daemon_beacon for command %q was delivered — the daemon does not start the telemetry loop (RunDaemon wiring deleted?); delivered payloads: %q",
		command, bodies())
}

// HangingTCPAddr returns the host:port of a listener that accepts connections
// and never writes a byte, plus a func that severs the listener and every
// accepted connection. Pointing a daemon's startup DSN at it holds the daemon
// alive in its first dial long enough for the (shortened) daemon tick to
// fire; severing lets the daemon fail out and return. Severing is also
// registered as a cleanup, so a failing test cannot leak a blocked daemon.
func HangingTCPAddr(t *testing.T) (addr string, sever func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for hanging TCP endpoint: %v", err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	var once sync.Once
	sever = func() {
		once.Do(func() {
			l.Close()
			mu.Lock()
			defer mu.Unlock()
			for _, c := range conns {
				c.Close()
			}
		})
	}
	t.Cleanup(sever)
	return l.Addr().String(), sever
}
