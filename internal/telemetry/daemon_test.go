package telemetry

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// withDaemonTick shortens the daemon loop for the duration of a test.
func withDaemonTick(t *testing.T, d time.Duration) {
	t.Helper()
	t.Cleanup(ShortenDaemonTickForTest(d))
}

func TestRunDaemonStopsOnContextCancel(t *testing.T) {
	clearEnv(t)
	withDaemonTick(t, time.Hour) // must not be what ends the loop

	c := initDrained(t, Config{
		Dir: t.TempDir(), Endpoint: "http://127.0.0.1:1",
		Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { defer close(done); c.RunDaemon(ctx, "stream") }()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunDaemon did not return after ctx cancel — daemon shutdown would hang")
	}
}

// TestRunDaemonDoesNotBeaconBeforeFirstTick pins the crash-loop protection: the
// per-day cap is process-local, so a daemon that beaconed at startup would emit
// one per restart. Waiting a full tick means a crash loop never beacons.
// deliveryCounter is a receiving endpoint that tallies NDJSON lines.
//
// Counting what arrives beats inspecting the spool: each tick beacons AND
// drains, so the file is gone again moments later either way.
func deliveryCounter(t *testing.T) (url string, count func() int) {
	t.Helper()
	var mu sync.Mutex
	var delivered int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r)
		mu.Lock()
		delivered += strings.Count(strings.TrimSpace(string(body)), "\n") + 1
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() int { mu.Lock(); defer mu.Unlock(); return delivered }
}

// startDaemon runs the loop and stops it during cleanup.
//
// The cleanup is registered AFTER withDaemonTick's, and cleanups are LIFO, so
// the loop is always finished before the tick is restored — including on the
// assertion-failure path, where a Fatal would otherwise leave the goroutine
// reading daemonTick while the restore writes it.
func startDaemon(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.RunDaemon(ctx, "stream") }()
	t.Cleanup(func() { cancel(); <-done })
}

// TestRunDaemonDoesNotBeaconBeforeFirstTick pins the crash-loop protection: the
// per-day cap is process-local, so a daemon that beaconed at startup would emit
// one per restart. Waiting a full tick means a crash loop never beacons.
//
// The tick is deliberately far longer than the observation window. Contention
// can only make the loop LATE, never early, so a wide margin cannot produce a
// false failure in either direction.
func TestRunDaemonDoesNotBeaconBeforeFirstTick(t *testing.T) {
	clearEnv(t)
	withDaemonTick(t, 30*time.Second)

	url, count := deliveryCounter(t)
	dir := t.TempDir()
	c := initDrained(t, Config{
		Dir: dir, Endpoint: url, Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	startDaemon(t, c)

	// A daemon that dies here — a crash loop — must have produced nothing.
	time.Sleep(250 * time.Millisecond)
	if n := countSpooledEvents(t, SpoolDir(dir)); n != 0 {
		t.Errorf("spooled %d events before the first tick", n)
	}
	if n := count(); n != 0 {
		t.Errorf("delivered %d events before the first tick", n)
	}
}

// TestRunDaemonBeaconsOncePerDay covers the other half: after a tick a beacon
// is delivered, and further ticks the same UTC day add nothing — so a daemon
// cannot emit a fine-grained uptime trace.
func TestRunDaemonBeaconsOncePerDay(t *testing.T) {
	clearEnv(t)
	withDaemonTick(t, 50*time.Millisecond)

	url, count := deliveryCounter(t)
	c := initDrained(t, Config{
		Dir: t.TempDir(), Endpoint: url, Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	startDaemon(t, c)

	// Poll rather than sleep a fixed span: under load the tick, the POST and
	// the handler are all late, and a fixed budget for them is what makes a
	// test like this flake on a busy CI runner.
	deadline := time.Now().Add(10 * time.Second)
	for count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := count(); n != 1 {
		t.Fatalf("after the first tick: %d events delivered, want 1", n)
	}

	// Many more ticks' worth of time. Contention only means FEWER ticks fire,
	// so this can fail only if the per-day cap is genuinely broken.
	time.Sleep(500 * time.Millisecond)
	if n := count(); n != 1 {
		t.Errorf("repeated ticks delivered %d events, want 1 (the per-day cap)", n)
	}
}

// TestRunDaemonDelivers is the point of the whole loop: Init drains once at
// startup, so a process that lives for months needs its own drain or its
// beacons are never delivered and simply age out.
func TestRunDaemonDelivers(t *testing.T) {
	clearEnv(t)
	withDaemonTick(t, 150*time.Millisecond)

	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r)
		mu.Lock()
		got = append(got, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()
	c := initDrained(t, Config{
		Dir: dir, Endpoint: srv.URL,
		Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	startDaemon(t, c)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("the daemon loop delivered nothing; beacons would age out unsent")
	}
	if !strings.Contains(got[0], `"event_type":"daemon_beacon"`) {
		t.Errorf("delivered payload is not a beacon: %s", got[0])
	}
	if strings.Contains(got[0], `"run_id"`) {
		t.Errorf("beacon carried a run_id — a months-lived process's run_id is a longitudinal key: %s", got[0])
	}
}

func TestRunDaemonIsInertWhenDisabled(t *testing.T) {
	clearEnv(t)
	withDaemonTick(t, 10*time.Millisecond)

	dir := t.TempDir()
	// No endpoint compiled in: the client is inert, so the loop must return at
	// once rather than tick forever in the background of every daemon.
	c := Init(Config{Dir: dir, Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})

	done := make(chan struct{})
	go func() { defer close(done); c.RunDaemon(context.Background(), "stream") }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunDaemon on a disabled client did not return")
	}
	if entries, err := os.ReadDir(SpoolDir(dir)); err == nil && len(entries) > 0 {
		t.Errorf("disabled daemon wrote to the spool: %v", entries)
	}

	var nilClient *Client
	nilClient.RunDaemon(context.Background(), "stream") // must not panic
}
