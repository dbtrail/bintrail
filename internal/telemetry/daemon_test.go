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
	prev := daemonTick
	daemonTick = d
	t.Cleanup(func() { daemonTick = prev })
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
func TestRunDaemonDoesNotBeaconBeforeFirstTick(t *testing.T) {
	clearEnv(t)
	withDaemonTick(t, 400*time.Millisecond)

	// Count what arrives rather than what is on disk: each tick beacons AND
	// drains, so the spool is empty again moments later either way.
	var mu sync.Mutex
	var delivered int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r)
		mu.Lock()
		delivered += strings.Count(strings.TrimSpace(string(body)), "\n") + 1
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	count := func() int { mu.Lock(); defer mu.Unlock(); return delivered }

	dir := t.TempDir()
	c := initDrained(t, Config{
		Dir: dir, Endpoint: srv.URL,
		Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { defer close(done); c.RunDaemon(ctx, "stream") }()
	// Registered after withDaemonTick so it runs BEFORE the tick is restored
	// (cleanups are LIFO): the loop must be finished before anything writes
	// daemonTick, including on the assertion-failure path.
	t.Cleanup(func() { cancel(); <-done })

	// Well inside the first tick: a daemon that dies here (a crash loop) must
	// have produced nothing at all — neither spooled nor sent.
	time.Sleep(80 * time.Millisecond)
	if n := countSpooledEvents(t, SpoolDir(dir)); n != 0 {
		t.Fatalf("spooled %d events before the first tick", n)
	}
	if n := count(); n != 0 {
		t.Fatalf("delivered %d events before the first tick", n)
	}

	// Past the first tick, exactly one beacon.
	time.Sleep(600 * time.Millisecond)
	if n := count(); n != 1 {
		t.Errorf("after one tick: %d events delivered, want 1", n)
	}

	// Later ticks the same UTC day add nothing — the per-day cap holds inside
	// the loop, so a daemon cannot beat out a fine-grained uptime trace.
	time.Sleep(900 * time.Millisecond)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); c.RunDaemon(ctx, "stream") }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

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
