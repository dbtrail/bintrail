package notify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached within deadline")
}

func TestWebhook_DeliversPayload(t *testing.T) {
	var got atomic.Pointer[Event]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}
		var ev Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			t.Errorf("decode: %v", err)
		}
		got.Store(&ev)
	}))
	defer srv.Close()

	w := NewWebhook(context.Background(), srv.URL)
	w.Notify(Event{Event: "verify_problem", Severity: "critical", Server: "wp", Summary: "2 mismatch"})
	waitFor(t, func() bool { return got.Load() != nil })
	ev := got.Load()
	if ev.Event != "verify_problem" || ev.Severity != "critical" || ev.Server != "wp" {
		t.Fatalf("payload mangled: %+v", ev)
	}
	if ev.Timestamp == "" {
		t.Fatal("empty Timestamp was not stamped at Notify time")
	}
	if _, err := time.Parse(time.RFC3339, ev.Timestamp); err != nil {
		t.Fatalf("timestamp %q is not RFC3339: %v", ev.Timestamp, err)
	}
}

func TestWebhook_RetriesUntilSuccess(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))
	defer srv.Close()

	w := NewWebhook(context.Background(), srv.URL)
	w.backoff = time.Millisecond // keep the test fast; production default is 1s
	w.Notify(Event{Event: "rotation_unhealthy"})
	waitFor(t, func() bool { return attempts.Load() >= 3 })
}

// TestWebhook_NotifyNeverBlocks: with the worker wedged on a hanging
// endpoint, Notify must keep returning immediately (drop, not block) — the
// callers sit on the daemon's capture-adjacent loops.
func TestWebhook_NotifyNeverBlocks(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	w := NewWebhook(context.Background(), srv.URL)
	done := make(chan struct{})
	go func() {
		for range queueSize + 10 {
			w.Notify(Event{Event: "continuity_gap_lost"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Notify blocked on a full queue")
	}
}

func TestEdge_FireRepeatResolve(t *testing.T) {
	now := time.Unix(1000, 0)
	e := NewEdge(time.Hour)
	e.now = func() time.Time { return now }

	if !e.Fire("k", "") {
		t.Fatal("first Fire must notify (transition into the bad state)")
	}
	if e.Fire("k", "") {
		t.Fatal("second Fire inside the repeat window must stay quiet")
	}
	now = now.Add(2 * time.Hour)
	if !e.Fire("k", "") {
		t.Fatal("Fire past repeatEvery must re-notify a persistent condition")
	}
	if !e.Resolve("k") {
		t.Fatal("Resolve of an active key must notify recovery once")
	}
	if e.Resolve("k") {
		t.Fatal("second Resolve must stay quiet")
	}
	if e.Resolve("never-fired") {
		t.Fatal("Resolve of a never-fired key must stay quiet")
	}
	if !e.Fire("k", "") {
		t.Fatal("Fire after Resolve is a new transition and must notify")
	}
}

// TestEdge_ChangedDetailBypassesWindow: a different detail under the same key
// is a NEW condition — it fires inside the repeat window AND refreshes it.
func TestEdge_ChangedDetailBypassesWindow(t *testing.T) {
	now := time.Unix(1000, 0)
	e := NewEdge(time.Hour)
	e.now = func() time.Time { return now }

	if !e.Fire("gap", "binlog file 42") {
		t.Fatal("setup fire")
	}
	if e.Fire("gap", "binlog file 42") {
		t.Fatal("same detail inside the window must stay quiet")
	}
	now = now.Add(30 * time.Minute)
	if !e.Fire("gap", "binlog file 99") {
		t.Fatal("a changed detail is a new condition and must fire inside the window")
	}
	// The changed-detail fire refreshed the window: 40 minutes after the
	// ORIGINAL fire (10 after the refresh) stays quiet.
	now = now.Add(40 * time.Minute)
	if e.Fire("gap", "binlog file 99") {
		t.Fatal("the changed-detail fire must refresh the repeat window")
	}
	// An empty detail never reads as a change (conditions without detail).
	if e.Fire("gap", "") {
		t.Fatal("empty detail must not read as a change")
	}
}
