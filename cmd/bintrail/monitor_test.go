package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/console"
)

// ─── derived monitor states (#402) ───────────────────────────────────────────

func TestMonitorJobSnapshot_stalledDerivation(t *testing.T) {
	job := &monitorJob{}
	job.set("running", "")

	// Fresh progress: plain running.
	job.progress()
	if st := job.snapshot(); st.State != "running" {
		t.Fatalf("state = %q, want running", st.State)
	}

	// Progress older than the threshold: derived stalled, stored state intact.
	job.mu.Lock()
	job.lastProgress = time.Now().UTC().Add(-monitorStalledAfter - time.Minute)
	job.mu.Unlock()
	st := job.snapshot()
	if st.State != "stalled" {
		t.Fatalf("state = %q, want stalled", st.State)
	}
	if !strings.Contains(st.LastError, "no progress") {
		t.Errorf("LastError = %q, want a no-progress explanation", st.LastError)
	}
	job.mu.Lock()
	if job.state != "running" {
		t.Errorf("stored state mutated to %q — derivation must be read-only", job.state)
	}
	job.mu.Unlock()
}

func TestMonitorJobSnapshot_lostPosition(t *testing.T) {
	job := &monitorJob{}
	job.set("running", "")
	job.progress()
	job.markLostPosition("binlog gap: events between A and B were purged")

	st := job.snapshot()
	if st.State != "lost_position" {
		t.Fatalf("state = %q, want lost_position", st.State)
	}
	if st.LastError != "binlog gap: events between A and B were purged" {
		t.Errorf("LastError = %q, want the gap detail", st.LastError)
	}

	// A wedged stream beats a historical data-loss note.
	job.mu.Lock()
	job.lastProgress = time.Now().UTC().Add(-monitorStalledAfter - time.Minute)
	job.mu.Unlock()
	if st := job.snapshot(); st.State != "stalled" {
		t.Errorf("state = %q, want stalled to take precedence over lost_position", st.State)
	}
}

func TestMonitorJobSnapshot_noDerivationOutsideRunning(t *testing.T) {
	for _, base := range []string{"pending", "failed", "stopped"} {
		job := &monitorJob{}
		job.set(base, "")
		job.markLostPosition("gap detail")
		job.mu.Lock()
		job.lastProgress = time.Now().UTC().Add(-time.Hour)
		job.mu.Unlock()
		if st := job.snapshot(); st.State != base {
			t.Errorf("state = %q, want %q (no derivation outside running)", st.State, base)
		}
	}

	// Running with zero lastProgress (not reachable via the normal flow, but
	// must not divide-by-zero into stalled).
	job := &monitorJob{}
	job.set("running", "")
	if st := job.snapshot(); st.State != "running" {
		t.Errorf("state = %q, want running when lastProgress is unset", st.State)
	}
}

func TestMonitorJobHooks_pendingFlipsToRunning(t *testing.T) {
	job := &monitorJob{}
	job.set("pending", "")
	hooks := job.streamHooks()

	if st := job.snapshot(); st.State != "pending" {
		t.Fatalf("state = %q, want pending before first checkpoint", st.State)
	}

	hooks.OnCheckpoint()
	if st := job.snapshot(); st.State != "running" {
		t.Fatalf("state = %q, want running after first checkpoint", st.State)
	}

	// OnIndexed is the equivalent attach signal.
	job2 := &monitorJob{}
	job2.set("pending", "")
	job2.streamHooks().OnIndexed(42)
	if st := job2.snapshot(); st.State != "running" {
		t.Fatalf("state = %q, want running after first indexed batch", st.State)
	}

	// OnGapAutoAdvance alone must NOT flip pending (it fires during startup,
	// before the stream is attached).
	job3 := &monitorJob{}
	job3.set("pending", "")
	job3.streamHooks().OnGapAutoAdvance("gap")
	if st := job3.snapshot(); st.State != "pending" {
		t.Fatalf("state = %q, want pending after gap auto-advance only", st.State)
	}
}

// ─── circuit breaker (#402) ──────────────────────────────────────────────────

func TestMonitorRun_circuitBreakerGivesUp(t *testing.T) {
	old := monitorGiveUpAfter
	monitorGiveUpAfter = 0 // any continuous crash-looping trips it immediately
	defer func() { monitorGiveUpAfter = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := &monitorSupervisor{
		baseCtx: ctx,
		jobs:    map[string]*monitorJob{},
		streamFn: func(context.Context, streamConfig) error {
			return errors.New("boom: cannot connect")
		},
	}
	job := &monitorJob{cancel: cancel, done: make(chan struct{})}
	job.set("pending", "")
	m.jobs["e1"] = job

	m.wg.Add(1)
	m.run(ctx, job, console.ServerEntry{ID: "e1", Name: "prod"}, streamConfig{})

	select {
	case <-job.done:
	default:
		t.Fatal("run returned but job.done is not closed")
	}
	st := job.snapshot()
	if st.State != "failed" {
		t.Fatalf("state = %q, want permanent failed", st.State)
	}
	if !strings.Contains(st.LastError, "gave up") {
		t.Errorf("LastError = %q, want a gave-up explanation", st.LastError)
	}
	if !strings.Contains(st.LastError, "boom") {
		t.Errorf("LastError = %q, want the underlying error preserved", st.LastError)
	}
}

func TestMonitorRun_cleanStopBypassesBreaker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already-cancelled daemon: streamFn returns, run must report stopped

	m := &monitorSupervisor{
		baseCtx:  ctx,
		jobs:     map[string]*monitorJob{},
		streamFn: func(c context.Context, _ streamConfig) error { return c.Err() },
	}
	job := &monitorJob{cancel: func() {}, done: make(chan struct{})}
	job.set("pending", "")

	m.wg.Add(1)
	m.run(ctx, job, console.ServerEntry{ID: "e2", Name: "x"}, streamConfig{})

	if st := job.snapshot(); st.State != "stopped" {
		t.Fatalf("state = %q, want stopped on cancellation", st.State)
	}
}

// ─── replica / duplicate detection (#402) ────────────────────────────────────

func TestGTIDSetContainsUUID(t *testing.T) {
	const (
		uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
		uuidB = "9f3829a2-3c4d-11ee-be56-0242ac120002"
	)
	set := uuidA + ":1-5,\n" + uuidB + ":1-30:40-44"

	tests := []struct {
		name string
		set  string
		uuid string
		want bool
	}{
		{"present", set, uuidA, true},
		{"present second entry", set, uuidB, true},
		{"case-insensitive (go-mysql lowercases)", set, strings.ToUpper(uuidA), true},
		{"absent", set, "00000000-0000-0000-0000-000000000000", false},
		{"empty set", "", uuidA, false},
		{"empty uuid", set, "", false},
		{"malformed set is never a match", "not-a-gtid-set", uuidA, false},
	}
	for _, tt := range tests {
		if got := gtidSetContainsUUID(tt.set, tt.uuid); got != tt.want {
			t.Errorf("%s: gtidSetContainsUUID(%q, %q) = %v, want %v", tt.name, tt.set, tt.uuid, got, tt.want)
		}
	}
}

func TestClassifyReplicaOverlap(t *testing.T) {
	const (
		primary = "3e11fa47-71ca-11e1-9e33-c80aa9429562" // monitored peer
		replica = "9f3829a2-3c4d-11ee-be56-0242ac120002" // candidate
		other   = "11111111-2222-3333-4444-555555555555"
	)

	// Candidate is a replica of the monitored peer: its executed set carries
	// transactions originated at the peer.
	rel := classifyReplicaOverlap(replica, primary+":1-100,"+replica+":1-5", primary, primary+":1-100")
	if !strings.Contains(rel, "replica of") {
		t.Errorf("replica direction: got %q", rel)
	}

	// Candidate is the primary of a monitored replica: the peer's
	// accumulated set carries the candidate's transactions.
	rel = classifyReplicaOverlap(primary, primary+":1-100", replica, primary+":1-90,"+replica+":1-5")
	if !strings.Contains(rel, "primary of") {
		t.Errorf("primary direction: got %q", rel)
	}

	// Same server added twice (case differs — go-mysql lowercases UUIDs).
	rel = classifyReplicaOverlap(strings.ToUpper(primary), primary+":1-10", primary, primary+":1-10")
	if !strings.Contains(rel, "same server") {
		t.Errorf("same-server: got %q", rel)
	}

	// Unrelated servers: no finding.
	if rel = classifyReplicaOverlap(other, other+":1-3", primary, primary+":1-100"); rel != "" {
		t.Errorf("unrelated: got %q, want empty", rel)
	}

	// Peer with no recorded executed set (position mode / never streamed):
	// only the replica direction can fire.
	if rel = classifyReplicaOverlap(other, other+":1-3", primary, ""); rel != "" {
		t.Errorf("no peer set, unrelated: got %q, want empty", rel)
	}
}
