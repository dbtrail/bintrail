package consoleapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── Refresh schema snapshot: the supervisor (#1296) ─────────────────────────
//
// The reload is the load-bearing half. A stream holds its resolver in memory
// and swaps it only on a DDL event, so a snapshot written underneath a running
// stream changes nothing: without the reload this feature is a button that
// reports success and fixes the problem it was pressed for exactly never.

func testSnapshotSupervisor(t *testing.T, snap func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error), reload func(context.Context, string) (bool, error)) *schemaSnapshotSupervisor {
	t.Helper()
	s := newSchemaSnapshotSupervisor(context.Background(), reload)
	s.snapshotFn = snap
	return s
}

func okSnapshot(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
	return metadata.SnapshotStats{SnapshotID: 7, TableCount: 12}, nil
}

func TestSchemaSnapshotSupervisor_reloadsTheStream(t *testing.T) {
	var reloaded []string
	s := testSnapshotSupervisor(t, okSnapshot, func(_ context.Context, id string) (bool, error) {
		reloaded = append(reloaded, id)
		return true, nil
	})
	st, err := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"}, 0, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(reloaded) != 1 || reloaded[0] != "srv-1" {
		t.Fatalf("the stream was not reloaded onto the new snapshot: %v", reloaded)
	}
	if !st.StreamReloaded {
		t.Error("stream_reloaded must be true once the stream restarted")
	}
	if st.State != "succeeded" || st.SnapshotID != 7 || st.Tables != 12 {
		t.Errorf("snapshot result not reported: %+v", st)
	}
}

// A failed reload is NOT a failed snapshot: the snapshot is durable, capture is
// simply still on the old one. Reporting it as a failure would hide that the
// snapshot worked; reporting it as a plain success would hide that nothing is
// fixed yet.
func TestSchemaSnapshotSupervisor_reloadFailureIsReportedNotSwallowed(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, func(context.Context, string) (bool, error) {
		return false, errors.New("stream did not stop within 15s")
	})
	st, err := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"}, 0, nil)
	if err != nil {
		t.Fatalf("a reload failure must not fail the job: %v", err)
	}
	if st.StreamReloaded {
		t.Error("stream_reloaded must stay false when the reload failed")
	}
	if !strings.Contains(st.ReloadError, "did not stop") {
		t.Errorf("reload_error = %q, want the reload's own error", st.ReloadError)
	}
	if st.State != "succeeded" {
		t.Errorf("state = %q; the snapshot itself succeeded", st.State)
	}
}

// With no reload hook the process does not supervise this stream. Saying so is
// the point: silence would read as "capture is on the new snapshot".
func TestSchemaSnapshotSupervisor_withoutReloadSaysCaptureIsUnchanged(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	st, _ := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"}, 0, nil)
	if st.StreamReloaded {
		t.Error("stream_reloaded must be false with no supervised stream")
	}
	if !strings.Contains(st.ReloadError, "nothing was restarted") {
		t.Errorf("reload_error = %q, want an actionable note", st.ReloadError)
	}
}

// The entry may be captured by ANOTHER process: the reload hook reports "no
// stream here" and that must never render as a restart, or the operator is told
// capture is on the new snapshot while it is still decoding against the old one
// — the silent no-op stream_reloaded exists to prevent.
func TestSchemaSnapshotSupervisor_noStreamHereIsNotAReload(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, func(context.Context, string) (bool, error) {
		return false, nil // supervised elsewhere (or not at all)
	})
	st, err := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"}, 0, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if st.StreamReloaded {
		t.Error("stream_reloaded must stay false when no stream was restarted here")
	}
	if !strings.Contains(st.ReloadError, "nothing was restarted") {
		t.Errorf("reload_error = %q, want the not-supervised note", st.ReloadError)
	}
}

// A failed snapshot must never reload the stream: restarting capture would be a
// side effect the operator did not ask for and gains nothing.
func TestSchemaSnapshotSupervisor_snapshotFailureSkipsTheReload(t *testing.T) {
	reloads := 0
	s := testSnapshotSupervisor(t,
		func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
			return metadata.SnapshotStats{}, errors.New("source unreachable")
		},
		func(context.Context, string) (bool, error) { reloads++; return true, nil })
	_, err := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"}, 0, nil)
	if err == nil {
		t.Fatal("a failed snapshot must be reported as an error")
	}
	if reloads != 0 {
		t.Error("a failed snapshot must not restart capture")
	}
}

// Tables validation excluded stay uncaptured after this run; the operator has
// to learn that from the result, not from the next degraded banner.
func TestSchemaSnapshotSupervisor_reportsExcludedTables(t *testing.T) {
	s := testSnapshotSupervisor(t, func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
		return metadata.SnapshotStats{SnapshotID: 8, TableCount: 3, ExcludedTables: []string{"shop.audit_raw"}}, nil
	}, func(context.Context, string) (bool, error) { return true, nil })
	st, _ := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"}, 0, nil)
	if len(st.ExcludedTables) != 1 || st.ExcludedTables[0] != "shop.audit_raw" {
		t.Errorf("excluded tables not reported: %+v", st)
	}
}

// One run at a time per server: two concurrent runs would race to restart the
// same stream.
func TestSchemaSnapshotSupervisor_singleFlightPerServer(t *testing.T) {
	release := make(chan struct{})
	s := testSnapshotSupervisor(t, func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
		<-release
		return metadata.SnapshotStats{}, nil
	}, func(context.Context, string) (bool, error) { return true, nil })
	if err := s.Trigger(console.SchemaSnapshotRequest{ServerID: "srv-1"}); err != nil {
		t.Fatalf("first trigger: %v", err)
	}
	if err := s.Trigger(console.SchemaSnapshotRequest{ServerID: "srv-1"}); !errors.Is(err, console.ErrSchemaSnapshotRunning) {
		t.Errorf("second trigger err = %v, want ErrSchemaSnapshotRunning", err)
	}
	// A different server is unaffected.
	if err := s.Trigger(console.SchemaSnapshotRequest{ServerID: "srv-2"}); err != nil {
		t.Errorf("another server must not be blocked: %v", err)
	}
	close(release)
}

func TestSchemaSnapshotSupervisor_statusIsIdleBeforeAnyRun(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	if got := s.Status("never-run"); got.State != "idle" {
		t.Errorf("state = %q, want idle", got.State)
	}
}

// ReloadSchema on a server this process does not supervise is a no-op success:
// there is no stream here to reload and the snapshot it was called for is
// still valid. (The supervised happy path needs a live index and is covered by
// the monitor integration tests.)
func TestMonitorReloadSchema_unsupervisedEntryIsNoOp(t *testing.T) {
	m := &monitorSupervisor{baseCtx: context.Background(), jobs: map[string]*monitorJob{}}
	reloaded, err := m.ReloadSchema(context.Background(), console.ServerEntry{ID: "not-here"})
	if err != nil {
		t.Errorf("unsupervised entry: %v, want nil", err)
	}
	if reloaded {
		t.Error("reloaded must be false when there was no stream here to reload")
	}
}

// A stream that does not drain within the timeout is NOT restarted, and the
// operator is told so. The job entry must be put back: ActiveJobs feeds the
// rotation provider, so an entry silently dropped from the map stops having its
// per-source index archived and pruned — with no warning anywhere.
func TestMonitorReloadSchema_undrainedStreamIsReportedAndKeepsTheJob(t *testing.T) {
	prev := monitorReloadDrainTimeout
	monitorReloadDrainTimeout = 20 * time.Millisecond
	t.Cleanup(func() { monitorReloadDrainTimeout = prev })

	cancelled := false
	job := &monitorJob{cancel: func() { cancelled = true }, done: make(chan struct{})} // never closed
	m := &monitorSupervisor{baseCtx: context.Background(), jobs: map[string]*monitorJob{"srv-1": job}}

	reloaded, err := m.ReloadSchema(context.Background(), console.ServerEntry{ID: "srv-1"})
	if reloaded {
		t.Error("a stream that never stopped was not reloaded")
	}
	if err == nil {
		t.Fatal("an undrained stream must be reported, not silently accepted")
	}
	if !strings.Contains(err.Error(), "NOT restarted") {
		t.Errorf("error = %q; it must say capture was not restarted", err)
	}
	if !cancelled {
		t.Error("the old stream must have been cancelled")
	}
	if got := m.jobs["srv-1"]; got != job {
		t.Error("the job must be put back — dropping it removes this source from rotation coverage with no warning")
	}
}

// A snapshot that never returns must not wedge the endpoint. metadata's
// snapshot taker holds no context and config.Connect's timeout covers only the
// TCP handshake, so a source blocked behind a metadata lock hangs the job — and
// a job stuck in "running" makes every later Trigger answer 409 for the life of
// the process, with a daemon restart the only way out.
func TestSchemaSnapshotSupervisor_timeoutFreesTheEndpoint(t *testing.T) {
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	reloads := make(chan string, 4)
	s := testSnapshotSupervisor(t, func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
		<-hang
		return metadata.SnapshotStats{}, nil
	}, func(_ context.Context, id string) (bool, error) { reloads <- id; return true, nil })
	// Shrink this supervisor's own bound. A package-level var would be read by
	// the job goroutines of every other test here — which outlive the test that
	// spawned them — so assigning to one is a data race, not a knob.
	s.timeout = 20 * time.Millisecond

	req := console.SchemaSnapshotRequest{ServerID: "srv-1"}
	if err := s.Trigger(req); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		st := s.Status("srv-1")
		if st.State == "failed" {
			if !strings.Contains(st.LastError, "did not answer") {
				t.Errorf("last_error = %q, want the timeout's own account", st.LastError)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never left running: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Retryable again — the 409 must not be permanent.
	if err := s.Trigger(req); err != nil {
		t.Errorf("a timed-out job must leave the endpoint usable, got %v", err)
	}
}

// The abandoned goroutine of a timed-out run must not restart the stream behind
// the retry's back, nor overwrite the newer job's status.
func TestSchemaSnapshotSupervisor_supersededRunNeitherReloadsNorPublishes(t *testing.T) {
	reloads := 0
	s := testSnapshotSupervisor(t, func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
		return metadata.SnapshotStats{SnapshotID: 1}, nil
	}, func(context.Context, string) (bool, error) { reloads++; return true, nil })

	// Generation 1 finishes late; a retry (generation 2) already owns the server.
	s.gens["srv-1"] = 2
	st, err := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"}, 1, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if reloads != 0 {
		t.Error("a superseded run must not restart the stream the newer run owns")
	}
	if st.StreamReloaded {
		t.Error("a superseded run must not claim a reload")
	}
	s.publish(console.SchemaSnapshotRequest{ServerID: "srv-1"}, 1, st, nil)
	if got := s.Status("srv-1"); got.State != "idle" {
		t.Errorf("a superseded run must not publish over the newer job: %+v", got)
	}
}
