package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// waitForSnapshotJob polls a server's schema-snapshot status until it leaves
// "running".
//
// Polling rather than a channel because the goroutine under test dies by
// panic: it never reaches a clean signal. The read goes through the same mutex
// the guard writes under, so observing a terminal state also orders the test's
// later reads after the goroutine's last write.
func waitForSnapshotJob(t *testing.T, s *schemaSnapshotSupervisor, serverID string) console.SchemaSnapshotStatus {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		st := s.Status(serverID)
		if st.State != "running" {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job never left the running state: %+v; the goroutine vanished and Trigger will "+
				"refuse every later snapshot for this server until the daemon restarts", st)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestSchemaSnapshotGoroutines_survivePanicAndReportFailure drives each of the
// two schema-snapshot goroutines to a panic INSIDE the goroutine, not on the
// dispatch side of the `go` that starts it, and asserts the daemon survives,
// the failure is reported, and the endpoint is usable again.
//
// Without the guards each subtest kills the whole test binary, which is what
// the panic does to a `watch` daemon that is also capturing.
//
// The assertion is on the reported failure, not merely on the absence of a
// crash: a subtest cannot pass by the goroutine simply never having run, since
// that leaves the slot "running" with an empty LastError and waitForSnapshotJob
// fails.
func TestSchemaSnapshotGoroutines_survivePanicAndReportFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup makes the named goroutine panic and returns the substring the
		// reported error must carry.
		setup func(s *schemaSnapshotSupervisor) string
		// forbid must NOT appear in the reported error.
		forbid string
	}{
		{
			name: "snapshot goroutine",
			setup: func(s *schemaSnapshotSupervisor) string {
				const sentinel = "induced panic while taking the snapshot"
				s.snapshotFn = func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
					panic(sentinel)
				}
				return sentinel
			},
			// A panic that is only recovered, without being routed into the
			// channel run selects on, would leave run waiting out s.timeout
			// and then reporting the source as unresponsive: the daemon's own
			// internal error, blamed on a lock the source is not holding.
			forbid: "did not answer within",
		},
		{
			name: "run goroutine",
			setup: func(s *schemaSnapshotSupervisor) string {
				// An arbitrary panic site inside run's OWN frames: a select
				// evaluates every channel operand on entry, so a nil context
				// makes s.ctx.Done() panic there. The guard must not care
				// where the panic came from; this is simply the cheapest site
				// a unit test can reach without adding a seam for publish.
				s.ctx = nil
				return "internal error:"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testSnapshotSupervisor(t, okSnapshot, func(context.Context, string) (bool, error) {
				return true, nil
			})
			// Short, so a panic that is recovered but never reported fails
			// here in seconds instead of hanging for the production timeout.
			s.timeout = 2 * time.Second
			want := tc.setup(s)

			req := console.SchemaSnapshotRequest{ServerID: "srv-1", ServerName: "srv"}
			if err := s.Trigger(req); err != nil {
				t.Fatalf("trigger: %v", err)
			}
			st := waitForSnapshotJob(t, s, "srv-1")

			if st.State != "failed" {
				t.Errorf("state = %q, want failed: a job whose goroutine died must not report anything else", st.State)
			}
			if !strings.Contains(st.LastError, want) {
				t.Errorf("last_error = %q, want it to carry %q; the operator has to be able to see that the "+
					"job died rather than watch it never finish", st.LastError, want)
			}
			if tc.forbid != "" && strings.Contains(st.LastError, tc.forbid) {
				t.Errorf("last_error = %q; it blames the source for an internal error", st.LastError)
			}
			if st.FinishedAt == "" {
				t.Error("finished_at is empty; the run is reported as still open")
			}

			// Trigger refuses while this server's slot reads "running". A
			// guard that logged and left it there would refuse this server's
			// schema refresh until the daemon restarts.
			if err := s.Trigger(req); err != nil {
				t.Fatalf("a second snapshot was refused after the first one panicked (%v); the endpoint is "+
					"wedged for this server until the daemon restarts", err)
			}
			waitForSnapshotJob(t, s, "srv-1")
		})
	}
}

// guardedPanic runs the guard the run goroutine registers, over a body that
// panics, so the tests below exercise the exact wiring run uses.
func guardedPanic(s *schemaSnapshotSupervisor, req console.SchemaSnapshotRequest, gen uint64, body func()) {
	defer recoverSnapshotJob(req, func(err error) { s.failIfRunning(req, gen, err) })
	body()
}

// TestRecoverSnapshotJob_releasesTheLockAPanicWasHoldingIt: publish writes the
// job's outcome inside `s.mu.Lock(); defer s.mu.Unlock()`, so a panic can be
// raised while the supervisor mutex is held. Deferred functions run
// last-in-first-out, so that unlock fires before the guard and the guard can
// take the mutex it needs. Pinned here because registering the guard AFTER the
// lock instead would deadlock the supervisor for good, and a deadlock is a
// worse outage than the crash this replaces.
func TestRecoverSnapshotJob_releasesTheLockAPanicWasHoldingIt(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	s.jobs["a"] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens["a"] = 1

	guardedPanic(s, req, 1, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		panic("raised while holding the supervisor mutex")
	})

	// Hangs here instead of failing if the guard ever deadlocks.
	if got := s.Status("a"); got.State != "failed" {
		t.Fatalf("state = %q, want failed", got.State)
	}
}

// TestRecoverSnapshotJob_doesNotRewriteAFinishedRun: publish sets the terminal
// state and THEN logs, inside one locked region, so a panic in that tail
// arrives with the snapshot already taken and recorded in the index. Reporting
// that as a failure is a false statement about durable work, and it sends the
// operator to re-run something that worked.
func TestRecoverSnapshotJob_doesNotRewriteAFinishedRun(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	s.jobs["a"] = &console.SchemaSnapshotStatus{
		State: "succeeded", SnapshotID: 7, Tables: 12, StreamReloaded: true, FinishedAt: nowStamp(),
	}
	s.gens["a"] = 1

	guardedPanic(s, req, 1, func() { panic("raised after the run published") })

	got := s.Status("a")
	if got.State != "succeeded" || got.SnapshotID != 7 || got.Tables != 12 || !got.StreamReloaded {
		t.Fatalf("status = %+v; the guard overwrote a run that had already published its snapshot", got)
	}
	if got.LastError != "" {
		t.Fatalf("last_error = %q on a succeeded run", got.LastError)
	}
}

// TestRecoverSnapshotJob_dropsASupersededRun: a run that timed out and was
// retried no longer owns this server's slot. The newer run has reset it to
// "running", so a still-running check alone would let the stale guard fail the
// run that owns it now, and the operator would watch a snapshot they just
// started report an error from the previous one.
func TestRecoverSnapshotJob_dropsASupersededRun(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	s.jobs["a"] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens["a"] = 2 // a newer run owns the slot

	guardedPanic(s, req, 1, func() { panic("raised by a superseded run") })

	if got := s.Status("a"); got.State != "running" || got.LastError != "" {
		t.Fatalf("status = %+v; a superseded run failed the newer one that owns the slot", got)
	}
}

// TestRecoverSnapshotJob_passesThroughWithoutAPanic: the guard runs on the
// success path of both goroutines too, and must be inert there.
func TestRecoverSnapshotJob_passesThroughWithoutAPanic(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	s.jobs["a"] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens["a"] = 1

	reported := false
	func() {
		defer recoverSnapshotJob(req, func(error) { reported = true })
	}()

	if reported {
		t.Error("the guard reported a failure for a goroutine that never panicked")
	}
	if got := s.Status("a"); got.State != "running" {
		t.Fatalf("state = %q, want running: the guard touched a job that never panicked", got.State)
	}
}

// TestSchemaSnapshotPanic_namesTheCaptureConsequence: execute both takes the
// snapshot and restarts the capture stream, and the guard cannot tell which
// half raised the panic. Reporting only "internal error" would leave an
// operator believing capture is still running when the restart is exactly where
// it died, which is silent data loss. The one call site that can reach the
// restart has to say so.
func TestSchemaSnapshotPanic_namesTheCaptureConsequence(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, func(context.Context, string) (bool, error) {
		panic("induced panic while restarting capture")
	})
	s.timeout = 2 * time.Second

	req := console.SchemaSnapshotRequest{ServerID: "srv-1", ServerName: "srv"}
	if err := s.Trigger(req); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	st := waitForSnapshotJob(t, s, "srv-1")

	if st.State != "failed" {
		t.Fatalf("state = %q, want failed", st.State)
	}
	if !strings.Contains(st.LastError, "press Start if capture is not running") {
		t.Errorf("last_error = %q; a panic raised in the goroutine that restarts capture must say capture "+
			"may be stopped and name the move that settles it", st.LastError)
	}
}

// TestSchemaSnapshotPanic_scrubsTheDSN: a panic value can carry a driver error,
// and a driver error commonly embeds the whole connection string. LastError is
// served over HTTP.
func TestSchemaSnapshotPanic_scrubsTheDSN(t *testing.T) {
	const dsn = "root:hunter2@tcp(127.0.0.1:3306)/app"
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv", SourceDSN: dsn}
	s.jobs["a"] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens["a"] = 1

	guardedPanic(s, req, 1, func() { panic("dial " + dsn + ": refused") })

	got := s.Status("a")
	if strings.Contains(got.LastError, "hunter2") || strings.Contains(got.LastError, dsn) {
		t.Fatalf("last_error = %q; it carries the source DSN to an HTTP client", got.LastError)
	}
}
