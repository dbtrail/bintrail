package consoleapp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// TestTableFilter covers the nil-vs-empty semantics: no filter means "verify
// everything" (nil map, never restricts), a non-empty filter tracks what it
// has and hasn't matched via the mutable seen copy.
func TestTableFilter(t *testing.T) {
	if filter, seen := tableFilter(nil); filter != nil || seen != nil {
		t.Fatalf("no filter: got filter=%v seen=%v, want both nil", filter, seen)
	}
	filter, seen := tableFilter([]string{"wp.posts", "wp.users"})
	if len(filter) != 2 || !filter["wp.posts"] || !filter["wp.users"] {
		t.Fatalf("filter = %v, want both entries set", filter)
	}
	delete(seen, "wp.posts")
	if len(seen) != 1 || !seen["wp.users"] {
		t.Fatalf("seen after delete = %v, want only wp.users left", seen)
	}
}

// TestToWireResult covers the field mapping from the engine's TableResult to
// the console DTO, including the explainable flag being caller-controlled
// (not derived from Status alone — a live-source mismatch must never claim
// explainable, since the engine has no explain support for that mode).
func TestToWireResult(t *testing.T) {
	res := verify.TableResult{
		Schema: "wp", Table: "posts", Status: verify.StatusMismatch, Detail: "row count differs",
		SourceRows: 10, ReconstructRows: 9, Anchor: "mysql-bin.000123:456",
	}
	got := toWireResult(res, true)
	want := console.VerifyTableResult{
		Schema: "wp", Table: "posts", Status: "mismatch",
		Reason: "row count differs", Detail: "row count differs",
		SourceRows: 10, ReconstructRows: 9, Anchor: "mysql-bin.000123:456", Explainable: true,
	}
	if got != want {
		t.Errorf("toWireResult = %+v, want %+v", got, want)
	}
	if got := toWireResult(res, false); got.Explainable {
		t.Error("explainable must be exactly what the caller passed, not re-derived from Status")
	}

	// An unrecognized engine status is normalized BEFORE it reaches the wire —
	// the same verify.NormalizeStatus decision the CLI's JSON report applies,
	// so the console can never serialize a status a consumer's switch would
	// fall through (#1127).
	bad := toWireResult(verify.TableResult{Schema: "wp", Table: "x", Status: "bogus", Detail: "who knows"}, false)
	if bad.Status != string(verify.StatusError) {
		t.Errorf("unknown status serialized as %q, want %q", bad.Status, verify.StatusError)
	}
	if !strings.Contains(bad.Reason, "unrecognized verify status") || !strings.Contains(bad.Reason, "who knows") {
		t.Errorf("unknown status reason lost the cause: %q", bad.Reason)
	}
	if bad.Detail != bad.Reason {
		t.Errorf("detail (legacy alias) = %q, must equal reason %q", bad.Detail, bad.Reason)
	}
}

// TestIndexDBName covers the tolerant DSN-parse-failure handling (mirrors
// internal/cli/verify.go: a bad DSN never crashes DB-name resolution, it's
// simply left empty).
func TestIndexDBName(t *testing.T) {
	if got := indexDBName("idx:pw@tcp(127.0.0.1:3306)/binlog_index"); got != "binlog_index" {
		t.Errorf("indexDBName = %q, want binlog_index", got)
	}
	if got := indexDBName("not a dsn"); got != "" {
		t.Errorf("indexDBName(invalid) = %q, want empty", got)
	}
}

// TestVerifySupervisor_Status_idleDefault: a server never triggered here
// reports idle, never a zero-value State.
func TestVerifySupervisor_Status_idleDefault(t *testing.T) {
	s := newVerifySupervisor(context.Background())
	if got := s.Status("never-triggered"); got.State != "idle" {
		t.Errorf("Status = %+v, want State=idle", got)
	}
}

// TestVerifySupervisor_Trigger_collision: a job already running for a server
// refuses a second Trigger with ErrVerifyRunning and leaves the running job
// untouched — exercises the exact single-flight guard the whole "one run at
// a time per server" invariant depends on.
func TestVerifySupervisor_Trigger_collision(t *testing.T) {
	s := newVerifySupervisor(context.Background())
	s.jobs["srv1"] = &verifyJob{status: console.VerifyStatus{State: "running", Since: "t0"}}

	err := s.Trigger(console.VerifyRequest{ServerID: "srv1"})
	if !errors.Is(err, console.ErrVerifyRunning) {
		t.Fatalf("Trigger while running: err = %v, want ErrVerifyRunning", err)
	}
	if got := s.Status("srv1"); got.Since != "t0" {
		t.Errorf("a rejected Trigger must not touch the in-flight job, got %+v", got)
	}
}

// TestVerifySupervisor_appendResult_accumulatesSummary: results grow in
// call order and each status bucket (including an unrecognized status
// falling through to Error — the tally goes through verify.Summary.Count,
// the same classification the CLI's JSON report uses) is tallied correctly —
// this is the exact bookkeeping "as they land" polling depends on.
func TestVerifySupervisor_appendResult_accumulatesSummary(t *testing.T) {
	s := newVerifySupervisor(context.Background())
	s.jobs["srv1"] = &verifyJob{status: console.VerifyStatus{State: "running"}}

	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "posts", Status: "match"})
	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "users", Status: "mismatch"})
	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "opts", Status: "inconclusive"})
	s.appendResult("srv1", console.VerifyTableResult{Schema: "wp", Table: "bad", Status: "something-unrecognized"})

	got := s.Status("srv1")
	if len(got.Results) != 4 || got.Results[0].Table != "posts" || got.Results[3].Table != "bad" {
		t.Fatalf("Results = %+v, want 4 entries in append order", got.Results)
	}
	want := console.VerifySummary{Match: 1, Mismatch: 1, Inconclusive: 1, Error: 1, Total: 4}
	if got.Summary != want {
		t.Errorf("Summary = %+v, want %+v (an unrecognized status must fall through to Error)", got.Summary, want)
	}

	// appendResult on an unknown server id (job cleared out from under it) is
	// a defensive no-op, never a panic.
	s.appendResult("unknown-server", console.VerifyTableResult{Schema: "a", Table: "b", Status: "match"})
}

// TestVerifySupervisor_setNote_doesNotChangeState: setNote must leave State
// as "running" — finish() is the sole terminal-state transition. Regression
// guard for the race where setNote setting State="succeeded" itself let a
// second concurrent Trigger in before the goroutine's own finish() ran,
// which could suppress that second run's real failure.
func TestVerifySupervisor_setNote_doesNotChangeState(t *testing.T) {
	s := newVerifySupervisor(context.Background())
	s.jobs["srv1"] = &verifyJob{status: console.VerifyStatus{State: "running"}}

	s.setNote("srv1", "only one baseline exists for this server yet — nothing to compare")
	if got := s.Status("srv1"); got.State != "running" || got.Note == "" {
		t.Fatalf("after setNote: %+v, want State still running with Note set", got)
	}
	// A concurrent Trigger must still see this job as in-flight.
	if err := s.Trigger(console.VerifyRequest{ServerID: "srv1"}); !errors.Is(err, console.ErrVerifyRunning) {
		t.Errorf("Trigger after setNote (before finish): err = %v, want ErrVerifyRunning", err)
	}

	s.finish("srv1", nil)
	got := s.Status("srv1")
	if got.State != "succeeded" || got.Note == "" || got.FinishedAt == "" {
		t.Errorf("after finish: %+v, want State=succeeded with Note preserved and FinishedAt set", got)
	}
}

// TestVerifySupervisor_Explain_preconditions: all three "nothing to explain"
// cases refuse with ErrExplainUnavailable and never dial a database — no job
// for the server, a live-source run (no explain support in the engine), and
// a baseline-anchored run where the requested table was never a mismatch.
func TestVerifySupervisor_Explain_preconditions(t *testing.T) {
	s := newVerifySupervisor(context.Background())

	if _, err := s.Explain("no-such-server", "wp", "posts"); !errors.Is(err, console.ErrExplainUnavailable) {
		t.Errorf("no job: err = %v, want ErrExplainUnavailable", err)
	}

	s.jobs["live"] = &verifyJob{status: console.VerifyStatus{State: "succeeded"}, mode: console.VerifyModeLiveSource}
	if _, err := s.Explain("live", "wp", "posts"); !errors.Is(err, console.ErrExplainUnavailable) {
		t.Errorf("live-source run: err = %v, want ErrExplainUnavailable (no explain support in that mode)", err)
	}

	s.jobs["baseline"] = &verifyJob{status: console.VerifyStatus{State: "succeeded"}, mode: console.VerifyModeBaselineAnchored}
	if _, err := s.Explain("baseline", "wp", "never_mismatched"); !errors.Is(err, console.ErrExplainUnavailable) {
		t.Errorf("table never cached as a mismatch: err = %v, want ErrExplainUnavailable", err)
	}
}
