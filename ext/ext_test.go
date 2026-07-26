package ext

import (
	"context"
	"strings"
	"testing"
)

type captureSink struct {
	events []AuditEvent
}

func (c *captureSink) Record(_ context.Context, ev AuditEvent) {
	c.events = append(c.events, ev)
}

func TestRecordNoSinkIsNoop(t *testing.T) {
	SetAuditSink(nil)
	// Must not panic.
	Record(context.Background(), AuditEvent{Action: "query.run"})
}

func TestRecordStampsTimeAndForwards(t *testing.T) {
	c := &captureSink{}
	SetAuditSink(c)
	t.Cleanup(func() { SetAuditSink(nil) })

	Record(context.Background(), AuditEvent{
		Surface: "cli",
		Action:  "recover.generate",
		Schema:  "shop",
		Table:   "orders",
		Detail:  map[string]string{"statements": "12", "dry_run": "true"},
	})

	if len(c.events) != 1 {
		t.Fatalf("got %d events, want 1", len(c.events))
	}
	ev := c.events[0]
	if ev.Time.IsZero() {
		t.Error("Record did not stamp zero Time")
	}
	if ev.Action != "recover.generate" || ev.Detail["statements"] != "12" {
		t.Errorf("event mangled: %+v", ev)
	}
}

type panicSink struct{}

func (panicSink) Record(context.Context, AuditEvent) { panic("third-party sink exploded") }

// TestRecordRecoversSinkPanic pins the docstring's claim: a sink cannot fail
// a user's query. A panic in third-party sink code must be swallowed by
// Record, never unwound into a caller whose artifact was already produced.
func TestRecordRecoversSinkPanic(t *testing.T) {
	SetAuditSink(panicSink{})
	t.Cleanup(func() { SetAuditSink(nil) })

	// Must not panic.
	Record(context.Background(), AuditEvent{Surface: "cli", Action: "query.run"})
}

// TestAuditSinkSwapIsRaceSafe drives Record/Auditing concurrently with
// SetAuditSink swaps — the audittest.Install pattern with a surface served on
// another goroutine. Meaningful under -race (CI always runs it): the old
// unsynchronized package var failed here.
func TestAuditSinkSwapIsRaceSafe(t *testing.T) {
	t.Cleanup(func() { SetAuditSink(nil) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			Auditing()
			Record(context.Background(), AuditEvent{Surface: "cli", Action: "query.run"})
		}
	}()
	for range 1000 {
		SetAuditSink(&captureSink{})
		SetAuditSink(nil)
	}
	<-done
}

func TestProcessActor(t *testing.T) {
	a := ProcessActor("")
	if !strings.HasPrefix(a, "os:") || a == "os:" {
		t.Errorf("ProcessActor() = %q, want os:<user>", a)
	}
	if got := ProcessActor("auditor"); !strings.HasSuffix(got, " profile:auditor") {
		t.Errorf("ProcessActor(auditor) = %q", got)
	}
}
