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

func TestProcessActor(t *testing.T) {
	a := ProcessActor("")
	if !strings.HasPrefix(a, "os:") || a == "os:" {
		t.Errorf("ProcessActor() = %q, want os:<user>", a)
	}
	if got := ProcessActor("auditor"); !strings.HasSuffix(got, " profile:auditor") {
		t.Errorf("ProcessActor(auditor) = %q", got)
	}
}
