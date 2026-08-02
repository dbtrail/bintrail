package consoleapp

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
)

type fakeSender struct{ events []notify.Event }

func (f *fakeSender) Notify(ev notify.Event) { f.events = append(f.events, ev) }

func testNotifier() (*watchNotifier, *fakeSender) {
	f := &fakeSender{}
	return &watchNotifier{send: f, edge: notify.NewEdge(0)}, f
}

func TestWatchNotifier_VerifyFinished(t *testing.T) {
	n, f := testNotifier()
	bad := console.VerifyRunRecord{
		ServerID: "s1", ServerName: "wp", Trigger: "scheduled",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Mode: console.VerifyModeBaselineAnchored,
			Summary: console.VerifySummary{Match: 3, Mismatch: 2, Total: 5}},
	}
	n.VerifyFinished(bad)
	if len(f.events) != 1 || f.events[0].Severity != "critical" || f.events[0].Event != "verify_problem" {
		t.Fatalf("mismatch run: want one critical verify_problem, got %+v", f.events)
	}
	if f.events[0].Details["mismatch"] != "2" || f.events[0].Server != "wp" {
		t.Fatalf("event details incomplete: %+v", f.events[0])
	}

	// Same condition again inside the repeat window: edge-suppressed.
	n.VerifyFinished(bad)
	if len(f.events) != 1 {
		t.Fatalf("repeat inside window must be suppressed, got %+v", f.events)
	}

	// Clean run: exactly one recovery event, then silence while it stays clean.
	clean := console.VerifyRunRecord{
		ServerID: "s1", ServerName: "wp",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Match: 5, Total: 5}},
	}
	n.VerifyFinished(clean)
	if len(f.events) != 2 || !f.events[1].Resolved || f.events[1].Severity != "info" {
		t.Fatalf("clean run after a problem must send one resolved event, got %+v", f.events)
	}
	n.VerifyFinished(clean)
	if len(f.events) != 2 {
		t.Fatalf("clean run with no prior problem must stay silent, got %+v", f.events)
	}

	// Error-only (no mismatch) is warning; a failed run reports its error.
	n2, f2 := testNotifier()
	n2.VerifyFinished(console.VerifyRunRecord{ServerID: "s2",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Error: 1, Total: 1}}})
	if len(f2.events) != 1 || f2.events[0].Severity != "warning" {
		t.Fatalf("error-only run: want warning, got %+v", f2.events)
	}
	n3, f3 := testNotifier()
	n3.VerifyFinished(console.VerifyRunRecord{ServerID: "s3",
		VerifyStatus: console.VerifyStatus{State: "failed", LastError: "connect index: boom"}})
	if len(f3.events) != 1 || f3.events[0].Severity != "warning" || f3.events[0].Summary != "verification run failed: connect index: boom" {
		t.Fatalf("failed run: %+v", f3.events)
	}

	// Skip records are bookkeeping, never notifications.
	n4, f4 := testNotifier()
	n4.VerifyFinished(console.VerifyRunRecord{ServerID: "s4", VerifyStatus: console.VerifyStatus{State: "skipped"}})
	if len(f4.events) != 0 {
		t.Fatalf("skip record must not notify: %+v", f4.events)
	}
}

func TestWatchNotifier_RotationCycle(t *testing.T) {
	n, f := testNotifier()
	n.RotationCycle(true, 0)
	n.RotationCycle(false, 3) // still unhealthy (deferring) — suppressed, same edge key
	if len(f.events) != 1 || f.events[0].Event != "rotation_unhealthy" || f.events[0].Severity != "warning" {
		t.Fatalf("want one rotation_unhealthy warning, got %+v", f.events)
	}
	n.RotationCycle(false, 0)
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("healthy cycle after unhealthy must resolve once, got %+v", f.events)
	}
	n.RotationCycle(false, 0)
	if len(f.events) != 2 {
		t.Fatalf("healthy with no prior problem must stay silent, got %+v", f.events)
	}
}

func TestWatchNotifier_Continuity(t *testing.T) {
	n, f := testNotifier()
	n.Continuity("wp", true, "binlog gap at file 42")
	n.Continuity("wp", true, "binlog gap at file 42")
	if len(f.events) != 1 || f.events[0].Severity != "critical" || f.events[0].Details["detail"] != "binlog gap at file 42" {
		t.Fatalf("want one critical continuity event, got %+v", f.events)
	}
	// A different server is its own edge key.
	n.Continuity("other", true, "")
	if len(f.events) != 2 {
		t.Fatalf("second server must fire independently, got %+v", f.events)
	}
	n.Continuity("wp", false, "")
	if len(f.events) != 3 || !f.events[2].Resolved {
		t.Fatalf("recovery must resolve once, got %+v", f.events)
	}
}

func TestContinuityTargets(t *testing.T) {
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "a", DSN: "dsn-a"},
		console.ServerEntry{Name: "same-as-boot", DSN: "dsn-boot"},
	)
	got := continuityTargets(reg, "dsn-boot")
	if len(got) != 2 || got[0].dsn != "dsn-boot" || got[1].dsn != "dsn-a" {
		t.Fatalf("want boot + a (registry entry sharing the boot DSN deduped), got %+v", got)
	}
	if got := continuityTargets(reg, ""); len(got) != 2 {
		t.Fatalf("source-less watch: want the 2 registry entries, got %+v", got)
	}
	if got := continuityTargets(nil, ""); got != nil {
		t.Fatalf("no registry, no boot: want nil, got %+v", got)
	}
}

func testRegistryWithEntries(t *testing.T, entries ...console.ServerEntry) *console.Registry {
	t.Helper()
	reg, err := console.LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, err := reg.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func TestRotationNotifyHooks(t *testing.T) {
	if hooks := rotationNotifyHooks(nil); hooks != nil {
		t.Fatalf("nil notifier must yield no hooks, got %d", len(hooks))
	}
	n, f := testNotifier()
	hooks := rotationNotifyHooks(n)
	if len(hooks) != 1 {
		t.Fatalf("want 1 hook, got %d", len(hooks))
	}
	hooks[0](true, 0)
	if len(f.events) != 1 {
		t.Fatalf("hook is not wired to the notifier: %+v", f.events)
	}
}
