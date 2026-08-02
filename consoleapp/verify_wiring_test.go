package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// TestRunScheduledVerifyCycle_endToEnd drives one scheduled cycle through the
// real registry→request→RunScheduled→run→finish→history chain (no ticker, no
// Docker: the DSN points at loopback port 1, so the run fails fast at
// "connect index" and the failure is what proves the wiring). Deleting the
// cycle body, swapping RunScheduled for Trigger's admission-only path, or
// dropping the skip recording would each fail this test.
func TestRunScheduledVerifyCycle_endToEnd(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "root:x@tcp(127.0.0.1:1)/nope"})
	id := reg.List()[0].ID
	hist, err := console.OpenVerifyHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	sup := newVerifySupervisor(context.Background(), hist, nil)

	runScheduledVerifyCycle(context.Background(), sup, reg, hist, nil)
	recs := hist.List(id)
	if len(recs) != 1 || recs[0].State != "failed" || recs[0].Trigger != console.VerifyTriggerScheduled {
		t.Fatalf("scheduled cycle did not drive the supervisor into history: %+v", recs)
	}
	if !strings.Contains(recs[0].LastError, "connect index") {
		t.Fatalf("unreachable index must fail the run at connect: %q", recs[0].LastError)
	}

	// A server with a run already in flight is recorded as a skip, not lost.
	if _, err := sup.begin(console.VerifyRequest{ServerID: id}, console.VerifyTriggerManual); err != nil {
		t.Fatal(err)
	}
	runScheduledVerifyCycle(context.Background(), sup, reg, hist, nil)
	recs = hist.List(id)
	if len(recs) != 2 || recs[0].State != console.VerifyStateSkipped || recs[0].SkipReason == "" {
		t.Fatalf("in-flight run must record a skip: %+v", recs)
	}

	// A cancelled daemon context stops the cycle before it touches a server.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	runScheduledVerifyCycle(cancelled, sup, reg, hist, nil)
	if got := hist.List(id); len(got) != 2 {
		t.Fatalf("cancelled ctx must not run or record anything: %+v", got)
	}
}

// TestWireVerify_wiring pins the enablement matrix and the notifier hook —
// each of these is one deletable line that would otherwise disconnect a
// shipped feature with the suite staying green.
func TestWireVerify_wiring(t *testing.T) {
	saveI, saveT := upVerifyInterval, upConsoleVerifyTrigger
	t.Cleanup(func() { upVerifyInterval, upConsoleVerifyTrigger = saveI, saveT })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serversPath := filepath.Join(t.TempDir(), "console-servers.yaml")
	reg, err := console.LoadRegistry(serversPath)
	if err != nil {
		t.Fatal(err)
	}

	// Fully off: nothing wired.
	upVerifyInterval, upConsoleVerifyTrigger = "", false
	cfg := console.Config{}
	if err := wireVerify(ctx, &cfg, reg, serversPath, nil); err != nil || cfg.VerifyCtrl != nil {
		t.Fatalf("off: ctrl=%v err=%v", cfg.VerifyCtrl, err)
	}

	// Trigger env alone: supervisor + history, no notification hook.
	upConsoleVerifyTrigger = true
	cfg = console.Config{}
	if err := wireVerify(ctx, &cfg, reg, serversPath, nil); err != nil {
		t.Fatal(err)
	}
	sup, ok := cfg.VerifyCtrl.(*verifySupervisor)
	if !ok || cfg.VerifyHistory == nil {
		t.Fatalf("trigger env must wire supervisor + history: %+v", cfg)
	}
	// onFinish is always wired since #1203 (health gauges observe every run,
	// notifier or not).
	if sup.onFinish == nil {
		t.Fatal("onFinish must always be wired (gauges)")
	}

	// A schedule alone implies the supervisor (no separate VERIFY_TRIGGER).
	upConsoleVerifyTrigger, upVerifyInterval = false, "24h"
	cfg = console.Config{}
	if err := wireVerify(ctx, &cfg, reg, serversPath, nil); err != nil || cfg.VerifyCtrl == nil {
		t.Fatalf("schedule alone must enable verify: ctrl=%v err=%v", cfg.VerifyCtrl, err)
	}

	// The notifier hook reaches the supervisor.
	n, _ := testNotifier()
	upConsoleVerifyTrigger, upVerifyInterval = true, ""
	cfg = console.Config{}
	if err := wireVerify(ctx, &cfg, reg, serversPath, n); err != nil {
		t.Fatal(err)
	}
	if cfg.VerifyCtrl.(*verifySupervisor).onFinish == nil {
		t.Fatal("notifier set but onFinish not wired — verify notifications silently dead")
	}

	// A bad interval refuses startup.
	upVerifyInterval = "bogus"
	if err := wireVerify(ctx, &console.Config{}, reg, serversPath, nil); err == nil {
		t.Fatal("bad --verify-interval must refuse startup")
	}

	// An unreadable history file degrades: daemon starts, supervisor wired,
	// no history store (the API then serves its distinct 403).
	upVerifyInterval, upConsoleVerifyTrigger = "", true
	if err := os.WriteFile(console.DefaultVerifyHistoryPath(serversPath), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg = console.Config{}
	if err := wireVerify(ctx, &cfg, reg, serversPath, nil); err != nil {
		t.Fatalf("corrupt history must degrade, not refuse startup: %v", err)
	}
	if cfg.VerifyCtrl == nil || cfg.VerifyHistory != nil {
		t.Fatalf("corrupt history: want supervisor without store, got ctrl=%v hist=%v", cfg.VerifyCtrl, cfg.VerifyHistory)
	}
}
