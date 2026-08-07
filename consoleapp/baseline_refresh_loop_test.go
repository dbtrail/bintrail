package consoleapp

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

func TestBaselineRefreshTargets(t *testing.T) {
	entries := []console.ServerEntry{
		{ID: "a", Name: "prod", DSN: "dsn-a", BaselineDir: "/b/a"},
		// S3-only destination: a refresh reads and writes snapshot FILES, so it
		// cannot refresh in place. Skipped, and warned about — silently doing
		// nothing is what an operator would misread as "it's working".
		{ID: "b", Name: "s3only", DSN: "dsn-b", BaselineS3: "s3://bucket/baselines/"},
		// No baseline destination at all.
		{ID: "c", Name: "nobaseline", DSN: "dsn-c"},
		// A view-only entry with no index DSN.
		{ID: "d", Name: "viewonly", BaselineDir: "/b/d"},
	}

	got := baselineRefreshTargets(entries, "boot-dsn", "/b/boot")
	want := map[string]string{"default": "/b/boot", "a": "/b/a"}
	if len(got) != len(want) {
		t.Fatalf("got %d target(s) %+v, want %d", len(got), got, len(want))
	}
	for _, r := range got {
		if want[r.ServerID] != r.BaselineDir {
			t.Errorf("target %q = %q, want %q", r.ServerID, r.BaselineDir, want[r.ServerID])
		}
	}
}

// TestBaselineRefreshTargets_bootNeedsBoth: the boot entry is only a target when
// the daemon has both halves — a --baseline-dir with no --index-dsn (or the
// reverse) has nothing to fold.
func TestBaselineRefreshTargets_bootNeedsBoth(t *testing.T) {
	if got := baselineRefreshTargets(nil, "", "/b/boot"); len(got) != 0 {
		t.Errorf("targets without an index DSN = %+v, want none", got)
	}
	if got := baselineRefreshTargets(nil, "boot-dsn", ""); len(got) != 0 {
		t.Errorf("targets without a baseline dir = %+v, want none", got)
	}
}

// TestStartBaselineRefreshLoop_startupContract pins WHICH startup conditions
// refuse and which only warn — the distinction is not cosmetic.
//
// A malformed interval is the operator's typo and can only ever be a typo, so it
// refuses. "No server is refreshable yet" is NOT: every tick recomputes the
// target set, a source-less `watch` starts with no servers at all and gains them
// from the console, and per-server baseline directories live in the registry.
// Refusing there would mean a compose file carrying the interval cannot boot a
// fresh install — the operator would have to add a server through a console that
// refused to start. It warns instead.
func TestStartBaselineRefreshLoop_startupContract(t *testing.T) {
	ctx := context.Background()
	sup := newBaselineSupervisor(ctx, t.TempDir(), false)

	for _, tc := range []struct {
		name     string
		sup      *baselineSupervisor
		interval string
		dsn, dir string
		wantErr  string
	}{
		{"disabled by default", sup, "", "", "", ""},
		{"unparseable interval", sup, "sometimes", "d", "/b", "--baseline-refresh-interval"},
		{"no supervisor wired", nil, "6h", "d", "/b", "without a baseline supervisor"},
		{"nothing refreshable yet: warns, starts anyway", sup, "6h", "", "", ""},
		{"configured", sup, "6h", "dsn", "/b", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := startBaselineRefreshLoop(ctx, nil, tc.sup, tc.dsn, tc.dir, tc.interval)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr == "":
			case err == nil:
				t.Fatalf("expected an error containing %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestBaselineSupervisor_singleFlightIsShared is the invariant that keeps a
// refresh from folding a snapshot another job is writing underneath it. The two
// job kinds have separate status slots but ONE lock.
func TestBaselineSupervisor_singleFlightIsShared(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), false)

	// A dump in flight blocks a refresh for the same server...
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	if err := sup.TriggerRefresh(refreshRequest{ServerID: "a", IndexDSN: "d", BaselineDir: "/b"}); err != console.ErrBaselineRunning {
		t.Fatalf("TriggerRefresh during a dump = %v, want ErrBaselineRunning", err)
	}
	// ...and not for a different one.
	if !sup.busyLocked("a") || sup.busyLocked("b") {
		t.Fatal("the single-flight is not per-server")
	}

	// A refresh in flight blocks a dump.
	sup.jobs = map[string]*console.BaselineStatus{}
	sup.refreshes["a"] = &console.BaselineStatus{State: "running"}
	if err := sup.Trigger(console.BaselineRequest{ServerID: "a"}); err != console.ErrBaselineRunning {
		t.Fatalf("Trigger during a refresh = %v, want ErrBaselineRunning", err)
	}
}

// TestBaselineSupervisor_statusSlotsAreSeparate: a manual dump must not erase
// the evidence that the automatic refresh has been failing.
func TestBaselineSupervisor_statusSlotsAreSeparate(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), false)
	sup.refreshes["a"] = &console.BaselineStatus{State: "failed", LastError: "capture gap"}
	sup.jobs["a"] = &console.BaselineStatus{State: "succeeded"}

	if got := sup.RefreshStatus("a"); got.State != "failed" || got.LastError != "capture gap" {
		t.Fatalf("RefreshStatus = %+v; a successful dump overwrote the refresh verdict", got)
	}
	if got := sup.Status("a"); got.State != "succeeded" {
		t.Fatalf("Status = %+v, want the dump's own verdict", got)
	}
	if got := sup.RefreshStatus("never-run"); got.State != "idle" {
		t.Fatalf("RefreshStatus for an unknown server = %+v, want idle", got)
	}
}

// TestRunBaselineRefreshCycle_survivesAPanic: this loop must never be able to
// take down a daemon that is also streaming replication. A refresh that stops is
// a degradation; a daemon that stops capturing is an outage.
func TestRunBaselineRefreshCycle_survivesAPanic(t *testing.T) {
	// A nil supervisor makes TriggerRefresh panic on the nil map write; the
	// cycle's recover must contain it.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a panic escaped the refresh cycle: %v", r)
		}
	}()
	runBaselineRefreshCycle(context.Background(), nil, &baselineSupervisor{}, "dsn", "/b")
}

// TestRunBaselineRefreshCycle_stopsOnCancel: shutdown must not start new work.
func TestRunBaselineRefreshCycle_stopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sup := newBaselineSupervisor(ctx, t.TempDir(), false)
	runBaselineRefreshCycle(ctx, nil, sup, "dsn", "/b")
	if got := sup.RefreshStatus("default"); got.State != "idle" {
		t.Fatalf("a cancelled cycle started work: %+v", got)
	}
}
