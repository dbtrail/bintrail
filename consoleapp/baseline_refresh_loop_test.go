package consoleapp

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
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
	// Cancellable: the configured cases start a real ticker goroutine, and a
	// Background context would leak one per sub-test.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)

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
		// The wiring, not the parser: minutes reach the loop only because the
		// flag stopped going through ParseRetain, which accepts hours and days
		// only. Parsing "15m" correctly in cliutil proves nothing about which
		// parser this call site actually reaches (#1469).
		{"minutes are an interval", sup, "15m", "dsn", "/b", ""},
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
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)

	// A dump in flight blocks a refresh for the same server...
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	if err := sup.TriggerRefresh(refreshRequest{ServerID: "a", IndexDSN: "d", BaselineDir: "/b"}, 0); err != console.ErrBaselineRunning {
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
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
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
	runBaselineRefreshCycle(context.Background(), nil, &baselineSupervisor{}, "dsn", "/b", 0)
}

// TestRunBaselineRefreshCycle_stopsOnCancel: shutdown must not start new work.
func TestRunBaselineRefreshCycle_stopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	runBaselineRefreshCycle(ctx, nil, sup, "dsn", "/b", 0)
	if got := sup.RefreshStatus("default"); got.State != "idle" {
		t.Fatalf("a cancelled cycle started work: %+v", got)
	}
}

func TestSnapshotsPer30Days(t *testing.T) {
	for _, tc := range []struct {
		interval time.Duration
		want     int64
	}{
		{5 * time.Minute, 8640},
		{15 * time.Minute, 2880},
		{time.Hour, 720},
		{24 * time.Hour, 30},
		// The regime a per-DAY projection got wrong: integer division sent
		// every interval longer than a day to zero, so a 7d refresh reported
		// "0 snapshots per day", which reads as none and is the opposite of
		// what it means.
		{7 * 24 * time.Hour, 4},
		{29 * 24 * time.Hour, 1},
		{30 * 24 * time.Hour, 1}, // the exact horizon: the last interval that still projects
		// Past the horizon there is no honest integer, so the caller is told
		// to omit the figure rather than print a zero.
		{31 * 24 * time.Hour, 0},
		{0, 0}, // guarded: a zero interval never reaches here, and must not divide
	} {
		if got := snapshotsPer30Days(tc.interval); got != tc.want {
			t.Errorf("snapshotsPer30Days(%v) = %d, want %d", tc.interval, got, tc.want)
		}
	}
}

// A projection that rounds to zero must not be logged at all: "0 snapshots"
// states the opposite of the truth about a long interval, and the interval
// logged beside it already says what the rate is.
func TestDiskArgs_omitsAnUnmeaningfulProjection(t *testing.T) {
	// Values, not just keys: a projection that is present but wrong reads as
	// authoritative, and a key-only assertion cannot tell the two apart.
	val := func(args []any, key string) (any, bool) {
		for i := 0; i+1 < len(args); i += 2 {
			if k, ok := args[i].(string); ok && k == key {
				return args[i+1], true
			}
		}
		return nil, false
	}
	const projection = "full_table_snapshots_per_server_per_30d"

	short := diskArgs(15*time.Minute, nil)
	got, ok := val(short, projection)
	if !ok {
		t.Fatalf("a 15m interval logged no projection: %v", short)
	}
	if got != int64(2880) {
		t.Errorf("15m projection = %v, want 2880", got)
	}
	// Past the horizon there is no honest integer, and "0" states the opposite
	// of the truth about a long interval.
	long := diskArgs(90*24*time.Hour, nil)
	if v, ok := val(long, projection); ok {
		t.Errorf("a 90d interval logged a projection of %v, which rounds to zero", v)
	}
	for _, args := range [][]any{short, long} {
		if _, ok := val(args, "interval"); !ok {
			t.Errorf("disk warning lost the interval it always carried: %v", args)
		}
		if _, ok := val(args, "dirs"); !ok {
			t.Errorf("disk warning lost the dirs it always carried: %v", args)
		}
	}
}

// TestRunBaselineRefreshCycle_countsAServerItCouldNotStart pins the mechanism
// that REPLACED a broken one, so it is worth saying what was broken.
//
// The first version of this feature timed the dispatch loop and called the
// result "how long the cycle took". TriggerRefresh ends in
// `go s.runRefresh(...)` and returns, so that span is goroutine launch:
// microseconds, whatever the refresh costs. Its overrun warning could never
// fire and the line beside it announced a cycle had finished while the fold
// was still running. CI was green throughout, because the only test drove the
// pure comparison with hand-written durations and never touched the call site.
//
// A skip is the loop-level evidence of the same condition, observed rather
// than timed: a server still busy when the next tick arrives IS a refresh that
// outran the interval. Seeding the busy slot directly is how the sibling
// single-flight test does it, and it makes this deterministic where timing a
// real fold would not be.
func TestRunBaselineRefreshCycle_countsAServerItCouldNotStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)

	// "default" is the boot target's id; a dump in flight for it makes the
	// shared single-flight refuse the refresh.
	sup.jobs["default"] = &console.BaselineStatus{State: "running"}

	dispatched, skipped := runBaselineRefreshCycle(ctx, nil, sup, "dsn", t.TempDir(), time.Minute)
	if dispatched != 0 || skipped != 1 {
		t.Fatalf("cycle reported dispatched=%d skipped=%d, want 0 and 1: a busy server must be COUNTED, "+
			"not swallowed at Debug where the default log level hides it", dispatched, skipped)
	}
}

// reportRefreshDuration is per REFRESH, not per tick, so it is the only place
// an overrun can be detected at all: see the test above for why timing the
// dispatch loop cannot.
func TestReportRefreshDuration_warnsOnlyOnOverrun(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	capture := func(level slog.Level, interval, took time.Duration) string {
		var buf bytes.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level})))
		reportRefreshDuration("srv", interval, took)
		return buf.String()
	}

	if out := capture(slog.LevelWarn, time.Hour, 5*time.Minute); out != "" {
		t.Errorf("a refresh inside its interval warned: %q", out)
	}
	if out := capture(slog.LevelWarn, time.Hour, time.Hour); out != "" {
		t.Errorf("a refresh exactly at its interval warned: %q", out)
	}
	// A manual refresh has no interval to be measured against and must not warn.
	if out := capture(slog.LevelWarn, 0, 12*time.Minute); out != "" {
		t.Errorf("a refresh with no configured interval warned: %q", out)
	}

	// Captured at Debug so the quiet branch is visible to the test. At Warn the
	// non-overrun path and a DELETED non-overrun path are the same empty
	// string, which is how a whole branch stays unguarded.
	quiet := capture(slog.LevelDebug, time.Hour, 5*time.Minute)
	for _, want := range []string{"level=DEBUG", "took=5m", "server=srv"} {
		if !strings.Contains(quiet, want) {
			t.Errorf("the non-overrun line lost %q: %q", want, quiet)
		}
	}

	out := capture(slog.LevelWarn, 5*time.Minute, 12*time.Minute)
	if out == "" {
		t.Fatal("a refresh that outran its interval said nothing")
	}
	for _, want := range []string{"level=WARN", "took=12m", "interval=5m", "server=srv"} {
		if !strings.Contains(out, want) {
			t.Errorf("overrun warning does not carry %q, so it cannot be acted on: %q", want, out)
		}
	}
}

// A skipped tick is the loop-level evidence that refreshes are not keeping up.
// It used to go to slog.Debug, below the console binary's default level, which
// at a short interval hides the dominant path rather than an edge case.
func TestReportDispatch_visibleOnlyWhenSomethingWasSkipped(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	capture := func(dispatched, skipped int) string {
		var buf bytes.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
		reportDispatch(time.Minute, dispatched, skipped)
		return buf.String()
	}

	if out := capture(3, 0); out != "" {
		t.Errorf("a healthy tick logged at Info; at a 1m interval that is a line a minute forever: %q", out)
	}
	out := capture(1, 2)
	if out == "" {
		t.Fatal("a tick that skipped a server said nothing at the default level")
	}
	for _, want := range []string{"skipped=2", "dispatched=1", "interval=1m"} {
		if !strings.Contains(out, want) {
			t.Errorf("dispatch line lost %q: %q", want, out)
		}
	}
}

// TestRunRefresh_doesNotAdviseTuningWhenNothingWasPublished reaches the CALL
// SITE, which is the whole point: the previous version of this guard drove
// reportRefreshDuration with hand-written durations and so could not see that
// the call site invoked it unconditionally, including for runs that refused.
// That is the same shape this PR indicts one function over.
//
// The interval is one nanosecond, so any real elapsed time exceeds it. An
// unconditional call therefore WARNS here, and the warning would tell an
// operator to raise the interval for a refresh that failed because it had no
// baseline to fold.
func TestRunRefresh_doesNotAdviseTuningWhenNothingWasPublished(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}

	// An empty baseline dir makes executeRefresh refuse: nothing to fold.
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: t.TempDir()},
		time.Now().UTC(), time.Nanosecond)

	out := buf.String()
	if !strings.Contains(out, "published nothing") {
		t.Fatalf("the refusal itself was not reported, so this test is not exercising the path it claims: %q", out)
	}
	if strings.Contains(out, "took longer than the configured interval") {
		t.Errorf("a refresh that published nothing was given scheduling advice; the capture gap or schema "+
			"change that stopped it is not fixed by raising the interval: %q", out)
	}
}

// The tick binds the two counters and hands them on, and it is the only place
// that happens. Before the seam existed the whole binding lived inside the
// ticker's closure, so swapping the arguments compiled and every test stayed
// green: both are ints, and the cycle and the reporter were only ever driven
// apart.
func TestRefreshTick_reportsTheCountersInTheRightOrder(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.jobs["default"] = &console.BaselineStatus{State: "running"}

	refreshTick(ctx, nil, sup, "dsn", t.TempDir(), time.Minute)

	out := buf.String()
	if !strings.Contains(out, "skipped=1") || !strings.Contains(out, "dispatched=0") {
		t.Errorf("the tick reported the counters swapped or not at all; the one busy server must read "+
			"skipped=1 dispatched=0: %q", out)
	}
}
