package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/indexer"
)

// logCapture is a slog.Handler that records every emitted record, so tests
// can assert on escalation levels and messages.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r)
	return nil
}
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

func (c *logCapture) has(level slog.Level, substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Level == level && strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

// captureSlog swaps the default logger for a capturing one until cleanup.
func captureSlog(t *testing.T) *logCapture {
	t.Helper()
	c := &logCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(c))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

// saveRotGlobals snapshots and restores the rot* package globals the built-in
// rotation fans out (the integration-tagged saveRotateVars helper is not
// available to unit tests).
func saveRotGlobals(t *testing.T) {
	t.Helper()
	saved := struct {
		retain, archiveDir, archiveS3, bintrailID, format string
		addFuture                                         int
		noReplace, retry, protect                         bool
	}{rotRetain, rotArchiveDir, rotArchiveS3, rotBintrailID, rotFormat,
		rotAddFuture, rotNoReplace, rotRetry, rotProtectUnarchived}
	t.Cleanup(func() {
		rotRetain, rotArchiveDir, rotArchiveS3, rotBintrailID, rotFormat =
			saved.retain, saved.archiveDir, saved.archiveS3, saved.bintrailID, saved.format
		rotAddFuture, rotNoReplace, rotRetry, rotProtectUnarchived =
			saved.addFuture, saved.noReplace, saved.retry, saved.protect
	})
}

// TestStartUpRotation_armsRotateGlobals is the guard-arming regression test:
// the entire data-loss safety of the built-in rotation reduces to the rot*
// fan-out in startUpRotation. If rotProtectUnarchived stopped being set, `up`
// would silently drop unarchived partitions by default and the integration
// tests (which arm the guard themselves) would stay green. Mirrors
// TestPopulateStreamFlags for the stream fan-out.
func TestStartUpRotation_armsRotateGlobals(t *testing.T) {
	saveRotGlobals(t)

	// Poison every global so the assertions prove startUpRotation overwrote
	// them rather than inheriting a lucky zero value.
	rotProtectUnarchived = false
	rotNoReplace = true
	rotArchiveDir = "/poison"
	rotArchiveS3 = "s3://poison"
	rotBintrailID = "poison"
	rotRetry = true
	rotFormat = "text"
	rotAddFuture = 99
	rotRetain = "poison"

	s, err := parseUpRotation("30d", "1h", 3, false)
	if err != nil {
		t.Fatalf("parseUpRotation: %v", err)
	}
	// Cancelled ctx + empty DSN provider: the immediate first cycle dedupes
	// to nothing (no DB touched), then the loop exits on ctx.Done. Wait for
	// the loop to fully exit so it can't race the next test's globals.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := startUpRotation(ctx, s, func() []string { return nil })
	<-done

	if !rotProtectUnarchived {
		t.Error("rotProtectUnarchived must be armed — without it the built-in rotation drops unarchived data")
	}
	if rotNoReplace {
		t.Error("rotNoReplace must be false (dropped partitions are replaced)")
	}
	if rotArchiveDir != "" || rotArchiveS3 != "" || rotBintrailID != "" {
		t.Errorf("archive fields must be empty (built-in rotation never archives): dir=%q s3=%q id=%q",
			rotArchiveDir, rotArchiveS3, rotBintrailID)
	}
	if rotRetry {
		t.Error("rotRetry must be false")
	}
	if rotFormat != "json" {
		t.Errorf("rotFormat = %q, want json (suppresses per-partition stdout chatter)", rotFormat)
	}
	if rotAddFuture != 3 {
		t.Errorf("rotAddFuture = %d, want 3", rotAddFuture)
	}
	if rotRetain != "30d" {
		t.Errorf("rotRetain = %q, want 30d", rotRetain)
	}
}

// TestStartUpRotation_disabledIsInert verifies the disabled path starts no
// loop and never consults the DSN provider.
func TestStartUpRotation_disabledIsInert(t *testing.T) {
	called := false
	done := startUpRotation(context.Background(), upRotationSettings{}, func() []string {
		called = true
		return nil
	})
	select {
	case <-done: // disabled path returns an already-closed channel
	default:
		t.Error("disabled rotation must return a closed done channel (no loop running)")
	}
	if called {
		t.Error("disabled rotation must not invoke the DSN provider")
	}
}

// TestStartUpRotation_escalatesAfterConsecutiveFailures exercises the
// detection half of the data-loss story: a rotation that fails every cycle
// must escalate from per-cycle Warns to an explicit Error after
// upRotationEscalateAfter consecutive cycles — otherwise the index grows
// unbounded while the logs read as routine noise.
func TestStartUpRotation_escalatesAfterConsecutiveFailures(t *testing.T) {
	saveRotGlobals(t)
	logs := captureSlog(t)

	prevN := upRotationEscalateAfter
	upRotationEscalateAfter = 2
	t.Cleanup(func() { upRotationEscalateAfter = prevN })

	s := upRotationSettings{
		enabled:   true,
		retain:    24 * time.Hour,
		retainRaw: "24h",
		interval:  5 * time.Millisecond,
		addFuture: 0,
		explicit:  true, // skip the upgrade guard; we are testing escalation
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Port 1 on loopback: connection refused immediately, every cycle fails.
	done := startUpRotation(ctx, s, func() []string {
		return []string{"root:x@tcp(127.0.0.1:1)/nope"}
	})

	deadline := time.After(15 * time.Second)
	for !logs.has(slog.LevelError, "made no progress for consecutive cycles") {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("rotation never escalated to Error after consecutive failing cycles")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

// TestGuardTrips covers the pure decision behind the upgrade guard at its
// edges: a regression flipping any of these silently disables rotation on
// fresh installs (unbounded growth) or shreds history it promised to protect.
func TestGuardTrips(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	retain := 30 * 24 * time.Hour
	name := func(age time.Duration) string { return indexer.PartitionName(now.Add(-age)) }

	cases := []struct {
		desc       string
		partitions []partitionInfo
		want       bool
	}{
		{"empty list (fresh install)", nil, false},
		{"p_future only (fresh install)", []partitionInfo{{Name: "p_future"}}, false},
		{"malformed names only", []partitionInfo{{Name: "p_bogus"}, {Name: "weird"}}, false},
		{"oldest inside the window", []partitionInfo{{Name: name(10 * 24 * time.Hour)}}, false},
		{"oldest exactly AT the 2x boundary (strict >, must NOT trip)",
			[]partitionInfo{{Name: name(2 * retain)}}, false},
		{"oldest just past the 2x boundary",
			[]partitionInfo{{Name: name(2*retain + time.Hour)}}, true},
		{"deep history mixed with recent partitions and p_future",
			[]partitionInfo{{Name: name(time.Hour)}, {Name: name(100 * 24 * time.Hour)}, {Name: "p_future"}}, true},
	}
	for _, tc := range cases {
		got, _ := guardTrips(tc.partitions, retain, now)
		if got != tc.want {
			t.Errorf("%s: guardTrips = %v, want %v", tc.desc, got, tc.want)
		}
	}
}

// TestParseUpRotation_explicitPropagates pins that the constructor is the
// sole author of the explicit field — the switch between "operator chose a
// retention" and "implicit default protected by the upgrade guard".
func TestParseUpRotation_explicitPropagates(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		s, err := parseUpRotation("30d", "1h", 3, explicit)
		if err != nil {
			t.Fatalf("parseUpRotation: %v", err)
		}
		if s.explicit != explicit {
			t.Errorf("explicit = %v, want %v", s.explicit, explicit)
		}
	}
}

// TestRunUp_explicitRetentionWiring pins the runUp call site — the literal
// Changed("rotate-retain") string. A typo there would make the upgrade guard
// engage even when the operator explicitly set a retention, silently ignoring
// their choice (and never dropping deep history they asked to drop).
func TestRunUp_explicitRetentionWiring(t *testing.T) {
	flag := upCmd.Flags().Lookup("rotate-retain")
	if flag == nil {
		t.Fatal("--rotate-retain not registered on upCmd")
	}
	savedChanged, savedValue := flag.Changed, flag.Value.String()
	savedCfg := upRotationCfg
	savedRetain, savedInterval, savedAdd := upRotateRetain, upRotateInterval, upRotateAddFuture
	savedSource, savedConsole, savedFormat := upSourceDSN, upConsole, upFormat
	t.Cleanup(func() {
		flag.Changed = savedChanged
		_ = flag.Value.Set(savedValue)
		upRotationCfg = savedCfg
		upRotateRetain, upRotateInterval, upRotateAddFuture = savedRetain, savedInterval, savedAdd
		upSourceDSN, upConsole, upFormat = savedSource, savedConsole, savedFormat
	})

	// Make runUp exit early at the source-dsn check — AFTER the rotation
	// block has populated upRotationCfg, BEFORE any phase touches a DB.
	upSourceDSN, upConsole, upFormat = "", false, "text"
	upRotateInterval, upRotateAddFuture = "1h", 3

	// Implicit: flag never set.
	flag.Changed = false
	upRotateRetain = "30d"
	_ = runUp(upCmd, nil) // returns the source-dsn error; irrelevant here
	if upRotationCfg.explicit {
		t.Error("explicit must be false when --rotate-retain was never set")
	}

	// Explicit: set through the flag set, exactly like CLI/env would.
	if err := upCmd.Flags().Set("rotate-retain", "7d"); err != nil {
		t.Fatalf("Set(rotate-retain): %v", err)
	}
	_ = runUp(upCmd, nil)
	if !upRotationCfg.explicit {
		t.Error("explicit must be true when --rotate-retain was set — the Changed(\"rotate-retain\") call site is broken")
	}
	if upRotationCfg.retainRaw != "7d" {
		t.Errorf("retainRaw = %q, want 7d", upRotationCfg.retainRaw)
	}
}

// TestRunUpRotationCycle_reportsFailure verifies the cycle aggregation: any
// DSN failing marks the whole cycle failed (feeding the escalation streak).
func TestRunUpRotationCycle_reportsFailure(t *testing.T) {
	saveRotGlobals(t)
	captureSlog(t) // silence the expected warnings

	s := upRotationSettings{
		enabled: true, retain: 24 * time.Hour, retainRaw: "24h",
		interval: time.Hour, explicit: true,
	}
	deferred, failed := runUpRotationCycle(context.Background(), s, func() []string {
		return []string{"root:x@tcp(127.0.0.1:1)/nope", "not-a-dsn"}
	})
	if !failed {
		t.Error("cycle with unreachable DSNs must report failed=true")
	}
	if deferred != 0 {
		t.Errorf("deferred = %d, want 0 (nothing rotated)", deferred)
	}
}

// TestUpRotateFlagsRegistered pins the flag names to the envBindings strings:
// bindCommandEnv silently skips bindings whose flag doesn't exist on the
// command, so a renamed flag would make BINTRAIL_ROTATE_RETAIN=off silently
// fail to disable rotation.
func TestUpRotateFlagsRegistered(t *testing.T) {
	for _, name := range []string{"rotate-retain", "rotate-interval", "rotate-add-future"} {
		if upCmd.Flags().Lookup(name) == nil {
			t.Errorf("upCmd is missing --%s — its BINTRAIL_* env binding would silently no-op", name)
		}
	}
}

func TestParseUpRotation_enabled(t *testing.T) {
	s, err := parseUpRotation("30d", "1h", 3, false)
	if err != nil {
		t.Fatalf("parseUpRotation: %v", err)
	}
	if !s.enabled {
		t.Fatal("expected enabled")
	}
	if s.retain != 30*24*time.Hour {
		t.Errorf("retain = %v, want 720h", s.retain)
	}
	if s.interval != time.Hour {
		t.Errorf("interval = %v, want 1h", s.interval)
	}
	if s.addFuture != 3 {
		t.Errorf("addFuture = %d, want 3", s.addFuture)
	}
	if s.retainRaw != "30d" {
		t.Errorf("retainRaw = %q, want 30d", s.retainRaw)
	}
}

func TestParseUpRotation_disabledForms(t *testing.T) {
	for _, retain := range []string{"off", "0", ""} {
		s, err := parseUpRotation(retain, "1h", 3, false)
		if err != nil {
			t.Errorf("parseUpRotation(%q): unexpected error %v", retain, err)
			continue
		}
		if s.enabled {
			t.Errorf("parseUpRotation(%q): expected disabled", retain)
		}
	}
}

func TestParseUpRotation_invalidRetain(t *testing.T) {
	_, err := parseUpRotation("1x", "1h", 3, false)
	if err == nil {
		t.Fatal("expected error for invalid retain unit")
	}
	if !strings.Contains(err.Error(), "off") {
		t.Errorf("error should mention the \"off\" escape hatch, got: %v", err)
	}
}

func TestParseUpRotation_invalidInterval(t *testing.T) {
	if _, err := parseUpRotation("7d", "soon", 3, false); err == nil {
		t.Fatal("expected error for unparseable interval")
	}
	if _, err := parseUpRotation("7d", "-1h", 3, false); err == nil {
		t.Fatal("expected error for non-positive interval")
	}
}

func TestParseUpRotation_negativeAddFuture(t *testing.T) {
	if _, err := parseUpRotation("7d", "1h", -1, false); err == nil {
		t.Fatal("expected error for negative add-future")
	}
}

func TestDedupeDSNs(t *testing.T) {
	in := []string{"a", "", "b", "a", "c", "b"}
	got := dedupeDSNs(in)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("dedupeDSNs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupeDSNs = %v, want %v", got, want)
		}
	}
}
