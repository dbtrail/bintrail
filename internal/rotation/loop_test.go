package rotation

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

// TestLoopOptions is the data-loss-safety regression test: the entire safety of
// the built-in rotation reduces to loopOptions arming ProtectUnarchived (and
// never archiving). If a field regressed, `up` would silently drop unarchived
// partitions by default and the integration tests (which exercise the engine
// guard directly) could stay green. Replaces the old armsRotateGlobals test,
// which guarded the rot*-global fan-out that loopOptions superseded.
func TestLoopOptions(t *testing.T) {
	s := Settings{Enabled: true, Retain: 30 * 24 * time.Hour, RetainRaw: "30d", AddFuture: 3}
	o := loopOptions(7*24*time.Hour, s)

	if !o.ProtectUnarchived {
		t.Error("ProtectUnarchived must be armed — without it the built-in rotation drops unarchived data")
	}
	if o.NoReplace {
		t.Error("NoReplace must be false (dropped partitions are replaced)")
	}
	if o.ArchiveDir != "" || o.ArchiveS3 != "" || o.BintrailID != "" {
		t.Errorf("archive fields must be empty (built-in rotation never archives): dir=%q s3=%q id=%q",
			o.ArchiveDir, o.ArchiveS3, o.BintrailID)
	}
	if o.Retry {
		t.Error("Retry must be false")
	}
	if o.Format != "json" {
		t.Errorf("Format = %q, want json (suppresses per-partition stdout chatter)", o.Format)
	}
	if o.AddFuture != 3 {
		t.Errorf("AddFuture = %d, want 3", o.AddFuture)
	}
	if o.RetainRaw != "30d" {
		t.Errorf("RetainRaw = %q, want 30d", o.RetainRaw)
	}
	if o.RetainDur != 7*24*time.Hour {
		t.Errorf("RetainDur = %v, want 168h (the guard-adjusted retain, not s.Retain)", o.RetainDur)
	}
}

// TestStartLoop_disabledIsInert verifies the disabled path starts no loop and
// never consults the DSN provider.
func TestStartLoop_disabledIsInert(t *testing.T) {
	called := false
	done := StartLoop(context.Background(), Settings{}, func() []string {
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

// TestStartLoop_escalatesAfterConsecutiveFailures exercises the detection half
// of the data-loss story: a rotation that fails every cycle must escalate from
// per-cycle Warns to an explicit Error after escalateAfter consecutive cycles —
// otherwise the index grows unbounded while the logs read as routine noise.
func TestStartLoop_escalatesAfterConsecutiveFailures(t *testing.T) {
	logs := captureSlog(t)

	prevN := escalateAfter
	escalateAfter = 2
	t.Cleanup(func() { escalateAfter = prevN })

	s := Settings{
		Enabled:   true,
		Retain:    24 * time.Hour,
		RetainRaw: "24h",
		Interval:  5 * time.Millisecond,
		AddFuture: 0,
		Explicit:  true, // skip the upgrade guard; we are testing escalation
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Port 1 on loopback: connection refused immediately, every cycle fails.
	done := StartLoop(ctx, s, func() []string {
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

// TestParseSettings_explicitPropagates pins that the constructor is the sole
// author of the Explicit field — the switch between "operator chose a
// retention" and "implicit default protected by the upgrade guard".
func TestParseSettings_explicitPropagates(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		s, err := ParseSettings("30d", "1h", 3, explicit)
		if err != nil {
			t.Fatalf("ParseSettings: %v", err)
		}
		if s.Explicit != explicit {
			t.Errorf("Explicit = %v, want %v", s.Explicit, explicit)
		}
	}
}

// TestRunCycle_reportsFailure verifies the cycle aggregation: any DSN failing
// marks the whole cycle failed (feeding the escalation streak).
func TestRunCycle_reportsFailure(t *testing.T) {
	captureSlog(t) // silence the expected warnings

	s := Settings{
		Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h",
		Interval: time.Hour, Explicit: true,
	}
	deferred, failed := runCycle(context.Background(), s, func() []string {
		return []string{"root:x@tcp(127.0.0.1:1)/nope", "not-a-dsn"}
	})
	if !failed {
		t.Error("cycle with unreachable DSNs must report failed=true")
	}
	if deferred != 0 {
		t.Errorf("deferred = %d, want 0 (nothing rotated)", deferred)
	}
}

func TestParseSettings_enabled(t *testing.T) {
	s, err := ParseSettings("30d", "1h", 3, false)
	if err != nil {
		t.Fatalf("ParseSettings: %v", err)
	}
	if !s.Enabled {
		t.Fatal("expected enabled")
	}
	if s.Retain != 30*24*time.Hour {
		t.Errorf("Retain = %v, want 720h", s.Retain)
	}
	if s.Interval != time.Hour {
		t.Errorf("Interval = %v, want 1h", s.Interval)
	}
	if s.AddFuture != 3 {
		t.Errorf("AddFuture = %d, want 3", s.AddFuture)
	}
	if s.RetainRaw != "30d" {
		t.Errorf("RetainRaw = %q, want 30d", s.RetainRaw)
	}
}

func TestParseSettings_disabledForms(t *testing.T) {
	for _, retain := range []string{"off", "0", ""} {
		s, err := ParseSettings(retain, "1h", 3, false)
		if err != nil {
			t.Errorf("ParseSettings(%q): unexpected error %v", retain, err)
			continue
		}
		if s.Enabled {
			t.Errorf("ParseSettings(%q): expected disabled", retain)
		}
	}
}

func TestParseSettings_invalidRetain(t *testing.T) {
	_, err := ParseSettings("1x", "1h", 3, false)
	if err == nil {
		t.Fatal("expected error for invalid retain unit")
	}
	if !strings.Contains(err.Error(), "off") {
		t.Errorf("error should mention the \"off\" escape hatch, got: %v", err)
	}
}

func TestParseSettings_invalidInterval(t *testing.T) {
	if _, err := ParseSettings("7d", "soon", 3, false); err == nil {
		t.Fatal("expected error for unparseable interval")
	}
	if _, err := ParseSettings("7d", "-1h", 3, false); err == nil {
		t.Fatal("expected error for non-positive interval")
	}
}

func TestParseSettings_negativeAddFuture(t *testing.T) {
	if _, err := ParseSettings("7d", "1h", -1, false); err == nil {
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
