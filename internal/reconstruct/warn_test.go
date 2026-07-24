package reconstruct

import (
	"bytes"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"testing"
)

// TestMaybeWarnEventVolume exercises the warning EMISSION (not just the
// predicate): a captured slog handler must see exactly one Warn record with the
// right attributes above threshold, and nothing at/below threshold or disabled.
func TestMaybeWarnEventVolume(t *testing.T) {
	cases := []struct {
		name      string
		n         int
		threshold int64
		wantWarn  bool
	}{
		{"above threshold warns", 101, 100, true},
		{"at threshold silent", 100, 100, false},
		{"below threshold silent", 99, 100, false},
		{"disabled (0) silent", 1 << 30, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			maybeWarnEventVolume("db", "orders", tc.n, tc.threshold, 1)
			out := buf.String()

			if !tc.wantWarn {
				if strings.Contains(out, "very large event window") {
					t.Fatalf("expected no warn (n=%d threshold=%d), got %q", tc.n, tc.threshold, out)
				}
				return
			}
			if c := strings.Count(out, "very large event window"); c != 1 {
				t.Fatalf("want exactly one warn record, got %d: %q", c, out)
			}
			for _, want := range []string{
				`"schema":"db"`,
				`"table":"orders"`,
				fmt.Sprintf(`"events":%d`, tc.n),
				fmt.Sprintf(`"threshold":%d`, tc.threshold),
				"--warn-event-threshold",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("warn record missing %s:\n%s", want, out)
				}
			}
		})
	}
}

// TestMaybeWarnEventVolume_scaledByParallelism is the #842 regression guard:
// 8 tables of 4M events each, run with Parallelism=8 against the default 5M
// per-table threshold, used to pass completely silently (4M < 5M per table)
// even though the process holds ~32M events' worth of change maps at once.
// Scaling the threshold by parallelism (5M/8 ≈ 625K) must now warn.
func TestMaybeWarnEventVolume_scaledByParallelism(t *testing.T) {
	cases := []struct {
		name        string
		n           int
		threshold   int64
		parallelism int
		wantWarn    bool
	}{
		{"8-way parallel, 4M events, 5M threshold: now warns", 4_000_000, 5_000_000, 8, true},
		{"serial (parallelism=1): unchanged, silent", 4_000_000, 5_000_000, 1, false},
		{"parallelism=0 treated as no scaling (defensive)", 4_000_000, 5_000_000, 0, false},
		{"disabled threshold stays disabled regardless of parallelism", 1 << 30, 0, 8, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			maybeWarnEventVolume("db", "orders", tc.n, tc.threshold, tc.parallelism)
			out := buf.String()

			if !tc.wantWarn {
				if strings.Contains(out, "very large event window") {
					t.Fatalf("expected no warn (n=%d threshold=%d parallelism=%d), got %q", tc.n, tc.threshold, tc.parallelism, out)
				}
				return
			}
			if c := strings.Count(out, "very large event window"); c != 1 {
				t.Fatalf("want exactly one warn record, got %d: %q", c, out)
			}
			if !strings.Contains(out, fmt.Sprintf(`"parallelism":%d`, tc.parallelism)) {
				t.Errorf("warn record missing parallelism attribute:\n%s", out)
			}
			if !strings.Contains(out, fmt.Sprintf(`"raw_threshold":%d`, tc.threshold)) {
				t.Errorf("warn record missing raw_threshold attribute:\n%s", out)
			}
		})
	}
}

func TestScaledEventThreshold(t *testing.T) {
	cases := []struct {
		name        string
		threshold   int64
		parallelism int
		want        int64
	}{
		{"disabled threshold passes through", 0, 8, 0},
		{"negative threshold passes through (disabled)", -1, 8, -1},
		{"parallelism=1 unchanged", 5_000_000, 1, 5_000_000},
		{"parallelism=0 unchanged (defensive floor)", 5_000_000, 0, 5_000_000},
		{"parallelism=8 divides evenly", 5_000_000, 8, 625_000},
		{"floors on uneven division", 10, 3, 3},
		{"never scales below 1", 5, 100, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scaledEventThreshold(tc.threshold, tc.parallelism); got != tc.want {
				t.Errorf("scaledEventThreshold(%d,%d) = %d, want %d", tc.threshold, tc.parallelism, got, tc.want)
			}
		})
	}
}

func TestEffectiveParallelism(t *testing.T) {
	if got := effectiveParallelism(FullTableConfig{Parallelism: 4}); got != 4 {
		t.Errorf("explicit Parallelism=4: got %d, want 4", got)
	}
	if got := effectiveParallelism(FullTableConfig{Parallelism: 0}); got != runtime.NumCPU() {
		t.Errorf("Parallelism=0 must default to NumCPU: got %d, want %d", got, runtime.NumCPU())
	}
	if got := effectiveParallelism(FullTableConfig{Parallelism: -1}); got != runtime.NumCPU() {
		t.Errorf("negative Parallelism must default to NumCPU: got %d, want %d", got, runtime.NumCPU())
	}
	// A single-table run must never divide by a big-box Parallelism/NumCPU:
	// only one table can ever be in flight, so the effective divisor is 1
	// regardless of how high Parallelism defaults (#842 false-alarm guard).
	if got := effectiveParallelism(FullTableConfig{Parallelism: 16, Tables: []string{"db.one"}}); got != 1 {
		t.Errorf("single-table run must clamp to 1: got %d, want 1", got)
	}
	// Parallelism below the table count is the real bound (fewer tables can
	// run concurrently than the table count would otherwise suggest).
	if got := effectiveParallelism(FullTableConfig{Parallelism: 4, Tables: []string{"db.a", "db.b", "db.c", "db.d", "db.e", "db.f", "db.g", "db.h"}}); got != 4 {
		t.Errorf("Parallelism smaller than table count must win: got %d, want 4", got)
	}
	// 8 tables of 4M events each with Parallelism=8 (the #842 issue's own
	// example) must clamp to 8, not silently drop below it.
	tables8 := make([]string, 8)
	for i := range tables8 {
		tables8[i] = fmt.Sprintf("db.t%d", i)
	}
	if got := effectiveParallelism(FullTableConfig{Parallelism: 8, Tables: tables8}); got != 8 {
		t.Errorf("Parallelism == table count must stay unclamped: got %d, want 8", got)
	}
}

func TestShouldWarnEvents(t *testing.T) {
	const thr = 5_000_000
	cases := []struct {
		name      string
		n         int64
		threshold int64
		want      bool
	}{
		{"below threshold", thr - 1, thr, false},
		{"at threshold", thr, thr, false},
		{"above threshold", thr + 1, thr, true},
		{"threshold 0 disables", 1 << 40, 0, false},
		{"threshold negative disables", 1 << 40, -1, false},
		{"zero events never warns", 0, thr, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldWarnEvents(tc.n, tc.threshold); got != tc.want {
				t.Fatalf("shouldWarnEvents(%d,%d) = %v, want %v", tc.n, tc.threshold, got, tc.want)
			}
		})
	}
}
