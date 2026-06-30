package reconstruct

import (
	"bytes"
	"fmt"
	"log/slog"
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

			maybeWarnEventVolume("db", "orders", tc.n, tc.threshold)
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
