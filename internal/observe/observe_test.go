package observe_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/bintrail/internal/observe"
)

func TestParseLevel(t *testing.T) {
	cases := []struct {
		input string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},      // empty → default info
		{"bogus", slog.LevelInfo}, // unrecognised → default info
	}
	for _, tc := range cases {
		got := observe.ParseLevel(tc.input)
		if got != tc.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestSetup_textFormat(t *testing.T) {
	var buf bytes.Buffer
	observe.Setup(&buf, "text", "info")
	slog.Info("hello", "key", "val")
	out := buf.String()
	if !strings.Contains(out, "hello") {
		t.Errorf("expected log line to contain 'hello', got: %q", out)
	}
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Error("text format should not produce JSON")
	}
}

func TestSetup_jsonFormat(t *testing.T) {
	var buf bytes.Buffer
	observe.Setup(&buf, "json", "info")
	slog.Info("hello", "key", "val")
	out := buf.String()
	if !strings.Contains(out, `"msg":"hello"`) {
		t.Errorf("expected JSON with msg field, got: %q", out)
	}
}

func TestSetup_levelFiltering(t *testing.T) {
	var buf bytes.Buffer
	observe.Setup(&buf, "text", "warn")
	slog.Info("should be filtered")
	if buf.Len() != 0 {
		t.Errorf("info message should be filtered at warn level, got: %q", buf.String())
	}
	slog.Warn("should appear")
	if !strings.Contains(buf.String(), "should appear") {
		t.Errorf("warn message should appear, got: %q", buf.String())
	}
}

func TestNop(t *testing.T) {
	logger := observe.Nop()
	// Should not panic and should produce no output (verified by not crashing)
	logger.Info("this goes nowhere")
	logger.Error("neither does this")
}
