// Package observe provides shared observability setup for Bintrail: structured
// logging via log/slog and Prometheus metrics for the stream command.
package observe

import (
	"io"
	"log/slog"
)

// Setup configures the global slog logger and returns it.
// format is "json" or "text" (default text for any other value).
// level is a slog level string: "debug", "info", "warn", "error".
// Output is written to w (typically os.Stderr).
func Setup(w io.Writer, format, level string) *slog.Logger {
	l := ParseLevel(level)
	opts := &slog.HandlerOptions{Level: l}

	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// ParseLevel converts a level string to slog.Level.
// Unrecognised strings default to slog.LevelInfo.
func ParseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}

// Nop returns a logger that discards all output. Useful in tests.
func Nop() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 100}))
}
