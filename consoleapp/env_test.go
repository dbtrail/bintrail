package consoleapp

import (
	"io"
	"os"
	"strings"
	"testing"
)

// captureStderr runs fn with os.Stderr redirected to a pipe and returns
// everything fn wrote to it.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	defer func() { os.Stderr = old }()
	fn()
	w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestParseAndSetEnvLongLines(t *testing.T) {
	t.Run("100KiB value loads and later vars survive", func(t *testing.T) {
		// Exceeds bufio.Scanner's default 64KiB token limit: before the
		// buffer raise this dropped LONG and every variable after it.
		t.Setenv("TEST_CONSOLE_PARSE_LONG", "")
		os.Unsetenv("TEST_CONSOLE_PARSE_LONG")
		t.Setenv("TEST_CONSOLE_PARSE_AFTER_LONG", "")
		os.Unsetenv("TEST_CONSOLE_PARSE_AFTER_LONG")
		longVal := strings.Repeat("a", 100*1024)
		stderr := captureStderr(t, func() {
			parseAndSetEnv("TEST_CONSOLE_PARSE_LONG=" + longVal + "\nTEST_CONSOLE_PARSE_AFTER_LONG=survived")
		})
		if got := os.Getenv("TEST_CONSOLE_PARSE_LONG"); got != longVal {
			t.Errorf("long value: got %d bytes, want %d", len(got), len(longVal))
		}
		if got := os.Getenv("TEST_CONSOLE_PARSE_AFTER_LONG"); got != "survived" {
			t.Errorf("var after long line: got %q, want %q", got, "survived")
		}
		if strings.Contains(stderr, "NOT loaded") {
			t.Errorf("unexpected scanner warning for 100KiB value: %s", stderr)
		}
	})

	t.Run("line over 1MiB warns loudly", func(t *testing.T) {
		t.Setenv("TEST_CONSOLE_PARSE_BEFORE_HUGE", "")
		os.Unsetenv("TEST_CONSOLE_PARSE_BEFORE_HUGE")
		t.Setenv("TEST_CONSOLE_PARSE_AFTER_HUGE", "")
		os.Unsetenv("TEST_CONSOLE_PARSE_AFTER_HUGE")
		hugeVal := strings.Repeat("a", 1024*1024+1)
		stderr := captureStderr(t, func() {
			parseAndSetEnv("TEST_CONSOLE_PARSE_BEFORE_HUGE=ok\nTEST_CONSOLE_PARSE_HUGE=" + hugeVal + "\nTEST_CONSOLE_PARSE_AFTER_HUGE=lost")
		})
		if got := os.Getenv("TEST_CONSOLE_PARSE_BEFORE_HUGE"); got != "ok" {
			t.Errorf("var before huge line: got %q, want %q", got, "ok")
		}
		if !strings.Contains(stderr, "NOT loaded") {
			t.Errorf("expected loud warning about unloaded variables, got: %q", stderr)
		}
		if _, ok := os.LookupEnv("TEST_CONSOLE_PARSE_AFTER_HUGE"); ok {
			t.Error("var after huge line unexpectedly loaded; warning would be a lie")
		}
	})
}
