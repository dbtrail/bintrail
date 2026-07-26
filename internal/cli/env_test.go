package cli

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
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

func TestParseAndSetEnv(t *testing.T) {
	t.Run("basic key=value", func(t *testing.T) {
		t.Setenv("TEST_PARSE_BASIC", "")
		os.Unsetenv("TEST_PARSE_BASIC")
		parseAndSetEnv("TEST_PARSE_BASIC=hello")
		if got := os.Getenv("TEST_PARSE_BASIC"); got != "hello" {
			t.Errorf("got %q, want %q", got, "hello")
		}
	})

	t.Run("skips comments and blank lines", func(t *testing.T) {
		t.Setenv("TEST_PARSE_SKIP", "")
		os.Unsetenv("TEST_PARSE_SKIP")
		parseAndSetEnv("# comment\n\n  \nTEST_PARSE_SKIP=world")
		if got := os.Getenv("TEST_PARSE_SKIP"); got != "world" {
			t.Errorf("got %q, want %q", got, "world")
		}
	})

	t.Run("double-quoted value", func(t *testing.T) {
		t.Setenv("TEST_PARSE_DQ", "")
		os.Unsetenv("TEST_PARSE_DQ")
		parseAndSetEnv(`TEST_PARSE_DQ="hello world"`)
		if got := os.Getenv("TEST_PARSE_DQ"); got != "hello world" {
			t.Errorf("got %q, want %q", got, "hello world")
		}
	})

	t.Run("single-quoted value", func(t *testing.T) {
		t.Setenv("TEST_PARSE_SQ", "")
		os.Unsetenv("TEST_PARSE_SQ")
		parseAndSetEnv("TEST_PARSE_SQ='hello world'")
		if got := os.Getenv("TEST_PARSE_SQ"); got != "hello world" {
			t.Errorf("got %q, want %q", got, "hello world")
		}
	})

	t.Run("does not overwrite existing vars", func(t *testing.T) {
		t.Setenv("TEST_PARSE_EXISTING", "original")
		parseAndSetEnv("TEST_PARSE_EXISTING=overwritten")
		if got := os.Getenv("TEST_PARSE_EXISTING"); got != "original" {
			t.Errorf("got %q, want %q", got, "original")
		}
	})

	t.Run("empty value", func(t *testing.T) {
		t.Setenv("TEST_PARSE_EMPTY", "")
		os.Unsetenv("TEST_PARSE_EMPTY")
		parseAndSetEnv("TEST_PARSE_EMPTY=")
		if v, ok := os.LookupEnv("TEST_PARSE_EMPTY"); !ok {
			t.Error("expected var to be set")
		} else if v != "" {
			t.Errorf("got %q, want empty string", v)
		}
	})

	t.Run("value containing equals sign", func(t *testing.T) {
		t.Setenv("TEST_PARSE_EQUALS", "")
		os.Unsetenv("TEST_PARSE_EQUALS")
		parseAndSetEnv("TEST_PARSE_EQUALS=user:pass@tcp(host)/db?parseTime=true")
		want := "user:pass@tcp(host)/db?parseTime=true"
		if got := os.Getenv("TEST_PARSE_EQUALS"); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("trims whitespace around key and value", func(t *testing.T) {
		t.Setenv("TEST_PARSE_WS", "")
		os.Unsetenv("TEST_PARSE_WS")
		parseAndSetEnv("  TEST_PARSE_WS  =  trimmed  ")
		if got := os.Getenv("TEST_PARSE_WS"); got != "trimmed" {
			t.Errorf("got %q, want %q", got, "trimmed")
		}
	})

	t.Run("100KiB value loads and later vars survive", func(t *testing.T) {
		// Exceeds bufio.Scanner's default 64KiB token limit: before the
		// buffer raise this dropped LONG and every variable after it.
		t.Setenv("TEST_PARSE_LONG", "")
		os.Unsetenv("TEST_PARSE_LONG")
		t.Setenv("TEST_PARSE_AFTER_LONG", "")
		os.Unsetenv("TEST_PARSE_AFTER_LONG")
		longVal := strings.Repeat("a", 100*1024)
		stderr := captureStderr(t, func() {
			parseAndSetEnv("TEST_PARSE_LONG=" + longVal + "\nTEST_PARSE_AFTER_LONG=survived")
		})
		if got := os.Getenv("TEST_PARSE_LONG"); got != longVal {
			t.Errorf("long value: got %d bytes, want %d", len(got), len(longVal))
		}
		if got := os.Getenv("TEST_PARSE_AFTER_LONG"); got != "survived" {
			t.Errorf("var after long line: got %q, want %q", got, "survived")
		}
		if strings.Contains(stderr, "NOT loaded") {
			t.Errorf("unexpected scanner warning for 100KiB value: %s", stderr)
		}
	})

	t.Run("line over 1MiB warns loudly", func(t *testing.T) {
		t.Setenv("TEST_PARSE_BEFORE_HUGE", "")
		os.Unsetenv("TEST_PARSE_BEFORE_HUGE")
		t.Setenv("TEST_PARSE_AFTER_HUGE", "")
		os.Unsetenv("TEST_PARSE_AFTER_HUGE")
		hugeVal := strings.Repeat("a", 1024*1024+1)
		stderr := captureStderr(t, func() {
			parseAndSetEnv("TEST_PARSE_BEFORE_HUGE=ok\nTEST_PARSE_HUGE=" + hugeVal + "\nTEST_PARSE_AFTER_HUGE=lost")
		})
		if got := os.Getenv("TEST_PARSE_BEFORE_HUGE"); got != "ok" {
			t.Errorf("var before huge line: got %q, want %q", got, "ok")
		}
		if !strings.Contains(stderr, "NOT loaded") {
			t.Errorf("expected loud warning about unloaded variables, got: %q", stderr)
		}
		if _, ok := os.LookupEnv("TEST_PARSE_AFTER_HUGE"); ok {
			t.Error("var after huge line unexpectedly loaded; warning would be a lie")
		}
	})

	t.Run("skips lines without equals", func(t *testing.T) {
		t.Setenv("TEST_PARSE_NOEQ", "")
		os.Unsetenv("TEST_PARSE_NOEQ")
		parseAndSetEnv("no equals sign here\nTEST_PARSE_NOEQ=found")
		if got := os.Getenv("TEST_PARSE_NOEQ"); got != "found" {
			t.Errorf("got %q, want %q", got, "found")
		}
	})
}

func TestBindCommandEnv(t *testing.T) {
	t.Run("binds env var to flag", func(t *testing.T) {
		var dsn string
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().StringVar(&dsn, "index-dsn", "", "test flag")

		t.Setenv("BINTRAIL_INDEX_DSN", "from-env")
		BindCommandEnv(cmd)

		if dsn != "from-env" {
			t.Errorf("dsn = %q, want %q", dsn, "from-env")
		}
		if f := cmd.Flags().Lookup("index-dsn"); !f.Changed {
			t.Error("expected flag to be marked as Changed")
		}
	})

	t.Run("does not bind when env var is empty", func(t *testing.T) {
		var dsn string
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().StringVar(&dsn, "index-dsn", "default", "test flag")

		t.Setenv("BINTRAIL_INDEX_DSN", "")
		BindCommandEnv(cmd)

		if dsn != "default" {
			t.Errorf("dsn = %q, want %q", dsn, "default")
		}
	})

	t.Run("binds persistent flag", func(t *testing.T) {
		var dsn string
		cmd := &cobra.Command{Use: "test"}
		cmd.PersistentFlags().StringVar(&dsn, "index-dsn", "", "test flag")

		t.Setenv("BINTRAIL_INDEX_DSN", "persistent-env")
		BindCommandEnv(cmd)

		if dsn != "persistent-env" {
			t.Errorf("dsn = %q, want %q", dsn, "persistent-env")
		}
	})

	t.Run("binds integer flag from env", func(t *testing.T) {
		var batchSize int
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().IntVar(&batchSize, "batch-size", 1000, "test flag")

		t.Setenv("BINTRAIL_BATCH_SIZE", "5000")
		BindCommandEnv(cmd)

		if batchSize != 5000 {
			t.Errorf("batchSize = %d, want 5000", batchSize)
		}
	})
}
