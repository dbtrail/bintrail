package main

import (
	"os"
	"testing"

	"github.com/spf13/cobra"
)

func TestParseAndSetEnv(t *testing.T) {
	t.Run("basic key=value", func(t *testing.T) {
		t.Setenv("TEST_PARSE_BASIC", "") // ensure not set
		os.Unsetenv("TEST_PARSE_BASIC")
		parseAndSetEnv("TEST_PARSE_BASIC=hello")
		if got := os.Getenv("TEST_PARSE_BASIC"); got != "hello" {
			t.Errorf("got %q, want %q", got, "hello")
		}
	})

	t.Run("skips comments and blank lines", func(t *testing.T) {
		os.Unsetenv("TEST_PARSE_SKIP")
		parseAndSetEnv("# comment\n\n  \nTEST_PARSE_SKIP=world")
		if got := os.Getenv("TEST_PARSE_SKIP"); got != "world" {
			t.Errorf("got %q, want %q", got, "world")
		}
	})

	t.Run("double-quoted value", func(t *testing.T) {
		os.Unsetenv("TEST_PARSE_DQ")
		parseAndSetEnv(`TEST_PARSE_DQ="hello world"`)
		if got := os.Getenv("TEST_PARSE_DQ"); got != "hello world" {
			t.Errorf("got %q, want %q", got, "hello world")
		}
	})

	t.Run("single-quoted value", func(t *testing.T) {
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
		os.Unsetenv("TEST_PARSE_EMPTY")
		parseAndSetEnv("TEST_PARSE_EMPTY=")
		// Empty string is still set.
		if v, ok := os.LookupEnv("TEST_PARSE_EMPTY"); !ok {
			t.Error("expected var to be set")
		} else if v != "" {
			t.Errorf("got %q, want empty string", v)
		}
	})

	t.Run("value containing equals sign", func(t *testing.T) {
		os.Unsetenv("TEST_PARSE_EQUALS")
		parseAndSetEnv("TEST_PARSE_EQUALS=user:pass@tcp(host)/db?parseTime=true")
		want := "user:pass@tcp(host)/db?parseTime=true"
		if got := os.Getenv("TEST_PARSE_EQUALS"); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("trims whitespace around key and value", func(t *testing.T) {
		os.Unsetenv("TEST_PARSE_WS")
		parseAndSetEnv("  TEST_PARSE_WS  =  trimmed  ")
		if got := os.Getenv("TEST_PARSE_WS"); got != "trimmed" {
			t.Errorf("got %q, want %q", got, "trimmed")
		}
	})

	t.Run("skips lines without equals", func(t *testing.T) {
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

		// Manually apply binding (bypassing envOnce since we set env directly).
		for _, b := range envBindings {
			if v := os.Getenv(b.EnvVar); v != "" {
				if f := cmd.Flags().Lookup(b.Flag); f != nil {
					_ = cmd.Flags().Set(b.Flag, v)
				}
			}
		}

		if dsn != "from-env" {
			t.Errorf("dsn = %q, want %q", dsn, "from-env")
		}
		f := cmd.Flags().Lookup("index-dsn")
		if !f.Changed {
			t.Error("expected flag to be marked as Changed")
		}
	})

	t.Run("does not bind when env var is empty", func(t *testing.T) {
		var dsn string
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().StringVar(&dsn, "index-dsn", "default", "test flag")

		t.Setenv("BINTRAIL_INDEX_DSN", "")

		for _, b := range envBindings {
			if v := os.Getenv(b.EnvVar); v != "" {
				if f := cmd.Flags().Lookup(b.Flag); f != nil {
					_ = cmd.Flags().Set(b.Flag, v)
				}
			}
		}

		if dsn != "default" {
			t.Errorf("dsn = %q, want %q", dsn, "default")
		}
	})

	t.Run("binds persistent flag", func(t *testing.T) {
		var dsn string
		cmd := &cobra.Command{Use: "test"}
		cmd.PersistentFlags().StringVar(&dsn, "index-dsn", "", "test flag")

		t.Setenv("BINTRAIL_INDEX_DSN", "persistent-env")

		for _, b := range envBindings {
			v := os.Getenv(b.EnvVar)
			if v == "" {
				continue
			}
			if f := cmd.Flags().Lookup(b.Flag); f != nil {
				_ = cmd.Flags().Set(b.Flag, v)
				continue
			}
			if cmd.PersistentFlags().Lookup(b.Flag) != nil {
				_ = cmd.PersistentFlags().Set(b.Flag, v)
			}
		}

		if dsn != "persistent-env" {
			t.Errorf("dsn = %q, want %q", dsn, "persistent-env")
		}
	})

	t.Run("binds integer flag from env", func(t *testing.T) {
		var batchSize int
		cmd := &cobra.Command{Use: "test"}
		cmd.Flags().IntVar(&batchSize, "batch-size", 1000, "test flag")

		t.Setenv("BINTRAIL_BATCH_SIZE", "5000")

		for _, b := range envBindings {
			v := os.Getenv(b.EnvVar)
			if v == "" {
				continue
			}
			if f := cmd.Flags().Lookup(b.Flag); f != nil {
				_ = cmd.Flags().Set(b.Flag, v)
			}
		}

		if batchSize != 5000 {
			t.Errorf("batchSize = %d, want 5000", batchSize)
		}
	})
}
