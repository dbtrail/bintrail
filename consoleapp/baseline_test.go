package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestBuildConsoleMydumperArgs covers the schema-filter branches and the
// invariants the dump relies on: --outputdir last (docker wrappers read the
// final arg as the mount path) and — the #811 guard — that the password never
// appears on argv (runMydumper delivers it via MYSQL_PWD in the child env).
func TestBuildConsoleMydumperArgs(t *testing.T) {
	t.Run("consistent lock-free flags always present", func(t *testing.T) {
		// These let a least-privilege replication user (no RELOAD/FLUSH_TABLES)
		// dump consistently — verified against a real Percona 8.0 source. Their
		// absence is the bug that produced a schema-only dump.
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"x"}, "/out")
		if valueAfter(args, "--sync-thread-lock-mode") != "NO_LOCK" {
			t.Errorf("missing --sync-thread-lock-mode NO_LOCK: %v", args)
		}
		if !has(args, "--trx-tables") {
			t.Errorf("missing --trx-tables: %v", args)
		}
	})

	t.Run("no schema filter excludes system schemas", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", nil, "/out")
		if has(args, "--database") {
			t.Errorf("no schema filter must not use --database: %v", args)
		}
		// A least-privilege user can't read the sys views, so an unfiltered dump
		// dies; the no-filter case must exclude the system schemas instead.
		if v := valueAfter(args, "--regex"); v != systemSchemaExcludeRegex {
			t.Errorf("--regex = %q, want the system-schema exclusion: %v", v, args)
		}
		assertOutputdirLast(t, args, "/out")
	})

	t.Run("single schema uses --database", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"wordpress"}, "/out")
		if v := valueAfter(args, "--database"); v != "wordpress" {
			t.Errorf("--database = %q, want wordpress: %v", v, args)
		}
		if has(args, "--regex") {
			t.Errorf("single schema must not use --regex: %v", args)
		}
	})

	t.Run("multiple schemas use anchored --regex", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"a", "b"}, "/out")
		if v := valueAfter(args, "--regex"); v != "^(a|b)\\." {
			t.Errorf("--regex = %q, want ^(a|b)\\. : %v", v, args)
		}
		if has(args, "--database") {
			t.Errorf("multiple schemas must not use --database: %v", args)
		}
	})

	t.Run("password never appears on argv (#811)", func(t *testing.T) {
		for _, schemas := range [][]string{nil, {"wordpress"}, {"a", "b"}} {
			args := buildConsoleMydumperArgs("h", 3306, "u", schemas, "/out")
			if has(args, "--password") {
				t.Errorf("schemas=%v: --password must never appear on argv: %v", schemas, args)
			}
		}
	})
}

// TestRunMydumper_deliversPasswordViaEnvNotArgv is the end-to-end regression
// guard for #811: the console's in-process dump passes the source password to
// mydumper as MYSQL_PWD, never on argv (world-readable via ps aux / cmdline).
func TestRunMydumper_deliversPasswordViaEnvNotArgv(t *testing.T) {
	dir := t.TempDir()
	record := filepath.Join(dir, "record.txt")
	t.Setenv("BINTRAIL_TEST_RECORD", record)

	// Fake `mydumper` resolved via PATH. Uses only bash builtins (printf,
	// redirection) so it needs nothing else on PATH.
	fakeBin := filepath.Join(dir, "mydumper")
	script := `#!/bin/bash
printf 'ARGS: %s\n' "$*" > "$BINTRAIL_TEST_RECORD"
printf 'MYSQL_PWD=%s\n' "$MYSQL_PWD" >> "$BINTRAIL_TEST_RECORD"
exit 0
`
	if err := os.WriteFile(fakeBin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mydumper: %v", err)
	}
	t.Setenv("PATH", dir)

	const pw = "consolesecretpw"
	err := runMydumper(context.Background(), "root:"+pw+"@tcp(127.0.0.1:3306)/", nil, filepath.Join(dir, "out"))
	if err != nil {
		t.Fatalf("runMydumper: %v", err)
	}

	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	var argsLine, pwdLine string
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "ARGS: "):
			argsLine = line
		case strings.HasPrefix(line, "MYSQL_PWD="):
			pwdLine = line
		}
	}
	if strings.Contains(argsLine, pw) {
		t.Errorf("password leaked onto argv: %q", argsLine)
	}
	if strings.Contains(argsLine, "--password") {
		t.Errorf("--password must not appear on argv: %q", argsLine)
	}
	if pwdLine != "MYSQL_PWD="+pw {
		t.Errorf("password not delivered via MYSQL_PWD env: got %q", pwdLine)
	}
}

func has(args []string, flag string) bool { return slices.Contains(args, flag) }

func valueAfter(args []string, flag string) string {
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		return ""
	}
	return args[i+1]
}

func assertOutputdirLast(t *testing.T, args []string, want string) {
	t.Helper()
	n := len(args)
	if n < 2 || args[n-2] != "--outputdir" || args[n-1] != want {
		t.Errorf("--outputdir %q must be the last arg pair: ...%v", want, strings.Join(args[max(0, n-3):], " "))
	}
}
