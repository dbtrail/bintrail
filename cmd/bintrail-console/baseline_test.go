package main

import (
	"slices"
	"strings"
	"testing"
)

// TestBuildConsoleMydumperArgs covers the schema-filter branches and the
// invariants the dump relies on: a password only when present, and --outputdir
// last (docker wrappers read the final arg as the mount path).
func TestBuildConsoleMydumperArgs(t *testing.T) {
	t.Run("consistent lock-free flags always present", func(t *testing.T) {
		// These let a least-privilege replication user (no RELOAD/FLUSH_TABLES)
		// dump consistently — verified against a real Percona 8.0 source. Their
		// absence is the bug that produced a schema-only dump.
		args := buildConsoleMydumperArgs("h", 3306, "u", "pw", []string{"x"}, "/out")
		if valueAfter(args, "--sync-thread-lock-mode") != "NO_LOCK" {
			t.Errorf("missing --sync-thread-lock-mode NO_LOCK: %v", args)
		}
		if !has(args, "--trx-tables") {
			t.Errorf("missing --trx-tables: %v", args)
		}
	})

	t.Run("no schema filter excludes system schemas", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", "pw", nil, "/out")
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
		args := buildConsoleMydumperArgs("h", 3306, "u", "pw", []string{"wordpress"}, "/out")
		if v := valueAfter(args, "--database"); v != "wordpress" {
			t.Errorf("--database = %q, want wordpress: %v", v, args)
		}
		if has(args, "--regex") {
			t.Errorf("single schema must not use --regex: %v", args)
		}
	})

	t.Run("multiple schemas use anchored --regex", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", "pw", []string{"a", "b"}, "/out")
		if v := valueAfter(args, "--regex"); v != "^(a|b)\\." {
			t.Errorf("--regex = %q, want ^(a|b)\\. : %v", v, args)
		}
		if has(args, "--database") {
			t.Errorf("multiple schemas must not use --database: %v", args)
		}
	})

	t.Run("empty password omits --password", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", "", nil, "/out")
		if has(args, "--password") {
			t.Errorf("empty password must not add --password: %v", args)
		}
	})

	t.Run("password present adds --password", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", "secret", nil, "/out")
		if v := valueAfter(args, "--password"); v != "secret" {
			t.Errorf("--password = %q, want secret", v)
		}
	})
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
