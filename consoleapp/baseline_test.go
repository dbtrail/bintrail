package consoleapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestBuildConsoleMydumperArgs covers the schema-filter branches and the
// invariants the dump relies on: --outputdir last (docker wrappers read the
// final arg as the mount path) and — the #811 guard — that the password never
// appears on argv (runMydumper delivers it via MYSQL_PWD in the child env).
func TestBuildConsoleMydumperArgs(t *testing.T) {
	t.Run("consistent lock-free flags always present in default (NO_LOCK) mode", func(t *testing.T) {
		// These let a least-privilege replication user (no RELOAD/FLUSH_TABLES)
		// dump consistently — verified against a real Percona 8.0 source. Their
		// absence is the bug that produced a schema-only dump.
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"x"}, "/out", false)
		if valueAfter(args, "--sync-thread-lock-mode") != "NO_LOCK" {
			t.Errorf("missing --sync-thread-lock-mode NO_LOCK: %v", args)
		}
		if !has(args, "--trx-tables") {
			t.Errorf("missing --trx-tables: %v", args)
		}
	})

	// #800: the opt-in point-consistent mode swaps NO_LOCK for mydumper's FTWRL
	// sync mode, which barriers every worker's snapshot open at the same instant
	// — a single point-in-time snapshot across ALL tables, at the cost of
	// requiring the RELOAD/FLUSH_TABLES_WITH_READ_LOCK privilege. --trx-tables
	// stays present: it shortens the FTWRL hold for transactional tables and is
	// documented as compatible with any --sync-thread-lock-mode value.
	t.Run("point-consistent mode uses FTWRL", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"x"}, "/out", true)
		if valueAfter(args, "--sync-thread-lock-mode") != "FTWRL" {
			t.Errorf("missing --sync-thread-lock-mode FTWRL: %v", args)
		}
		if !has(args, "--trx-tables") {
			t.Errorf("missing --trx-tables: %v", args)
		}
		if has(args, "NO_LOCK") {
			t.Errorf("point-consistent mode must not pass NO_LOCK: %v", args)
		}
	})

	t.Run("no schema filter excludes system schemas", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", nil, "/out", false)
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
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"wordpress"}, "/out", false)
		if v := valueAfter(args, "--database"); v != "wordpress" {
			t.Errorf("--database = %q, want wordpress: %v", v, args)
		}
		if has(args, "--regex") {
			t.Errorf("single schema must not use --regex: %v", args)
		}
	})

	t.Run("multiple schemas use anchored --regex", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"a", "b"}, "/out", false)
		if v := valueAfter(args, "--regex"); v != "^(a|b)\\." {
			t.Errorf("--regex = %q, want ^(a|b)\\. : %v", v, args)
		}
		if has(args, "--database") {
			t.Errorf("multiple schemas must not use --database: %v", args)
		}
	})

	t.Run("password never appears on argv (#811)", func(t *testing.T) {
		for _, schemas := range [][]string{nil, {"wordpress"}, {"a", "b"}} {
			for _, pointConsistent := range []bool{false, true} {
				args := buildConsoleMydumperArgs("h", 3306, "u", schemas, "/out", pointConsistent)
				if has(args, "--password") {
					t.Errorf("schemas=%v pointConsistent=%v: --password must never appear on argv: %v", schemas, pointConsistent, args)
				}
			}
		}
	})
}

// TestDumpableTableCountQuery covers the pure query-building logic behind the
// NO_LOCK cross-table skew warning (#800): it must mirror
// buildConsoleMydumperArgs' own schema-selection branches so the advisory count
// approximates what mydumper will actually dump.
func TestDumpableTableCountQuery(t *testing.T) {
	t.Run("no schema filter excludes system schemas", func(t *testing.T) {
		query, args := dumpableTableCountQuery(nil)
		if args != nil {
			t.Errorf("args = %v, want nil", args)
		}
		if !strings.Contains(query, "NOT IN ('mysql','sys','performance_schema','information_schema')") {
			t.Errorf("query missing system-schema exclusion: %q", query)
		}
	})

	t.Run("single schema", func(t *testing.T) {
		query, args := dumpableTableCountQuery([]string{"wordpress"})
		if !slices.Equal(args, []any{"wordpress"}) {
			t.Errorf("args = %v, want [wordpress]", args)
		}
		if !strings.Contains(query, "TABLE_SCHEMA IN (?)") {
			t.Errorf("query missing single placeholder: %q", query)
		}
	})

	t.Run("multiple schemas", func(t *testing.T) {
		query, args := dumpableTableCountQuery([]string{"a", "b"})
		if !slices.Equal(args, []any{"a", "b"}) {
			t.Errorf("args = %v, want [a b]", args)
		}
		if !strings.Contains(query, "TABLE_SCHEMA IN (?,?)") {
			t.Errorf("query missing two placeholders: %q", query)
		}
	})
}

// TestCheckPointConsistentPrivileges covers the privilege-combination matrix
// established empirically against the pinned mydumper build (#800): BOTH
// BACKUP_ADMIN and RELOAD/FLUSH_TABLES are required, and — critically — having
// BACKUP_ADMIN without the other one must still be rejected here rather than
// let mydumper run, because that specific half-privileged combination segfaults
// mydumper instead of failing cleanly.
func TestCheckPointConsistentPrivileges(t *testing.T) {
	newMockWithPrivileges := func(t *testing.T, privileges []string) *sql.DB {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		rows := sqlmock.NewRows([]string{"PRIVILEGE_TYPE"})
		for _, p := range privileges {
			rows.AddRow(p)
		}
		mock.ExpectQuery("SELECT PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES").WillReturnRows(rows)
		return db
	}

	tests := []struct {
		name       string
		privileges []string
		wantErr    bool
		wantSubstr string
	}{
		{
			name:       "BACKUP_ADMIN + RELOAD: ok",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "BACKUP_ADMIN", "RELOAD"},
			wantErr:    false,
		},
		{
			name:       "BACKUP_ADMIN + FLUSH_TABLES (dynamic priv alternative to RELOAD): ok",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "BACKUP_ADMIN", "FLUSH_TABLES"},
			wantErr:    false,
		},
		{
			name:       "neither: clear error naming both",
			privileges: []string{"SELECT", "REPLICATION CLIENT"},
			wantErr:    true,
			wantSubstr: "BOTH the BACKUP_ADMIN and the RELOAD",
		},
		{
			// The dangerous half-privileged case: mydumper segfaults here rather
			// than failing cleanly, so this MUST be rejected before mydumper runs.
			name:       "BACKUP_ADMIN only, no RELOAD/FLUSH_TABLES: rejected (would segfault mydumper)",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "BACKUP_ADMIN"},
			wantErr:    true,
			wantSubstr: "requires the RELOAD (or FLUSH_TABLES) privilege",
		},
		{
			name:       "RELOAD only, no BACKUP_ADMIN: rejected",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "RELOAD"},
			wantErr:    true,
			wantSubstr: "requires the BACKUP_ADMIN privilege",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newMockWithPrivileges(t, tt.privileges)
			err := checkPointConsistentPrivilegesDB(context.Background(), db)
			if tt.wantErr && err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.wantSubstr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantSubstr)
			}
		})
	}
}

// TestCountDumpableTables verifies countDumpableTables runs the query built by
// dumpableTableCountQuery and scans the result, using sqlmock so no live MySQL is
// needed.
func TestCountDumpableTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.TABLES").
		WithArgs("mydb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	count, err := countDumpableTables(context.Background(), db, []string{"mydb"})
	if err != nil {
		t.Fatalf("countDumpableTables: %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
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
	// pointConsistent=false: true would hit checkPointConsistentPrivileges' HARD
	// gate, which needs a real, successful DB connection+query and would fail
	// against this fake setup (nothing listens on 127.0.0.1:3306), aborting
	// before the fake mydumper ever runs. false only triggers the best-effort
	// NO_LOCK skew warning, whose connection failure is swallowed (Debug-logged).
	err := runMydumper(context.Background(), "root:"+pw+"@tcp(127.0.0.1:3306)/", nil, filepath.Join(dir, "out"), false)
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
