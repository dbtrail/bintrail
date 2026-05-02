package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	pcTestSourceDSN = "user:pass@tcp(db.example.com:3306)/myapp"
	pcTestUser1     = "app_user"
	pcTestSHA1_1    = "*A4B6157319038724E3560894F7F932C8886EBFCF"
)

func writeShimYAML(t *testing.T, dir string, body string) string {
	t.Helper()
	path := filepath.Join(dir, "shim.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func resetPCFlags() {
	pcOut = "proxysql-setup.sql"
	pcShimConfig = "shim.yaml"
	pcMySQLPort = 3306
	pcShimPort = 3308
	pcProxySQLMySQLPort = 6033
}

const validShimYAML = `listen: ':3308'
tenants:
  - server_id: '1'
    source_dsn: 'user:pass@tcp(db:3306)/myapp'
    agent_url: 'http://localhost:8600'
    agent_token: 'btk_abc'
    mysql_user: app_user
    mysql_pass_sha1: '*A4B6157319038724E3560894F7F932C8886EBFCF'
`

func TestRunProxySQLConfig(t *testing.T) {
	t.Run("happy path single tenant", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		resetPCFlags()

		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatal(err)
		}

		data, err := os.ReadFile(filepath.Join(dir, "proxysql-setup.sql"))
		if err != nil {
			t.Fatalf("expected output file: %v", err)
		}
		out := string(data)

		wants := []string{
			"-- Bintrail BYOS time-travel SQL",
			"docs/byos-time-travel-sql.md",
			"BEGIN;",
			"DELETE FROM mysql_servers WHERE hostgroup_id IN (990, 991);",
			"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (990, 'db.example.com', 3306);",
			"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (991, '127.0.0.1', 3308);",
			"DELETE FROM mysql_users WHERE default_hostgroup = 990;",
			"INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('app_user', '*A4B6157319038724E3560894F7F932C8886EBFCF', 990, 1);",
			"DELETE FROM mysql_query_rules WHERE rule_id IN (990001, 990002, 990003);",
			"VALUES (990001, 1, '\\b_flashback\\.', 991, 1);",
			"VALUES (990002, 1, '\\b_diff\\.', 991, 1);",
			"VALUES (990003, 1, '\\b_snapshot\\.', 991, 1);",
			"COMMIT;",
			"LOAD MYSQL SERVERS TO RUNTIME;",
			"LOAD MYSQL USERS TO RUNTIME;",
			"LOAD MYSQL QUERY RULES TO RUNTIME;",
			"SAVE MYSQL SERVERS TO DISK;",
			"SAVE MYSQL USERS TO DISK;",
			"SAVE MYSQL QUERY RULES TO DISK;",
		}
		for _, w := range wants {
			if !strings.Contains(out, w) {
				t.Errorf("output missing %q; full output:\n%s", w, out)
			}
		}

		info, _ := os.Stat(filepath.Join(dir, "proxysql-setup.sql"))
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("perm = %o, want 0600", perm)
		}
	})

	t.Run("happy path two tenants", func(t *testing.T) {
		two := validShimYAML + `  - server_id: '2'
    source_dsn: 'user:pass@tcp(db:3306)/myapp2'
    agent_url: 'http://localhost:8600'
    agent_token: 'btk_xyz'
    mysql_user: app_user2
    mysql_pass_sha1: '*BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB'
`
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, two)
		resetPCFlags()

		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatal(err)
		}

		data, _ := os.ReadFile(filepath.Join(dir, "proxysql-setup.sql"))
		out := string(data)

		if !strings.Contains(out, "DELETE FROM mysql_users WHERE default_hostgroup = 990;") {
			t.Errorf("expected hostgroup-scoped DELETE; got:\n%s", out)
		}
		if !strings.Contains(out, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('app_user', ") {
			t.Error("expected INSERT for app_user")
		}
		if !strings.Contains(out, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('app_user2', ") {
			t.Error("expected INSERT for app_user2")
		}
	})

	t.Run("error when source DSN missing", func(t *testing.T) {
		t.Setenv("BINTRAIL_SOURCE_DSN", "")
		resetPCFlags()
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "BINTRAIL_SOURCE_DSN") {
			t.Errorf("error should name the env var, got %v", err)
		}
	})

	t.Run("error when DSN invalid", func(t *testing.T) {
		t.Setenv("BINTRAIL_SOURCE_DSN", "not-a-valid-dsn-format")
		resetPCFlags()
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error for invalid DSN")
		}
		if !strings.Contains(err.Error(), "BINTRAIL_SOURCE_DSN") {
			t.Errorf("error should mention the var, got %v", err)
		}
	})

	t.Run("error when DSN uses unix socket", func(t *testing.T) {
		t.Setenv("BINTRAIL_SOURCE_DSN", "user:pass@unix(/tmp/mysql.sock)/myapp")
		resetPCFlags()
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error for unix socket")
		}
		if !strings.Contains(err.Error(), "unix socket") {
			t.Errorf("error should mention unix socket, got %v", err)
		}
	})

	t.Run("error when shim.yaml missing", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error when shim.yaml missing")
		}
		if !strings.Contains(err.Error(), "shim config not found") {
			t.Errorf("expected 'shim config not found' in error, got %v", err)
		}
	})

	t.Run("error when tenant missing mysql_user", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants:\n  - mysql_pass_sha1: '*ABC'\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "mysql_user") {
			t.Errorf("error should name the missing field, got %v", err)
		}
	})

	t.Run("error when tenant missing mysql_pass_sha1", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants:\n  - mysql_user: app_user\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "mysql_pass_sha1") {
			t.Errorf("error should name the missing field, got %v", err)
		}
	})

	t.Run("error when shim.yaml has no tenants", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants: []\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "no tenants") {
			t.Errorf("expected 'no tenants' in error, got %v", err)
		}
	})

	t.Run("error when tenant credential contains newline", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants:\n  - mysql_user: \"bad\\nuser\"\n    mysql_pass_sha1: '*ABC'\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error for newline")
		}
		if !strings.Contains(err.Error(), "control character") {
			t.Errorf("expected 'control character' in error, got %v", err)
		}
	})

	t.Run("--out - writes to stdout", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		resetPCFlags()
		pcOut = "-"

		r, w, _ := os.Pipe()
		origStdout := os.Stdout
		os.Stdout = w
		t.Cleanup(func() { os.Stdout = origStdout })

		done := make(chan []byte)
		go func() {
			var buf bytes.Buffer
			io.Copy(&buf, r)
			done <- buf.Bytes()
		}()

		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatal(err)
		}
		w.Close()

		out := string(<-done)
		if !strings.Contains(out, "INSERT INTO mysql_users") {
			t.Errorf("stdout missing expected SQL:\n%s", out)
		}
	})

	t.Run("refuses to overwrite", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		os.WriteFile(filepath.Join(dir, "proxysql-setup.sql"), []byte("existing"), 0o644)
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error when output file exists")
		}
		if !strings.Contains(err.Error(), "already exists") {
			t.Errorf("expected 'already exists' in error, got %v", err)
		}
	})
}

func TestGenerateProxySQLSetupSQLDeterministic(t *testing.T) {
	tenants := []shimTenant{{MySQLUser: pcTestUser1, MySQLPassSHA1: pcTestSHA1_1}}
	a := generateProxySQLSetupSQL("db.example.com", 3306, 3308, 6033, tenants)
	b := generateProxySQLSetupSQL("db.example.com", 3306, 3308, 6033, tenants)
	if a != b {
		t.Errorf("generateProxySQLSetupSQL must be deterministic; got two different outputs")
	}
}

func TestGenerateProxySQLSetupSQLSQLInjection(t *testing.T) {
	// A user/password containing single quotes must be safely escaped.
	tenants := []shimTenant{
		{MySQLUser: "ev'il", MySQLPassSHA1: "*A'B"},
	}
	out := generateProxySQLSetupSQL("db", 3306, 3308, 6033, tenants)

	want := "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('ev''il', '*A''B', 990, 1);"
	if !strings.Contains(out, want) {
		t.Errorf("expected escaped SQL %q; got:\n%s", want, out)
	}
}

// TestGenerateProxySQLSetupSQLRenameIdempotent verifies that renaming a
// tenant in shim.yaml between runs leaves no orphan row: the second
// run's DELETE WHERE default_hostgroup = 990 catches the previous
// tenant's row even though its username is no longer in the current
// list. This locks in the design rationale for scoping the DELETE by
// hostgroup rather than by username.
func TestGenerateProxySQLSetupSQLRenameIdempotent(t *testing.T) {
	first := generateProxySQLSetupSQL("db", 3306, 3308, 6033,
		[]shimTenant{{MySQLUser: "old_user", MySQLPassSHA1: "*OLDHASH"}})
	second := generateProxySQLSetupSQL("db", 3306, 3308, 6033,
		[]shimTenant{{MySQLUser: "new_user", MySQLPassSHA1: "*NEWHASH"}})

	// Both runs emit the same blanket DELETE, scoped only by hostgroup,
	// so the second apply also removes 'old_user' even though the name
	// no longer appears anywhere in the second SQL file.
	wantDelete := "DELETE FROM mysql_users WHERE default_hostgroup = 990;"
	if !strings.Contains(first, wantDelete) || !strings.Contains(second, wantDelete) {
		t.Errorf("both runs must contain hostgroup-scoped DELETE %q", wantDelete)
	}
	if strings.Contains(second, "old_user") {
		t.Error("second-run SQL must not reference the renamed-away tenant")
	}
	if !strings.Contains(second, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('new_user',") {
		t.Errorf("second-run SQL must INSERT the new tenant; got:\n%s", second)
	}
}

// TestGenerateProxySQLSetupSQLHostgroupPairing locks in the
// destination_hostgroup for each rule_id so a future swap of
// `passthroughHostgroup` and `shimHostgroup` would be caught even if
// individual fragment assertions still pass.
func TestGenerateProxySQLSetupSQLHostgroupPairing(t *testing.T) {
	tenants := []shimTenant{{MySQLUser: pcTestUser1, MySQLPassSHA1: pcTestSHA1_1}}
	out := generateProxySQLSetupSQL("db", 3306, 3308, 6033, tenants)

	wants := []string{
		// passthrough server lives in passthrough hostgroup
		"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (990,",
		// shim server lives in shim hostgroup
		"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (991,",
		// users default to passthrough hostgroup (real MySQL by default)
		"default_hostgroup, active) VALUES ('app_user', '" + pcTestSHA1_1 + "', 990, 1);",
		// virtual-schema rules route to shim hostgroup, never passthrough
		"VALUES (990001, 1, '\\b_flashback\\.', 991, 1);",
		"VALUES (990002, 1, '\\b_diff\\.', 991, 1);",
		"VALUES (990003, 1, '\\b_snapshot\\.', 991, 1);",
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("hostgroup pairing missing %q; full SQL:\n%s", w, out)
		}
	}
	// And explicitly: no rule should ever route a virtual schema to the
	// passthrough hostgroup.
	for _, bad := range []string{
		"VALUES (990001, 1, '\\b_flashback\\.', 990, 1)",
		"VALUES (990002, 1, '\\b_diff\\.', 990, 1)",
		"VALUES (990003, 1, '\\b_snapshot\\.', 990, 1)",
	} {
		if strings.Contains(out, bad) {
			t.Errorf("virtual-schema rule must not target passthrough hostgroup, found %q", bad)
		}
	}
}

func TestRunProxySQLConfigStrictYAML(t *testing.T) {
	// A typo in shim.yaml (mysql_user_name vs mysql_user) used to silently
	// parse as empty, surfacing as the misleading "mysql_user is empty" error.
	// UnmarshalStrict now reports the unknown key directly.
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, "tenants:\n  - mysql_user_name: app_user\n    mysql_pass_sha1: '*ABC'\n")
	resetPCFlags()

	err := runProxySQLConfig(proxysqlConfigCmd, nil)
	if err == nil {
		t.Fatal("expected error for unknown YAML field")
	}
	if !strings.Contains(err.Error(), "mysql_user_name") {
		t.Errorf("error should name the unknown field, got %v", err)
	}
}

func TestParseProxySQLBackendIPv6(t *testing.T) {
	// Bracketed IPv6 with port.
	host, port, err := parseProxySQLBackend("u:p@tcp([2001:db8::1]:3306)/x", 3306)
	if err != nil {
		t.Fatal(err)
	}
	if host != "2001:db8::1" {
		t.Errorf("got host %q, want '2001:db8::1' (without brackets)", host)
	}
	if port != 3306 {
		t.Errorf("got port %d", port)
	}
}

func TestParseProxySQLBackendEmptyHost(t *testing.T) {
	_, _, err := parseProxySQLBackend("u:p@tcp(:3306)/x", 3306)
	if err == nil {
		t.Fatal("expected error for empty host")
	}
	if !strings.Contains(err.Error(), "empty host") {
		t.Errorf("expected 'empty host' in error, got %v", err)
	}
}

func TestRunProxySQLConfigPortRangeValidation(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)

	t.Run("zero port rejected", func(t *testing.T) {
		resetPCFlags()
		pcMySQLPort = 0
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil || !strings.Contains(err.Error(), "out of range") {
			t.Errorf("expected out-of-range error for port 0, got %v", err)
		}
	})

	t.Run("uint16-overflow port rejected", func(t *testing.T) {
		// 70000 used to silently truncate to uint16 → 4464, generating broken SQL.
		resetPCFlags()
		pcShimPort = 70000
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil || !strings.Contains(err.Error(), "out of range") {
			t.Errorf("expected out-of-range error for port 70000, got %v", err)
		}
	})
}

func TestLoadShimTenantsControlChars(t *testing.T) {
	// Reject control chars beyond plain \r\n: \t in mysql_user, \0 in
	// mysql_pass_sha1. Both would corrupt the generated SQL output.
	cases := []struct {
		name     string
		yamlBody string
		wantSub  string
	}{
		{
			name:     "tab in mysql_user",
			yamlBody: "tenants:\n  - mysql_user: \"app\\tuser\"\n    mysql_pass_sha1: '*ABC'\n",
			wantSub:  "mysql_user contains control character",
		},
		{
			name:     "null byte in pass",
			yamlBody: "tenants:\n  - mysql_user: app_user\n    mysql_pass_sha1: \"*A\\u0000B\"\n",
			wantSub:  "mysql_pass_sha1 contains control character",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeShimYAML(t, dir, tc.yamlBody)
			_, err := loadShimTenants(path)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("expected %q in error, got %v", tc.wantSub, err)
			}
		})
	}
}

func TestParseProxySQLBackend(t *testing.T) {
	t.Run("DSN with port", func(t *testing.T) {
		host, port, err := parseProxySQLBackend("u:p@tcp(db.example.com:3307)/x", 3306)
		if err != nil {
			t.Fatal(err)
		}
		if host != "db.example.com" || port != 3307 {
			t.Errorf("got %s:%d", host, port)
		}
	})

	t.Run("DSN missing port falls back to flag", func(t *testing.T) {
		// go-sql-driver normalises the address to host:3306 if port is missing,
		// but we still verify the fallback logic works for an addr without ':'.
		host, port, err := parseProxySQLBackend("u:p@tcp(db.example.com)/x", 3306)
		if err != nil {
			t.Fatal(err)
		}
		if host != "db.example.com" {
			t.Errorf("got host %s", host)
		}
		if port != 3306 {
			t.Errorf("got port %d, want 3306", port)
		}
	})
}
