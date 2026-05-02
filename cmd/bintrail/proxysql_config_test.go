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
			"DELETE FROM mysql_servers WHERE hostgroup_id IN (990, 991);",
			"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (990, 'db.example.com', 3306);",
			"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (991, '127.0.0.1', 3308);",
			"DELETE FROM mysql_users WHERE username IN ('app_user');",
			"INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('app_user', '*A4B6157319038724E3560894F7F932C8886EBFCF', 990, 1);",
			"DELETE FROM mysql_query_rules WHERE rule_id IN (990001, 990002, 990003);",
			"VALUES (990001, 1, '\\b_flashback\\.', 991, 1);",
			"VALUES (990002, 1, '\\b_diff\\.', 991, 1);",
			"VALUES (990003, 1, '\\b_snapshot\\.', 991, 1);",
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

		if !strings.Contains(out, "DELETE FROM mysql_users WHERE username IN ('app_user', 'app_user2');") {
			t.Errorf("expected combined DELETE for both users; got:\n%s", out)
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
		if !strings.Contains(err.Error(), "newline") {
			t.Errorf("expected 'newline' in error, got %v", err)
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

	wants := []string{
		"DELETE FROM mysql_users WHERE username IN ('ev''il');",
		"INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('ev''il', '*A''B', 990, 1);",
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("expected escaped SQL %q; got:\n%s", w, out)
		}
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
