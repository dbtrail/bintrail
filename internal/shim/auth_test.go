package shim

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewTenantAuthEmpty(t *testing.T) {
	_, err := NewTenantAuth(nil)
	if err == nil {
		t.Fatal("expected error for nil users")
	}
	_, err = NewTenantAuth(map[string]string{})
	if err == nil {
		t.Fatal("expected error for empty users")
	}
	_, err = NewTenantAuth(map[string]string{"": "p"})
	if err == nil {
		t.Fatal("expected error for users that are all blank")
	}
}

// TestNewTenantAuthRejectsEmptyPassword pins the #254 defense-in-depth
// invariant: even a caller that bypasses LoadTenants must not be able
// to construct a TenantAuth that authenticates known usernames with
// any password. Empty cleartext + mysql_native_password handshake =
// "any password accepted" — the exact bug v0.7.0 / 0.7.1 shipped.
func TestNewTenantAuthRejectsEmptyPassword(t *testing.T) {
	_, err := NewTenantAuth(map[string]string{"alice": ""})
	if err == nil {
		t.Fatal("expected error for empty password")
	}
	if !strings.Contains(err.Error(), "empty mysql_password") {
		t.Errorf("error should explicitly name the missing field, got %v", err)
	}
}

func TestTenantAuthCheckUsername(t *testing.T) {
	a, err := NewTenantAuth(map[string]string{"alice": "alicepw", "bob": "bobpw"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"alice", true},
		{"bob", true},
		{"carol", false},
		{"", false},
	} {
		got, _ := a.CheckUsername(tc.name)
		if got != tc.want {
			t.Errorf("CheckUsername(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestTenantAuthGetCredentialReturnsCleartext(t *testing.T) {
	a, _ := NewTenantAuth(map[string]string{"alice": "alicepw"})
	pw, found, err := a.GetCredential("alice")
	if err != nil || !found {
		t.Errorf("GetCredential(alice): found=%v err=%v", found, err)
	}
	if pw != "alicepw" {
		t.Errorf("GetCredential(alice) password = %q, want %q", pw, "alicepw")
	}
	_, found, _ = a.GetCredential("eve")
	if found {
		t.Error("GetCredential(eve) should not be found")
	}
}

func TestLoadTenantsHappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shim.yaml")
	body := `listen: ':3308'
tenants:
  - server_id: '1'
    source_dsn: 'u:p@tcp(db:3306)/x'
    agent_url: 'http://localhost:8600'
    agent_token: 'btk_a'
    mysql_user: alice
    mysql_password: 'alicepw'
  - server_id: '2'
    source_dsn: 'u:p@tcp(db:3306)/y'
    agent_url: 'http://localhost:8600'
    agent_token: 'btk_b'
    mysql_user: bob
    mysql_password: 'bobpw'
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	users, err := LoadTenants(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 || users["alice"] != "alicepw" || users["bob"] != "bobpw" {
		t.Errorf("got %v, want {alice:alicepw bob:bobpw}", users)
	}
}

func TestLoadTenantsErrors(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file", func(t *testing.T) {
		_, err := LoadTenants(filepath.Join(dir, "missing.yaml"))
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("no tenants", func(t *testing.T) {
		path := filepath.Join(dir, "empty.yaml")
		os.WriteFile(path, []byte("tenants: []\n"), 0o600)
		_, err := LoadTenants(path)
		if err == nil || !strings.Contains(err.Error(), "no tenants") {
			t.Errorf("unexpected: %v", err)
		}
	})

	t.Run("tenant missing mysql_user", func(t *testing.T) {
		path := filepath.Join(dir, "user.yaml")
		os.WriteFile(path, []byte("tenants:\n  - mysql_password: 'p'\n"), 0o600)
		_, err := LoadTenants(path)
		if err == nil || !strings.Contains(err.Error(), "mysql_user is empty") {
			t.Errorf("unexpected: %v", err)
		}
	})

	t.Run("tenant missing mysql_password", func(t *testing.T) {
		path := filepath.Join(dir, "pw.yaml")
		os.WriteFile(path, []byte("tenants:\n  - mysql_user: alice\n"), 0o600)
		_, err := LoadTenants(path)
		if err == nil || !strings.Contains(err.Error(), "mysql_password is empty") {
			t.Errorf("unexpected: %v", err)
		}
	})

	t.Run("legacy mysql_pass_sha1 alone rejected", func(t *testing.T) {
		// Operators upgrading from 0.7.0 / 0.7.1 see this. The error
		// names the migration path explicitly so they don't have to
		// dig through the changelog.
		path := filepath.Join(dir, "legacy.yaml")
		os.WriteFile(path, []byte("tenants:\n  - mysql_user: alice\n    mysql_pass_sha1: '*ABC'\n"), 0o600)
		_, err := LoadTenants(path)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "mysql_password is required") {
			t.Errorf("error should explain the migration, got %v", err)
		}
		// Pin the migration recipe: a future shortening to "use
		// mysql_password" would lose the operator-helpful "Replace
		// mysql_pass_sha1" guidance.
		if !strings.Contains(err.Error(), "Replace mysql_pass_sha1") {
			t.Errorf("error should give the literal replacement hint, got %v", err)
		}
	})

	t.Run("strict YAML rejects typo", func(t *testing.T) {
		path := filepath.Join(dir, "typo.yaml")
		os.WriteFile(path, []byte("tenants:\n  - mysql_user_name: alice\n"), 0o600)
		_, err := LoadTenants(path)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "mysql_user_name") {
			t.Errorf("error should name the unknown field, got %v", err)
		}
	})
}

// TestLoadTenantsBothFieldsSet pins the contract that when an operator
// has both `mysql_password` (new) and `mysql_pass_sha1` (legacy) in
// shim.yaml — typically during a half-completed migration — the
// cleartext wins and the SHA1 is dropped. A future "compat" refactor
// that flipped the priority (e.g. "if SHA1 is present, prefer it for
// backward compat") would silently re-introduce #254 because the
// shim cannot use the SHA1 to authenticate ProxySQL-forwarded
// connections.
func TestLoadTenantsBothFieldsSet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shim.yaml")
	body := `tenants:
  - mysql_user: alice
    mysql_password: 'cleartext_alice'
    mysql_pass_sha1: '*STALEHASH'
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	users, err := LoadTenants(path)
	if err != nil {
		t.Fatalf("expected success when both fields set; got %v", err)
	}
	if got := users["alice"]; got != "cleartext_alice" {
		t.Errorf("expected cleartext to win; got %q", got)
	}
}
