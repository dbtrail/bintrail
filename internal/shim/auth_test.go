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
	_, err = NewTenantAuth([]string{})
	if err == nil {
		t.Fatal("expected error for empty users")
	}
	_, err = NewTenantAuth([]string{"", ""})
	if err == nil {
		t.Fatal("expected error for users that are all blank")
	}
}

func TestTenantAuthCheckUsername(t *testing.T) {
	a, err := NewTenantAuth([]string{"alice", "bob"})
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

func TestTenantAuthGetCredentialOnlyForKnownUsers(t *testing.T) {
	a, _ := NewTenantAuth([]string{"alice"})
	_, found, err := a.GetCredential("alice")
	if err != nil || !found {
		t.Errorf("GetCredential(alice): found=%v err=%v", found, err)
	}
	_, found, _ = a.GetCredential("eve")
	if found {
		t.Error("GetCredential(eve) should not be found")
	}
}

func TestLoadTenantUsersHappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shim.yaml")
	body := `listen: ':3308'
tenants:
  - server_id: '1'
    source_dsn: 'u:p@tcp(db:3306)/x'
    agent_url: 'http://localhost:8600'
    agent_token: 'btk_a'
    mysql_user: alice
    mysql_pass_sha1: '*ABC'
  - server_id: '2'
    source_dsn: 'u:p@tcp(db:3306)/y'
    agent_url: 'http://localhost:8600'
    agent_token: 'btk_b'
    mysql_user: bob
    mysql_pass_sha1: '*DEF'
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	users, err := LoadTenantUsers(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 || users[0] != "alice" || users[1] != "bob" {
		t.Errorf("got %v, want [alice bob]", users)
	}
}

func TestLoadTenantUsersErrors(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file", func(t *testing.T) {
		_, err := LoadTenantUsers(filepath.Join(dir, "missing.yaml"))
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("no tenants", func(t *testing.T) {
		path := filepath.Join(dir, "empty.yaml")
		os.WriteFile(path, []byte("tenants: []\n"), 0o600)
		_, err := LoadTenantUsers(path)
		if err == nil || !strings.Contains(err.Error(), "no tenants") {
			t.Errorf("unexpected: %v", err)
		}
	})

	t.Run("tenant missing mysql_user", func(t *testing.T) {
		path := filepath.Join(dir, "user.yaml")
		os.WriteFile(path, []byte("tenants:\n  - mysql_pass_sha1: '*ABC'\n"), 0o600)
		_, err := LoadTenantUsers(path)
		if err == nil || !strings.Contains(err.Error(), "mysql_user is empty") {
			t.Errorf("unexpected: %v", err)
		}
	})

	t.Run("strict YAML rejects typo", func(t *testing.T) {
		path := filepath.Join(dir, "typo.yaml")
		os.WriteFile(path, []byte("tenants:\n  - mysql_user_name: alice\n"), 0o600)
		_, err := LoadTenantUsers(path)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "mysql_user_name") {
			t.Errorf("error should name the unknown field, got %v", err)
		}
	})
}
