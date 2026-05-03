package shim

import (
	"errors"
	"fmt"
	"os"

	yaml "go.yaml.in/yaml/v2"
)

// TenantAuth implements server.CredentialProvider. It accepts only
// usernames that appear in the loaded shim.yaml and accepts any
// password for those users.
//
// Why password validation is intentionally weak: the shim's deployed
// position is behind ProxySQL on localhost. ProxySQL authenticates
// the application connection against `mysql_pass_sha1` (which the
// customer wrote into shim.yaml after running `bintrail init-shim`,
// since init-shim only emits TODO comments for that field) and only
// then forwards the connection to the shim's hostgroup. Re-validating
// the password at the shim would require implementing the
// mysql_native_password challenge against the same `*HEX` value
// ProxySQL already verified — duplicate work for no extra security.
//
// The username allowlist is the defense-in-depth layer: even if a
// caller bypasses ProxySQL and connects to :3308 directly, only
// usernames the operator declared in shim.yaml are accepted. This
// is materially better than the previous AcceptAuth, which let any
// connection in.
type TenantAuth struct {
	users map[string]struct{}
}

// NewTenantAuth builds a TenantAuth from the usernames in shim.yaml's
// tenant list. An empty users list returns an error rather than
// silently accepting nothing — the customer almost certainly
// misconfigured.
func NewTenantAuth(users []string) (TenantAuth, error) {
	if len(users) == 0 {
		return TenantAuth{}, errors.New("shim: no mysql_user values in shim.yaml; cannot start with empty allowlist")
	}
	m := make(map[string]struct{}, len(users))
	for _, u := range users {
		if u == "" {
			continue
		}
		m[u] = struct{}{}
	}
	if len(m) == 0 {
		return TenantAuth{}, errors.New("shim: every tenant in shim.yaml has an empty mysql_user")
	}
	return TenantAuth{users: m}, nil
}

// CheckUsername implements server.CredentialProvider.
func (a TenantAuth) CheckUsername(u string) (bool, error) {
	_, ok := a.users[u]
	return ok, nil
}

// GetCredential implements server.CredentialProvider. Returns the
// empty plaintext + found=true so the wire-protocol challenge
// succeeds against any password the client sends. CheckUsername has
// already gated this on the username being in the shim.yaml
// allowlist.
func (a TenantAuth) GetCredential(u string) (password string, found bool, err error) {
	_, ok := a.users[u]
	return "", ok, nil
}

// LoadTenantUsers reads shim.yaml from path and returns the
// non-empty mysql_user values declared on each tenant. Used by the
// `bintrail shim` subcommand to construct a TenantAuth at startup.
//
// This intentionally duplicates a small slice of cmd/bintrail/
// proxysql_config.go's shim.yaml parser rather than importing it,
// because internal/shim must not depend on cmd/bintrail. A future
// refactor can lift the shared reader into this package.
func LoadTenantUsers(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	// AgentURL / AgentToken are retained here even though `bintrail
	// init-shim` no longer emits them: they appear in shim.yaml files
	// scaffolded by older bintrail versions, and yaml.UnmarshalStrict
	// rejects unknown keys. Declaring them keeps customer files from
	// earlier versions parsing cleanly during the migration window.
	var cfg struct {
		Tenants []struct {
			ServerID      string `yaml:"server_id"`
			SourceDSN     string `yaml:"source_dsn"`
			AgentURL      string `yaml:"agent_url"`
			AgentToken    string `yaml:"agent_token"`
			MySQLUser     string `yaml:"mysql_user"`
			MySQLPassSHA1 string `yaml:"mysql_pass_sha1"`
		} `yaml:"tenants"`
		Listen string `yaml:"listen"`
	}
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("%s has no tenants", path)
	}
	users := make([]string, 0, len(cfg.Tenants))
	for i, t := range cfg.Tenants {
		if t.MySQLUser == "" {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user is empty (uncomment and fill in the TODO line)", path, i+1)
		}
		users = append(users, t.MySQLUser)
	}
	return users, nil
}
