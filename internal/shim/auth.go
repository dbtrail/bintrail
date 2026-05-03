package shim

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	yaml "go.yaml.in/yaml/v2"
)

// TenantAuth implements server.CredentialProvider against the
// per-tenant cleartext password stored in shim.yaml's mysql_password
// field. Both halves of the auth flow validate against this same
// value: ProxySQL's frontend (which derives mysql_pass_sha1 from it
// via bintrail proxysql-config) and the shim's backend (which hands
// the cleartext to go-mysql/server's mysql_native_password handshake).
//
// Why the shim now stores cleartext: go-mysql/server's
// CredentialProvider API requires the cleartext password to drive its
// challenge/response check. Storing only mysql_pass_sha1 (as earlier
// versions did) made every ProxySQL → shim connection fail because
// the library could not validate ProxySQL-forwarded auth without the
// cleartext. shim.yaml is operator-owned and 0o600; it already holds
// source_dsn (which contains a MySQL password), so adding the tenant
// cleartext alongside is a no-op against the existing trust model.
//
// Validation is real (not "any password accepted"): the username must
// appear in the tenants block AND the client's wire-protocol response
// must scramble against the stored cleartext. ProxySQL is still the
// outer gate — the shim's role is to make sure direct connections to
// :3308 are not silently authenticated by virtue of the username
// alone.
type TenantAuth struct {
	users map[string]string // username → cleartext mysql_password
}

// NewTenantAuth builds a TenantAuth from a map of username →
// cleartext password. An empty map, or any tenant with an empty
// password, returns an error.
//
// Empty passwords are rejected at the type boundary (not just at the
// LoadTenants entry point) because GetCredential returns whatever is
// in the map: an empty string here would make the
// mysql_native_password handshake accept any client that also sent
// an empty password — recreating the exact #254 silent-auth bug for
// any future caller that bypasses LoadTenants. Pushing the invariant
// down into the type means tests and library callers cannot
// accidentally weaken the auth model.
func NewTenantAuth(users map[string]string) (TenantAuth, error) {
	if len(users) == 0 {
		return TenantAuth{}, errors.New("shim: no tenants in shim.yaml; cannot start with empty allowlist")
	}
	clean := make(map[string]string, len(users))
	for u, p := range users {
		if u == "" {
			continue
		}
		if p == "" {
			return TenantAuth{}, fmt.Errorf("shim: tenant %q has empty mysql_password; refusing to start with effective auth bypass", u)
		}
		clean[u] = p
	}
	if len(clean) == 0 {
		return TenantAuth{}, errors.New("shim: every tenant in shim.yaml has an empty mysql_user")
	}
	return TenantAuth{users: clean}, nil
}

// CheckUsername implements server.CredentialProvider.
func (a TenantAuth) CheckUsername(u string) (bool, error) {
	_, ok := a.users[u]
	return ok, nil
}

// GetCredential implements server.CredentialProvider. Returns the
// stored cleartext password so go-mysql/server's mysql_native_password
// handshake can validate the client's scrambled response against it.
func (a TenantAuth) GetCredential(u string) (password string, found bool, err error) {
	p, ok := a.users[u]
	return p, ok, nil
}

// LoadTenants reads shim.yaml from path and returns username →
// cleartext password for each tenant. Used by `bintrail shim` to
// construct a TenantAuth at startup.
//
// Strict YAML parsing rejects unknown fields, so legacy fields kept
// in the struct (agent_url, agent_token from < 0.7.0; mysql_pass_sha1
// from < 0.7.2) parse cleanly even though they are no longer
// load-bearing. Operators upgrading from 0.7.1 see a runtime warning
// pointing at mysql_password.
func LoadTenants(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg struct {
		Tenants []struct {
			ServerID      string `yaml:"server_id"`
			SourceDSN     string `yaml:"source_dsn"`
			AgentURL      string `yaml:"agent_url"`
			AgentToken    string `yaml:"agent_token"`
			MySQLUser     string `yaml:"mysql_user"`
			MySQLPassword string `yaml:"mysql_password"`
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
	users := make(map[string]string, len(cfg.Tenants))
	for i, t := range cfg.Tenants {
		if t.MySQLUser == "" {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user is empty (uncomment and fill in the TODO line)", path, i+1)
		}
		if t.MySQLPassword == "" {
			if t.MySQLPassSHA1 != "" {
				return nil, fmt.Errorf(
					"%s tenant #%d (mysql_user=%s): mysql_password is required; mysql_pass_sha1 alone is no longer accepted (>= 0.7.2). "+
						"Replace mysql_pass_sha1 with mysql_password: '<cleartext>' — the SHA1 is recomputed by `bintrail proxysql-config`",
					path, i+1, t.MySQLUser)
			}
			return nil, fmt.Errorf("%s tenant #%d (mysql_user=%s): mysql_password is empty (set the cleartext password your application uses to connect)", path, i+1, t.MySQLUser)
		}
		if t.MySQLPassSHA1 != "" {
			slog.Warn(
				"shim.yaml: mysql_pass_sha1 is no longer used by `bintrail shim` (the SHA1 is now recomputed by `bintrail proxysql-config` from mysql_password); the field is ignored",
				"tenant", i+1, "mysql_user", t.MySQLUser,
			)
		}
		users[t.MySQLUser] = t.MySQLPassword
	}
	return users, nil
}
