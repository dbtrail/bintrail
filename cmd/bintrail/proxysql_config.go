package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"unicode"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	yaml "go.yaml.in/yaml/v2"
)

// Hostgroups and rule IDs are placed in the 990* range so they are
// extremely unlikely to collide with operator-managed ProxySQL config.
// The DELETE-then-INSERT pattern in the generated SQL only touches
// rows in this range plus the named tenant users.
const (
	passthroughHostgroup = 990
	shimHostgroup        = 991
	ruleIDFlashback      = 990001
	ruleIDDiff           = 990002
	ruleIDSnapshot       = 990003
)

var proxysqlConfigCmd = &cobra.Command{
	Use:   "proxysql-config",
	Short: "Generate ProxySQL setup SQL from .bintrail.env and shim.yaml",
	Long: `Emits a SQL script that, when applied to a ProxySQL admin port (default
6032), configures ProxySQL to route _flashback / _diff / _snapshot virtual
schemas to the dbtrail-shim hostgroup and everything else to the customer's
real MySQL.

Reads BINTRAIL_SOURCE_DSN from .bintrail.env (host of the passthrough
backend) and shim.yaml (tenant credentials). The SQL is idempotent —
re-running it produces the same final state.

Use --out - to write to stdout instead of a file.`,
	RunE: runProxySQLConfig,
}

var (
	pcOut               string
	pcShimConfig        string
	pcMySQLPort         uint
	pcShimPort          uint
	pcProxySQLMySQLPort uint
)

func init() {
	proxysqlConfigCmd.Flags().StringVar(&pcOut, "out", "proxysql-setup.sql", "Output path for the generated SQL (use - for stdout)")
	proxysqlConfigCmd.Flags().StringVar(&pcShimConfig, "shim-config", "shim.yaml", "Path to the shim.yaml produced by 'bintrail init-shim' and edited by you")
	proxysqlConfigCmd.Flags().UintVar(&pcMySQLPort, "mysql-port", 3306, "Fallback MySQL port if BINTRAIL_SOURCE_DSN does not include one")
	proxysqlConfigCmd.Flags().UintVar(&pcShimPort, "shim-port", 3308, "Port the dbtrail-shim is listening on (matches shim.yaml's listen)")
	proxysqlConfigCmd.Flags().UintVar(&pcProxySQLMySQLPort, "proxysql-mysql-port", 6033, "ProxySQL's client-facing MySQL protocol port (used in the help comment)")
	bindCommandEnv(proxysqlConfigCmd)
	rootCmd.AddCommand(proxysqlConfigCmd)
}

// shimTenant declares every field bintrail init-shim emits in a tenant
// block. The strict YAML decoder used by loadShimTenants requires the
// struct to know about every key — so we declare ServerID, SourceDSN,
// AgentURL, AgentToken (legacy, kept for parse-compat), and MySQLPassSHA1
// (legacy, kept for parse-compat) just to satisfy strict mode and let
// it catch real typos like "mysql_user_name".
type shimTenant struct {
	ServerID      string `yaml:"server_id"`
	SourceDSN     string `yaml:"source_dsn"`
	AgentURL      string `yaml:"agent_url"`
	AgentToken    string `yaml:"agent_token"`
	MySQLUser     string `yaml:"mysql_user"`
	MySQLPassword string `yaml:"mysql_password"`
	// MySQLPassSHA1 is the deprecated < 0.7.2 field. It used to be the
	// only way to declare a tenant's password, but the shim cannot use
	// it (go-mysql/server's mysql_native_password handler needs the
	// cleartext to validate the client's scrambled response). Now the
	// SHA1 is recomputed from MySQLPassword at proxysql-config time.
	// Kept here purely so UnmarshalStrict accepts shim.yaml files that
	// still have the old field; loadShimTenants emits a clear error
	// pointing to the migration path.
	MySQLPassSHA1 string `yaml:"mysql_pass_sha1"`
}

type shimConfig struct {
	Listen  string       `yaml:"listen"`
	Tenants []shimTenant `yaml:"tenants"`
}

func runProxySQLConfig(cmd *cobra.Command, args []string) error {
	sourceDSN := os.Getenv("BINTRAIL_SOURCE_DSN")
	if sourceDSN == "" {
		return fmt.Errorf("missing required env var: BINTRAIL_SOURCE_DSN\nRun 'bintrail config init' to scaffold .bintrail.env, then set this value.")
	}
	for _, p := range []struct {
		name string
		val  uint
	}{
		{"--mysql-port", pcMySQLPort},
		{"--shim-port", pcShimPort},
		{"--proxysql-mysql-port", pcProxySQLMySQLPort},
	} {
		if p.val == 0 || p.val > 65535 {
			return fmt.Errorf("%s=%d is out of range (1..65535)", p.name, p.val)
		}
	}

	host, port, err := parseProxySQLBackend(sourceDSN, uint16(pcMySQLPort))
	if err != nil {
		return err
	}

	tenants, err := loadShimTenants(pcShimConfig)
	if err != nil {
		return err
	}

	content := generateProxySQLSetupSQL(host, port, uint16(pcShimPort), uint16(pcProxySQLMySQLPort), tenants)

	if pcOut == "-" {
		_, err := io.WriteString(os.Stdout, content)
		return err
	}

	// O_EXCL closes the stat-then-write TOCTOU window: if the file
	// appears between our check and write, OpenFile errors out instead
	// of silently overwriting.
	f, err := os.OpenFile(pcOut, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("file already exists: %s\nRemove it first or edit it directly.", pcOut)
		}
		return fmt.Errorf("create %s: %w", pcOut, err)
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return fmt.Errorf("write %s: %w", pcOut, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", pcOut, err)
	}

	fmt.Printf("ProxySQL setup SQL written to %s\n", pcOut)
	fmt.Printf("Apply it: mysql -u admin -P 6032 -h <proxysql-host> < %s\n", pcOut)
	return nil
}

// parseProxySQLBackend extracts the host and port from a go-sql-driver
// DSN. Uses net.SplitHostPort so bracketed IPv6 addresses ("[::1]:3306")
// are handled correctly. If the DSN address has no port, fallbackPort
// is used; an empty host is rejected.
func parseProxySQLBackend(dsn string, fallbackPort uint16) (host string, port uint16, err error) {
	cfg, parseErr := drivermysql.ParseDSN(dsn)
	if parseErr != nil {
		return "", 0, fmt.Errorf("invalid BINTRAIL_SOURCE_DSN: %w", parseErr)
	}
	if strings.EqualFold(cfg.Net, "unix") {
		return "", 0, fmt.Errorf("BINTRAIL_SOURCE_DSN uses a unix socket; ProxySQL routing requires a TCP host:port")
	}
	addr := cfg.Addr
	if addr == "" {
		return "", 0, fmt.Errorf("BINTRAIL_SOURCE_DSN has no address")
	}
	h, p, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		// No port in addr — treat the whole thing as host (and reject if it
		// itself looks like a bracketed IPv6 with no port: "[::1]").
		h = strings.Trim(addr, "[]")
		p = ""
	}
	if h == "" {
		return "", 0, fmt.Errorf("BINTRAIL_SOURCE_DSN has an empty host: %q", addr)
	}
	if p == "" {
		return h, fallbackPort, nil
	}
	portN, convErr := strconv.ParseUint(p, 10, 16)
	if convErr != nil {
		return "", 0, fmt.Errorf("invalid port in BINTRAIL_SOURCE_DSN: %w", convErr)
	}
	return h, uint16(portN), nil
}

// loadShimTenants reads shim.yaml from path, validates each tenant has
// non-empty mysql_user and mysql_password free of newlines, and returns
// the resulting list.
func loadShimTenants(path string) ([]shimTenant, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("shim config not found at %s\nRun 'bintrail init-shim' to scaffold one, then fill in mysql_user / mysql_password.", path)
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg shimConfig
	// Strict mode rejects unknown YAML keys so a typo like "mysql_user_name:"
	// surfaces as a clear parse error rather than silently parsing as an
	// empty mysql_user.
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("%s has no tenants", path)
	}
	for i, t := range cfg.Tenants {
		if t.MySQLUser == "" {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user is empty (uncomment and fill in the TODO line)", path, i+1)
		}
		if t.MySQLPassword == "" {
			if t.MySQLPassSHA1 != "" {
				return nil, fmt.Errorf(
					"%s tenant #%d (mysql_user=%s): mysql_password is required; mysql_pass_sha1 alone is no longer accepted (>= 0.7.2). "+
						"Replace mysql_pass_sha1 with mysql_password: '<cleartext>' — the SHA1 is recomputed here automatically",
					path, i+1, t.MySQLUser)
			}
			return nil, fmt.Errorf("%s tenant #%d (mysql_user=%s): mysql_password is empty (set the cleartext password your application uses to connect)", path, i+1, t.MySQLUser)
		}
		if r := firstControlRune(t.MySQLUser); r >= 0 {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user contains control character U+%04X", path, i+1, r)
		}
		if r := firstControlRune(t.MySQLPassword); r >= 0 {
			return nil, fmt.Errorf("%s tenant #%d: mysql_password contains control character U+%04X", path, i+1, r)
		}
		// Mirror internal/shim/auth.go's behaviour: when both fields
		// are set, mysql_password wins and the legacy SHA1 is silently
		// dropped. Warn so an operator who half-migrated (added the
		// new field but forgot to delete the old) can clean up rather
		// than leaving stale state in shim.yaml.
		if t.MySQLPassSHA1 != "" {
			slog.Warn(
				"shim.yaml: mysql_pass_sha1 is no longer used (the SHA1 is recomputed from mysql_password); the field is ignored — remove it to clean up",
				"tenant", i+1, "mysql_user", t.MySQLUser,
			)
		}
	}
	return cfg.Tenants, nil
}

// nativePasswordHash returns ProxySQL's `*<UPPER_HEX>` storage form for
// a cleartext mysql_native_password — i.e. SHA1(SHA1(password)). This
// matches what `SELECT PASSWORD('<pw>')` produces in MySQL 5.7 and what
// ProxySQL stores in mysql_users.password under default config.
//
// We compute the hash here (rather than asking the operator to run a
// SHA1 recipe and paste it into shim.yaml) so shim.yaml has a single
// source of truth: the cleartext password. The shim itself needs that
// cleartext to validate ProxySQL-forwarded auth responses, so it has
// to live in shim.yaml regardless; emitting the SHA1 ourselves removes
// the manual derivation step the operator used to perform.
func nativePasswordHash(password string) string {
	first := sha1.Sum([]byte(password))
	second := sha1.Sum(first[:])
	return "*" + strings.ToUpper(fmt.Sprintf("%x", second))
}

// firstControlRune returns the first control rune in s (per
// unicode.IsControl), or -1 if none. Control characters in tenant
// credentials are rejected because they corrupt the SQL output:
// sqlQuote only escapes ', not '\n', '\t', '\0', etc.
func firstControlRune(s string) rune {
	for _, r := range s {
		if unicode.IsControl(r) {
			return r
		}
	}
	return -1
}

func generateProxySQLSetupSQL(host string, mysqlPort, shimPort, proxysqlMySQLPort uint16, tenants []shimTenant) string {
	var sb strings.Builder
	sb.WriteString("-- Bintrail BYOS time-travel SQL — ProxySQL setup\n")
	sb.WriteString("-- Generated by bintrail proxysql-config. See docs/byos-time-travel-sql.md.\n")
	sb.WriteString("--\n")
	sb.WriteString("-- This script manages the following ProxySQL resources, all in the\n")
	sb.WriteString("-- 990* numeric range to avoid colliding with operator-managed rules:\n")
	fmt.Fprintf(&sb, "--   * mysql_servers in hostgroups %d (passthrough) and %d (shim)\n", passthroughHostgroup, shimHostgroup)
	fmt.Fprintf(&sb, "--   * mysql_query_rules with rule_id in %d..%d\n", ruleIDFlashback, ruleIDSnapshot)
	sb.WriteString("--   * mysql_users named in shim.yaml (these become bintrail-managed)\n")
	sb.WriteString("--\n")
	sb.WriteString("-- Apply this file to the ProxySQL admin port:\n")
	sb.WriteString("--     mysql -u admin -P 6032 -h <proxysql-host> < proxysql-setup.sql\n")
	sb.WriteString("--\n")
	fmt.Fprintf(&sb, "-- Your application then connects to ProxySQL on port %d.\n", proxysqlMySQLPort)
	sb.WriteString("-- Re-running this script is idempotent.\n")
	sb.WriteString("\n")

	// Wrap the table edits in a transaction so a partial failure (e.g. a
	// constraint violation on one INSERT) rolls back the whole change set.
	// LOAD/SAVE statements are admin commands rather than DML and are
	// emitted after the COMMIT.
	sb.WriteString("BEGIN;\n")
	sb.WriteString("\n")

	fmt.Fprintf(&sb, "DELETE FROM mysql_servers WHERE hostgroup_id IN (%d, %d);\n", passthroughHostgroup, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (%d, %s, %d);\n", passthroughHostgroup, sqlQuote(host), mysqlPort)
	fmt.Fprintf(&sb, "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (%d, '127.0.0.1', %d);\n", shimHostgroup, shimPort)
	sb.WriteString("\n")

	// DELETE is scoped strictly to bintrail-managed rows by default_hostgroup,
	// never by username alone. This:
	//   * cleans rows from a previous run whose username was renamed in
	//     shim.yaml between runs (the old row still lives in hostgroup 990).
	//   * does NOT destroy an operator's pre-existing user that happens to
	//     share a name with a tenant — if there is a collision, the INSERT
	//     below fails loudly with a PRIMARY KEY violation rather than
	//     silently overwriting operator config.
	fmt.Fprintf(&sb, "DELETE FROM mysql_users WHERE default_hostgroup = %d;\n", passthroughHostgroup)
	for _, t := range tenants {
		fmt.Fprintf(&sb, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES (%s, %s, %d, 1);\n",
			sqlQuote(t.MySQLUser), sqlQuote(nativePasswordHash(t.MySQLPassword)), passthroughHostgroup)
	}
	sb.WriteString("\n")

	fmt.Fprintf(&sb, "DELETE FROM mysql_query_rules WHERE rule_id IN (%d, %d, %d);\n", ruleIDFlashback, ruleIDDiff, ruleIDSnapshot)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_flashback\\.', %d, 1);\n", ruleIDFlashback, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_diff\\.', %d, 1);\n", ruleIDDiff, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_snapshot\\.', %d, 1);\n", ruleIDSnapshot, shimHostgroup)
	sb.WriteString("\n")

	sb.WriteString("COMMIT;\n")
	sb.WriteString("\n")

	sb.WriteString("LOAD MYSQL SERVERS TO RUNTIME;\n")
	sb.WriteString("LOAD MYSQL USERS TO RUNTIME;\n")
	sb.WriteString("LOAD MYSQL QUERY RULES TO RUNTIME;\n")
	sb.WriteString("SAVE MYSQL SERVERS TO DISK;\n")
	sb.WriteString("SAVE MYSQL USERS TO DISK;\n")
	sb.WriteString("SAVE MYSQL QUERY RULES TO DISK;\n")

	return sb.String()
}

// sqlQuote wraps s as a SQL single-quoted string literal, doubling any
// embedded single quotes. ProxySQL admin uses SQLite-style quoting so
// this is the safe escape.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
