package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

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

// shimTenant is the subset of a shim.yaml tenant block this command
// needs. Unknown fields are ignored so the shim can extend its schema
// without breaking us.
type shimTenant struct {
	MySQLUser     string `yaml:"mysql_user"`
	MySQLPassSHA1 string `yaml:"mysql_pass_sha1"`
}

type shimConfig struct {
	Tenants []shimTenant `yaml:"tenants"`
}

func runProxySQLConfig(cmd *cobra.Command, args []string) error {
	sourceDSN := os.Getenv("BINTRAIL_SOURCE_DSN")
	if sourceDSN == "" {
		return fmt.Errorf("missing required env var: BINTRAIL_SOURCE_DSN\nRun 'bintrail config init' to scaffold .bintrail.env, then set this value.")
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

	if _, err := os.Stat(pcOut); err == nil {
		return fmt.Errorf("file already exists: %s\nRemove it first or edit it directly.", pcOut)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot check %s: %w", pcOut, err)
	}

	if err := os.WriteFile(pcOut, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", pcOut, err)
	}

	fmt.Printf("ProxySQL setup SQL written to %s\n", pcOut)
	fmt.Printf("Apply it: mysql -u admin -P 6032 -h <proxysql-host> < %s\n", pcOut)
	return nil
}

// parseProxySQLBackend extracts the host and port from a go-sql-driver
// DSN. If the DSN address has no port, fallbackPort is used.
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
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return addr, fallbackPort, nil
	}
	h := addr[:idx]
	p, convErr := strconv.ParseUint(addr[idx+1:], 10, 16)
	if convErr != nil {
		return "", 0, fmt.Errorf("invalid port in BINTRAIL_SOURCE_DSN: %w", convErr)
	}
	return h, uint16(p), nil
}

// loadShimTenants reads shim.yaml from path, validates each tenant has
// non-empty mysql_user and mysql_pass_sha1 free of newlines, and returns
// the resulting list.
func loadShimTenants(path string) ([]shimTenant, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("shim config not found at %s\nRun 'bintrail init-shim' to scaffold one, then fill in mysql_user / mysql_pass_sha1.", path)
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg shimConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("%s has no tenants", path)
	}
	for i, t := range cfg.Tenants {
		if t.MySQLUser == "" {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user is empty (uncomment and fill in the TODO line)", path, i+1)
		}
		if t.MySQLPassSHA1 == "" {
			return nil, fmt.Errorf("%s tenant #%d: mysql_pass_sha1 is empty (uncomment and fill in the TODO line)", path, i+1)
		}
		if strings.ContainsAny(t.MySQLUser, "\r\n") {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user contains a newline character", path, i+1)
		}
		if strings.ContainsAny(t.MySQLPassSHA1, "\r\n") {
			return nil, fmt.Errorf("%s tenant #%d: mysql_pass_sha1 contains a newline character", path, i+1)
		}
	}
	return cfg.Tenants, nil
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

	fmt.Fprintf(&sb, "DELETE FROM mysql_servers WHERE hostgroup_id IN (%d, %d);\n", passthroughHostgroup, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (%d, %s, %d);\n", passthroughHostgroup, sqlQuote(host), mysqlPort)
	fmt.Fprintf(&sb, "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (%d, '127.0.0.1', %d);\n", shimHostgroup, shimPort)
	sb.WriteString("\n")

	sb.WriteString("DELETE FROM mysql_users WHERE username IN (")
	for i, t := range tenants {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sqlQuote(t.MySQLUser))
	}
	sb.WriteString(");\n")
	for _, t := range tenants {
		fmt.Fprintf(&sb, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES (%s, %s, %d, 1);\n",
			sqlQuote(t.MySQLUser), sqlQuote(t.MySQLPassSHA1), passthroughHostgroup)
	}
	sb.WriteString("\n")

	fmt.Fprintf(&sb, "DELETE FROM mysql_query_rules WHERE rule_id IN (%d, %d, %d);\n", ruleIDFlashback, ruleIDDiff, ruleIDSnapshot)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_flashback\\.', %d, 1);\n", ruleIDFlashback, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_diff\\.', %d, 1);\n", ruleIDDiff, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_snapshot\\.', %d, 1);\n", ruleIDSnapshot, shimHostgroup)
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
