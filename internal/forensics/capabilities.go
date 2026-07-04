// Package forensics inspects a MySQL-family source server's forensic data
// sources — performance_schema and the audit-log plugin family — so binlog
// changes can be attributed to users, hosts, and client programs ("who
// changed this"). Ported from the dbtrail SaaS agent (agent/handler/
// forensics.go) and setup-guide service (services/forensics_guide.py); JSON
// field names match the SaaS wire contract (models/forensics.py) so later
// surfaces (CLI, console, MCP, agent WS) stay wire-compatible.
//
// This package is mechanism only. The entitlement seam is Enabled (gate.go),
// checked at surface entry points — never inside this library. All SQL here
// is read-only against the source server: capabilities are detected and
// remediation is suggested, but bintrail never writes to a monitored server
// (the "validate, never set" convention).
package forensics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// Capabilities describes the forensic data sources available on a MySQL
// server.
type Capabilities struct {
	PerformanceSchema PerfSchemaCapabilities `json:"performance_schema"`
	AuditLog          AuditLogCapabilities   `json:"audit_log"`
	ServerInfo        ServerInfo             `json:"server_info"`
}

// PerfSchemaConsumers reports the state of the two setup_consumers rows the
// forensic activity queries depend on.
type PerfSchemaConsumers struct {
	EventsStatementsHistory     bool `json:"events_statements_history"`
	EventsStatementsHistoryLong bool `json:"events_statements_history_long"`
}

// PerfSchemaCapabilities reports whether performance_schema is enabled and
// which relevant pieces of it are usable.
type PerfSchemaCapabilities struct {
	Enabled           bool                `json:"enabled"`
	Consumers         PerfSchemaConsumers `json:"consumers"`
	ThreadsAccessible bool                `json:"threads_accessible"`
}

// AuditLogCapabilities reports whether an audit-log plugin is active and, if
// so, which variant and how it is configured.
type AuditLogCapabilities struct {
	Installed    bool   `json:"installed"`
	PluginName   string `json:"plugin_name,omitempty"`
	PluginStatus string `json:"plugin_status,omitempty"`
	// Variant is "percona", "mariadb" (also the AWS RDS/Aurora fork of
	// MariaDB server_audit), or "mysql_enterprise".
	Variant string `json:"variant,omitempty"`
	// Config holds the audit-related SHOW GLOBAL VARIABLES that matched
	// (format, file path, policy, ...).
	Config map[string]string `json:"config,omitempty"`
}

// ServerInfo identifies the server version and variant.
type ServerInfo struct {
	Version        string `json:"version,omitempty"`
	VersionComment string `json:"version_comment,omitempty"`
	// Variant is "percona", "mariadb", or "mysql".
	Variant string `json:"variant,omitempty"`
}

// DetectCapabilities checks what forensic data sources are available on the
// MySQL server reachable via sourceDB.
//
// It inspects:
//   - performance_schema: enabled status, relevant consumers, threads access
//   - audit_log plugin: installed status, variant (Percona/MySQL/MariaDB), config
//   - server info: MySQL version and server variant (Percona/MariaDB/MySQL)
//
// Detection is best-effort: individual probe failures degrade to "not
// available" (logged at WARN) rather than failing the whole call. An error is
// returned only when the server itself is unreachable.
func DetectCapabilities(ctx context.Context, sourceDB *sql.DB) (Capabilities, error) {
	if err := sourceDB.PingContext(ctx); err != nil {
		return Capabilities{}, fmt.Errorf("connect to MySQL: %w", err)
	}
	return Capabilities{
		PerformanceSchema: detectPerfSchema(ctx, sourceDB),
		AuditLog:          detectAuditLog(ctx, sourceDB),
		ServerInfo:        detectServerInfo(ctx, sourceDB),
	}, nil
}

// detectPerfSchema checks whether performance_schema is enabled and which
// relevant consumers are active.
func detectPerfSchema(ctx context.Context, db *sql.DB) PerfSchemaCapabilities {
	var caps PerfSchemaCapabilities

	var varName, varValue string
	err := db.QueryRowContext(ctx,
		"SHOW GLOBAL VARIABLES LIKE 'performance_schema'",
	).Scan(&varName, &varValue)
	if err != nil {
		slog.Warn("forensics: could not check performance_schema variable", "error", err)
		return caps
	}
	caps.Enabled = strings.EqualFold(varValue, "ON")
	if !caps.Enabled {
		return caps
	}

	// Check relevant consumers.
	rows, err := db.QueryContext(ctx,
		"SELECT NAME, ENABLED FROM performance_schema.setup_consumers "+
			"WHERE NAME IN ('events_statements_history', 'events_statements_history_long')",
	)
	if err != nil {
		slog.Warn("forensics: could not query setup_consumers", "error", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var name, enabled string
			if err := rows.Scan(&name, &enabled); err != nil {
				slog.Warn("forensics: scan consumer row", "error", err)
				continue
			}
			on := strings.EqualFold(enabled, "YES")
			switch name {
			case "events_statements_history":
				caps.Consumers.EventsStatementsHistory = on
			case "events_statements_history_long":
				caps.Consumers.EventsStatementsHistoryLong = on
			}
		}
		if err := rows.Err(); err != nil {
			slog.Warn("forensics: iterate consumer rows", "error", err)
		}
	}

	// Check if the threads table is accessible.
	var threadCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM performance_schema.threads WHERE TYPE = 'FOREGROUND' LIMIT 1",
	).Scan(&threadCount)
	caps.ThreadsAccessible = err == nil

	return caps
}

// detectAuditLog checks whether an audit log plugin is installed and determines
// its variant (Percona, MySQL Enterprise, or MariaDB).
func detectAuditLog(ctx context.Context, db *sql.DB) AuditLogCapabilities {
	var caps AuditLogCapabilities

	rows, err := db.QueryContext(ctx,
		"SELECT PLUGIN_NAME, PLUGIN_STATUS, PLUGIN_DESCRIPTION "+
			"FROM information_schema.PLUGINS "+
			"WHERE UPPER(PLUGIN_NAME) LIKE '%AUDIT%' AND PLUGIN_STATUS = 'ACTIVE'",
	)
	if err != nil {
		slog.Warn("forensics: could not query audit plugins", "error", err)
		return caps
	}
	defer rows.Close()

	for rows.Next() {
		var name, status, description string
		if err := rows.Scan(&name, &status, &description); err != nil {
			slog.Warn("forensics: scan audit plugin row", "error", err)
			continue
		}

		// Skip the RDS internal audit plugin — it's not queryable via SQL.
		upperName := strings.ToUpper(name)
		if strings.Contains(upperName, "RDS_SECURITY") {
			continue
		}

		caps.Installed = true
		caps.PluginName = name
		caps.PluginStatus = status

		// Determine variant based on plugin name. audit_log_filter is
		// Percona-exclusive (its PLUGIN_DESCRIPTION is just "Audit log",
		// so the name/description substring check below would miss it).
		switch {
		case strings.EqualFold(name, "audit_log_filter") ||
			strings.Contains(upperName, "PERCONA") || strings.Contains(strings.ToUpper(description), "PERCONA"):
			caps.Variant = "percona"
		case strings.Contains(upperName, "SERVER_AUDIT") || strings.Contains(upperName, "MARIADB"):
			caps.Variant = "mariadb"
		default:
			caps.Variant = "mysql_enterprise"
		}

		// Try to get the audit log configuration.
		caps.Config = detectAuditLogConfig(ctx, db)
		break // Only need the first active audit plugin.
	}
	if err := rows.Err(); err != nil {
		slog.Warn("forensics: iterate audit plugin rows", "error", err)
	}

	return caps
}

// detectAuditLogConfig reads audit log configuration variables.
//
// Uses string formatting instead of prepared-statement placeholders for
// SHOW GLOBAL VARIABLES — some MySQL/RDS configurations reject prepared
// statements for SHOW commands. Variable names are hardcoded constants.
//
// If none of the well-known variable names match (common on RDS where
// SERVER_AUDIT exists but MariaDB-style config vars do not), falls back
// to a wildcard query to discover any audit-related variables.
func detectAuditLogConfig(ctx context.Context, db *sql.DB) map[string]string {
	configVars := []string{
		"audit_log_format",
		"audit_log_file",
		"audit_log_policy",
		"audit_log_filter_format",  // Percona Audit Log Filter plugin
		"audit_log_filter_file",    // Percona Audit Log Filter plugin
		"server_audit_logging",     // MariaDB
		"server_audit_file_path",   // MariaDB
		"server_audit_output_type", // MariaDB
	}

	config := map[string]string{}
	for _, v := range configVars {
		var name, value string
		//nolint:gosec // v is a hardcoded constant, not user input
		err := db.QueryRowContext(ctx,
			fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", v),
		).Scan(&name, &value)
		if err == nil {
			config[name] = value
		} else if !errors.Is(err, sql.ErrNoRows) {
			slog.Warn("forensics: detectAuditLogConfig SHOW variable", "variable", v, "error", err)
		}
	}
	if len(config) > 0 {
		return config
	}

	// Fallback: if none of the well-known vars matched, try a wildcard
	// to discover any audit-related variables (covers RDS managed plugins
	// which use different parameter names).
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL VARIABLES LIKE '%audit%'")
	if err != nil {
		slog.Warn("forensics: detectAuditLogConfig wildcard query", "error", err)
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var name, value string
		if scanErr := rows.Scan(&name, &value); scanErr != nil {
			slog.Warn("forensics: detectAuditLogConfig wildcard scan", "error", scanErr)
			continue
		}
		config[name] = value
	}
	if rowErr := rows.Err(); rowErr != nil {
		slog.Warn("forensics: detectAuditLogConfig wildcard rows", "error", rowErr)
	}
	if len(config) == 0 {
		return nil
	}
	return config
}

// detectServerInfo reads MySQL version metadata to determine the server variant
// (Percona Server, MariaDB, or MySQL Community/Enterprise).
// Same fmt.Sprintf rationale as detectAuditLogConfig (RDS prepared-statement quirk).
func detectServerInfo(ctx context.Context, db *sql.DB) ServerInfo {
	var info ServerInfo

	for _, v := range []string{"version", "version_comment"} {
		var name, value string
		//nolint:gosec // v is a hardcoded constant, not user input
		err := db.QueryRowContext(ctx,
			fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", v),
		).Scan(&name, &value)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				slog.Warn("forensics: detectServerInfo SHOW variable", "variable", v, "error", err)
			}
			continue
		}
		switch v {
		case "version":
			info.Version = value
		case "version_comment":
			info.VersionComment = value
		}
	}

	// Derive variant from version_comment.
	if info.VersionComment != "" {
		upper := strings.ToUpper(info.VersionComment)
		switch {
		case strings.Contains(upper, "PERCONA"):
			info.Variant = "percona"
		case strings.Contains(upper, "MARIADB"):
			info.Variant = "mariadb"
		default:
			info.Variant = "mysql"
		}
	}

	return info
}
