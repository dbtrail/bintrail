package forensics

import "strings"

// SetupRecommendation is a single actionable recommendation for improving
// forensic data. All recommendations are advisory — bintrail never writes to
// a monitored server; the operator applies RuntimeSQL / MycnfSnippet manually.
type SetupRecommendation struct {
	Category        string   `json:"category"` // "performance_schema" or "audit_plugin"
	Title           string   `json:"title"`
	Description     string   `json:"description"`
	Impact          string   `json:"impact"`
	PerformanceNote string   `json:"performance_note"`
	RuntimeSQL      []string `json:"runtime_sql"`
	MycnfSnippet    string   `json:"mycnf_snippet"`
	Priority        string   `json:"priority"` // "high", "medium", or "low"
}

// SetupGuide is a tailored set of setup recommendations based on detected
// capabilities.
type SetupGuide struct {
	Summary         string                `json:"summary"`
	Recommendations []SetupRecommendation `json:"recommendations"`
}

// mycnfPerfSchemaFull is the full my.cnf snippet for enabling
// performance_schema from scratch.
const mycnfPerfSchemaFull = "[mysqld]\n" +
	"performance_schema = ON\n" +
	"performance-schema-consumer-events-statements-history = ON\n" +
	"performance-schema-consumer-events-statements-history-long = ON\n" +
	"performance_schema_events_statements_history_long_size = 10000"

// BuildSetupGuide produces tailored guidance from detected capabilities: what
// forensic data source is missing or degraded, why it matters, and the exact
// runtime SQL / my.cnf snippet to fix it. Pure function — port of the SaaS
// forensics_guide.py decision tree.
func BuildSetupGuide(caps Capabilities) SetupGuide {
	var recommendations []SetupRecommendation
	ps := caps.PerformanceSchema
	audit := caps.AuditLog
	variant := caps.ServerInfo.Variant
	if variant == "" {
		variant = "mysql"
	}

	// ── Performance Schema ──

	if !ps.Enabled {
		recommendations = append(recommendations, SetupRecommendation{
			Category: "performance_schema",
			Title:    "Enable Performance Schema",
			Description: "performance_schema is disabled on this server. It provides real-time " +
				"connection metadata and statement history essential for forensic " +
				"investigation. Enabling it requires a MySQL restart.",
			Impact: "Enables who-changed tracing, user activity queries, connection " +
				"history, and client attribution (program name, source host).",
			PerformanceNote: "Typically adds 5-10% memory overhead. CPU impact is negligible " +
				"for most workloads. Enabled by default in MySQL 8.0+.",
			RuntimeSQL:   []string{},
			MycnfSnippet: mycnfPerfSchemaFull,
			Priority:     "high",
		})
	} else {
		if !ps.Consumers.EventsStatementsHistory {
			recommendations = append(recommendations, SetupRecommendation{
				Category: "performance_schema",
				Title:    "Enable statement history consumer",
				Description: "The events_statements_history consumer is disabled. This " +
					"per-connection ring buffer stores recent SQL statements for " +
					"each active thread.",
				Impact: "Enables per-connection recent SQL history. Useful for seeing " +
					"what a specific connection executed recently.",
				PerformanceNote: "Minimal overhead — stores the last 10 statements per thread " +
					"by default. Memory usage scales with max_connections.",
				RuntimeSQL: []string{
					"UPDATE performance_schema.setup_consumers\n" +
						"SET ENABLED = 'YES'\n" +
						"WHERE NAME = 'events_statements_history';",
				},
				MycnfSnippet: "[mysqld]\n" +
					"performance-schema-consumer-events-statements-history = ON",
				Priority: "medium",
			})
		}

		if !ps.Consumers.EventsStatementsHistoryLong {
			recommendations = append(recommendations, SetupRecommendation{
				Category: "performance_schema",
				Title:    "Enable global statement history consumer",
				Description: "The events_statements_history_long consumer is disabled. This " +
					"global ring buffer stores recent SQL statements across all " +
					"connections — critical for forensic investigation of past activity.",
				Impact: "Enables user_activity queries across all connections. " +
					"This is the primary data source for forensic " +
					"statement analysis.",
				PerformanceNote: "Stores the last 10,000 statements globally by default. " +
					"Memory usage is fixed (not per-connection). Adjust " +
					"performance_schema_events_statements_history_long_size " +
					"to control buffer size.",
				RuntimeSQL: []string{
					"UPDATE performance_schema.setup_consumers\n" +
						"SET ENABLED = 'YES'\n" +
						"WHERE NAME = 'events_statements_history_long';",
				},
				MycnfSnippet: "[mysqld]\n" +
					"performance-schema-consumer-events-statements-history-long = ON\n" +
					"performance_schema_events_statements_history_long_size = 10000",
				Priority: "high",
			})
		}
	}

	// ── Audit Plugin ──

	if !audit.Installed {
		recommendations = append(recommendations, auditRecommendation(variant))
	} else if isRDSAuditPath(audit.Config) {
		recommendations = append(recommendations, SetupRecommendation{
			Category: "audit_plugin",
			Title:    "Grant IAM permissions for RDS audit log access",
			Description: "The audit log plugin is active and writing to the RDS-managed " +
				"filesystem. To read these logs, the IAM role of the host running " +
				"bintrail needs RDS log download permissions. Without these " +
				"permissions, audit-log reads will fail with an access denied error.",
			Impact: "Enables bintrail to download and parse audit logs directly " +
				"from RDS via the AWS API, providing full SQL statement history " +
				"with user attribution.",
			PerformanceNote: "Read-only API calls with no impact on the RDS instance. " +
				"Large audit log files (50 MB+) may take a few seconds to " +
				"download and parse.",
			RuntimeSQL: []string{},
			MycnfSnippet: "# IAM policy to attach to the role of the host running bintrail:\n" +
				"{\n" +
				"  \"Effect\": \"Allow\",\n" +
				"  \"Action\": [\n" +
				"    \"rds:DescribeDBLogFiles\",\n" +
				"    \"rds:DownloadDBLogFilePortion\"\n" +
				"  ],\n" +
				"  \"Resource\": \"arn:aws:rds:*:*:db:*\"\n" +
				"}",
			Priority: "medium",
		})
	}

	// ── Build summary ──

	var summary string
	if len(recommendations) == 0 {
		summary = "All forensic data sources are fully configured. " +
			"performance_schema consumers are enabled and " +
			"an audit log plugin is installed."
	} else {
		var parts []string
		if !ps.Enabled {
			parts = append(parts, "performance_schema is disabled")
		} else if !(ps.Consumers.EventsStatementsHistory && ps.Consumers.EventsStatementsHistoryLong) {
			parts = append(parts, "statement history consumers are not fully enabled")
		}
		if !audit.Installed {
			parts = append(parts, "no audit log plugin is installed")
		} else if isRDSAuditPath(audit.Config) {
			parts = append(parts, "RDS audit log requires IAM permissions for access")
		}
		if len(parts) > 0 {
			summary = "Forensic data can be improved. " +
				capitalizeFirst(strings.Join(parts, "; ")) +
				". See recommendations below."
		} else {
			summary = "See recommendations below."
		}
	}

	return SetupGuide{Summary: summary, Recommendations: recommendations}
}

// capitalizeFirst uppercases the first byte of an ASCII sentence. Unlike
// Python's str.capitalize() (which the SaaS used), it does NOT lowercase the
// rest — "RDS audit log ..." must not become "Rds audit log ...".
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	if c := s[0]; c >= 'a' && c <= 'z' {
		return string(c-'a'+'A') + s[1:]
	}
	return s
}

// isRDSAuditPath detects RDS-managed audit logs by the /rdsdbdata/ path prefix.
func isRDSAuditPath(auditConfig map[string]string) bool {
	for _, key := range []string{"server_audit_file_path", "audit_log_file"} {
		if val := auditConfig[key]; val != "" && strings.Contains(val, "/rdsdbdata/") {
			return true
		}
	}
	return false
}

// auditRecommendation builds variant-specific audit plugin installation guidance.
func auditRecommendation(variant string) SetupRecommendation {
	if variant == "percona" {
		return SetupRecommendation{
			Category: "audit_plugin",
			Title:    "Install Percona Audit Log Filter Plugin",
			Description: "No audit log plugin detected. Percona Server includes the free, " +
				"open-source Audit Log Filter plugin — Percona's recommended audit " +
				"plugin, which supersedes the legacy Audit Log plugin — capturing " +
				"comprehensive query history for long-term forensic investigation.",
			Impact: "Captures all SQL statements with user, host, timestamp, and " +
				"connection metadata. Provides forensic data beyond " +
				"performance_schema's ring buffer — persisted to disk.",
			PerformanceNote: "Logging every statement adds moderate I/O overhead. Narrow " +
				"the filter definition to the connection and table_access classes " +
				"(e.g. insert/update/delete only, skipping read) for lower overhead " +
				"than logging everything including SELECTs.",
			RuntimeSQL: []string{
				"CREATE TABLE IF NOT EXISTS mysql.audit_log_filter (\n" +
					"  filter_id INT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
					"  name VARCHAR(255) NOT NULL,\n" +
					"  filter JSON NOT NULL,\n" +
					"  PRIMARY KEY (filter_id),\n" +
					"  UNIQUE KEY filter_name (name)\n" +
					") ENGINE=InnoDB;",
				"CREATE TABLE IF NOT EXISTS mysql.audit_log_user (\n" +
					"  username VARCHAR(32) NOT NULL,\n" +
					"  userhost VARCHAR(255) NOT NULL,\n" +
					"  filtername VARCHAR(255) NOT NULL,\n" +
					"  PRIMARY KEY (username, userhost),\n" +
					"  FOREIGN KEY (filtername) REFERENCES mysql.audit_log_filter(name)\n" +
					") ENGINE=InnoDB;",
				"INSTALL PLUGIN audit_log_filter SONAME 'audit_log_filter.so';",
				`SELECT audit_log_filter_set_filter('log_all', '{"filter": {"log": true}}');`,
				"SELECT audit_log_filter_set_user('%', 'log_all');",
			},
			MycnfSnippet: "[mysqld]\n" +
				"plugin-load-add = audit_log_filter.so\n" +
				"audit_log_filter_file = /var/log/mysql/audit_filter.log\n" +
				"\n" +
				"# audit_log_filter_file/_format/_strategy require a server restart to\n" +
				"# take effect (not dynamic) — the default NEW format is left as-is here\n" +
				"# since it's the one bintrail's audit-log reader is verified against.\n" +
				"# Which events get logged is controlled separately, at runtime, via\n" +
				"# audit_log_filter_set_filter().",
			Priority: "medium",
		}
	}

	if variant == "mariadb" {
		return SetupRecommendation{
			Category: "audit_plugin",
			Title:    "Enable MariaDB Audit Plugin",
			Description: "No audit log plugin detected. MariaDB includes the Server Audit " +
				"Plugin that captures connection and query events for forensic " +
				"investigation.",
			Impact: "Captures SQL statements, connection events, and DDL with user " +
				"attribution. Persisted to disk for long-term forensic analysis.",
			PerformanceNote: "FILE output type adds moderate I/O overhead. Use " +
				"server_audit_events = CONNECT for minimal impact (connection " +
				"events only) or CONNECT,QUERY for full logging.",
			RuntimeSQL: []string{
				"INSTALL SONAME 'server_audit';",
				"SET GLOBAL server_audit_logging = ON;",
				"SET GLOBAL server_audit_output_type = 'FILE';",
				"SET GLOBAL server_audit_file_path = '/var/log/mysql/server_audit.log';",
			},
			MycnfSnippet: "[mysqld]\n" +
				"plugin-load-add = server_audit\n" +
				"server_audit_logging = ON\n" +
				"server_audit_output_type = FILE\n" +
				"server_audit_file_path = /var/log/mysql/server_audit.log",
			Priority: "medium",
		}
	}

	// MySQL Community / Enterprise / unknown.
	return SetupRecommendation{
		Category: "audit_plugin",
		Title:    "Install an audit log plugin",
		Description: "No audit log plugin detected. MySQL Enterprise Edition includes " +
			"the Enterprise Audit plugin (commercial license required). " +
			"Alternatively, consider migrating to Percona Server (free, " +
			"drop-in replacement) for the open-source Percona Audit Log Filter Plugin.",
		Impact: "Captures all SQL statements with user, host, timestamp, and " +
			"connection metadata. Provides forensic data beyond " +
			"performance_schema's ring buffer — persisted to disk.",
		PerformanceNote: "Audit logging adds moderate I/O overhead depending on query " +
			"volume and log format.",
		RuntimeSQL: []string{
			"-- MySQL Enterprise Audit (requires Enterprise Edition):\n" +
				"INSTALL PLUGIN audit_log SONAME 'audit_log.so';",
		},
		MycnfSnippet: "# Option 1: MySQL Enterprise Audit (requires Enterprise license)\n" +
			"[mysqld]\n" +
			"plugin-load-add = audit_log.so\n" +
			"audit_log_format = JSON\n" +
			"audit_log_policy = ALL\n" +
			"\n" +
			"# Option 2: Migrate to Percona Server (free, drop-in replacement)\n" +
			"# and use the Percona Audit Log Filter Plugin (see Percona docs)",
		Priority: "medium",
	}
}
