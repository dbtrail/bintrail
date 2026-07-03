package forensics

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// varRows builds a SHOW GLOBAL VARIABLES-shaped resultset with one row.
func varRows(name, value string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow(name, value)
}

// emptyVarRows builds a SHOW GLOBAL VARIABLES-shaped resultset with no rows
// (variable does not exist → sql.ErrNoRows on Scan).
func emptyVarRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"Variable_name", "Value"})
}

func consumerRows(pairs ...[2]string) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"NAME", "ENABLED"})
	for _, p := range pairs {
		rows.AddRow(p[0], p[1])
	}
	return rows
}

// auditConfigVarNames mirrors the well-known variable list probed by
// detectAuditLogConfig, in probe order.
var auditConfigVarNames = []string{
	"audit_log_format",
	"audit_log_file",
	"audit_log_policy",
	"server_audit_logging",
	"server_audit_file_path",
	"server_audit_output_type",
}

// expectAuditConfigVars registers the six well-known SHOW GLOBAL VARIABLES
// probes; vars named in values return a row, the rest return empty.
func expectAuditConfigVars(m sqlmock.Sqlmock, values map[string]string) {
	for _, v := range auditConfigVarNames {
		e := m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE '" + v + "'")
		if val, ok := values[v]; ok {
			e.WillReturnRows(varRows(v, val))
		} else {
			e.WillReturnRows(emptyVarRows())
		}
	}
}

func TestDetectPerfSchema(t *testing.T) {
	forcedErr := errors.New("forced failure")

	tests := []struct {
		name  string
		setup func(m sqlmock.Sqlmock)
		want  PerfSchemaCapabilities
	}{
		{
			name: "enabled with both consumers on",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(varRows("performance_schema", "ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers WHERE NAME IN").
					WillReturnRows(consumerRows(
						[2]string{"events_statements_history", "YES"},
						[2]string{"events_statements_history_long", "YES"}))
				m.ExpectQuery("FROM performance_schema.threads WHERE TYPE = 'FOREGROUND' LIMIT 1").
					WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(5))
			},
			want: PerfSchemaCapabilities{
				Enabled: true,
				Consumers: PerfSchemaConsumers{
					EventsStatementsHistory:     true,
					EventsStatementsHistoryLong: true,
				},
				ThreadsAccessible: true,
			},
		},
		{
			name: "enabled with history_long off",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(varRows("performance_schema", "ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers WHERE NAME IN").
					WillReturnRows(consumerRows(
						[2]string{"events_statements_history", "YES"},
						[2]string{"events_statements_history_long", "NO"}))
				m.ExpectQuery("FROM performance_schema.threads WHERE TYPE = 'FOREGROUND' LIMIT 1").
					WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(5))
			},
			want: PerfSchemaCapabilities{
				Enabled:           true,
				Consumers:         PerfSchemaConsumers{EventsStatementsHistory: true},
				ThreadsAccessible: true,
			},
		},
		{
			name: "consumer missing from the resultset is treated as off",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(varRows("performance_schema", "ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers WHERE NAME IN").
					WillReturnRows(consumerRows([2]string{"events_statements_history", "YES"}))
				m.ExpectQuery("FROM performance_schema.threads WHERE TYPE = 'FOREGROUND' LIMIT 1").
					WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(5))
			},
			want: PerfSchemaCapabilities{
				Enabled:           true,
				Consumers:         PerfSchemaConsumers{EventsStatementsHistory: true},
				ThreadsAccessible: true,
			},
		},
		{
			name: "disabled runs no further probes",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(varRows("performance_schema", "OFF"))
			},
			want: PerfSchemaCapabilities{},
		},
		{
			name: "variable query failure degrades to disabled",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnError(forcedErr)
			},
			want: PerfSchemaCapabilities{},
		},
		{
			name: "consumers query failure keeps enabled and threads probes",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(varRows("performance_schema", "ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers WHERE NAME IN").
					WillReturnError(forcedErr)
				m.ExpectQuery("FROM performance_schema.threads WHERE TYPE = 'FOREGROUND' LIMIT 1").
					WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(5))
			},
			want: PerfSchemaCapabilities{Enabled: true, ThreadsAccessible: true},
		},
		{
			name: "threads query failure means threads not accessible",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(varRows("performance_schema", "ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers WHERE NAME IN").
					WillReturnRows(consumerRows(
						[2]string{"events_statements_history", "YES"},
						[2]string{"events_statements_history_long", "YES"}))
				m.ExpectQuery("FROM performance_schema.threads WHERE TYPE = 'FOREGROUND' LIMIT 1").
					WillReturnError(forcedErr)
			},
			want: PerfSchemaCapabilities{
				Enabled: true,
				Consumers: PerfSchemaConsumers{
					EventsStatementsHistory:     true,
					EventsStatementsHistoryLong: true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			tt.setup(mock)

			got := detectPerfSchema(t.Context(), db)
			if got != tt.want {
				t.Errorf("detectPerfSchema = %+v, want %+v", got, tt.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestDetectAuditLog(t *testing.T) {
	pluginRows := func(rows ...[3]string) *sqlmock.Rows {
		r := sqlmock.NewRows([]string{"PLUGIN_NAME", "PLUGIN_STATUS", "PLUGIN_DESCRIPTION"})
		for _, row := range rows {
			r.AddRow(row[0], row[1], row[2])
		}
		return r
	}

	tests := []struct {
		name        string
		setup       func(m sqlmock.Sqlmock)
		want        AuditLogCapabilities
		checkConfig map[string]string // non-nil: assert Config equals this
	}{
		{
			name: "no audit plugins",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").WillReturnRows(pluginRows())
			},
			want: AuditLogCapabilities{},
		},
		{
			name: "percona variant by description",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows([3]string{"audit_log", "ACTIVE", "Percona Audit Log"}))
				expectAuditConfigVars(m, map[string]string{
					"audit_log_format": "JSON",
					"audit_log_file":   "/var/lib/mysql/audit.log",
				})
			},
			want: AuditLogCapabilities{
				Installed:    true,
				PluginName:   "audit_log",
				PluginStatus: "ACTIVE",
				Variant:      "percona",
			},
			checkConfig: map[string]string{
				"audit_log_format": "JSON",
				"audit_log_file":   "/var/lib/mysql/audit.log",
			},
		},
		{
			name: "mariadb variant by SERVER_AUDIT name",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows([3]string{"SERVER_AUDIT", "ACTIVE", "Audit the server activity"}))
				expectAuditConfigVars(m, map[string]string{
					"server_audit_logging":   "ON",
					"server_audit_file_path": "/rdsdbdata/log/audit/server_audit.log",
				})
			},
			want: AuditLogCapabilities{
				Installed:    true,
				PluginName:   "SERVER_AUDIT",
				PluginStatus: "ACTIVE",
				Variant:      "mariadb",
			},
			checkConfig: map[string]string{
				"server_audit_logging":   "ON",
				"server_audit_file_path": "/rdsdbdata/log/audit/server_audit.log",
			},
		},
		{
			name: "mysql_enterprise as the default variant",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows([3]string{"audit_log", "ACTIVE", "Oracle Audit Log"}))
				expectAuditConfigVars(m, nil)
				// No well-known vars matched → wildcard fallback.
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE '%audit%'").
					WillReturnRows(varRows("audit_log_connection_policy", "ALL"))
			},
			want: AuditLogCapabilities{
				Installed:    true,
				PluginName:   "audit_log",
				PluginStatus: "ACTIVE",
				Variant:      "mysql_enterprise",
			},
			checkConfig: map[string]string{"audit_log_connection_policy": "ALL"},
		},
		{
			name: "RDS internal security plugin is skipped",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows([3]string{"RDS_SECURITY_AUDIT", "ACTIVE", "internal"}))
			},
			want: AuditLogCapabilities{},
		},
		{
			name: "RDS internal plugin skipped but real plugin after it wins",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows(
						[3]string{"RDS_SECURITY_AUDIT", "ACTIVE", "internal"},
						[3]string{"SERVER_AUDIT", "ACTIVE", "Audit the server activity"}))
				expectAuditConfigVars(m, nil)
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE '%audit%'").WillReturnRows(emptyVarRows())
			},
			want: AuditLogCapabilities{
				Installed:    true,
				PluginName:   "SERVER_AUDIT",
				PluginStatus: "ACTIVE",
				Variant:      "mariadb",
			},
		},
		{
			name: "plugins query failure degrades to not installed",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").WillReturnError(errors.New("denied"))
			},
			want: AuditLogCapabilities{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			tt.setup(mock)

			got := detectAuditLog(t.Context(), db)
			if got.Installed != tt.want.Installed || got.PluginName != tt.want.PluginName ||
				got.PluginStatus != tt.want.PluginStatus || got.Variant != tt.want.Variant {
				t.Errorf("detectAuditLog = %+v, want %+v", got, tt.want)
			}
			if tt.checkConfig != nil {
				if len(got.Config) != len(tt.checkConfig) {
					t.Errorf("Config = %v, want %v", got.Config, tt.checkConfig)
				}
				for k, v := range tt.checkConfig {
					if got.Config[k] != v {
						t.Errorf("Config[%q] = %q, want %q", k, got.Config[k], v)
					}
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestDetectServerInfo(t *testing.T) {
	tests := []struct {
		name    string
		version string
		comment string // empty = variable absent
		want    ServerInfo
	}{
		{
			name:    "mysql community",
			version: "8.0.36",
			comment: "MySQL Community Server - GPL",
			want:    ServerInfo{Version: "8.0.36", VersionComment: "MySQL Community Server - GPL", Variant: "mysql"},
		},
		{
			name:    "percona server",
			version: "8.0.36-28",
			comment: "Percona Server (GPL), Release 28",
			want:    ServerInfo{Version: "8.0.36-28", VersionComment: "Percona Server (GPL), Release 28", Variant: "percona"},
		},
		{
			name:    "mariadb",
			version: "10.11.6-MariaDB",
			comment: "mariadb.org binary distribution",
			want:    ServerInfo{Version: "10.11.6-MariaDB", VersionComment: "mariadb.org binary distribution", Variant: "mariadb"},
		},
		{
			name:    "missing version_comment leaves variant empty",
			version: "8.0.36",
			comment: "",
			want:    ServerInfo{Version: "8.0.36"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").
				WillReturnRows(varRows("version", tt.version))
			e := mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version_comment'")
			if tt.comment == "" {
				e.WillReturnRows(emptyVarRows())
			} else {
				e.WillReturnRows(varRows("version_comment", tt.comment))
			}

			got := detectServerInfo(t.Context(), db)
			if got != tt.want {
				t.Errorf("detectServerInfo = %+v, want %+v", got, tt.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestDetectCapabilities exercises the composed entry point end-to-end against
// a mocked server: p_s enabled with one consumer off, no audit plugin, MySQL
// community — the most common self-managed 8.0 shape.
func TestDetectCapabilities(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
		WillReturnRows(varRows("performance_schema", "ON"))
	mock.ExpectQuery("FROM performance_schema.setup_consumers WHERE NAME IN").
		WillReturnRows(consumerRows(
			[2]string{"events_statements_history", "YES"},
			[2]string{"events_statements_history_long", "NO"}))
	mock.ExpectQuery("FROM performance_schema.threads WHERE TYPE = 'FOREGROUND' LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(3))
	mock.ExpectQuery("FROM information_schema.PLUGINS").
		WillReturnRows(sqlmock.NewRows([]string{"PLUGIN_NAME", "PLUGIN_STATUS", "PLUGIN_DESCRIPTION"}))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").
		WillReturnRows(varRows("version", "8.0.36"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version_comment'").
		WillReturnRows(varRows("version_comment", "MySQL Community Server - GPL"))

	caps, err := DetectCapabilities(t.Context(), db)
	if err != nil {
		t.Fatalf("DetectCapabilities: %v", err)
	}
	if !caps.PerformanceSchema.Enabled {
		t.Error("PerformanceSchema.Enabled = false, want true")
	}
	if !caps.PerformanceSchema.Consumers.EventsStatementsHistory {
		t.Error("EventsStatementsHistory = false, want true")
	}
	if caps.PerformanceSchema.Consumers.EventsStatementsHistoryLong {
		t.Error("EventsStatementsHistoryLong = true, want false")
	}
	if !caps.PerformanceSchema.ThreadsAccessible {
		t.Error("ThreadsAccessible = false, want true")
	}
	if caps.AuditLog.Installed {
		t.Error("AuditLog.Installed = true, want false")
	}
	if caps.ServerInfo.Variant != "mysql" {
		t.Errorf("ServerInfo.Variant = %q, want mysql", caps.ServerInfo.Variant)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDetectCapabilitiesUnreachableServer pins the one hard-error path: an
// unreachable server must return an error, not all-false capabilities that
// would read as "nothing is configured".
func TestDetectCapabilitiesUnreachableServer(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectPing().WillReturnError(errors.New("connection refused"))

	if _, err := DetectCapabilities(t.Context(), db); err == nil {
		t.Fatal("expected error for unreachable server, got nil")
	}
}
