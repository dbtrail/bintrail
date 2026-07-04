package doctor

import (
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// The forensics checks are advisory: forensics is an optional capability, so
// every branch — including total probe failure — must resolve to PASS or WARN,
// never FAIL (a missing audit plugin must not block `up` from streaming).
// Each table below asserts the branch outcome AND that structural invariant.

func TestCheckPerformanceSchema(t *testing.T) {
	forcedErr := errors.New("forced query failure")

	psVarRows := func(val string) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("performance_schema", val)
	}
	consumerRows := func(pairs ...[2]string) *sqlmock.Rows {
		rows := sqlmock.NewRows([]string{"NAME", "ENABLED"})
		for _, p := range pairs {
			rows.AddRow(p[0], p[1])
		}
		return rows
	}

	tests := []struct {
		name             string
		setup            func(m sqlmock.Sqlmock)
		wantStatus       CheckStatus
		wantDetailFrag   string
		wantRemedFrags   []string // all must be present in Remediation
		wantNoRemedation bool
	}{
		{
			name: "ON with both consumers enabled",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(psVarRows("ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers").
					WillReturnRows(consumerRows(
						[2]string{"events_statements_history", "YES"},
						[2]string{"events_statements_history_long", "YES"}))
			},
			wantStatus:       StatusPass,
			wantDetailFrag:   "statement history consumers enabled",
			wantNoRemedation: true,
		},
		{
			name: "ON with history_long consumer off warns with runtime SQL and the RDS reboot caveat",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(psVarRows("ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers").
					WillReturnRows(consumerRows(
						[2]string{"events_statements_history", "YES"},
						[2]string{"events_statements_history_long", "NO"}))
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "events_statements_history_long",
			wantRemedFrags: []string{
				"UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' WHERE NAME = 'events_statements_history_long';",
				"re-assert it after every reboot",
			},
		},
		{
			name: "ON with consumers absent from the resultset treats both as off",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(psVarRows("ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers").
					WillReturnRows(consumerRows())
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "events_statements_history, events_statements_history_long",
			wantRemedFrags: []string{"WHERE NAME = 'events_statements_history';", "WHERE NAME = 'events_statements_history_long';"},
		},
		{
			name: "OFF warns with the my.cnf remediation",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(psVarRows("OFF"))
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "performance_schema=OFF",
			wantRemedFrags: []string{"performance_schema = ON", "parameter group"},
		},
		{
			name: "variable query failure degrades to WARN",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnError(forcedErr)
			},
			wantStatus:       StatusWarn,
			wantDetailFrag:   "could not read the performance_schema variable",
			wantNoRemedation: true,
		},
		{
			name: "consumers query failure degrades to WARN",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'performance_schema'").
					WillReturnRows(psVarRows("ON"))
				m.ExpectQuery("FROM performance_schema.setup_consumers").
					WillReturnError(forcedErr)
			},
			wantStatus:       StatusWarn,
			wantDetailFrag:   "could not read setup_consumers",
			wantNoRemedation: true,
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

			got := checkPerformanceSchema(t.Context(), db)
			if got.Status == StatusFail {
				t.Errorf("forensics checks must NEVER fail — got FAIL (detail=%q)", got.Detail)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			for _, frag := range tt.wantRemedFrags {
				if !strings.Contains(got.Remediation, frag) {
					t.Errorf("Remediation missing %q:\n%s", frag, got.Remediation)
				}
			}
			if tt.wantNoRemedation && got.Remediation != "" && tt.wantStatus == StatusPass {
				t.Errorf("PASS should not carry remediation, got %q", got.Remediation)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestCheckAuditPlugin(t *testing.T) {
	pluginRows := func(names ...string) *sqlmock.Rows {
		rows := sqlmock.NewRows([]string{"PLUGIN_NAME"})
		for _, n := range names {
			rows.AddRow(n)
		}
		return rows
	}

	tests := []struct {
		name           string
		setup          func(m sqlmock.Sqlmock)
		wantStatus     CheckStatus
		wantDetailFrag string
		wantRemedFrags []string
	}{
		{
			name: "active audit plugin passes",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows("SERVER_AUDIT"))
			},
			wantStatus:     StatusPass,
			wantDetailFrag: "SERVER_AUDIT active",
		},
		{
			name: "RDS internal security plugin does not count",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows("RDS_SECURITY_AUDIT"))
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "no audit log plugin active",
			wantRemedFrags: []string{"INSTALL SONAME 'server_audit';", "MARIADB_AUDIT_PLUGIN"},
		},
		{
			name: "RDS internal plugin skipped, real plugin after it passes",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows("RDS_SECURITY_AUDIT", "audit_log"))
			},
			wantStatus:     StatusPass,
			wantDetailFrag: "audit_log active",
		},
		{
			name: "no audit plugin warns with variant-specific install guidance",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnRows(pluginRows())
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "no audit log plugin active",
			wantRemedFrags: []string{
				"INSTALL SONAME 'server_audit';",
				"INSTALL PLUGIN audit_log_filter SONAME 'audit_log_filter.so';",
				"MARIADB_AUDIT_PLUGIN",
				"server_audit_logging=1",
			},
		},
		{
			name: "plugins query failure degrades to WARN",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM information_schema.PLUGINS").
					WillReturnError(errors.New("forced query failure"))
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "could not query information_schema.PLUGINS",
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

			got := checkAuditPlugin(t.Context(), db)
			if got.Status == StatusFail {
				t.Errorf("forensics checks must NEVER fail — got FAIL (detail=%q)", got.Detail)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			for _, frag := range tt.wantRemedFrags {
				if !strings.Contains(got.Remediation, frag) {
					t.Errorf("Remediation missing %q:\n%s", frag, got.Remediation)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}
