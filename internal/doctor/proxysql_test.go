package doctor

import (
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCheckProxySQLRulesOn(t *testing.T) {
	forcedErr := errors.New("forced query failure")

	// ruleRows builds a runtime_mysql_query_rules resultset. ProxySQL's admin
	// interface serves TEXT columns, which is what the check scans.
	ruleRows := func(pairs ...[2]string) *sqlmock.Rows {
		rows := sqlmock.NewRows([]string{"rule_id", "active"})
		for _, p := range pairs {
			rows.AddRow(p[0], p[1])
		}
		return rows
	}
	allSix := func(active string) *sqlmock.Rows {
		return ruleRows(
			[2]string{"990001", active}, [2]string{"990002", active},
			[2]string{"990003", active}, [2]string{"990004", active},
			[2]string{"990005", active}, [2]string{"990006", active})
	}

	tests := []struct {
		name           string
		setup          func(m sqlmock.Sqlmock)
		wantStatus     CheckStatus
		wantDetailFrag string
		wantRemedFrags []string
	}{
		{
			name: "all six rules active passes",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM runtime_mysql_query_rules").WillReturnRows(allSix("1"))
			},
			wantStatus:     StatusPass,
			wantDetailFrag: "rules 990001-990006 live",
		},
		{
			name: "missing hint rule warns naming the id and the silent-present hazard",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM runtime_mysql_query_rules").WillReturnRows(ruleRows(
					[2]string{"990001", "1"}, [2]string{"990002", "1"},
					[2]string{"990003", "1"}, [2]string{"990005", "1"},
					[2]string{"990006", "1"}))
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "missing: 990004",
			wantRemedFrags: []string{"bintrail proxysql-config", "DBTRAIL_AT"},
		},
		{
			name: "present but inactive rule warns as inactive",
			setup: func(m sqlmock.Sqlmock) {
				rows := ruleRows(
					[2]string{"990001", "1"}, [2]string{"990002", "1"},
					[2]string{"990003", "1"}, [2]string{"990004", "0"},
					[2]string{"990005", "1"}, [2]string{"990006", "1"})
				m.ExpectQuery("FROM runtime_mysql_query_rules").WillReturnRows(rows)
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "inactive: 990004",
			wantRemedFrags: []string{"bintrail proxysql-config"},
		},
		{
			name: "empty resultset warns with every rule missing",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM runtime_mysql_query_rules").WillReturnRows(ruleRows())
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "missing: 990001, 990002, 990003, 990004, 990005, 990006",
		},
		{
			name: "query failure warns pointing at the admin-vs-traffic port confusion",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM runtime_mysql_query_rules").WillReturnError(forcedErr)
			},
			wantStatus:     StatusWarn,
			wantDetailFrag: "could not query runtime_mysql_query_rules",
			wantRemedFrags: []string{"6032"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			tt.setup(mock)

			got := checkProxySQLRulesOn(t.Context(), db)

			if got.Name != proxySQLRulesCheckName {
				t.Errorf("Name = %q, want %q", got.Name, proxySQLRulesCheckName)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q (detail=%q), want %q", got.Status, got.Detail, tt.wantStatus)
			}
			// The check is advisory by contract (#820): it must never flip
			// doctor's exit code.
			if got.Status == StatusFail {
				t.Errorf("Status = FAIL — the ProxySQL rules check must be warn-only")
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want fragment %q", got.Detail, tt.wantDetailFrag)
			}
			for _, frag := range tt.wantRemedFrags {
				if !strings.Contains(got.Remediation, frag) {
					t.Errorf("Remediation = %q, want fragment %q", got.Remediation, frag)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet sqlmock expectations: %v", err)
			}
		})
	}
}

func TestCheckProxySQLRulesBadDSNWarns(t *testing.T) {
	got := CheckProxySQLRules(t.Context(), "not a dsn")
	if got.Status != StatusWarn {
		t.Errorf("Status = %q, want WARN", got.Status)
	}
	if !strings.Contains(got.Detail, "could not connect to the ProxySQL admin interface") {
		t.Errorf("Detail = %q, want the connect-failure prefix", got.Detail)
	}
	if !strings.Contains(got.Remediation, "6032") {
		t.Errorf("Remediation = %q, want the admin-port hint", got.Remediation)
	}
}
