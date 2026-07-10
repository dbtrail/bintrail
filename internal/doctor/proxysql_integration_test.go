//go:build integration

package doctor

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestCheckProxySQLRulesAgainstPlainMySQL pins the most likely operator
// mistake for --proxysql-admin: pointing it at a MySQL server (or ProxySQL's
// traffic port) instead of the admin interface. A plain MySQL has no
// runtime_mysql_query_rules table, so the check must WARN — never FAIL, and
// never PASS — over the real wire protocol.
func TestCheckProxySQLRulesAgainstPlainMySQL(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	got := CheckProxySQLRules(t.Context(), testutil.BaseDSN()+"/")
	if got.Status != StatusWarn {
		t.Errorf("Status = %q (detail=%q), want WARN", got.Status, got.Detail)
	}
	if !strings.Contains(got.Detail, "runtime_mysql_query_rules") {
		t.Errorf("Detail = %q, want it to name runtime_mysql_query_rules", got.Detail)
	}
	if !strings.Contains(got.Remediation, "6032") {
		t.Errorf("Remediation = %q, want the admin-port hint", got.Remediation)
	}
}

// TestCheckProxySQLRulesUnreachableWarns: a dead admin endpoint is WARN (the
// check is advisory), not FAIL.
func TestCheckProxySQLRulesUnreachableWarns(t *testing.T) {
	got := CheckProxySQLRules(t.Context(), "admin:admin@tcp(127.0.0.1:1)/?timeout=1s")
	if got.Status != StatusWarn {
		t.Errorf("Status = %q (detail=%q), want WARN", got.Status, got.Detail)
	}
	if !strings.Contains(got.Detail, "could not connect") {
		t.Errorf("Detail = %q, want the connect-failure prefix", got.Detail)
	}
}
