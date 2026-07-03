//go:build integration

package forensics

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationReadAuditLog_Discovery exercises the SHOW GLOBAL VARIABLES
// discovery path against the integration MySQL container (127.0.0.1:13306,
// root/testroot). Stock MySQL (mysql:8.4 in docker-compose.yml) ships no
// audit plugin, so the expected outcome is the graceful
// ErrAuditNotConfigured path — proving discovery neither errors out nor
// invents a log file on a plugin-less server. If the container flavor ever
// changes to one with an audit plugin configured, the test skips instead of
// asserting a stale expectation.
func TestIntegrationReadAuditLog_Discovery(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	db, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Probe the audit variables directly so the assertion below stays
	// truthful if the container gains an audit plugin one day.
	var name, value string
	hasAuditVar := db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE 'audit_log_file'").Scan(&name, &value) == nil ||
		db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE 'server_audit_file_path'").Scan(&name, &value) == nil

	res, err := ReadAuditLog(ctx, db, AuditReadOptions{})
	if hasAuditVar {
		t.Skipf("integration container has an audit plugin configured (%s=%s); not asserting the not-configured path (ReadAuditLog err=%v, %d events)",
			name, value, err, len(res.Events))
	}
	if !errors.Is(err, ErrAuditNotConfigured) {
		t.Fatalf("ReadAuditLog on a server without an audit plugin: err = %v, want ErrAuditNotConfigured", err)
	}
}
