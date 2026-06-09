//go:build integration

package doctor

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIndexChecksWithAbsentDatabase covers #384 end-to-end against a real
// MySQL: an --index-dsn naming a database that does not exist must NOT fail
// the index checks (the driver pings with error 1049 before any probing
// logic runs) — the connection check passes against the server, and the
// write-access check verifies the CREATE DATABASE privilege via the probe,
// then drops the probe-created database so the diagnostic leaves no state.
func TestIndexChecksWithAbsentDatabase(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := t.Context()

	// A name that is never created by any fixture. Cleanup defensively in
	// case an assertion below fails midway.
	dbName := fmt.Sprintf("bintrail_doctor_%d", time.Now().UnixNano())
	dsn := fmt.Sprintf("%s/%s?parseTime=true", testutil.BaseDSN(), dbName)
	t.Cleanup(func() {
		cleanup, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
		if err != nil {
			return
		}
		defer cleanup.Close()
		_, _ = cleanup.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	})

	conn := checkIndexConnection(ctx, dsn, dbName)
	if conn.Status != StatusPass {
		t.Errorf("checkIndexConnection with absent DB: Status = %q (detail=%q), want PASS", conn.Status, conn.Detail)
	}

	write := checkIndexWriteAccess(ctx, dsn, dbName)
	if write.Status != StatusPass {
		t.Errorf("checkIndexWriteAccess with absent DB: Status = %q (detail=%q), want PASS", write.Status, write.Detail)
	}

	// The probe must not leave the database behind (#384).
	server, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	defer server.Close()
	var found string
	scanErr := server.QueryRowContext(ctx,
		"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", dbName).Scan(&found)
	if !errors.Is(scanErr, sql.ErrNoRows) {
		t.Errorf("probe-created database %q still exists after the check (scan err=%v)", dbName, scanErr)
	}

	// Sanity: a genuinely unreachable server must still FAIL, not be
	// mistaken for the absent-DB case.
	bad := checkIndexConnection(ctx, "root:wrong@tcp(127.0.0.1:1)/nope?timeout=1s", "nope")
	if bad.Status != StatusFail {
		t.Errorf("unreachable server: Status = %q, want FAIL", bad.Status)
	}
}
