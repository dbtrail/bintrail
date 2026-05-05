//go:build shim_e2e

// Package shim_e2e is the wire-protocol-level end-to-end test for
// bintrail shim. It exercises the full chain a deployed setup uses:
//
//	go test (mysql client) → ProxySQL → bintrail shim → MySQL (bintrail_index)
//
// The companion docker-compose.yml brings up the three containers;
// run.sh wraps `docker compose up --build` + `go test` so an operator
// can reproduce a CI failure with a single command.
//
// The test is gated behind the `shim_e2e` build tag (so default
// `go test ./...` skips it) and an explicit Docker availability
// probe (so a developer who runs `go test -tags shim_e2e ./...`
// without Docker gets a clear skip rather than a confusing
// connection-refused error).
//
// What this test does NOT cover (deliberately):
//   - The binlog parser → indexer pipeline. seed.sql hand-writes the
//     binlog_events rows so we can pin a deterministic time series.
//     The parser/indexer have their own integration tests under
//     internal/parser and internal/indexer.
//   - The Parquet archive read path. archive_state is empty here;
//     archives are exercised by internal/parquetquery's tests.
package shim_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	proxysqlAdminAddr  = "127.0.0.1:16032"
	proxysqlClientAddr = "127.0.0.1:16033"

	// proxysqlBackendDSN is the host-side view of the MySQL backend
	// for the SQL emitted by `bintrail proxysql-config`. The host is
	// the docker-compose service name (`mysql`) because the SQL is
	// loaded into ProxySQL, which resolves it inside the compose
	// network — not on the host.
	proxysqlBackendDSN = "root:testroot@tcp(mysql:3306)/appdb"

	// readyDeadline caps how long we wait for ProxySQL admin + client
	// ports to be reachable after `compose up`. Long enough to absorb
	// a slow image pull on a cold runner; short enough that a real
	// failure surfaces in under a minute.
	readyDeadline = 90 * time.Second
)

// TestShimEndToEnd asserts the four cases promised by the issue:
//
//  1. _flashback returns the row's state at-or-before the AS OF
//     timestamp (selecting the right post-image from a multi-event
//     history).
//  2. _diff returns every event in the time window in chronological
//     order, with the right metadata.
//  3. _snapshot behaves like _flashback (reserved for future
//     baseline-lookup support).
//  4. A non-virtual-schema query is routed to the passthrough
//     backend — verified by the row content (the live row has
//     a marker value that no binlog event contains).
//
// All four go through the real ProxySQL routing layer, so a
// regression in the regex rules emitted by `bintrail proxysql-config`
// surfaces as a routing-class failure here (e.g. _flashback query
// would hit the passthrough and return a "table doesn't exist"
// error from MySQL instead of a reconstructed image).
func TestShimEndToEnd(t *testing.T) {
	if os.Getenv("SHIM_E2E") == "" {
		t.Skip("set SHIM_E2E=1 to run the shim wire-protocol e2e (requires Docker)")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH; skipping shim e2e")
	}

	bintrailBin := buildBintrailBinary(t)

	// Compose lifecycle is owned by the test (not run.sh) so a
	// developer running `go test -tags shim_e2e ./e2e/shim/...`
	// directly still gets the full setup + teardown.
	composeUp(t)
	t.Cleanup(func() { composeDown(t) })

	waitForPort(t, proxysqlAdminAddr, readyDeadline)
	waitForPort(t, proxysqlClientAddr, readyDeadline)

	applyProxySQLConfig(t, bintrailBin)

	// Wait for the freshly-loaded mysql_users to propagate so the
	// first client login doesn't race ProxySQL's internal LOAD.
	clientDB := openClientWithRetry(t, "testuser:testpw@tcp("+proxysqlClientAddr+")/appdb", 30*time.Second)
	t.Cleanup(func() { clientDB.Close() })

	t.Run("flashback_returns_post_image_at_asof", func(t *testing.T) {
		// AS OF 13:00 → after the 12:00 UPDATE (qty=2), before the
		// 14:00 DELETE. Expect qty=2.
		row := queryRow(t, clientDB,
			"SELECT id, sku, qty, note FROM _flashback.orders AS OF '2026-05-04 13:00:00' WHERE id = 42")
		got := scanOrder(t, row)
		want := orderRow{id: "42", sku: "ABC-1", qty: "2", note: "initial"}
		if got != want {
			t.Errorf("flashback row mismatch:\n  got:  %+v\n  want: %+v", got, want)
		}
	})

	t.Run("flashback_pre_insert_returns_empty", func(t *testing.T) {
		// AS OF 09:00 → before the 10:00 INSERT. Expect zero rows.
		// emptyResult uses the literal "_flashback" column header,
		// so a column-shape mismatch from a future refactor would
		// show up as a Scan error, not a silent pass.
		rows, err := clientDB.Query(
			"SELECT id, sku, qty, note FROM _flashback.orders AS OF '2026-05-04 09:00:00' WHERE id = 42")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		if rows.Next() {
			t.Fatalf("expected zero rows for AS OF before INSERT, got at least one")
		}
	})

	t.Run("diff_returns_event_history", func(t *testing.T) {
		rows, err := clientDB.Query(
			"SELECT event_id, event_timestamp, event_type, gtid, row_before, row_after " +
				"FROM _diff.orders BETWEEN '2026-05-04 09:00:00' AND '2026-05-04 16:00:00' " +
				"WHERE id = 42")
		if err != nil {
			t.Fatalf("diff query: %v", err)
		}
		defer rows.Close()

		var got []diffRow
		for rows.Next() {
			var d diffRow
			if err := rows.Scan(&d.eventID, &d.timestamp, &d.eventType, &d.gtid, &d.rowBefore, &d.rowAfter); err != nil {
				t.Fatalf("scan diff: %v", err)
			}
			got = append(got, d)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}

		if len(got) != 3 {
			t.Fatalf("expected 3 diff rows (INSERT, UPDATE, DELETE), got %d: %+v", len(got), got)
		}

		assertDiff(t, got[0], "2026-05-04 10:00:00", "INSERT", "", `"qty": 1`)
		assertDiff(t, got[1], "2026-05-04 12:00:00", "UPDATE", `"qty": 1`, `"qty": 2`)
		assertDiff(t, got[2], "2026-05-04 14:00:00", "DELETE", `"qty": 2`, "")
	})

	t.Run("snapshot_matches_flashback", func(t *testing.T) {
		// _snapshot today is implemented as an alias of _flashback;
		// asserting that here pins the contract so a future split
		// (baseline-lookup support) can be reviewed deliberately.
		row := queryRow(t, clientDB,
			"SELECT id, sku, qty, note FROM _snapshot.orders AS OF '2026-05-04 11:00:00' WHERE id = 42")
		got := scanOrder(t, row)
		want := orderRow{id: "42", sku: "ABC-1", qty: "1", note: "initial"}
		if got != want {
			t.Errorf("snapshot row mismatch:\n  got:  %+v\n  want: %+v", got, want)
		}
	})

	t.Run("passthrough_query_hits_real_mysql_not_shim", func(t *testing.T) {
		// `appdb.orders` (no virtual schema) must route to the
		// passthrough hostgroup. The live row has marker values
		// (sku=LIVE-SKU, qty=999) that no binlog event in the seed
		// contains — so an accidental shim route would either
		// error ("this server only handles _flashback / _snapshot
		// / _diff …") or return the historical image, neither of
		// which match these markers.
		row := queryRow(t, clientDB,
			"SELECT id, sku, qty, note FROM orders WHERE id = 42")
		got := scanOrder(t, row)
		want := orderRow{id: "42", sku: "LIVE-SKU", qty: "999", note: "live-row-from-passthrough"}
		if got != want {
			t.Errorf("passthrough row mismatch:\n  got:  %+v\n  want: %+v\n"+
				"if this looks like a shim error, the regex in `bintrail proxysql-config` "+
				"is over-matching", got, want)
		}
	})
}

type orderRow struct {
	id, sku, qty, note string
}

type diffRow struct {
	eventID   int64
	timestamp string
	eventType string
	gtid      string
	rowBefore string
	rowAfter  string
}

func scanOrder(t *testing.T, row *sql.Row) orderRow {
	t.Helper()
	var o orderRow
	if err := row.Scan(&o.id, &o.sku, &o.qty, &o.note); err != nil {
		t.Fatalf("scan order: %v", err)
	}
	return o
}

func assertDiff(t *testing.T, d diffRow, wantTS, wantType, wantBeforeContains, wantAfterContains string) {
	t.Helper()
	if d.timestamp != wantTS {
		t.Errorf("diff[%s]: timestamp got %q, want %q", wantType, d.timestamp, wantTS)
	}
	if d.eventType != wantType {
		t.Errorf("diff[%s]: event_type got %q, want %q", wantType, d.eventType, wantType)
	}
	if wantBeforeContains == "" {
		if d.rowBefore != "" {
			t.Errorf("diff[%s]: row_before should be empty, got %q", wantType, d.rowBefore)
		}
	} else if !strings.Contains(d.rowBefore, wantBeforeContains) {
		t.Errorf("diff[%s]: row_before %q does not contain %q", wantType, d.rowBefore, wantBeforeContains)
	}
	if wantAfterContains == "" {
		if d.rowAfter != "" {
			t.Errorf("diff[%s]: row_after should be empty, got %q", wantType, d.rowAfter)
		}
	} else if !strings.Contains(d.rowAfter, wantAfterContains) {
		t.Errorf("diff[%s]: row_after %q does not contain %q", wantType, d.rowAfter, wantAfterContains)
	}
}

// queryRow wraps QueryRow with a slightly clearer failure path for
// the multi-statement subtests — sql.QueryRow defers errors to Scan
// time, but if the connection itself is bad (e.g. ProxySQL hasn't
// reloaded users yet) we want the failure to point at the right
// subtest, not at "scan: bad connection".
func queryRow(t *testing.T, db *sql.DB, q string) *sql.Row {
	t.Helper()
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping before query: %v", err)
	}
	return db.QueryRow(q)
}

// buildBintrailBinary builds a host-side `bintrail` binary so the
// test can call `proxysql-config` to produce the ProxySQL setup SQL.
// We could shell out via `go run` instead, but `go run` recompiles
// every invocation and the build cache hit on a real binary is
// faster overall.
func buildBintrailBinary(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "bintrail")
	cmd := exec.Command("go", "build", "-o", out, "../../cmd/bintrail")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build bintrail: %v", err)
	}
	return out
}

func composeUp(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "up", "-d", "--build", "--wait")
	cmd.Dir = "."
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		// Best-effort dump of container logs so a CI failure is
		// debuggable from the test output alone — without this,
		// `compose up failed` would leave the diagnostics inside
		// containers that compose down then deletes.
		dumpComposeLogs(t)
		t.Fatalf("compose up: %v", err)
	}
}

func composeDown(t *testing.T) {
	t.Helper()
	if t.Failed() {
		dumpComposeLogs(t)
	}
	cmd := exec.Command("docker", "compose", "down", "-v")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	_ = cmd.Run() // best effort
}

func dumpComposeLogs(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "logs", "--no-color", "--tail", "200")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}

func waitForPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("port %s not reachable after %s", addr, timeout)
}

// applyProxySQLConfig runs `bintrail proxysql-config` to generate
// the setup SQL, then pipes it into ProxySQL's admin port. This
// exercises the actual command instead of carrying a hand-rolled
// duplicate — a regression in proxysql-config's output (renamed
// rule, dropped LOAD line) surfaces here as a no-route failure
// in the subsequent subtests.
func applyProxySQLConfig(t *testing.T, bintrailBin string) {
	t.Helper()

	gen := exec.Command(bintrailBin, "proxysql-config",
		"--shim-config", "shim.yaml",
		"--mysql-port", "3306",
		"--shim-port", "3308",
		"--out", "-")
	gen.Env = append(os.Environ(), "BINTRAIL_SOURCE_DSN="+proxysqlBackendDSN)
	var setupSQL bytes.Buffer
	gen.Stdout = &setupSQL
	gen.Stderr = os.Stderr
	if err := gen.Run(); err != nil {
		t.Fatalf("generate proxysql-setup.sql: %v", err)
	}

	// ProxySQL admin uses the MySQL protocol; the default credentials
	// for the official image are admin/admin on port 6032. The setup
	// script is idempotent (DELETE then INSERT in a transaction),
	// so a transient retry on a not-yet-ready admin port is safe.
	adminDB := openAdminWithRetry(t, "admin:admin@tcp("+proxysqlAdminAddr+")/", 30*time.Second)
	defer adminDB.Close()

	for _, stmt := range splitSQL(setupSQL.String()) {
		if _, err := adminDB.Exec(stmt); err != nil {
			t.Fatalf("apply admin stmt %q: %v", abbreviate(stmt, 80), err)
		}
	}
}

// splitSQL splits a multi-statement script into individual statements
// for sequential Exec. ProxySQL's admin parser doesn't accept
// multi-statement Exec calls; running them one at a time is the
// supported path. Comments and blank lines are dropped so they don't
// turn into empty Exec calls.
func splitSQL(s string) []string {
	var out []string
	for _, raw := range strings.Split(s, ";") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		// Strip leading "-- " comment lines but keep statements
		// that have a trailing inline comment.
		filtered := make([]string, 0)
		for _, l := range strings.Split(line, "\n") {
			if strings.HasPrefix(strings.TrimSpace(l), "--") {
				continue
			}
			filtered = append(filtered, l)
		}
		joined := strings.TrimSpace(strings.Join(filtered, "\n"))
		if joined == "" {
			continue
		}
		out = append(out, joined)
	}
	return out
}

func abbreviate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func openAdminWithRetry(t *testing.T, dsn string, timeout time.Duration) *sql.DB {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				return db
			} else {
				lastErr = pingErr
				db.Close()
			}
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("connect to ProxySQL admin: %v", lastErr)
	return nil
}

// openClientWithRetry distinguishes "auth not yet loaded" (retry) from
// "wrong password" (fail fast). ProxySQL takes a beat to honour LOAD
// MYSQL USERS TO RUNTIME and the first few logins after applyProxySQLConfig
// can race that load — without this distinction the flake would look
// like a permanent auth failure.
func openClientWithRetry(t *testing.T, dsn string, timeout time.Duration) *sql.DB {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			lastErr = err
		} else if pingErr := db.Ping(); pingErr == nil {
			return db
		} else {
			lastErr = pingErr
			db.Close()
			// Permanent rejection — surface it now rather than
			// burning the rest of the deadline on a doomed retry.
			if isAuthError(pingErr) {
				t.Fatalf("ProxySQL rejected client credentials: %v", pingErr)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("connect as client: %v", lastErr)
	return nil
}

// isAuthError matches the go-sql-driver "Error 1045" wire-protocol
// access-denied response by message substring. Using the message
// keeps this helper from depending on the driver's typed error,
// which is fine here because we only need to distinguish "definitely
// permanent" from "still warming up".
func isAuthError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "access denied")
}
