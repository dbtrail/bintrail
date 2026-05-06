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
	"errors"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
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

	// All time-travel queries use SELECT * — the shim's parser
	// (internal/shim/parser.go) hard-requires a literal star;
	// `SELECT id, sku, ...` falls through to "this server only
	// handles _flashback / _snapshot / _diff" rejection.
	t.Run("flashback_returns_post_image_at_asof", func(t *testing.T) {
		// AS OF 13:00 → after the 12:00 UPDATE (qty=2), before the
		// 14:00 DELETE. Expect qty=2.
		gotCols, gotRow := queryRowMapWithCols(t, clientDB,
			"SELECT * FROM _flashback.orders AS OF '2026-05-04 13:00:00' WHERE id = 42")
		wantRow := map[string]string{"id": "42", "sku": "ABC-1", "qty": "2", "note": "initial"}
		if !maps.Equal(gotRow, wantRow) {
			t.Errorf("flashback row mismatch:\n  got:  %+v\n  want: %+v", gotRow, wantRow)
		}
		// Column order must match the source table's DDL
		// (id, sku, qty, note) so a side-by-side comparison
		// with `SELECT * FROM appdb.orders` lines up — this
		// is the schema_snapshots → handler.columnOrderFor
		// path. A regression where the resolver stops being
		// consulted would silently revert to alphabetical
		// (id, note, qty, sku) and the row map check above
		// would still pass.
		wantCols := []string{"id", "sku", "qty", "note"}
		if !slices.Equal(gotCols, wantCols) {
			t.Errorf("flashback column order = %v, want %v\n"+
				"if alphabetical, schema_snapshots lookup is broken", gotCols, wantCols)
		}
	})

	t.Run("flashback_pre_insert_returns_empty", func(t *testing.T) {
		// AS OF 09:00 → before the 10:00 INSERT. Expect zero rows.
		// The shim's emptyResult returns a one-column resultset
		// with the literal header "_flashback"; we don't scan, just
		// verify Next() returns false.
		rows, err := clientDB.Query(
			"SELECT * FROM _flashback.orders AS OF '2026-05-04 09:00:00' WHERE id = 42")
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
			"SELECT * FROM _diff.orders BETWEEN '2026-05-04 09:00:00' AND '2026-05-04 16:00:00' " +
				"WHERE id = 42")
		if err != nil {
			t.Fatalf("diff query: %v", err)
		}
		defer rows.Close()

		// _diff's resultset shape is fixed by handler.runDiff:
		// (event_id, event_timestamp, event_type, gtid, row_before, row_after)
		// so positional scan is safe here — unlike the flashback
		// case where column order is JSON-key-sorted.
		//
		// row_before and row_after scan into sql.NullString because
		// go-mysql's BuildSimpleTextResultset encodes the empty
		// string as a NULL on the wire, and the shim deliberately
		// emits "" for INSERTs (no before-image) and DELETEs (no
		// after-image). diffRow normalises both back to "".
		var got []diffRow
		for rows.Next() {
			var (
				d                   diffRow
				rowBefore, rowAfter sql.NullString
			)
			if err := rows.Scan(&d.eventID, &d.timestamp, &d.eventType, &d.gtid, &rowBefore, &rowAfter); err != nil {
				t.Fatalf("scan diff: %v", err)
			}
			d.rowBefore = rowBefore.String // "" when invalid (NULL on the wire)
			d.rowAfter = rowAfter.String
			got = append(got, d)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}

		if len(got) != 3 {
			t.Fatalf("expected 3 diff rows (INSERT, UPDATE, DELETE), got %d: %+v", len(got), got)
		}

		// Substring match on JSON includes a delimiter (`,` or `}`)
		// so `"qty":1` doesn't accidentally match `"qty":10` or
		// `"qty":100`. The shim emits keys in DDL order
		// (id, sku, qty, note) so qty is never the last key — the
		// trailing `,` is always a stable delimiter here.
		assertDiff(t, got[0], "2026-05-04 10:00:00", "INSERT", "", `"qty":1,`)
		assertDiff(t, got[1], "2026-05-04 12:00:00", "UPDATE", `"qty":1,`, `"qty":2,`)
		assertDiff(t, got[2], "2026-05-04 14:00:00", "DELETE", `"qty":2,`, "")

		// Pin the full JSON key ordering. A prefix-only check
		// (e.g. `{"id":42,"sku":"ABC-1",`) would only prove
		// id < sku, leaving "alphabetised the tail" regressions
		// undetected. Full-string equality eliminates the entire
		// reorder-class bug.
		const wantRowAfter = `{"id":42,"sku":"ABC-1","qty":2,"note":"initial"}`
		if got[1].rowAfter != wantRowAfter {
			t.Errorf("_diff JSON key order regression:\n  got:  %s\n  want: %s\n"+
				"if columns alphabetise, the marshalImageOrdered path is broken",
				got[1].rowAfter, wantRowAfter)
		}
	})

	t.Run("snapshot_matches_flashback", func(t *testing.T) {
		// _snapshot must return the same row image as _flashback
		// for the same AS OF. They share an implementation
		// (handler.runPointInTime); pinning the contract here
		// keeps a future split (baseline-lookup support) deliberate.
		got := queryRowMap(t, clientDB,
			"SELECT * FROM _snapshot.orders AS OF '2026-05-04 11:00:00' WHERE id = 42")
		want := map[string]string{"id": "42", "sku": "ABC-1", "qty": "1", "note": "initial"}
		if !maps.Equal(got, want) {
			t.Errorf("snapshot row mismatch:\n  got:  %+v\n  want: %+v", got, want)
		}
	})

	t.Run("auth_rejection_returns_1045_not_1449", func(t *testing.T) {
		// Guards against the issue #262 / v0.7.4 regression
		// class: TenantAuth must surface bad credentials as
		// ER_ACCESS_DENIED_ERROR (1045), not ER_NO_SUCH_USER
		// (1449) or a generic conn-reset. ProxySQL's monitor
		// probe SHUNNs the shim hostgroup if it sees anything
		// other than 1045 — leaving the routing silently broken.
		// Unit tests cannot catch this because the wire-shape
		// rewriting happens inside go-mysql/server's handshake
		// rather than in TenantAuth itself.
		badDB, err := sql.Open("mysql", "wronguser:wrongpw@tcp("+proxysqlClientAddr+")/appdb")
		if err != nil {
			t.Fatalf("open with bad creds: %v", err)
		}
		defer badDB.Close()

		err = badDB.Ping()
		if err == nil {
			t.Fatalf("expected ping with bad creds to fail, got nil")
		}
		var mysqlErr *mysql.MySQLError
		if !errors.As(err, &mysqlErr) {
			t.Fatalf("expected *mysql.MySQLError, got %T: %v", err, err)
		}
		if mysqlErr.Number != 1045 {
			t.Fatalf("expected error code 1045 (ER_ACCESS_DENIED_ERROR), got %d: %s",
				mysqlErr.Number, mysqlErr.Message)
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
		got := queryRowMap(t, clientDB,
			"SELECT * FROM orders WHERE id = 42")
		want := map[string]string{"id": "42", "sku": "LIVE-SKU", "qty": "999", "note": "live-row-from-passthrough"}
		if !maps.Equal(got, want) {
			t.Errorf("passthrough row mismatch:\n  got:  %+v\n  want: %+v\n"+
				"if this looks like a shim error, the regex in `bintrail proxysql-config` "+
				"is over-matching", got, want)
		}
	})
}

type diffRow struct {
	eventID   int64
	timestamp string
	eventType string
	gtid      string
	rowBefore string
	rowAfter  string
}

// queryRowMap runs a single-row SELECT and returns just the
// {column → value} map. Use queryRowMapWithCols when the test
// also needs to assert column ORDER (the shim now honours the
// source DDL via schema_snapshots, so order differences across
// shim vs passthrough are themselves a regression to catch).
func queryRowMap(t *testing.T, db *sql.DB, q string) map[string]string {
	t.Helper()
	_, row := queryRowMapWithCols(t, db, q)
	return row
}

// queryRowMapWithCols is the underlying single-row SELECT helper.
// Returns (column-order, {column → value} map) so callers can
// assert both shape and content. Map-based scan is mandatory:
// positional Scan would silently swap fields between the shim
// (DDL order via schema_snapshots) and the passthrough (DDL order
// from MySQL itself) the moment those orders diverge — e.g. if a
// future ALTER TABLE rebuilds the table in a different physical
// order than the snapshot reflects.
func queryRowMapWithCols(t *testing.T, db *sql.DB, q string) ([]string, map[string]string) {
	t.Helper()
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping before query: %v", err)
	}
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		// cols=[_flashback] means the shim returned its
		// emptyResult (no matching event); cols=[id sku qty note]
		// means the passthrough hostgroup ran the query against
		// real MySQL and matched nothing — different bugs.
		t.Fatalf("expected exactly one row, got zero (query=%q, cols=%v)", q, cols)
	}

	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		t.Fatalf("scan: %v", err)
	}

	out := make(map[string]string, len(cols))
	for i, c := range cols {
		if raw[i] == nil {
			out[c] = "<nil>"
			continue
		}
		out[c] = string(raw[i])
	}

	if rows.Next() {
		t.Fatalf("expected exactly one row, got at least two")
	}
	return cols, out
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

	// ProxySQL admin uses the MySQL protocol on port 6032. The
	// stock `admin:admin` user is loopback-only, so we connect as
	// the `radminuser:radminpw` extra credential declared in
	// proxysql.cnf (see proxysql.cnf for the rationale).
	adminDB := openAdminWithRetry(t, "radminuser:radminpw@tcp("+proxysqlAdminAddr+")/", 30*time.Second)
	defer adminDB.Close()

	// Pin to a single connection so the BEGIN/COMMIT pair emitted by
	// proxysql-config actually wraps the inner DELETE/INSERTs. Without
	// this each Exec may pull a different conn from the pool, which
	// silently demotes the transaction to a sequence of independent
	// statements — and a half-applied script leaves the next run with
	// a primary-key collision on the INSERT-after-DELETE pattern.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := adminDB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire admin conn: %v", err)
	}
	defer conn.Close()

	for _, stmt := range splitSQL(setupSQL.String()) {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			// Dump container logs first — ProxySQL's admin
			// rejection messages often clarify *why* a stmt
			// failed (typo in regex, bad hostgroup id) and
			// they live in proxysql's stdout, not in the
			// MySQL wire error we get back here.
			dumpComposeLogs(t)
			t.Fatalf("apply admin stmt %q: %v", abbreviate(stmt, 80), err)
		}
	}
}

// splitSQL splits a multi-statement script into individual statements
// for sequential Exec. ProxySQL's admin parser doesn't accept
// multi-statement Exec calls; running them one at a time is the
// supported path. Comments and blank lines are dropped so they don't
// turn into empty Exec calls.
//
// This is a naive split-on-`;`: it would mangle a statement
// containing a `;` inside a string literal. proxysql-config's output
// happens to contain no such literals today (the only single-quoted
// strings are hostnames, usernames, regex patterns, and the SHA1
// hash) — if that ever changes, replace this with a real tokenizer.
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

// isAuthError reports whether err is a permanent auth-rejection
// (vs. "ProxySQL is still warming up"). Type-checks for go-sql-driver's
// MySQLError with code 1045 (ER_ACCESS_DENIED_ERROR) — the wire
// code is stable across MySQL/ProxySQL wording changes, unlike a
// substring match on "access denied" which could miss future
// ProxySQL-specific messages or false-positive on unrelated errors
// that happen to contain that phrase.
func isAuthError(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1045
}
