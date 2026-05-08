package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/bintrail/internal/shim"
)

// startTestShim launches serveLoop on a localhost listener with the
// supplied tenants and returns the bound TCP address. The shim is
// cleaned up via t.Cleanup. indexDB is nil — the wire paths these
// tests exercise (handshake auth + parser-level rejection) never reach
// the indexer, so spinning up a real MySQL would buy nothing and force
// the file behind a `//go:build integration` tag that does not match
// what the test actually depends on.
//
// userSchemas is passed through verbatim so callers can validate that
// per-tenant default-schema seeding lands on the right Handler when
// they care; nil is fine for tests that don't.
func startTestShim(t *testing.T, tenants map[string]string, userSchemas map[string]string) string {
	t.Helper()

	auth, err := shim.NewTenantAuth(tenants)
	if err != nil {
		t.Fatalf("NewTenantAuth: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// indexDB=nil: handler construction does not dereference it
		// and the test queries never reach a path that would.
		serveLoop(ctx, listener, (*sql.DB)(nil), auth, shim.Config{}, userSchemas)
	}()

	t.Cleanup(func() {
		cancel()
		listener.Close()
		// Bound the wait so a serveLoop shutdown regression (e.g. a
		// future change that blocks on an in-flight handleConn) shows
		// up as a 2s test failure rather than a 10-min Go-test-timeout
		// hang with no diagnostic.
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("serveLoop did not return within 2s of shutdown")
		}
	})
	return listener.Addr().String()
}

// pingWithUser opens one short-lived connection as user/pass and runs
// db.Ping. Returns the underlying error so callers can inspect the
// MySQL error code.
func pingWithUser(t *testing.T, addr, user, pass string) error {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?timeout=2s&readTimeout=2s&writeTimeout=2s", user, pass, addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// TestShim_AuthCredentialMatrix is the wire-level regression guard for
// PR #264 / issue #262 (auth → MySQL error code 1045, not 1449) and
// for cross-tenant credential isolation (issue #269). The unit tests
// in shim_test.go::TestClassifyHandshakeErr verify the *log*
// classification path against synthetic errors; this test verifies the
// actual wire bytes a real MySQL client sees.
//
// A regression that demoted GetCredential's server.ErrAccessDenied to
// (found=false, err=nil) — the original #262 bug shape — would surface
// here as a 1449 from go-sql-driver instead of a 1045, and ProxySQL's
// monitor probe would SHUNN the hostgroup again.
//
// The cross-tenant rows (A's user with B's pass, and vice versa)
// extend the matrix to cover credential isolation between tenants
// sharing one shim process. Schema isolation between tenants is
// already covered by TestBuildUserSchemas in shim_test.go — the
// wire-level claim here is narrower: a tenant cannot authenticate
// using a different tenant's password.
func TestShim_AuthCredentialMatrix(t *testing.T) {
	tenants := map[string]string{
		"alice": "secret_a",
		"bob":   "secret_b",
	}
	addr := startTestShim(t, tenants, nil)

	cases := []struct {
		name     string
		user     string
		pass     string
		wantOK   bool
		wantCode uint16 // ignored when wantOK is true
	}{
		{"alice with own password succeeds", "alice", "secret_a", true, 0},
		{"bob with own password succeeds", "bob", "secret_b", true, 0},
		{"alice with bob's password rejected (cross-tenant)", "alice", "secret_b", false, 1045},
		{"bob with alice's password rejected (cross-tenant)", "bob", "secret_a", false, 1045},
		{"alice with empty password rejected", "alice", "", false, 1045},
		{"alice with wrong password rejected", "alice", "nope", false, 1045},
		{"unknown user rejected", "ghost", "any", false, 1045},
		// Empty-username path: go-mysql/server returns its own auth
		// error before TenantAuth.GetCredential is consulted. Code 1045
		// is still the contract — what matters is "no silent success".
		{"empty username rejected", "", "x", false, 1045},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := pingWithUser(t, addr, tc.user, tc.pass)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("Ping(%q,%q) = %v, want success", tc.user, tc.pass, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Ping(%q,%q) succeeded, want auth failure code %d", tc.user, tc.pass, tc.wantCode)
			}
			var myErr *mysql.MySQLError
			if !errors.As(err, &myErr) {
				t.Fatalf("Ping(%q,%q) error = %v (%T), want *mysql.MySQLError", tc.user, tc.pass, err, err)
			}
			if myErr.Number != tc.wantCode {
				t.Errorf(
					"Ping(%q,%q) error code = %d (%s), want %d (ProxySQL's monitor SHUNNs on 1449; see #262)",
					tc.user, tc.pass, myErr.Number, myErr.Message, tc.wantCode,
				)
			}
		})
	}
}

// TestShim_MalformedTimeTravelReturnsWireError pins issue #268: a
// malformed _flashback / _diff / _snapshot query must reach the client
// as a MySQL wire-protocol error within a short bound — never hang,
// never return rows. Each subtest issues one malformed query against a
// shared authenticated *sql.DB (lazy connection pool) and expects an
// error from QueryContext.
//
// The bound is tight enough to catch a regression that introduced a
// blocking operation in the parser dispatch path — a future refactor
// that, say, hit the index DB before parsing would block on the nil
// DB and trip the elapsed-time assertion below instead of returning
// a clean parser error.
func TestShim_MalformedTimeTravelReturnsWireError(t *testing.T) {
	// Seed a default schema via userSchemas so the parser reaches the
	// regex / shape validation path. Without this every test case
	// would short-circuit at parser.go's "no schema selected" check
	// (which fires after the virtual-schema sentinel screen) and
	// return that error instead of the shape-error messages we want
	// to pin.
	addr := startTestShim(t,
		map[string]string{"alice": "secret_a"},
		map[string]string{"alice": "testdb"},
	)

	// timeout=2s bounds the initial connect. readTimeout/writeTimeout are
	// deliberately omitted so a hung handler surfaces via the ctx deadline
	// + the elapsed-time assertion below — a driver-side i/o timeout
	// would fire earlier and produce a generic "i/o timeout" message
	// indistinguishable from the substring-match failure path.
	dsn := fmt.Sprintf("alice:secret_a@tcp(%s)/?timeout=2s", addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	cases := []struct {
		name string
		sql  string
		// wantErrSubstr is matched against the wire error message so
		// the assertion fails loudly if a future change starts
		// returning the generic "this server only handles..." reply
		// for inputs that should reach the parser. Every case here
		// contains the sentinel `_flashback.` / `_diff.` / `_snapshot.`
		// that activates the parser path; the parser then rejects
		// each with a substring-stable message.
		wantErrSubstr string
	}{
		{
			name:          "flashback missing AS OF clause",
			sql:           "SELECT * FROM _flashback.users WHERE id = 1",
			wantErrSubstr: "malformed time-travel",
		},
		{
			name: "flashback unparseable timestamp",
			sql:  "SELECT * FROM _flashback.users AS OF 'not-a-time' WHERE id = 1",
			// "invalid AS OF timestamp" is unique to parseAsOfMatch's
			// time-parse failure path; a bare "AS OF" substring would
			// also match the malformed-shape template, so a regression
			// rerouting this case to the malformed branch would pass
			// silently against the looser assertion.
			wantErrSubstr: "invalid AS OF timestamp",
		},
		{
			name:          "diff missing BETWEEN clause",
			sql:           "SELECT * FROM _diff.users WHERE id = 1",
			wantErrSubstr: "malformed time-travel",
		},
		{
			name:          "diff bounds out of order",
			sql:           "SELECT * FROM _diff.users BETWEEN '2026-01-02' AND '2026-01-01' WHERE id = 1",
			wantErrSubstr: "BETWEEN bounds out of order",
		},
		{
			name:          "snapshot missing WHERE",
			sql:           "SELECT * FROM _snapshot.users AS OF '2026-01-01 00:00:00'",
			wantErrSubstr: "malformed time-travel",
		},
	}

	// Two bounds working together:
	//   ctxDeadline caps the wall-clock — a true deadlock surfaces here
	//     instead of hanging the whole test process indefinitely.
	//   responseBudget is the assertion that actually pins the issue
	//     #268 invariant: a healthy parser path is sub-millisecond, so
	//     anything slower than 1s indicates a regression introduced a
	//     blocking operation (a DB lookup, a synchronous network call,
	//     etc.) before the regex rejection. Keeping responseBudget well
	//     below ctxDeadline means the assertion fails with a "took N;
	//     hang regression" message rather than the generic deadline
	//     error you would get from ctx alone.
	const (
		ctxDeadline    = 5 * time.Second
		responseBudget = 1 * time.Second
	)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), ctxDeadline)
			defer cancel()

			start := time.Now()
			rows, err := db.QueryContext(ctx, tc.sql)
			elapsed := time.Since(start)

			if elapsed >= responseBudget {
				t.Fatalf("query took %v (>= %v); a hang regression in the parser dispatch path would manifest here", elapsed, responseBudget)
			}
			if err == nil {
				// Some drivers surface deferred server errors on
				// rows.Close() rather than on QueryContext, so include
				// it in the failure message — otherwise a regression
				// where the error moved to that branch would look like
				// a clean success here.
				closeErr := rows.Close()
				t.Fatalf("query %q succeeded (rows.Close err=%v); want wire error", tc.sql, closeErr)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Errorf("error = %q, want substring %q", err.Error(), tc.wantErrSubstr)
			}
		})
	}
}
