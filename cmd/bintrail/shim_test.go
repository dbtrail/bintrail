package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/bintrail/internal/shim"
)

// TestAllowGapsDefaultIsStrict pins the load-bearing claim of issue #257:
// the shim CLI's --allow-gaps flag defaults to false. A regression that
// flips this default (e.g. someone copy-pasting from `bintrail recover`,
// which is intentionally permissive) would silently re-introduce the
// silent-partial-result bug — archive failures absorbed as slog.Warn
// while the connecting MySQL client gets an apparently-successful
// resultset missing rows. This test exists specifically because the
// default value is what the issue's fix turns on; framework wiring is
// not the load-bearing claim.
func TestAllowGapsDefaultIsStrict(t *testing.T) {
	flag := shimCmd.Flags().Lookup("allow-gaps")
	if flag == nil {
		t.Fatal("--allow-gaps flag not registered on shim command")
	}
	if flag.DefValue != "false" {
		t.Errorf("--allow-gaps default = %q, want %q (strict mode is the security-relevant default; see #257)", flag.DefValue, "false")
	}
}

// TestIsLoopbackAddr locks in the security-relevant guard that
// determines whether the shim emits the "non-loopback bind" warning
// at startup. A regression that classified 0.0.0.0 as loopback would
// silently degrade the auth model.
func TestIsLoopbackAddr(t *testing.T) {
	cases := []struct {
		name string
		addr net.Addr
		want bool
	}{
		{"IPv4 loopback", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3308}, true},
		{"IPv4 loopback alt", &net.TCPAddr{IP: net.ParseIP("127.0.0.5"), Port: 3308}, true},
		{"IPv6 loopback", &net.TCPAddr{IP: net.ParseIP("::1"), Port: 3308}, true},
		{"unspecified IPv4 (0.0.0.0)", &net.TCPAddr{IP: net.IPv4zero, Port: 3308}, false},
		{"unspecified IPv6 (::)", &net.TCPAddr{IP: net.IPv6unspecified, Port: 3308}, false},
		{"private IPv4", &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 3308}, false},
		{"public IPv4", &net.TCPAddr{IP: net.ParseIP("8.8.8.8"), Port: 3308}, false},
		{"nil IP", &net.TCPAddr{IP: nil, Port: 3308}, false},
		{"non-TCP addr", &net.UnixAddr{Name: "/tmp/sock", Net: "unix"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLoopbackAddr(tc.addr); got != tc.want {
				t.Errorf("isLoopbackAddr(%v) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}

// TestNextAcceptBackoff pins the doubling-with-cap behaviour. A
// regression here matters because the backoff is what stops a wedged
// listener from filling the log at ~10 lines/sec — and a buggy reset
// (e.g. always returning initial) would silently re-spin.
func TestNextAcceptBackoff(t *testing.T) {
	cases := []struct {
		name    string
		current time.Duration
		want    time.Duration
	}{
		{"zero seeds at initial", 0, initialAcceptBackoff},
		{"negative seeds at initial", -1, initialAcceptBackoff},
		{"100ms doubles to 200ms", 100 * time.Millisecond, 200 * time.Millisecond},
		{"200ms doubles to 400ms", 200 * time.Millisecond, 400 * time.Millisecond},
		{"2s doubles to 4s", 2 * time.Second, 4 * time.Second},
		{"4s doubles to cap", 4 * time.Second, maxAcceptBackoff},
		{"at cap stays at cap", maxAcceptBackoff, maxAcceptBackoff},
		{"above cap clamps to cap", 30 * time.Second, maxAcceptBackoff},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextAcceptBackoff(tc.current); got != tc.want {
				t.Errorf("nextAcceptBackoff(%v) = %v, want %v", tc.current, got, tc.want)
			}
		})
	}
}

// TestAcceptBackoffSequence walks the steady-state usage: starting
// from zero (post-success), each call models another consecutive
// failure. Verifies the cap is reached in a bounded number of steps —
// today's constants reach the 5s cap on the 7th failure (100, 200,
// 400, 800, 1600, 3200, 5000ms).
func TestAcceptBackoffSequence(t *testing.T) {
	var d time.Duration
	steps := 0
	for d < maxAcceptBackoff {
		d = nextAcceptBackoff(d)
		steps++
		if steps > 20 {
			t.Fatalf("backoff did not reach cap after %d steps; got %v", steps, d)
		}
	}
	if d != maxAcceptBackoff {
		t.Errorf("after %d steps, got %v; want exactly %v", steps, d, maxAcceptBackoff)
	}
}

// TestServeLoopExitsOnContextCancel asserts that serveLoop returns
// within 1 second of ctx cancellation when driven against a listener
// whose Accept never succeeds. This catches the operator-visible
// failure mode the cancellable select was added to defend against:
// a regression that introduces a multi-second uninterruptible sleep
// in the error branch (e.g. `time.Sleep(maxAcceptBackoff)`) would
// block past the 1s bound and fail.
//
// What this test does NOT catch: a regression to a short fixed sleep
// (e.g. the literal `time.Sleep(100*time.Millisecond)` from before
// this PR). With a short sleep, the top-of-loop ctx check at the next
// iteration still exits within ~100ms of cancel — under the bound.
// That's a deliberate scoping choice: a 100ms shutdown delay is not
// the operator pain point this PR addresses; a 5s wedge is. Tightening
// the bound below ~50ms would make the test flaky on slow CI without
// catching a meaningfully worse regression.
func TestServeLoopExitsOnContextCancel(t *testing.T) {
	listener := &alwaysErrorListener{}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		// db / srv / auth / cfg are unused on this path because Accept
		// never returns a connection — handleConn is unreachable.
		serveLoop(ctx, listener, nil, nil, shim.TenantAuth{}, shim.Config{}, nil)
		close(done)
	}()

	// Reap the goroutine deterministically on test failure so a
	// regression that wedges serveLoop doesn't leak it past
	// t.Fatalf into other tests.
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// Brief spin so the loop is in a non-trivial state when cancel
	// arrives — not load-bearing for correctness; cancellation works
	// from any iteration.
	time.Sleep(150 * time.Millisecond)
	cancel()

	const exitBound = 1 * time.Second
	select {
	case <-done:
		// ok
	case <-time.After(exitBound):
		t.Fatalf(
			"serveLoop did not exit within %v of ctx cancel; "+
				"a multi-second uninterruptible sleep regression in the error branch would block this long",
			exitBound,
		)
	}
}

// alwaysErrorListener is a minimal net.Listener whose Accept always
// returns a synthetic non-net.ErrClosed error until Close is called.
// Used by TestServeLoopExitsOnContextCancel to drive the accept
// loop's error branch deterministically without binding a real port.
type alwaysErrorListener struct {
	closed atomic.Bool
}

func (l *alwaysErrorListener) Accept() (net.Conn, error) {
	if l.closed.Load() {
		return nil, net.ErrClosed
	}
	return nil, errors.New("synthetic accept failure")
}

func (l *alwaysErrorListener) Close() error {
	l.closed.Store(true)
	return nil
}

func (l *alwaysErrorListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}

// TestClassifyHandshakeErr pins the #262 log-volume invariants. A
// future refactor that flips the cases — e.g. demoting "real" errors
// to debug, or promoting access-denied back to ERROR — would silently
// re-introduce either the noisy probe stack traces or the SHUNNED
// alarm storm.
//
// EOF / mysql.ErrBadConn are go-mysql's wrapped read errors when
// ProxySQL's monitor opens a TCP socket and closes it; they must be
// debug. ER_ACCESS_DENIED_ERROR for a real tenant user is what
// TenantAuth.GetCredential causes go-mysql/server to return — info,
// never error. ER_ACCESS_DENIED_ERROR for the ProxySQL `monitor`
// user is demoted to debug per issue #316 (the monitor probes every
// ~1-10s and would otherwise drown real auth failures in info-level
// noise). Anything else stays at error.
func TestClassifyHandshakeErr(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantLevel slog.Level
	}{
		{"raw io.EOF", io.EOF, slog.LevelDebug},
		{"unexpected EOF", io.ErrUnexpectedEOF, slog.LevelDebug},
		// fmt.Errorf+%w produces an Unwrap-compatible chain that exercises the same
		// errors.Is path go-mysql's pingcap-wrapped reads do. We avoid importing
		// github.com/pingcap/errors directly per CLAUDE.md ("Do not import transitive
		// deps directly") — the production behaviour is what matters, and errors.Is
		// resolves both wrap shapes the same way.
		{"wrapped ErrBadConn", fmt.Errorf("io.ReadFull(header) failed: %w", gomysql.ErrBadConn), slog.LevelDebug},
		// ProxySQL monitor user → debug (issue #316).
		{"ER_ACCESS_DENIED_ERROR monitor user is debug", gomysql.NewDefaultError(gomysql.ER_ACCESS_DENIED_ERROR, "monitor", "127.0.0.1:46948", "YES"), slog.LevelDebug},
		// Real tenant user → info (the load-bearing claim — without this,
		// the #316 demote would silence every real auth failure too).
		{"ER_ACCESS_DENIED_ERROR tenant user is info", gomysql.NewDefaultError(gomysql.ER_ACCESS_DENIED_ERROR, "alice", "127.0.0.1:46948", "YES"), slog.LevelInfo},
		{"unrelated MyError stays error", gomysql.NewDefaultError(gomysql.ER_HANDSHAKE_ERROR), slog.LevelError},
		{"plain unrelated error", errors.New("protocol mismatch"), slog.LevelError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			level, msg := classifyHandshakeErr(tc.err)
			if level != tc.wantLevel {
				t.Errorf("classifyHandshakeErr level = %v, want %v (msg=%q)", level, tc.wantLevel, msg)
			}
			if msg == "" {
				t.Errorf("classifyHandshakeErr msg empty for %v", tc.err)
			}
		})
	}
}

// TestIsMonitorAuthFailure pins the parser against the literal
// ER_ACCESS_DENIED_ERROR message go-mysql v1.13.0 emits. A go-mysql
// upgrade that changes the format would silently route monitor
// failures back through the info branch — issue #316 would regress.
// The case-insensitive match is deliberate: ProxySQL ships with the
// lowercase form, but a tenant deliberately named `Monitor` would
// also probe at the same cadence, so robust matching wins over
// surgical case-sensitivity.
func TestIsMonitorAuthFailure(t *testing.T) {
	// Build the exact message classifyHandshakeErr unwraps — this is
	// what (*Conn).handshake constructs via mysql.NewDefaultError when
	// TenantAuth.GetCredential returns ErrAccessDenied.
	monitorErr := gomysql.NewDefaultError(gomysql.ER_ACCESS_DENIED_ERROR, "monitor", "127.0.0.1:46948", "YES")
	tenantErr := gomysql.NewDefaultError(gomysql.ER_ACCESS_DENIED_ERROR, "alice", "127.0.0.1:46948", "YES")

	cases := []struct {
		name string
		msg  string
		want bool
	}{
		{"production go-mysql format with monitor", monitorErr.Message, true},
		{"production go-mysql format with tenant", tenantErr.Message, false},
		{"uppercase monitor matches", "Access denied for user 'MONITOR'@'127.0.0.1' (using password: YES)", true},
		{"mixed-case monitor matches", "Access denied for user 'Monitor'@'127.0.0.1' (using password: YES)", true},
		{"monitor-prefixed user does not match", "Access denied for user 'monitor_special'@'127.0.0.1' (using password: YES)", false},
		{"no quotes returns false (cannot parse)", "Access denied for user monitor", false},
		{"single quote without close returns false", "Access denied for user 'monitor", false},
		{"empty string returns false", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMonitorAuthFailure(tc.msg); got != tc.want {
				t.Errorf("isMonitorAuthFailure(%q) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}

// TestBuildUserSchemas pins the #263 plumbing. Each branch of the
// extraction is the regression surface for the original bug: a typo
// in the DSN parse, dropping the empty-DBName guard, or skipping the
// empty-SourceDSN early-out would all silently re-introduce the
// "USE <db> required" failure mode for some tenant subset.
func TestBuildUserSchemas(t *testing.T) {
	cfgs := []shim.TenantConfig{
		{MySQLUser: "alice", MySQLPassword: "p", SourceDSN: "u:p@tcp(db:3306)/source_a"},
		{MySQLUser: "bob", MySQLPassword: "p", SourceDSN: "u:p@tcp(db:3306)/"}, // no DB name
		{MySQLUser: "carol", MySQLPassword: "p", SourceDSN: ""},                // empty DSN
		{MySQLUser: "dave", MySQLPassword: "p", SourceDSN: "::not-a-dsn::"},    // unparseable
		{MySQLUser: "eve", MySQLPassword: "p", SourceDSN: "u:p@tcp(db:3306)/source_e?parseTime=true"},
	}
	got := buildUserSchemas(cfgs)
	want := map[string]string{
		"alice": "source_a",
		"eve":   "source_e",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for u, schema := range want {
		if got[u] != schema {
			t.Errorf("buildUserSchemas[%q] = %q, want %q", u, got[u], schema)
		}
	}
	// Tenants with bad/empty DSN must be ABSENT, not present-with-empty:
	// handleConn keys off `ok` membership in the map; an empty-string
	// entry would seed `Handler.UseDB("")` which silently breaks the
	// pre-#263 fallback path.
	for _, u := range []string{"bob", "carol", "dave"} {
		if _, ok := got[u]; ok {
			t.Errorf("buildUserSchemas[%q] should be absent (bad/empty DSN), got %q", u, got[u])
		}
	}
}
