package streamrun

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	drivermysql "github.com/go-sql-driver/mysql"
)

// isTLSUnsupportedError gates the --ssl-mode=preferred plaintext downgrade: it
// must return true ONLY for a genuine "server does not support TLS" condition,
// never for auth/network/position errors — else credentials get resent in the
// clear on an unrelated failure (#947). It covers both drivers: go-sql-driver's
// ErrNoTLS sentinel (index/source helper connections) and go-mysql's exact
// message (client/auth.go, binlog syncer).
func TestIsTLSUnsupportedError(t *testing.T) {
	goMySQLSentinel := errors.New("the MySQL Server does not support TLS required by the client")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"go-sql-driver ErrNoTLS", drivermysql.ErrNoTLS, true},
		{"go-sql-driver ErrNoTLS wrapped", fmt.Errorf("failed to ping MySQL: %w", drivermysql.ErrNoTLS), true},
		{"go-mysql sentinel", goMySQLSentinel, true},
		{"go-mysql sentinel wrapped", fmt.Errorf("StartSync(bin.0001, 4): %w", goMySQLSentinel), true},
		{"record header (plaintext reply)", fmt.Errorf("dial: %w", tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}), true},
		{"access denied must NOT downgrade", errors.New("ERROR 1045 (28000): Access denied for user 'repl'@'%'"), false},
		{"connection refused must NOT downgrade", errors.New("dial tcp 10.0.0.1:3306: connect: connection refused"), false},
		{"deadline must NOT downgrade", context.DeadlineExceeded, false},
		{"bad position must NOT downgrade", errors.New("StartSync(bin.0009, 999999): could not find first log file name in binary log index file"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTLSUnsupportedError(tt.err); got != tt.want {
				t.Fatalf("isTLSUnsupportedError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// connectHelper wires --ssl-mode into the actual connect. A closed TCP port
// (connection refused, deterministic — no MySQL needed) proves the security
// composition: required surfaces the TLS-required hint and never yields a
// connection, and preferred does NOT downgrade on a non-TLS error (#946/#947).
func TestConnectHelper_FailsClosedNoSilentDowngrade(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // now a closed port → connection refused
	dsn := fmt.Sprintf("root:x@tcp(%s)/db?timeout=1s", addr)

	t.Run("required surfaces the TLS-required hint, never a plaintext handle", func(t *testing.T) {
		db, err := connectHelper(dsn, "index database", "required", "", "", "")
		if db != nil {
			db.Close()
			t.Fatal("required must not return a connection to a dead endpoint")
		}
		if err == nil || !strings.Contains(err.Error(), "requires TLS") {
			t.Fatalf("expected a --ssl-mode=required 'requires TLS' hint, got: %v", err)
		}
	})

	t.Run("preferred does NOT downgrade on a non-TLS error", func(t *testing.T) {
		db, err := connectHelper(dsn, "index database", "preferred", "", "", "")
		if db != nil {
			db.Close()
			t.Fatal("preferred must not return a connection when the endpoint is down")
		}
		if err == nil {
			t.Fatal("expected an error")
		}
		// A connection-refused (non-TLS) error must NOT take the cleartext-retry
		// path — that path stamps "(cleartext retry)".
		if strings.Contains(err.Error(), "cleartext retry") {
			t.Fatalf("preferred downgraded on a non-TLS error (should not): %v", err)
		}
	})
}

// tlsHint is double-duty: it gates both the fail-closed error suffix and the
// DSN-override warning. Pin its contract (#946).
func TestTLSHint(t *testing.T) {
	for _, m := range []string{"required", "verify-ca", "verify-identity"} {
		h := tlsHint(m)
		if h == "" || !strings.Contains(h, "requires TLS") || !strings.Contains(h, m) {
			t.Errorf("tlsHint(%q) = %q, want non-empty naming the mode + 'requires TLS'", m, h)
		}
	}
	for _, m := range []string{"disabled", "preferred", ""} {
		if h := tlsHint(m); h != "" {
			t.Errorf("tlsHint(%q) = %q, want empty", m, h)
		}
	}
}
