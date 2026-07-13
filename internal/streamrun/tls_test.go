package streamrun

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
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
