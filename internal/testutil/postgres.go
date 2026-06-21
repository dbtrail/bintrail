package testutil

import (
	"os"
	"testing"
)

// PostgresDSN returns the test PostgreSQL DSN from BINTRAIL_TEST_PG_DSN, or "".
// A suitable target is a local PostgreSQL with wal_level=logical and a role that
// has REPLICATION (e.g. the spike's container:
// postgres://postgres:testpg@localhost:15533/pgtest).
func PostgresDSN() string { return os.Getenv("BINTRAIL_TEST_PG_DSN") }

// SkipIfNoPostgres skips the test unless BINTRAIL_TEST_PG_DSN is set, and returns
// the DSN. It is presence-only (no driver dependency) on purpose: the MySQL-based
// CI integration jobs run `go test -tags integration ./...` WITHOUT setting it, so
// PostgreSQL integration tests must skip cleanly there rather than fail or hang —
// a live Postgres CI cell arrives with #534's matrix. Mirrors SkipIfNoMySQL.
func SkipIfNoPostgres(t *testing.T) string {
	t.Helper()
	dsn := PostgresDSN()
	if dsn == "" {
		t.Skip("skipping: BINTRAIL_TEST_PG_DSN not set (no live PostgreSQL with logical replication)")
	}
	return dsn
}
