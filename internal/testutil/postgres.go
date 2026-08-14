package testutil

import (
	"os"
	"strings"
	"testing"
)

// PostgresDSN returns the test PostgreSQL DSN from BINTRAIL_TEST_PG_DSN, or "".
// A suitable target is a local PostgreSQL with wal_level=logical and a role that
// has REPLICATION (e.g. the spike's container:
// postgres://postgres:testpg@localhost:15533/pgtest).
func PostgresDSN() string { return os.Getenv("BINTRAIL_TEST_PG_DSN") }

// PostgresRequired reports whether PostgreSQL integration tests must RUN (and
// fail rather than skip) when no DSN is configured. The dedicated CI job
// (integration-postgres-source) sets BINTRAIL_REQUIRE_POSTGRES=1, where a live
// PostgreSQL source is guaranteed — so a missing DSN there is a misconfiguration
// to surface loudly, not a green-via-skip. The MySQL integration matrix leaves it
// unset, so PG tests skip cleanly there. Mirrors MariaDBRequired.
func PostgresRequired() bool {
	return os.Getenv("BINTRAIL_REQUIRE_POSTGRES") == "1"
}

// RequiredPGExtensions returns the set of PostgreSQL extensions the
// extension-type integration tests must exercise, from the comma-separated
// BINTRAIL_REQUIRE_PGEXT (e.g. "postgis" or "vector"). When an extension named
// here cannot be created in the test database, the extension-type round-trip
// test FAILS instead of skipping its cases: the dedicated CI cells run images
// that guarantee the extension (postgis/postgis, pgvector/pgvector), so an
// unavailable extension there is a misconfiguration to surface loudly, not a
// green-via-skip. Cells without the env var (the plain postgres matrix) skip
// the extension cases cleanly. Mirrors PostgresRequired / MariaDBRequired.
func RequiredPGExtensions() map[string]bool {
	req := map[string]bool{}
	for _, e := range strings.Split(os.Getenv("BINTRAIL_REQUIRE_PGEXT"), ",") {
		if e = strings.TrimSpace(e); e != "" {
			req[e] = true
		}
	}
	return req
}

// SkipIfNoPostgres skips the test unless BINTRAIL_TEST_PG_DSN is set, and returns
// the DSN. It is presence-only (no driver dependency) on purpose: the MySQL-based
// CI integration jobs run `go test -tags integration ./...` WITHOUT setting it, so
// PostgreSQL integration tests must skip cleanly there rather than fail or hang —
// the live Postgres CI cell is integration-postgres-source (#534's matrix), which
// sets BINTRAIL_REQUIRE_POSTGRES=1 so a missing DSN there fails loud instead of
// passing as green-via-skip. Mirrors SkipIfNoMySQL / SkipOrFailMariaDB.
func SkipIfNoPostgres(t *testing.T) string {
	t.Helper()
	dsn := PostgresDSN()
	if dsn == "" {
		if PostgresRequired() {
			t.Fatal("BINTRAIL_TEST_PG_DSN not set but BINTRAIL_REQUIRE_POSTGRES=1 — the integration-postgres-source CI job must provide a live PostgreSQL server")
		}
		t.Skip("skipping: BINTRAIL_TEST_PG_DSN not set (no live PostgreSQL with logical replication)")
	}
	return dsn
}
