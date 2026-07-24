package testutil

import "testing"

// TestMySQLRequired verifies the BINTRAIL_REQUIRE_MYSQL env-var gate mirrors
// MariaDBRequired / PostgresRequired. This is the switch that turns a MySQL
// connectivity failure in SkipIfNoMySQL from a silent t.Skip into a t.Fatal
// (issue #949 — no green-via-skip on the flagship MySQL integration job).
//
// The actual t.Fatalf/t.Skipf call in skipOrFailMySQL is not exercised here:
// a failing subtest propagates Fail() up to its parent (by the testing
// package's own design), so simulating the "required" branch via t.Run would
// make this test intentionally — and misleadingly — report FAIL. Neither the
// MariaDB nor Postgres sibling guards test that call path either.
func TestMySQLRequired(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		t.Setenv("BINTRAIL_REQUIRE_MYSQL", "")
		if MySQLRequired() {
			t.Fatal("expected MySQLRequired() to be false when BINTRAIL_REQUIRE_MYSQL is unset")
		}
	})

	t.Run("set", func(t *testing.T) {
		t.Setenv("BINTRAIL_REQUIRE_MYSQL", "1")
		if !MySQLRequired() {
			t.Fatal("expected MySQLRequired() to be true when BINTRAIL_REQUIRE_MYSQL=1")
		}
	})

	t.Run("other_value_not_required", func(t *testing.T) {
		t.Setenv("BINTRAIL_REQUIRE_MYSQL", "true")
		if MySQLRequired() {
			t.Fatal("expected MySQLRequired() to be false for any value other than \"1\" (matches MariaDBRequired/PostgresRequired's exact-match semantics)")
		}
	})
}
