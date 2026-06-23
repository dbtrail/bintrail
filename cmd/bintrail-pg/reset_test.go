package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

func resetPGResetFlags() {
	pgResetQueryDSN = ""
	pgResetIndexDSN = ""
	pgResetSlot = ""
	pgResetForce = false
	pgResetIndexOnly = false
}

func clearPGResetEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{"BINTRAIL_PG_QUERY_DSN", "BINTRAIL_PG_SLOT"} {
		t.Setenv(v, "")
	}
}

// runReset invokes the entry point with a throwaway command; the validation paths
// under test return before cmd.Context()/any connection is used.
func runReset() error { return runPGReset(&cobra.Command{}, nil) }

func TestPGResetConfig_MissingIndexDSN(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetForce = true
	if err := runReset(); err == nil || !strings.Contains(err.Error(), "index-dsn") {
		t.Fatalf("expected a missing index-dsn error, got %v", err)
	}
}

func TestPGResetConfig_MissingQuerySlotUnlessIndexOnly(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetIndexDSN = "user:pass@tcp(localhost:3306)/idx"
	pgResetForce = true
	// Not index-only and no query-dsn/slot → must name both as missing.
	err := runReset()
	if err == nil {
		t.Fatal("expected an error for missing query-dsn/slot, got nil")
	}
	for _, want := range []string{"--query-dsn", "--slot", "--index-only"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err, want)
		}
	}
}

func TestPGResetConfig_RequiresForce(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetIndexDSN = "idx"
	pgResetQueryDSN = "pg"
	pgResetSlot = "s"
	// All settings present but --force absent → refuse the destructive op.
	if err := runReset(); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected a --force refusal, got %v", err)
	}
}

func TestPGResetConfig_EnvFallbackForQuerySlot(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetIndexDSN = "idx"
	pgResetForce = false
	t.Setenv("BINTRAIL_PG_QUERY_DSN", "pg-from-env")
	t.Setenv("BINTRAIL_PG_SLOT", "slot-from-env")
	// query/slot satisfied via env → validation passes → the only error is --force.
	// (If the env fallback didn't run, this would fail with a missing-settings error.)
	err := runReset()
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("env fallback for query/slot not applied (got %v)", err)
	}
}

func TestPGResetConfig_IndexOnlySkipsQuerySlot(t *testing.T) {
	clearPGResetEnv(t)
	resetPGResetFlags()
	pgResetIndexDSN = "idx"
	pgResetIndexOnly = true
	pgResetForce = false
	// --index-only must NOT require query-dsn/slot; the only validation error is --force.
	if err := runReset(); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("--index-only should not require query/slot; got %v", err)
	}
}

func TestIsTableMissingErr(t *testing.T) {
	if !isTableMissingErr(&mysql.MySQLError{Number: 1146}) {
		t.Error("1146 should be recognized as table-missing")
	}
	if isTableMissingErr(&mysql.MySQLError{Number: 1054}) {
		t.Error("1054 (unknown column) is not table-missing")
	}
	if isTableMissingErr(errors.New("plain")) {
		t.Error("a plain error is not table-missing")
	}
}
