package main

import (
	"bytes"
	"context"
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

// ─── resetPlan orchestration (seam-injected; no live DBs) ───────────────────────

// okClear is a clearFn that records invocation and reports n rows cleared.
func recordingClear(n int64, called *bool) func(context.Context) (int64, bool, error) {
	return func(context.Context) (int64, bool, error) { *called = true; return n, false, nil }
}

// TestResetPlan_DropFailureLeavesCheckpoint is the load-bearing ordering guarantee
// (the reason the command exists): if the slot drop fails, the checkpoint clear must
// NOT run — the safe half-state is "slot live, checkpoint intact", and the next stream
// keeps working. A bug that cleared anyway would orphan a live slot from its checkpoint.
func TestResetPlan_DropFailureLeavesCheckpoint(t *testing.T) {
	cleared := false
	dropFn := func(context.Context) (bool, error) { return false, errors.New("slot is active for PID 123") }
	clearFn := recordingClear(1, &cleared)

	err := resetPlan(context.Background(), false, "s", &bytes.Buffer{}, dropFn, clearFn)
	if err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("expected the drop error to surface, got %v", err)
	}
	if cleared {
		t.Error("checkpoint was cleared after the slot drop FAILED — violates the fail-safe ordering")
	}
}

func TestResetPlan_IndexOnlySkipsDrop(t *testing.T) {
	dropped := false
	dropFn := func(context.Context) (bool, error) { dropped = true; return true, nil }
	cleared := false
	var out bytes.Buffer
	if err := resetPlan(context.Background(), true, "s", &out, dropFn, recordingClear(1, &cleared)); err != nil {
		t.Fatalf("resetPlan(index-only): %v", err)
	}
	if dropped {
		t.Error("--index-only must NOT drop the slot")
	}
	if !cleared {
		t.Error("--index-only must still clear the checkpoint")
	}
	if !strings.Contains(out.String(), "--index-only") || !strings.Contains(out.String(), "Cleared 1") {
		t.Errorf("unexpected output: %s", out.String())
	}
}

func TestResetPlan_HappyPath(t *testing.T) {
	var out bytes.Buffer
	dropFn := func(context.Context) (bool, error) { return true, nil }
	cleared := false
	if err := resetPlan(context.Background(), false, "myslot", &out, dropFn, recordingClear(1, &cleared)); err != nil {
		t.Fatalf("resetPlan: %v", err)
	}
	if !strings.Contains(out.String(), `Dropped replication slot "myslot"`) || !strings.Contains(out.String(), "Cleared 1 index checkpoint row") {
		t.Errorf("unexpected output: %s", out.String())
	}
}

func TestResetPlan_AbsentSlot(t *testing.T) {
	var out bytes.Buffer
	dropFn := func(context.Context) (bool, error) { return false, nil } // already absent
	cleared := false
	if err := resetPlan(context.Background(), false, "s", &out, dropFn, recordingClear(0, &cleared)); err != nil {
		t.Fatalf("resetPlan: %v", err)
	}
	if !strings.Contains(out.String(), "already absent") || !strings.Contains(out.String(), "Cleared 0") {
		t.Errorf("unexpected output: %s", out.String())
	}
}

func TestResetPlan_TableMissingNamesAmbiguity(t *testing.T) {
	var out bytes.Buffer
	dropFn := func(context.Context) (bool, error) { return true, nil }
	clearFn := func(context.Context) (int64, bool, error) { return 0, true, nil } // 1146
	if err := resetPlan(context.Background(), false, "s", &out, dropFn, clearFn); err != nil {
		t.Fatalf("resetPlan: %v", err)
	}
	// Must NOT claim a clean success — it names the wrong-database possibility.
	if !strings.Contains(out.String(), "wrong database") {
		t.Errorf("table-missing output should name the wrong-DSN ambiguity: %s", out.String())
	}
}

func TestResetPlan_ClearFailsAfterDropMentionsIndexOnly(t *testing.T) {
	dropFn := func(context.Context) (bool, error) { return true, nil }
	clearFn := func(context.Context) (int64, bool, error) { return 0, false, errors.New("index unreachable") }
	err := resetPlan(context.Background(), false, "s", &bytes.Buffer{}, dropFn, clearFn)
	if err == nil || !strings.Contains(err.Error(), "--index-only") {
		t.Fatalf("a clear failure after a successful drop must hint --index-only, got %v", err)
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
