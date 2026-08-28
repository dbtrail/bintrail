package doctor

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestPreflightErrorIsTypedAndClassed: Err/ErrExcluding return a
// *PreflightError that keeps the untyped error's exact message, names the
// failing checks (names only), and classifies as config_invalid for usage
// telemetry — that is the whole wiring `up` relies on (#1503).
func TestPreflightErrorIsTypedAndClassed(t *testing.T) {
	r := &Report{}
	r.add(CheckResult{Name: "Index disk capacity", Status: StatusFail, Detail: "root:hunter2@tcp(db.internal)/x"})
	r.add(CheckResult{Name: "log_bin enabled", Status: StatusPass})
	r.add(CheckResult{Name: "binlog_format=ROW", Status: StatusFail, Detail: "STATEMENT"})

	var pe *PreflightError
	err := r.ErrExcluding(CapacityCheckName)
	if !errors.As(err, &pe) {
		t.Fatalf("ErrExcluding returned %T, want *PreflightError", err)
	}
	if got, want := err.Error(), "1 preflight check(s) failed"; got != want {
		t.Errorf("message = %q, want the pre-#1503 bytes %q", got, want)
	}
	if len(pe.Checks) != 1 || pe.Checks[0] != "binlog_format=ROW" || pe.Failed != 1 {
		t.Errorf("PreflightError = %+v, want Failed=1 Checks=[binlog_format=ROW]", pe)
	}
	for _, c := range pe.Checks {
		if c == "STATEMENT" || c == "root:hunter2@tcp(db.internal)/x" {
			t.Fatalf("PreflightError carries a Detail, not a name: %q", c)
		}
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassConfigInvalid {
		t.Errorf("ClassifyError(preflight) = %q, want %q", got, telemetry.ClassConfigInvalid)
	}

	// Err counts every failure, capacity included, and prints the same shape.
	err = r.Err()
	if !errors.As(err, &pe) || pe.Failed != 2 || len(pe.Checks) != 2 {
		t.Fatalf("Err = %v (%T), want *PreflightError with 2 failures", err, err)
	}
	if got, want := err.Error(), "2 preflight check(s) failed"; got != want {
		t.Errorf("Err message = %q, want %q", got, want)
	}
	if r2 := (&Report{}); r2.Err() != nil || r2.ErrExcluding() != nil {
		t.Error("a report with no failures must return a nil error, not a typed nil")
	}
}
