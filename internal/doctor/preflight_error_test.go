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

// TestPreflightErrorClassFollowsTheFailingChecks: the class is derived from
// which checks failed — a refusal for connectivity or grants must not read as
// a misconfigured server (#1503 review).
func TestPreflightErrorClassFollowsTheFailingChecks(t *testing.T) {
	cases := []struct {
		name   string
		checks []string
		want   string
	}{
		{"source unreachable", []string{SourceConnectionCheckName}, telemetry.ClassDBConnection},
		{"index unreachable", []string{IndexConnectionCheckName}, telemetry.ClassDBConnection},
		{"pg source unreachable", []string{PGSourceConnectionCheckName}, telemetry.ClassDBConnection},
		{"missing replication grants", []string{ReplicationGrantsCheckName}, telemetry.ClassDBPermission},
		{"no index write access", []string{IndexWriteAccessCheckName}, telemetry.ClassDBPermission},
		{"server variable", []string{"binlog_format=ROW"}, telemetry.ClassConfigInvalid},
		{"retention", []string{"Binlog retention >= 2 days"}, telemetry.ClassConfigInvalid},
		{"extension check", []string{"Audit log plugin"}, telemetry.ClassConfigInvalid},
		{"extension panic", []string{ExtensionPanicCheckName}, telemetry.ClassInternal},
		{"panic beats grants and variables", []string{"log_bin enabled", ReplicationGrantsCheckName, ExtensionPanicCheckName}, telemetry.ClassInternal},
		{"connection beats a panic", []string{ExtensionPanicCheckName, SourceConnectionCheckName}, telemetry.ClassDBConnection},
		{"grants beat variables", []string{"log_bin enabled", ReplicationGrantsCheckName, "binlog_format=ROW"}, telemetry.ClassDBPermission},
		{"connection beats grants", []string{ReplicationGrantsCheckName, IndexConnectionCheckName, IndexWriteAccessCheckName}, telemetry.ClassDBConnection},
		{"connection beats everything, any position", []string{"binlog_format=ROW", SourceConnectionCheckName}, telemetry.ClassDBConnection},
		{"no names", nil, telemetry.ClassConfigInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := &PreflightError{Failed: len(c.checks), Checks: c.checks}
			if got := telemetry.ClassifyError(err); got != c.want {
				t.Errorf("ClassifyError = %q, want %q", got, c.want)
			}
		})
	}

	// Through the real report path, not a hand-built value: the connection
	// check's name as the producer spells it.
	r := &Report{}
	r.add(CheckResult{Name: SourceConnectionCheckName, Status: StatusFail, Detail: "dial tcp: connection refused"})
	if got := telemetry.ClassifyError(r.Err()); got != telemetry.ClassDBConnection {
		t.Errorf("report with a failed source connection classifies %q, want %q", got, telemetry.ClassDBConnection)
	}
}
