package doctor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

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
	if len(pe.Checks) != 1 || pe.Checks[0] != "binlog_format=ROW" {
		t.Errorf("PreflightError = %+v, want Checks=[binlog_format=ROW]", pe)
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
	if !errors.As(err, &pe) || len(pe.Checks) != 2 {
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
		{"no schema access", []string{SchemaAccessCheckName}, telemetry.ClassDBPermission},
		{"empty schema stays configuration", []string{"Schema visibility"}, telemetry.ClassConfigInvalid},
		{"server variable", []string{"binlog_format=ROW"}, telemetry.ClassConfigInvalid},
		{"disk capacity on standalone doctor", []string{CapacityCheckName}, telemetry.ClassConfigInvalid},
		{"extension check", []string{"Some extension check"}, telemetry.ClassConfigInvalid},
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
			err := &PreflightError{Checks: c.checks}
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

// TestBuildRefusedSourceClassifiesAsDBConnection drives the REAL Build with a
// source nobody listens on, and pins the check's name as a literal: the
// classifier keys on that exact string, and a test holding the constant on
// both sides would stay green if the producer ever spelled it out again.
func TestBuildRefusedSourceClassifiesAsDBConnection(t *testing.T) {
	r := Build(context.Background(), "root:x@tcp(127.0.0.1:1)/x?timeout=1s", "", "", 0)
	if len(r.Checks) == 0 || r.Checks[0].Name != "Source MySQL connection" || r.Checks[0].Status != StatusFail {
		t.Fatalf("Build against a refused port: checks = %+v, want a failed \"Source MySQL connection\" first", r.Checks)
	}
	err := r.Err()
	var pe *PreflightError
	if !errors.As(err, &pe) || len(pe.Checks) != 1 || pe.Checks[0] != "Source MySQL connection" {
		t.Fatalf("Err() = %v (%T), want *PreflightError naming the source connection", err, err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassDBConnection {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassDBConnection)
	}
}

// TestBootRefusalKeepsTheClass: the daemons' refusal wrapper must keep the
// *PreflightError in the chain; a %v here would report every refused boot as
// unknown.
func TestBootRefusalKeepsTheClass(t *testing.T) {
	err := BootRefusal(&PreflightError{Checks: []string{SourceConnectionCheckName}})
	if !strings.HasPrefix(err.Error(), "preflight failed (use --skip-doctor") {
		t.Errorf("message = %q", err.Error())
	}
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("BootRefusal lost the typed error: %T", err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassDBConnection {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassDBConnection)
	}
}

// TestReplicationGrantsFailureNameIsPinned drives the real grants check to
// its missing-privilege FAIL and pins the name as a literal, the bytes
// PreflightError.TelemetryClass keys on for db_permission.
func TestReplicationGrantsFailureNameIsPinned(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(
		sqlmock.NewRows([]string{"Grants for u@%"}).AddRow("GRANT SELECT ON *.* TO `u`@`%`"))

	got := checkReplicationGrants(context.Background(), db)
	if got.Status != StatusFail || got.Name != "REPLICATION SLAVE + CLIENT grants" {
		t.Fatalf("checkReplicationGrants = %+v, want a FAIL named \"REPLICATION SLAVE + CLIENT grants\"", got)
	}
	r := &Report{}
	r.add(got)
	if c := telemetry.ClassifyError(r.Err()); c != telemetry.ClassDBPermission {
		t.Errorf("ClassifyError = %q, want %q", c, telemetry.ClassDBPermission)
	}
}
