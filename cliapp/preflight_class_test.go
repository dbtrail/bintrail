package cliapp

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestUpPreflightRefusalClassifiesAsConfigInvalid pins the wiring #1503
// exists for: the error `up` returns when the doctor refuses boot — the same
// helper runUp and watch call — reaches the telemetry hook as
// config_invalid, not unknown.
func TestUpPreflightRefusalClassifiesAsConfigInvalid(t *testing.T) {
	r := &doctor.Report{}
	r.Add(doctor.CheckResult{Name: "binlog_format=ROW", Status: doctor.StatusFail, Detail: "STATEMENT"})

	fatal, _ := upPreflightOutcome(r)
	if fatal == nil {
		t.Fatal("a ROW-format failure must refuse boot")
	}
	var pe *doctor.PreflightError
	if !errors.As(fatal, &pe) {
		t.Fatalf("fatal is %T, want *doctor.PreflightError", fatal)
	}
	wrapped := doctor.BootRefusal(fatal)
	if !strings.HasPrefix(wrapped.Error(), "preflight failed (use --skip-doctor") {
		t.Errorf("refusal message = %q", wrapped.Error())
	}
	if got := telemetry.ClassifyError(wrapped); got != telemetry.ClassConfigInvalid {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassConfigInvalid)
	}
}
