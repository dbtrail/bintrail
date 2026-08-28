package consoleapp

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestWatchPreflightRefusalClassifies mirrors cliapp's test for the daemon
// that ships in the compose stack: watch's own upPreflightOutcome plus the
// shared doctor.BootRefusal must hand the telemetry hook the refusal's class,
// not unknown (#1503).
func TestWatchPreflightRefusalClassifies(t *testing.T) {
	r := &doctor.Report{}
	r.Add(doctor.CheckResult{Name: doctor.SourceConnectionCheckName, Status: doctor.StatusFail, Detail: "dial tcp: connection refused"})
	r.Add(doctor.CheckResult{Name: doctor.CapacityCheckName, Status: doctor.StatusFail})

	fatal, warn := upPreflightOutcome(r)
	if fatal == nil || warn {
		t.Fatalf("a source-connection failure must refuse boot (fatal=%v warn=%v)", fatal, warn)
	}
	var pe *doctor.PreflightError
	if !errors.As(fatal, &pe) {
		t.Fatalf("fatal is %T, want *doctor.PreflightError", fatal)
	}
	if got := telemetry.ClassifyError(doctor.BootRefusal(fatal)); got != telemetry.ClassDBConnection {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassDBConnection)
	}

	// Capacity alone is advisory: boot proceeds with a warning, no error.
	only := &doctor.Report{}
	only.Add(doctor.CheckResult{Name: doctor.CapacityCheckName, Status: doctor.StatusFail})
	if fatal, warn := upPreflightOutcome(only); fatal != nil || !warn {
		t.Errorf("capacity-only must warn, not refuse (fatal=%v warn=%v)", fatal, warn)
	}
}
