package ext

import (
	"context"
	"testing"
)

func TestRunDoctorChecksNoRegistrationReturnsNil(t *testing.T) {
	orig := doctorChecks
	doctorChecks = nil
	t.Cleanup(func() { doctorChecks = orig })

	if got := RunDoctorChecks(context.Background(), "src", "idx"); got != nil {
		t.Fatalf("RunDoctorChecks() = %v, want nil", got)
	}
}

func TestRunDoctorChecksConcatenatesAndPassesDSNs(t *testing.T) {
	orig := doctorChecks
	doctorChecks = nil
	t.Cleanup(func() { doctorChecks = orig })

	var gotSource, gotIndex string
	RegisterDoctorCheck(func(_ context.Context, sourceDSN, indexDSN string) []DoctorCheck {
		gotSource, gotIndex = sourceDSN, indexDSN
		return []DoctorCheck{
			{Name: "a", Status: "pass"},
			{Name: "b", Status: "warn", Detail: "d", Remediation: "r"},
		}
	})
	RegisterDoctorCheck(func(_ context.Context, _, _ string) []DoctorCheck {
		return []DoctorCheck{{Name: "c", Status: "fail"}}
	})

	checks := RunDoctorChecks(context.Background(), "src-dsn", "idx-dsn")
	if len(checks) != 3 || checks[0].Name != "a" || checks[1].Name != "b" || checks[2].Name != "c" {
		t.Fatalf("checks = %+v, want names a, b, c in order", checks)
	}
	if checks[1].Detail != "d" || checks[1].Remediation != "r" {
		t.Errorf("check fields mangled: %+v", checks[1])
	}
	if gotSource != "src-dsn" || gotIndex != "idx-dsn" {
		t.Errorf("check received (%q, %q), want (src-dsn, idx-dsn)", gotSource, gotIndex)
	}
}
