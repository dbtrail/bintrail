package cliapp

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/doctor"
)

func TestExtCheckResultKnownStatusesPassThrough(t *testing.T) {
	for _, status := range []string{"pass", "fail", "warn", "skip"} {
		got := extCheckResult(ext.DoctorCheck{Name: "n", Status: status, Detail: "d", Remediation: "r"})
		if got.Status != doctor.CheckStatus(status) || got.Name != "n" || got.Detail != "d" || got.Remediation != "r" {
			t.Errorf("status %q: got %+v", status, got)
		}
	}
}

func TestExtCheckResultUnknownStatusBecomesWarn(t *testing.T) {
	// The doctor constants are lowercase; an uppercase "PASS" is unknown and
	// must degrade to a counted WARN rather than an uncounted malformed entry.
	got := extCheckResult(ext.DoctorCheck{Name: "n", Status: "PASS", Detail: "d"})
	if got.Status != doctor.StatusWarn {
		t.Fatalf("status = %q, want %q", got.Status, doctor.StatusWarn)
	}
	if !strings.Contains(got.Detail, `unknown status "PASS"`) || !strings.HasPrefix(got.Detail, "d") {
		t.Errorf("detail = %q, want original detail plus an unknown-status note", got.Detail)
	}
}

// TestAppendExtDoctorChecksDefaultNoop pins the stock-binary behavior: with
// no registered checks the report is untouched.
func TestAppendExtDoctorChecksDefaultNoop(t *testing.T) {
	report := &doctor.Report{}
	appendExtDoctorChecks(context.Background(), report, "src", "idx")
	if len(report.Checks) != 0 || report.Warnings != 0 {
		t.Fatalf("report changed with nothing registered: %+v", report)
	}
}
