//go:build integration

package doctor

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationForensicsChecksNeverFail runs the two forensics doctor checks
// against the live test server. The container's performance_schema / audit
// plugin state is whatever it is, so the assertion is the status contract:
// forensics is optional — the checks may PASS or WARN but must never FAIL,
// and a WARN must carry actionable detail.
func TestIntegrationForensicsChecksNeverFail(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("connect to test MySQL: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	checks := []struct {
		name string
		run  func() CheckResult
	}{
		{"checkPerformanceSchema", func() CheckResult { return checkPerformanceSchema(ctx, db) }},
		{"checkAuditPlugin", func() CheckResult { return checkAuditPlugin(ctx, db) }},
	}
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			got := c.run()
			if got.Status != StatusPass && got.Status != StatusWarn {
				t.Errorf("Status = %q (detail=%q), want pass or warn — forensics checks never fail", got.Status, got.Detail)
			}
			if got.Status == StatusWarn && got.Detail == "" {
				t.Error("WARN with no detail gives the operator nothing to act on")
			}
		})
	}
}

// TestIntegrationBuildIncludesForensicsChecks asserts the Build wiring: the
// source-checks section must surface both forensics checks in the report.
func TestIntegrationBuildIncludesForensicsChecks(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	report := Build(t.Context(), testutil.BaseDSN()+"/", "", "", 0)

	for _, name := range []string{"performance_schema (forensics)", "Audit log plugin (forensics)"} {
		found := false
		for _, c := range report.Checks {
			if c.Name != name {
				continue
			}
			found = true
			if c.Status == StatusFail {
				t.Errorf("%s reported FAIL (detail=%q) — forensics checks never fail", name, c.Detail)
			}
		}
		if !found {
			var names []string
			for _, c := range report.Checks {
				names = append(names, c.Name)
			}
			t.Errorf("Build report is missing %q; got %v", name, names)
		}
	}
}
