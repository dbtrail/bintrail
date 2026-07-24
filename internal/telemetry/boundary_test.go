package telemetry_test

import (
	"os/exec"
	"strings"
	"testing"
)

const modulePrefix = "github.com/dbtrail/dbtrail"

// TestTelemetryImportsNothingFromThisModule is the primary trust artifact of
// the whole telemetry feature, and the one a hostile reviewer can check in
// thirty seconds.
//
// The privacy claim is that telemetry cannot carry your data. A field allowlist
// alone does not establish that: an allowlist is blind to PROVENANCE, so
// nothing in it would stop a future contributor writing
// `run_id = serverid.Resolve(...)` with every field name still green. What does
// establish it is reach — a package that cannot import internal/config or
// internal/parser cannot construct a DSN or a row, and one that cannot import
// internal/serverid cannot learn a bintrail_id, no matter what anyone writes
// inside it.
//
// The assertion is deliberately "no package from this module at all" rather
// than a blocklist of the dangerous ones. A blocklist has to be maintained and
// is outflanked the day someone adds internal/newthing; this cannot be
// outflanked, and it is a stricter promise that happens to already hold.
//
// If this test ever fails, the fix is almost never to relax it.
func TestTelemetryImportsNothingFromThisModule(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", modulePrefix+"/internal/telemetry").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	const self = modulePrefix + "/internal/telemetry"
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		pkg := strings.TrimSpace(line)
		if pkg == self || !strings.HasPrefix(pkg, modulePrefix+"/") {
			continue
		}
		t.Errorf("internal/telemetry links %s.\n"+
			"Telemetry must be unable to REACH anything that knows about DSNs, rows, "+
			"schemas or server identity — that structural limit is what makes the "+
			"metadata-only claim verifiable rather than merely intended. "+
			"Move whatever you needed to the caller instead.", pkg)
	}
}

// TestTelemetryPackagesAreSelfContained pins the same property from the other
// direction, so the guard above cannot be defeated by adding a helper package
// that telemetry imports and that in turn imports the world.
func TestTelemetryPackagesAreSelfContained(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{join .Imports \"\\n\"}}",
		modulePrefix+"/internal/telemetry").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		pkg := strings.TrimSpace(line)
		if strings.HasPrefix(pkg, modulePrefix+"/") {
			t.Errorf("internal/telemetry directly imports %s; it must depend only on the standard library and leaf third-party packages", pkg)
		}
	}
}
