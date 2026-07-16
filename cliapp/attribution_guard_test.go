package cliapp

import (
	"os/exec"
	"strings"
	"testing"
)

// TestAttributionCommandsNotOnCore pins that the who-changed attribution
// commands are NOT registered on the core root command. The surface was
// retired from the core distribution; embedding distributions that provide
// it register their commands via cliapp.AddCommands. A registration
// reappearing here means the retirement regressed.
func TestAttributionCommandsNotOnCore(t *testing.T) {
	retired := []string{"who-changed", "user-activity", "connection-history"}

	have := make(map[string]bool)
	for _, c := range rootCmd.Commands() {
		have[c.Use] = true
	}
	for _, name := range retired {
		if have[name] {
			t.Errorf("attribution command %q is registered on the core rootCmd — it belongs to embedding distributions (cliapp.AddCommands), not the core", name)
		}
	}
}

// TestCoreBinaryIsAttributionFree is the anti-reintroduction guard, in the
// style of TestCoreBinaryIsUIFree: the core bintrail binary must not link the
// retired attribution library. Any new import path back into
// internal/forensics, however indirect, fails this test.
func TestCoreBinaryIsAttributionFree(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps",
		"github.com/dbtrail/dbtrail/cmd/bintrail").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	const banned = "github.com/dbtrail/dbtrail/internal/forensics"
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		pkg := strings.TrimSpace(line)
		if pkg == banned || strings.HasPrefix(pkg, banned+"/") {
			t.Errorf("cmd/bintrail links %s — the attribution surface was retired from the core", pkg)
		}
	}
}
