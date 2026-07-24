package cliapp

import (
	"os/exec"
	"path"
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

// TestShippedBinariesAreAttributionFree is the anti-reintroduction guard, in
// the style of TestCoreBinaryIsUIFree: no shipped binary may link the retired
// attribution library. Any new import path back into internal/forensics,
// however indirect, fails this test. `go list -deps` only loads packages —
// it never builds — so cmd/bintrail-console's CGO/DuckDB dependency is fine
// here.
func TestShippedBinariesAreAttributionFree(t *testing.T) {
	binaries := []string{
		"github.com/dbtrail/dbtrail/cmd/bintrail",
		"github.com/dbtrail/dbtrail/cmd/bintrail-console",
		"github.com/dbtrail/dbtrail/cmd/bintrail-mcp",
		"github.com/dbtrail/dbtrail/cmd/bintrail-pg",
	}
	const banned = "github.com/dbtrail/dbtrail/internal/forensics"
	for _, bin := range binaries {
		t.Run(path.Base(bin), func(t *testing.T) {
			out, err := exec.Command("go", "list", "-deps", bin).CombinedOutput()
			if err != nil {
				t.Fatalf("go list -deps %s: %v\n%s", bin, err, out)
			}
			for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
				pkg := strings.TrimSpace(line)
				if pkg == banned || strings.HasPrefix(pkg, banned+"/") {
					t.Errorf("%s links %s — the attribution surface was retired from the core", bin, pkg)
				}
			}
		})
	}
}
