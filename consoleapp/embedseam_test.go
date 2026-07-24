package consoleapp

import (
	"os/exec"
	"strings"
	"testing"
)

// TestConsoleAppLinksExtSeamConsumers is the embed-seam guard, the mirror
// image of cliapp's TestCoreBinaryIsUIFree: consoleapp is the IMPORTABLE
// entrypoint an embedding distribution builds its console binary from, so its
// dependency graph must contain both internal/console (the package that reads
// ext.ConsoleAuth at server construction) and ext (the seam itself). If either
// link is broken — e.g. the console server construction moves somewhere
// consoleapp no longer reaches — ext.SetConsoleAuth called from an embedding
// main() before consoleapp.Main silently becomes a no-op in every buildable
// console binary, with every other test still green.
//
// The module path is deliberate: `go test` runs with the package directory as
// cwd, and the module path resolves identically from anywhere.
func TestConsoleAppLinksExtSeamConsumers(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps",
		"github.com/dbtrail/dbtrail/consoleapp").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	deps := make(map[string]bool)
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		deps[strings.TrimSpace(line)] = true
	}
	for _, required := range []string{
		"github.com/dbtrail/dbtrail/internal/console",
		"github.com/dbtrail/dbtrail/ext",
	} {
		if !deps[required] {
			t.Errorf("consoleapp does not link %s — the importable console entrypoint must reach the ext seam consumers, or embedding builds cannot enable them", required)
		}
	}
}
