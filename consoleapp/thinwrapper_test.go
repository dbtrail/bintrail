package consoleapp

import (
	"encoding/json"
	"os/exec"
	"slices"
	"testing"
)

// TestConsoleBinaryIsThinWrapper is the export guard (the consoleapp sibling
// of cliapp's TestCoreBinaryIsUIFree): cmd/bintrail-console must stay a thin
// wrapper — exactly one Go file (main.go, holding only the -ldflags-injected
// build vars and os.Exit) delegating to this package's Main. Command logic
// creeping back into the cmd directory would put it out of reach of builds
// that import consoleapp and wrap the OSS core.
//
// The module path is deliberate: `go test` runs with the package directory as
// cwd, and the module path resolves identically from anywhere.
func TestConsoleBinaryIsThinWrapper(t *testing.T) {
	out, err := exec.Command("go", "list", "-json",
		"github.com/dbtrail/dbtrail/cmd/bintrail-console").Output()
	if err != nil {
		t.Fatalf("go list -json: %v\n%s", err, out)
	}
	var pkg struct {
		GoFiles        []string
		CgoFiles       []string
		IgnoredGoFiles []string
		Imports        []string
	}
	if err := json.Unmarshal(out, &pkg); err != nil {
		t.Fatalf("decoding go list -json output: %v", err)
	}
	if !slices.Equal(pkg.GoFiles, []string{"main.go"}) {
		t.Errorf("cmd/bintrail-console compiles %v — the wrapper must stay exactly [main.go]; new command code belongs in consoleapp", pkg.GoFiles)
	}
	// GoFiles alone is blind to files the current build context excludes:
	// go list reports cgo files under CgoFiles and build-constrained files
	// (e.g. //go:build windows glue) under IgnoredGoFiles. Either would ship
	// command logic in some build of the binary without ever appearing in
	// GoFiles on the platform running this test.
	if len(pkg.CgoFiles) != 0 {
		t.Errorf("cmd/bintrail-console has cgo files %v — the wrapper must stay exactly [main.go]; new command code belongs in consoleapp", pkg.CgoFiles)
	}
	if len(pkg.IgnoredGoFiles) != 0 {
		t.Errorf("cmd/bintrail-console has build-constrained files %v excluded on this platform — the wrapper must stay exactly [main.go] on every platform; new command code belongs in consoleapp", pkg.IgnoredGoFiles)
	}
	const seam = "github.com/dbtrail/dbtrail/consoleapp"
	if !slices.Contains(pkg.Imports, seam) {
		t.Errorf("cmd/bintrail-console imports %v — it must delegate to %s", pkg.Imports, seam)
	}
}
