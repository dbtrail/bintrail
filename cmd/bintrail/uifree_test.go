package main

import (
	"os/exec"
	"strings"
	"testing"
)

// TestCoreBinaryIsUIFree is the decouple guard: the core bintrail binary must
// not link the web console — it lives in the standalone bintrail-console
// binary. A new import path from any core command back into internal/console,
// however indirect, fails this test.
//
// go.yaml.in/yaml/v2 is deliberately NOT banned: while the console's server
// registry is its loudest user, the shim's auth config (internal/shim/auth.go)
// and proxysql-config (both parse shim.yaml) legitimately keep it in the core.
//
// The module path is deliberate: `go test` runs with the package directory as
// cwd, and the module path resolves identically from anywhere.
func TestCoreBinaryIsUIFree(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps",
		"github.com/dbtrail/bintrail/cmd/bintrail").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	const banned = "github.com/dbtrail/bintrail/internal/console"
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		// Exact match or a subpackage — never a substring (avoids false
		// positives while still catching a future internal/console/dto).
		pkg := strings.TrimSpace(line)
		if pkg == banned || strings.HasPrefix(pkg, banned+"/") {
			t.Errorf("cmd/bintrail links %s — the web console must only be linked by cmd/bintrail-console", pkg)
		}
	}
}
