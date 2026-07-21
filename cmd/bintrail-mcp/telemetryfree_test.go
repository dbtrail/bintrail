package main

import (
	"os/exec"
	"strings"
	"testing"
)

// TestMCPServerIsTelemetryFree: the MCP server is invoked by an AI agent inside
// an IDE or chat session, so there is no human present to have consented to
// anything and no terminal on which a notice could be shown. It is excluded
// from usage telemetry entirely — not merely defaulted off, but unable to
// report, which is a promise the binary's dependency graph can keep on its own.
//
// Same mechanism as cliapp's UI-free and pg-free guards.
func TestMCPServerIsTelemetryFree(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps",
		"github.com/dbtrail/dbtrail/cmd/bintrail-mcp").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	const banned = "github.com/dbtrail/dbtrail/internal/telemetry"
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		pkg := strings.TrimSpace(line)
		if pkg == banned || strings.HasPrefix(pkg, banned+"/") {
			t.Errorf("cmd/bintrail-mcp links %s — an agent-invoked server must not be able to report usage", pkg)
		}
	}
}
