//go:build integration

package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// ─── Build verification ──────────────────────────────────────────────────────

// TestMakeBuildMCP verifies that `make build-mcp` compiles successfully and
// the resulting binary responds to --help.
func TestMakeBuildMCP(t *testing.T) {
	// Locate the project root (two levels up from cmd/bintrail-mcp/).
	projectRoot := filepath.Join("..", "..")

	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "bintrail-mcp")

	// Run make build-mcp, overriding the output path.
	makeCmd := exec.Command("make", "build-mcp", "MCP_BINARY="+binPath)
	makeCmd.Dir = projectRoot
	if out, err := makeCmd.CombinedOutput(); err != nil {
		t.Fatalf("make build-mcp failed: %v\n%s", err, out)
	}

	// Run --help and verify it exits cleanly (exit 0).
	helpCmd := exec.Command(binPath, "--help")
	out, err := helpCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bintrail-mcp --help failed: %v\n%s", err, out)
	}

	output := string(out)
	if !strings.Contains(output, "-http") {
		t.Errorf("expected --help output to mention -http flag, got:\n%s", output)
	}
}
