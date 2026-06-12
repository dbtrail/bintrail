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

// TestStdioCleanDisconnectExitsZero pins #473 end-to-end: a client
// closing stdin after initialize is the normal end of an MCP stdio
// session, so the server must exit 0 with no ERROR log line —
// supervisors and exit-code checks must not record a failure for
// every normal disconnect.
func TestStdioCleanDisconnectExitsZero(t *testing.T) {
	projectRoot := filepath.Join("..", "..")
	binPath := filepath.Join(t.TempDir(), "bintrail-mcp")

	makeCmd := exec.Command("make", "build-mcp", "MCP_BINARY="+binPath)
	makeCmd.Dir = projectRoot
	if out, err := makeCmd.CombinedOutput(); err != nil {
		t.Fatalf("make build-mcp failed: %v\n%s", err, out)
	}

	cmd := exec.Command(binPath)
	cmd.Stdin = strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"disconnect-test","version":"1.0"}}}` + "\n" +
			`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n",
	) // reader drains → stdin closes → clean client disconnect
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("clean disconnect must exit 0, got %v\n%s", err, out)
	}
	if strings.Contains(string(out), "ERROR") {
		t.Errorf("clean disconnect must not log ERROR, got:\n%s", out)
	}
}
