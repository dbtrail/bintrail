//go:build integration

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/bintrail/internal/testutil"
)

// TestMCPE2E builds the bintrail-mcp binary and exercises the full JSON-RPC
// stdio protocol: initialize handshake → tools/list → tools/call query →
// tools/call status. This validates that the binary speaks correct MCP wire
// format from end to end.
func TestMCPE2E(t *testing.T) {
	db, dbName, dsn := setupTestDB(t)

	// Insert a test event and a completed index_state row so both query
	// and status tools have something to return.
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, "2026-02-19 10:00:00", nil,
		"mydb", "orders", 1, "99", nil, nil, []byte(`{"id":99,"amount":42}`))
	testutil.MustExec(t, db, `INSERT INTO index_state
		(binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
		VALUES ('mysql-bin.000001', 4096, 200, 1, 'completed', '2026-02-19 09:00:00', '2026-02-19 09:01:00')`)
	_ = dbName

	// Build the binary with coverage instrumentation.
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "bintrail-mcp")
	coverDir := filepath.Join(tmpDir, "coverage")
	if err := os.MkdirAll(coverDir, 0o755); err != nil {
		t.Fatalf("mkdir coverDir: %v", err)
	}

	buildCmd := exec.Command("go", "build", "-cover", "-o", binPath, "github.com/dbtrail/bintrail/cmd/bintrail-mcp")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}

	// Start the subprocess.
	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(),
		"GOCOVERDIR="+coverDir,
		"BINTRAIL_INDEX_DSN="+dsn,
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		stdin.Close()
		cmd.Wait()
	})

	reader := bufio.NewReader(stdoutPipe)

	// ── 1. Initialize ────────────────────────────────────────────────────────

	sendRequest(t, stdin, 1, "initialize", map[string]any{
		"protocolVersion": "2025-06-18",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "e2e-test", "version": "v0.0.1"},
	})

	initResp := readResponse(t, reader)
	result, ok := initResp["result"].(map[string]any)
	if !ok {
		t.Fatalf("initialize: expected result object, got: %v", initResp)
	}
	serverInfo, _ := result["serverInfo"].(map[string]any)
	if serverInfo["name"] != "bintrail" {
		t.Errorf("initialize: expected serverInfo.name=bintrail, got %v", serverInfo["name"])
	}

	// ── 2. Initialized notification (no response expected) ───────────────────

	sendNotification(t, stdin, "notifications/initialized", map[string]any{})

	// ── 3. tools/list ────────────────────────────────────────────────────────

	sendRequest(t, stdin, 2, "tools/list", map[string]any{})

	listResp := readResponse(t, reader)
	listResult, ok := listResp["result"].(map[string]any)
	if !ok {
		t.Fatalf("tools/list: expected result object, got: %v", listResp)
	}
	tools, _ := listResult["tools"].([]any)
	if len(tools) != 3 {
		t.Errorf("tools/list: expected 3 tools, got %d", len(tools))
	}

	toolNames := make(map[string]bool)
	for _, tool := range tools {
		if m, ok := tool.(map[string]any); ok {
			toolNames[m["name"].(string)] = true
		}
	}
	for _, want := range []string{"query", "recover", "status"} {
		if !toolNames[want] {
			t.Errorf("tool %q not found in list", want)
		}
	}

	// ── 4. tools/call query ───────────────────────────────────────────────────

	sendRequest(t, stdin, 3, "tools/call", map[string]any{
		"name": "query",
		"arguments": map[string]any{
			"index_dsn": dsn,
			"schema":    "mydb",
			"table":     "orders",
			"format":    "json",
		},
	})

	queryResp := readResponse(t, reader)
	queryResult, ok := queryResp["result"].(map[string]any)
	if !ok {
		t.Fatalf("tools/call query: expected result object, got: %v", queryResp)
	}
	content, _ := queryResult["content"].([]any)
	if len(content) == 0 {
		t.Fatal("tools/call query: expected non-empty content")
	}
	firstContent, _ := content[0].(map[string]any)
	text, _ := firstContent["text"].(string)

	var rows []any
	if err := json.Unmarshal([]byte(text), &rows); err != nil {
		t.Fatalf("query result not valid JSON: %v\ntext: %s", err, text)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 query row, got %d", len(rows))
	}

	// ── 5. tools/call status ─────────────────────────────────────────────────

	sendRequest(t, stdin, 4, "tools/call", map[string]any{
		"name": "status",
		"arguments": map[string]any{
			"index_dsn": dsn,
		},
	})

	statusResp := readResponse(t, reader)
	statusResult, ok := statusResp["result"].(map[string]any)
	if !ok {
		t.Fatalf("tools/call status: expected result object, got: %v", statusResp)
	}
	statusContent, _ := statusResult["content"].([]any)
	if len(statusContent) == 0 {
		t.Fatal("tools/call status: expected non-empty content")
	}
	statusFirst, _ := statusContent[0].(map[string]any)
	statusText, _ := statusFirst["text"].(string)

	if !strings.Contains(statusText, "=== Indexed Files ===") {
		t.Errorf("status: expected '=== Indexed Files ===' in output, got: %s", statusText)
	}
	if !strings.Contains(statusText, "mysql-bin.000001") {
		t.Errorf("status: expected binlog filename in output, got: %s", statusText)
	}

	// Close stdin to let the server exit cleanly, then collect coverage.
	stdin.Close()
	cmd.Wait()

	// Convert coverage data if present.
	if entries, _ := os.ReadDir(coverDir); len(entries) > 0 {
		outFile := filepath.Join(tmpDir, "coverage.txt")
		covCmd := exec.Command("go", "tool", "covdata", "textfmt",
			"-i="+coverDir, "-o="+outFile)
		if out, err := covCmd.CombinedOutput(); err != nil {
			t.Logf("covdata textfmt (non-fatal): %v\n%s", err, out)
		} else {
			t.Logf("E2E coverage data written to %s", outFile)
		}
	}
}

// ─── JSON-RPC helpers ─────────────────────────────────────────────────────────

func sendRequest(t *testing.T, w io.Writer, id int, method string, params any) {
	t.Helper()
	msg := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if _, err := fmt.Fprintf(w, "%s\n", data); err != nil {
		t.Fatalf("write request: %v", err)
	}
}

func sendNotification(t *testing.T, w io.Writer, method string, params any) {
	t.Helper()
	msg := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}
	if _, err := fmt.Fprintf(w, "%s\n", data); err != nil {
		t.Fatalf("write notification: %v", err)
	}
}

// readResponse reads lines until it finds a JSON-RPC response (has an "id"
// field). Notifications (no "id") are silently skipped since the server may
// send them asynchronously between responses.
func readResponse(t *testing.T, r *bufio.Reader) map[string]any {
	t.Helper()
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		var msg map[string]any
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("parse response JSON: %v\nline: %s", err, line)
		}
		// Notifications have no "id" field — skip them.
		if _, hasID := msg["id"]; hasID {
			return msg
		}
	}
}
