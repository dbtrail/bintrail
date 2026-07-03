package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/forensics"
)

// TestAddForensicsCommands_RegistersAll pins the registration set on a
// throwaway root (the AddReadCommands convention: each command's registration
// test lives here; cmd/bintrail pins that main.go actually calls it).
func TestAddForensicsCommands_RegistersAll(t *testing.T) {
	root := &cobra.Command{Use: "test-root"}
	AddForensicsCommands(root)

	want := []string{"who-changed", "user-activity", "connection-history", "ddl-history"}
	have := map[string]bool{}
	for _, c := range root.Commands() {
		have[c.Use] = true
	}
	for _, name := range want {
		if !have[name] {
			t.Errorf("forensics command %q not registered by AddForensicsCommands", name)
		}
	}
}

// TestForensicsCommands_GateClosed proves the entitlement seam is enforced at
// every forensics command entry point: with the gate closed, each command
// refuses before touching flags, databases, or the network (#701 D1 — policy
// at the surface, mechanism-free library).
func TestForensicsCommands_GateClosed(t *testing.T) {
	orig := forensics.Enabled
	forensics.Enabled = func() bool { return false }
	t.Cleanup(func() { forensics.Enabled = orig })

	cases := []struct {
		name string
		run  func(*cobra.Command, []string) error
		cmd  *cobra.Command
	}{
		{"who-changed", runWhoChanged, whoChangedCmd},
		{"user-activity", runUserActivity, userActivityCmd},
		{"connection-history", runConnectionHistory, connectionHistoryCmd},
		{"ddl-history", runDDLHistory, ddlHistoryCmd},
	}
	for _, tc := range cases {
		err := tc.run(tc.cmd, nil)
		if err == nil {
			t.Errorf("%s: gate closed but the command ran", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), "forensics is not enabled in this build") {
			t.Errorf("%s: gate error = %q, want the standard disabled message", tc.name, err)
		}
	}
}

func TestRunWhoChanged_FlagValidation(t *testing.T) {
	// Validation runs before any connection attempt, so no database is needed.
	reset := func() {
		wcIndexDSN, wcSourceDSN = "unused:dsn@tcp(127.0.0.1:1)/x", ""
		wcSchema, wcTable, wcPK = "shop", "orders", ""
		wcSince, wcUntil = "", ""
		wcLimit, wcOrder, wcFormat = 100, "ASC", "table"
	}

	reset()
	wcFormat = "csv"
	if err := runWhoChanged(whoChangedCmd, nil); err == nil || !strings.Contains(err.Error(), "--format") {
		t.Errorf("invalid format: got %v, want a --format error", err)
	}

	reset()
	wcOrder = "sideways"
	if err := runWhoChanged(whoChangedCmd, nil); err == nil || !strings.Contains(err.Error(), "--order") {
		t.Errorf("invalid order: got %v, want an --order error", err)
	}

	reset()
	wcLimit = 0
	if err := runWhoChanged(whoChangedCmd, nil); err == nil || !strings.Contains(err.Error(), "--limit") {
		t.Errorf("invalid limit: got %v, want a --limit error", err)
	}

	reset()
	wcSince = "not-a-time"
	if err := runWhoChanged(whoChangedCmd, nil); err == nil || !strings.Contains(err.Error(), "--since") {
		t.Errorf("invalid since: got %v, want a --since error", err)
	}
}

func TestRunConnectionHistory_RequiresUserOrHost(t *testing.T) {
	chUser, chHost = "", ""
	chFormat, chOrder = "table", "DESC"
	err := runConnectionHistory(connectionHistoryCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--user or --host") {
		t.Errorf("got %v, want the one-of --user/--host error", err)
	}
}

func TestRenderWhoChangedTable(t *testing.T) {
	ts := time.Date(2026, 6, 15, 10, 0, 10, 0, time.UTC)
	conn := uint32(42)
	long := strings.Repeat("UPDATE orders SET status = 'x' ", 10)
	res := forensics.WhoChangedResult{
		Events: []forensics.WhoChangedEvent{
			{
				EventID: 1, Timestamp: ts, EventType: "UPDATE",
				Schema: "shop", Table: "orders", PKValues: "42",
				ConnectionID: &conn, QueryText: &long,
				Attribution: &forensics.Attribution{
					User: "alice", Host: "app1", ClientProgram: "payroll-svc",
					Source: forensics.AttributionSourceAuditLog, Confidence: forensics.ConfidenceExact,
				},
			},
			{
				EventID: 2, Timestamp: ts.Add(time.Minute), EventType: "DELETE",
				Schema: "shop", Table: "orders", PKValues: "43",
			},
		},
		TotalCount: 2,
		Notes:      []string{"note one", "note two"},
		FallbackQueries: []forensics.FallbackQuery{
			{Description: "check processlist", SQL: "SELECT 1"},
		},
	}

	var buf bytes.Buffer
	renderWhoChangedTable(&buf, res)
	out := buf.String()

	for _, want := range []string{
		"TIMESTAMP", "WHO", "SOURCE", "CONFIDENCE", // header
		"2026-06-15 10:00:10", "UPDATE", "alice@app1 (payroll-svc)", "audit_log", "exact",
		"unknown", // unattributed event
		"...",     // long query_text truncated
		"Notes:", "note one", "note two",
		"Fallback queries", "check processlist", "SELECT 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}
	// The full untruncated statement must NOT be dumped into the table.
	if strings.Contains(out, long) {
		t.Errorf("query_text not truncated in table cell:\n%s", out)
	}
}

func TestRenderActivityTable(t *testing.T) {
	res := forensics.ActivityResult{
		Events: []map[string]any{{
			"connection_id": int64(7), "user": "app_rw", "host": "10.0.0.5",
			"rows_affected": int64(3), "duration_ms": 1.34,
			"sql_text": "UPDATE t SET x=1\nWHERE id=2",
		}},
		Source: "performance_schema",
		Count:  1,
	}
	var buf bytes.Buffer
	renderActivityTable(&buf, res)
	out := buf.String()
	for _, want := range []string{"CONN", "app_rw", "10.0.0.5", "1.3", "UPDATE t SET x=1 WHERE id=2", "Source: performance_schema"} {
		if !strings.Contains(out, want) {
			t.Errorf("activity table missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "\nWHERE") {
		t.Errorf("multi-line SQL not collapsed to one table line:\n%s", out)
	}

	// Fallback shape: no data, a note, and executable SQL.
	res = forensics.ActivityResult{
		Source: "fallback",
		Note:   "performance_schema query failed: denied",
		FallbackQueries: []forensics.FallbackQuery{
			{Description: "processlist", SQL: "SELECT * FROM information_schema.PROCESSLIST"},
		},
	}
	buf.Reset()
	renderActivityTable(&buf, res)
	out = buf.String()
	for _, want := range []string{"No results.", "Source: fallback", "Note: performance_schema query failed", "PROCESSLIST"} {
		if !strings.Contains(out, want) {
			t.Errorf("fallback rendering missing %q:\n%s", want, out)
		}
	}
}

func TestTruncateCell(t *testing.T) {
	if got := truncateCell("short", 10); got != "short" {
		t.Errorf("truncateCell(short) = %q", got)
	}
	if got := truncateCell("a\nb\tc  d", 10); got != "a b c d" {
		t.Errorf("whitespace collapse: %q", got)
	}
	long := strings.Repeat("x", 100)
	got := truncateCell(long, 10)
	if len(got) != 10 || !strings.HasSuffix(got, "...") {
		t.Errorf("truncation: %q (len %d)", got, len(got))
	}
}
