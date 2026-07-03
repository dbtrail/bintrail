package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/forensics"
)

// withForensicsDisabled swaps the entitlement gate closed for the duration
// of the test, restoring the original function afterwards. This simulates a
// future licensed build (#701 D1) where forensics.Enabled reports false.
func withForensicsDisabled(t *testing.T) {
	t.Helper()
	orig := forensics.Enabled
	forensics.Enabled = func() bool { return false }
	t.Cleanup(func() { forensics.Enabled = orig })
}

// ─── Dispatch happy paths ────────────────────────────────────────────────────
//
// The raw JSON payloads below are the wire contract for the SaaS WS server
// (nethalo/dbtrail#1507): field names mirror models/forensics.py. Changing a
// json tag on the request structs must break one of these tests.

func TestDispatch_forensicsCapabilities(t *testing.T) {
	h := &stubHandler{
		capabilities: func(context.Context) (forensics.Capabilities, error) {
			return forensics.Capabilities{
				ServerInfo: forensics.ServerInfo{Version: "8.0.42", Variant: "mysql"},
			}, nil
		},
	}
	// No payload beyond the envelope — Data intentionally absent.
	resp := dispatch(context.Background(), h, Command{ID: "c1", Type: "forensics_capabilities"})

	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.ID != "c1" || resp.Type != "forensics_capabilities" {
		t.Errorf("envelope not preserved: %+v", resp)
	}
	caps, ok := resp.Data.(forensics.Capabilities)
	if !ok {
		t.Fatalf("Data type = %T, want forensics.Capabilities", resp.Data)
	}
	if caps.ServerInfo.Version != "8.0.42" {
		t.Errorf("ServerInfo.Version = %q, want 8.0.42", caps.ServerInfo.Version)
	}
	// The wire shape must carry the SaaS field names (models/forensics.py).
	wire, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	for _, key := range []string{`"performance_schema"`, `"audit_log"`, `"server_info"`} {
		if !strings.Contains(string(wire), key) {
			t.Errorf("wire response missing %s: %s", key, wire)
		}
	}
}

func TestDispatch_forensicsEnrich(t *testing.T) {
	var gotIDs []int64
	h := &stubHandler{
		enrich: func(_ context.Context, req ForensicsEnrichRequest) (forensics.EnrichResult, error) {
			gotIDs = req.ThreadIDs
			return forensics.EnrichResult{
				Threads: map[string]*forensics.ThreadInfo{
					"42": {User: "app", Host: "10.0.0.5", ConnectionID: 42},
				},
				Source:   "performance_schema",
				NotFound: []int64{7},
			}, nil
		},
	}
	// SaaS wire shape: {"thread_ids": [...]}.
	cmd := Command{ID: "e1", Type: "forensics_enrich", Data: json.RawMessage(`{"thread_ids":[42,7]}`)}
	resp := dispatch(context.Background(), h, cmd)

	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if len(gotIDs) != 2 || gotIDs[0] != 42 || gotIDs[1] != 7 {
		t.Errorf("thread_ids decoded as %v, want [42 7]", gotIDs)
	}
	res, ok := resp.Data.(forensics.EnrichResult)
	if !ok {
		t.Fatalf("Data type = %T, want forensics.EnrichResult", resp.Data)
	}
	if res.Threads["42"] == nil || res.Threads["42"].User != "app" {
		t.Errorf("unexpected threads: %+v", res.Threads)
	}
	wire, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	for _, key := range []string{`"threads"`, `"connection_id":42`, `"not_found":[7]`} {
		if !strings.Contains(string(wire), key) {
			t.Errorf("wire response missing %s: %s", key, wire)
		}
	}
}

func TestDispatch_forensicsActivity(t *testing.T) {
	var got ForensicsActivityRequest
	h := &stubHandler{
		activity: func(_ context.Context, req ForensicsActivityRequest) (forensics.ActivityResult, error) {
			got = req
			return forensics.ActivityResult{
				Events: []map[string]any{{"sql_text": "UPDATE t SET x=1"}},
				Source: "performance_schema",
				Count:  1,
			}, nil
		},
	}
	// SaaS wire shape (models/forensics.py ForensicsQueryParams).
	raw := `{"query_type":"user_activity","user":"app","host":"10.0.0.5",` +
		`"schema":"shop","since":"2026-07-01T00:00:00","until":"2026-07-02 00:00:00",` +
		`"limit":25,"order":"ASC"}`
	resp := dispatch(context.Background(), h, Command{ID: "a1", Type: "forensics_activity", Data: json.RawMessage(raw)})

	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	want := ForensicsActivityRequest{
		QueryType: "user_activity", User: "app", Host: "10.0.0.5", Schema: "shop",
		Since: "2026-07-01T00:00:00", Until: "2026-07-02 00:00:00", Limit: 25, Order: "ASC",
	}
	if got != want {
		t.Errorf("decoded request = %+v, want %+v", got, want)
	}
	if _, ok := resp.Data.(forensics.ActivityResult); !ok {
		t.Fatalf("Data type = %T, want forensics.ActivityResult", resp.Data)
	}
}

func TestDispatch_forensicsUsers(t *testing.T) {
	h := &stubHandler{
		users: func(context.Context) (ForensicsUsersResult, error) {
			return ForensicsUsersResult{Users: []string{"app", "root"}}, nil
		},
	}
	resp := dispatch(context.Background(), h, Command{ID: "u1", Type: "forensics_users"})

	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	res, ok := resp.Data.(ForensicsUsersResult)
	if !ok {
		t.Fatalf("Data type = %T, want ForensicsUsersResult", resp.Data)
	}
	if len(res.Users) != 2 {
		t.Errorf("unexpected users: %v", res.Users)
	}
	// Mirrors the SaaS agent HTTP shape: {"users": [...]}.
	wire, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	if !strings.Contains(string(wire), `"users":["app","root"]`) {
		t.Errorf("wire response missing users list: %s", wire)
	}
}

func TestDispatch_forensicsAuditLog(t *testing.T) {
	var got ForensicsAuditLogRequest
	h := &stubHandler{
		auditLog: func(_ context.Context, req ForensicsAuditLogRequest) (forensics.AuditReadResult, error) {
			got = req
			return forensics.AuditReadResult{
				Events:         []forensics.AuditEvent{{Timestamp: "2026-07-01T12:00:00Z", User: "app"}},
				TotalScanned:   10,
				FormatDetected: forensics.AuditFormatJSON,
				Variant:        forensics.AuditVariantPercona,
				FilePath:       "/var/lib/mysql/audit.log",
				FilesRead:      1,
			}, nil
		},
	}
	// SaaS wire shape (models/forensics.py AuditLogFileParams) + tail_lines,
	// which is OSS-only tuning.
	raw := `{"since":"2026-07-01T00:00:00Z","user":"app","event_type":"query",` +
		`"limit":100,"offset":10,"include_rotated":true,"tail_lines":5000}`
	resp := dispatch(context.Background(), h, Command{ID: "l1", Type: "forensics_audit_log", Data: json.RawMessage(raw)})

	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if got.Since.IsZero() || !got.Since.Equal(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("since decoded as %v", got.Since)
	}
	if !got.Until.IsZero() {
		t.Errorf("absent until must decode as zero time, got %v", got.Until)
	}
	if got.User != "app" || got.EventType != "query" || got.Limit != 100 ||
		got.Offset != 10 || !got.IncludeRotated || got.TailLines != 5000 {
		t.Errorf("decoded request = %+v", got)
	}
	res, ok := resp.Data.(forensics.AuditReadResult)
	if !ok {
		t.Fatalf("Data type = %T, want forensics.AuditReadResult", resp.Data)
	}
	if res.FormatDetected != forensics.AuditFormatJSON {
		t.Errorf("FormatDetected = %q", res.FormatDetected)
	}
	wire, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	for _, key := range []string{`"events"`, `"total_scanned":10`, `"format_detected":"json"`, `"file_path"`} {
		if !strings.Contains(string(wire), key) {
			t.Errorf("wire response missing %s: %s", key, wire)
		}
	}
}

// ─── Malformed payloads ──────────────────────────────────────────────────────

func TestDispatch_forensicsMalformedPayloads(t *testing.T) {
	// Handler funcs are nil — reaching one would panic and fail the test,
	// proving malformed payloads are rejected before the handler runs.
	h := &stubHandler{}
	for _, cmdType := range []string{"forensics_enrich", "forensics_activity", "forensics_audit_log"} {
		t.Run(cmdType, func(t *testing.T) {
			resp := dispatch(context.Background(), h, Command{ID: "m1", Type: cmdType, Data: json.RawMessage(`{invalid`)})
			if !strings.Contains(resp.Error, "invalid "+cmdType+" payload") {
				t.Errorf("error = %q, want 'invalid %s payload'", resp.Error, cmdType)
			}
		})
	}
}

// ─── Entitlement gate (#701 D1) ──────────────────────────────────────────────

func TestDispatch_forensicsGateClosed(t *testing.T) {
	withForensicsDisabled(t)

	// All handler funcs nil: a dispatch that slips past the gate panics.
	h := &stubHandler{}
	for _, cmdType := range []string{
		"forensics_capabilities", "forensics_enrich", "forensics_activity",
		"forensics_users", "forensics_audit_log",
	} {
		t.Run(cmdType, func(t *testing.T) {
			resp := dispatch(context.Background(), h, Command{ID: "g1", Type: cmdType, Data: json.RawMessage(`{}`)})
			if resp.Error != "forensics disabled in this build" {
				t.Errorf("error = %q, want 'forensics disabled in this build'", resp.Error)
			}
			if resp.ID != "g1" || resp.Type != cmdType {
				t.Errorf("envelope not preserved: %+v", resp)
			}
			if resp.Data != nil {
				t.Errorf("Data should be nil when gated, got %v", resp.Data)
			}
		})
	}
}

// TestDispatch_forensicsGateClosed_legacyQueryUngated pins the back-compat
// decision: the legacy forensics_query command predates the forensics
// library and is NOT part of the gated attribution family.
func TestDispatch_forensicsGateClosed_legacyQueryUngated(t *testing.T) {
	withForensicsDisabled(t)

	called := false
	h := &stubHandler{
		forensics: func(context.Context, ForensicsQueryRequest) (*ForensicsResult, error) {
			called = true
			return &ForensicsResult{}, nil
		},
	}
	resp := dispatch(context.Background(), h, Command{ID: "g2", Type: "forensics_query", Data: json.RawMessage(`{"query":"recent_queries"}`)})
	if resp.Error != "" {
		t.Fatalf("legacy forensics_query must not be gated, got error: %s", resp.Error)
	}
	if !called {
		t.Error("legacy forensics_query handler was not invoked")
	}
}

// TestDispatch_forensicsGateClosed_unknownTypeFallthrough proves the gate
// pre-check does not swallow unknown forensics_-prefixed types — they still
// hit the default unknown-command branch in either gate state.
func TestDispatch_forensicsGateClosed_unknownTypeFallthrough(t *testing.T) {
	h := &stubHandler{}
	for _, disable := range []bool{false, true} {
		if disable {
			withForensicsDisabled(t)
		}
		resp := dispatch(context.Background(), h, Command{ID: "g3", Type: "forensics_nope", Data: json.RawMessage(`{}`)})
		if !strings.Contains(resp.Error, "unknown command type") {
			t.Errorf("gate disabled=%v: error = %q, want 'unknown command type'", disable, resp.Error)
		}
	}
}

// ─── Nil source DB ───────────────────────────────────────────────────────────

// TestDefaultHandler_forensicsCommandsRequireSourceDSN mirrors
// TestDefaultHandler_forensicsRequiresSourceDSN for the attribution family:
// every command that inspects the source server must fail with a clear
// error (never panic) when the agent runs without --source-dsn.
func TestDefaultHandler_forensicsCommandsRequireSourceDSN(t *testing.T) {
	h := &DefaultHandler{} // SourceDB nil
	ctx := context.Background()

	calls := map[string]func() error{
		"forensics_capabilities": func() error { _, err := h.HandleForensicsCapabilities(ctx); return err },
		"forensics_enrich": func() error {
			_, err := h.HandleForensicsEnrich(ctx, ForensicsEnrichRequest{ThreadIDs: []int64{1}})
			return err
		},
		"forensics_activity": func() error {
			_, err := h.HandleForensicsActivity(ctx, ForensicsActivityRequest{QueryType: "ddl_history"})
			return err
		},
		"forensics_users": func() error { _, err := h.HandleForensicsUsers(ctx); return err },
		"forensics_audit_log": func() error {
			_, err := h.HandleForensicsAuditLog(ctx, ForensicsAuditLogRequest{})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil || !strings.Contains(err.Error(), "require --source-dsn") {
				t.Errorf("error = %v, want 'require --source-dsn'", err)
			}
		})
	}
}

// TestResolveAuditSourceHost pins the audit-source-host resolution the RDS/
// CloudWatch remote reader depends on (#705): a per-request SourceHost wins,
// otherwise the agent's own source host (from --source-dsn). An empty result
// keeps the audit tier on the local-file path — the exact wiring gap that made
// the RDS/Aurora reader dead code before it was threaded through, so this
// guards against a refactor silently dropping the fallback.
func TestResolveAuditSourceHost(t *testing.T) {
	tests := []struct {
		name        string
		reqHost     string
		handlerHost string
		want        string
	}{
		{"request overrides handler", "req.us-east-1.rds.amazonaws.com", "agent.us-west-2.rds.amazonaws.com", "req.us-east-1.rds.amazonaws.com"},
		{"falls back to agent source host", "", "agent.us-west-2.rds.amazonaws.com", "agent.us-west-2.rds.amazonaws.com"},
		{"both empty stays local-only", "", "", ""},
		{"request set, handler empty", "req.eu-west-1.rds.amazonaws.com", "", "req.eu-west-1.rds.amazonaws.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveAuditSourceHost(tt.reqHost, tt.handlerHost); got != tt.want {
				t.Errorf("resolveAuditSourceHost(%q, %q) = %q, want %q", tt.reqHost, tt.handlerHost, got, tt.want)
			}
		})
	}
}

// TestHandleForensicsAuditLog_ThreadsSource proves HandleForensicsAuditLog
// passes req.Source into the AuditReadOptions it hands to ReadAuditLog: an
// unsupported source reaches ReadAuditLog's dispatch and errors immediately
// (before any SQL/AWS), which only happens if the field is threaded — a dropped
// Source would default to "" (auto) and not produce this error. Complements
// TestResolveAuditSourceHost, which pins the host fallback logic.
func TestHandleForensicsAuditLog_ThreadsSource(t *testing.T) {
	db, _, err := sqlmock.New() // no queries expected: dispatch errors first
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	h := &DefaultHandler{SourceDB: db, SourceHost: "agent.host"}
	_, err = h.HandleForensicsAuditLog(context.Background(), ForensicsAuditLogRequest{Source: "bogus-source"})
	if err == nil || !strings.Contains(err.Error(), "unsupported audit source") {
		t.Fatalf("err = %v, want 'unsupported audit source' — req.Source not threaded into the read options?", err)
	}
}
