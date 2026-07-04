package console

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/forensics"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestForensicsGate_Disabled proves the entitlement seam is enforced at every
// forensics HTTP entry point: with forensics.Enabled() false, each handler
// refuses with 403 before touching a bundle or a source connection (#701 D1).
func TestForensicsGate_Disabled(t *testing.T) {
	orig := forensics.Enabled
	forensics.Enabled = func() bool { return false }
	t.Cleanup(func() { forensics.Enabled = orig })

	s := newBootServer(nil)
	cases := []struct {
		name   string
		method string
		path   string
		body   string
		run    func(*Server, http.ResponseWriter, *http.Request)
	}{
		{"capabilities", "GET", "/api/forensics/capabilities", "", (*Server).handleForensicsCapabilities},
		{"users", "GET", "/api/forensics/users", "", (*Server).handleForensicsUsers},
		{"who-changed", "POST", "/api/forensics/who-changed", `{"schema":"app","table":"users"}`, (*Server).handleForensicsWhoChanged},
		{"activity", "POST", "/api/forensics/activity", `{"query_type":"user_activity","user":"app_rw"}`, (*Server).handleForensicsActivity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, body)
			tc.run(s, rec, req)
			if rec.Code != http.StatusForbidden {
				t.Errorf("%s: status = %d, want 403; body = %s", tc.name, rec.Code, rec.Body.String())
			}
		})
	}
}

// TestForensicsGate_RBACActive proves the v1 posture from the issue: forensics
// refuses outright while an RBAC redaction profile is active, matching the
// Verify/recover-cascade precedent (forensic output carries unredacted SQL
// text and session identity that the redaction pipeline does not cover yet).
// Table-driven across all 4 handlers, mirroring TestForensicsGate_Disabled —
// an earlier version of this test only covered handleForensicsCapabilities.
func TestForensicsGate_RBACActive(t *testing.T) {
	s := newBootServer(nil)
	s.redactCols = append(s.redactCols, query.SchemaTableColumn{Schema: "app", Table: "users", Column: "ssn"})

	cases := []struct {
		name   string
		method string
		path   string
		body   string
		run    func(*Server, http.ResponseWriter, *http.Request)
	}{
		{"capabilities", "GET", "/api/forensics/capabilities", "", (*Server).handleForensicsCapabilities},
		{"users", "GET", "/api/forensics/users", "", (*Server).handleForensicsUsers},
		{"who-changed", "POST", "/api/forensics/who-changed", `{"schema":"app","table":"users"}`, (*Server).handleForensicsWhoChanged},
		{"activity", "POST", "/api/forensics/activity", `{"query_type":"user_activity","user":"app_rw"}`, (*Server).handleForensicsActivity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, body)
			tc.run(s, rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("%s: status = %d, want 403; body = %s", tc.name, rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "access-control profile") {
				t.Errorf("%s: expected an access-control-profile refusal message, got: %s", tc.name, rec.Body.String())
			}
		})
	}
}

// TestHandleForensicsCapabilities_NoSourceConfigured: a server with no source
// DSN degrades to source_configured=false rather than erroring — capabilities
// detection has nothing to probe, and that is a setup prompt, not a failure.
func TestHandleForensicsCapabilities_NoSourceConfigured(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/forensics/capabilities", nil)
	s.handleForensicsCapabilities(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var resp forensicsCapabilitiesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.SourceConfigured {
		t.Error("SourceConfigured should be false: the boot bundle has no registry SourceDSN")
	}
	if resp.SetupGuide != nil {
		t.Error("SetupGuide should be nil when no source is configured")
	}
}

// TestHandleForensicsUsers_NoSourceConfigured mirrors the capabilities
// degradation: an empty list, never an error, so a filter dropdown can still
// render (just empty) for a server with no source connection.
func TestHandleForensicsUsers_NoSourceConfigured(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/forensics/users", nil)
	s.handleForensicsUsers(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var resp forensicsUsersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Users == nil || len(resp.Users) != 0 {
		t.Errorf("Users = %v, want an empty (non-nil) slice", resp.Users)
	}
}

// TestHandleForensicsWhoChanged_RequiresSchemaTable guards the same
// precondition as the CLI and the forensics library itself.
func TestHandleForensicsWhoChanged_RequiresSchemaTable(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/forensics/who-changed", strings.NewReader(`{"schema":"app"}`))
	s.handleForensicsWhoChanged(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

// TestHandleForensicsWhoChanged_DegradesWithoutSource drives the handler
// against a real (mocked) index with no source connection configured: the
// binlog-only tier still answers — degradation is a result, not an error.
func TestHandleForensicsWhoChanged_DegradesWithoutSource(t *testing.T) {
	db, mock, closeFn := newSQLMock(t)
	defer closeFn()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
	}
	rows := sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		nil, nil, "app", "users", int64(parser.EventUpdate), "7",
		[]byte(`["email"]`), []byte(`{"email":"a@x"}`), []byte(`{"email":"b@x"}`), int64(0),
		nil, nil,
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

	s := newBootServer(db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/forensics/who-changed",
		strings.NewReader(`{"schema":"app","table":"users"}`))
	s.handleForensicsWhoChanged(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var res forensics.WhoChangedResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Events) != 1 {
		t.Fatalf("events = %d, want 1: %s", len(res.Events), rec.Body.String())
	}
	if res.Events[0].Attribution != nil {
		t.Errorf("with no source and no connection_id, attribution should be nil, got %+v", res.Events[0].Attribution)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestHandleForensicsActivity_BadQueryType and TestHandleForensicsActivity_NoSourceConfigured
// cover the request-shape and precondition guards ahead of the source-dependent path.
func TestHandleForensicsActivity_BadQueryType(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/forensics/activity", strings.NewReader(`{"query_type":"bogus"}`))
	s.handleForensicsActivity(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestHandleForensicsActivity_UserActivityRequiresUser(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/forensics/activity", strings.NewReader(`{"query_type":"user_activity"}`))
	s.handleForensicsActivity(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestHandleForensicsActivity_ConnectionHistoryRequiresUserOrHost(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/forensics/activity", strings.NewReader(`{"query_type":"connection_history"}`))
	s.handleForensicsActivity(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestHandleForensicsActivity_NoSourceConfigured(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/forensics/activity",
		strings.NewReader(`{"query_type":"ddl_history"}`))
	s.handleForensicsActivity(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "no source connection configured") {
		t.Errorf("expected a no-source-configured message, got: %s", rec.Body.String())
	}
}

// TestCapabilitiesResponse_AdvertisesForensics checks the nav-visibility gate
// this issue adds to the shared /api/capabilities endpoint: on, unless the
// build has forensics disabled or an RBAC profile is active.
func TestCapabilitiesResponse_AdvertisesForensics(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/capabilities", nil)
	s.handleCapabilities(rec, req)
	var resp capabilitiesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Forensics {
		t.Errorf("capabilities.forensics should be true by default (OSS ships forensics enabled): %s", rec.Body.String())
	}

	s.redactCols = append(s.redactCols, query.SchemaTableColumn{Schema: "app", Table: "users", Column: "ssn"})
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/capabilities", nil)
	s.handleCapabilities(rec, req)
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Forensics {
		t.Errorf("capabilities.forensics should be false under an active RBAC profile: %s", rec.Body.String())
	}

	// The other conjunct: with no RBAC profile active, a closed entitlement
	// gate must still zero out the nav-visibility flag — otherwise the nav
	// item would show while every underlying forensics endpoint 403s.
	s2 := newBootServer(nil)
	orig := forensics.Enabled
	forensics.Enabled = func() bool { return false }
	t.Cleanup(func() { forensics.Enabled = orig })
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/capabilities", nil)
	s2.handleCapabilities(rec, req)
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Forensics {
		t.Errorf("capabilities.forensics should be false with the entitlement gate closed: %s", rec.Body.String())
	}
}

// registerDeadSource adds a registry entry whose SourceDSN points at a
// closed local port — config.Connect fails fast (no network hang) without
// needing live infra, exercising "configured but unreachable" the same way
// servers_api_test.go's dead-index-DSN tests do for the index side. DSN
// (the index connection) is deliberately left empty: none of
// capabilities/users/activity ever resolve the index bundle.
func registerDeadSource(t *testing.T, s *Server) ServerEntry {
	t.Helper()
	e, err := s.cm.reg.Add(ServerEntry{Name: "dead-source", SourceDSN: "u:p@tcp(127.0.0.1:1)/db?timeout=200ms"})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// TestForensicsHandlers_SourceConfiguredButUnreachable closes the coverage
// gap the "no source configured" vs "configured but unreachable" distinction
// shipped with: a regression that re-conflates the two (relabeling a real
// outage as "never set up", the exact bug this endpoint design fixed once)
// must fail these tests, not just look right in the source.
func TestForensicsHandlers_SourceConfiguredButUnreachable(t *testing.T) {
	s := newBootServer(nil)
	e := registerDeadSource(t, s)

	t.Run("capabilities", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/forensics/capabilities", nil)
		req.Header.Set(serverHeader, e.ID)
		s.handleForensicsCapabilities(rec, req)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502 (not 200/'not configured'); body = %s", rec.Code, rec.Body.String())
		}
	})
	t.Run("users", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/forensics/users", nil)
		req.Header.Set(serverHeader, e.ID)
		s.handleForensicsUsers(rec, req)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502 (not 200 with an empty user list); body = %s", rec.Code, rec.Body.String())
		}
	})
	t.Run("activity", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/forensics/activity", strings.NewReader(`{"query_type":"user_activity","user":"app_rw"}`))
		req.Header.Set(serverHeader, e.ID)
		s.handleForensicsActivity(rec, req)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502 (not 400 'set the source connection first'); body = %s", rec.Code, rec.Body.String())
		}
	})
}

// Who-changed's own "configured but unreachable" behavior (index-only
// degrade + a Notes entry, never an error) needs a REAL, working index
// alongside a dead source — resolveOr and openForensicsSource both key off
// the same selected server, so a sqlmock index can't be paired with a
// registry source entry the way the other three handlers' tests are above
// (those never touch the index at all). See
// TestIntegrationForensicsWhoChanged_UnreachableSource in
// console_integration_test.go for that case, against real MySQL.

// TestHandleForensicsWhoChanged_LimitClamped proves the clamp end-to-end: an
// over-limit request must reach the index fetch already capped, not just
// satisfy clampLimit in isolation (TestClampLimit in api_test.go tests the
// generic helper, never this handler's actual wiring of it).
func TestHandleForensicsWhoChanged_LimitClamped(t *testing.T) {
	db, mock, closeFn := newSQLMock(t)
	defer closeFn()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
	}
	// The mock only needs to prove which LIMIT value the query carried — an
	// empty result set is fine, WithArgs is what does the real assertion.
	mock.ExpectQuery("FROM binlog_events").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), whoChangedMaxLimit).
		WillReturnRows(sqlmock.NewRows(cols))

	s := newBootServer(db)
	rec := httptest.NewRecorder()
	body := `{"schema":"app","table":"users","limit":99999}`
	s.handleForensicsWhoChanged(rec, httptest.NewRequest("POST", "/api/forensics/who-changed", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("query did not carry the clamped limit (%d): %v", whoChangedMaxLimit, err)
	}
}
