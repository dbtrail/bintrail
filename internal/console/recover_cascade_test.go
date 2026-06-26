package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestRowsContainDeleteOn locks the auto-cascade gate's delete precondition: a
// recover only routes to cascade synthesis when the matched rows actually contain
// a DELETE on the target table (an INSERT/UPDATE undo never cascades).
func TestRowsContainDeleteOn(t *testing.T) {
	rows := []query.ResultRow{
		{TableName: "orders", EventType: event.EventInsert},
		{TableName: "orders", EventType: event.EventUpdate},
	}
	if rowsContainDeleteOn(rows, "orders") {
		t.Error("no DELETE present → want false")
	}
	rows = append(rows, query.ResultRow{TableName: "orders", EventType: event.EventDelete})
	if !rowsContainDeleteOn(rows, "orders") {
		t.Error("a DELETE on orders → want true")
	}
	if rowsContainDeleteOn(rows, "customers") {
		t.Error("the DELETE is on orders, not customers → want false")
	}
}

// TestRecoverCascade_validation covers the request-validation branches that
// reject before any DB work — so a nil-DB boot server is enough.
func TestRecoverCascade_validation(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"missing schema+table", `{}`, http.StatusBadRequest},
		{"missing table", `{"schema":"app"}`, http.StatusBadRequest},
		{"pk and pks together", `{"schema":"app","table":"parent","pk":"1","pks":["2"]}`, http.StatusBadRequest},
		{"max_depth negative", `{"schema":"app","table":"parent","max_depth":-1}`, http.StatusBadRequest},
		{"bad lookback", `{"schema":"app","table":"parent","lookback":"nope"}`, http.StatusBadRequest},
		{"bad since", `{"schema":"app","table":"parent","since":"not-a-time"}`, http.StatusBadRequest},
		{"invalid JSON", `{bad`, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newBootServer(nil)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/api/recover-cascade", strings.NewReader(c.body))
			s.handleRecoverCascade(rec, req)
			if rec.Code != c.want {
				t.Errorf("code = %d, want %d (body=%s)", rec.Code, c.want, rec.Body.String())
			}
		})
	}
}

// TestRecoverCascade_refusedUnderRBAC locks the leak guard: with a redact/deny
// profile active the endpoint refuses BEFORE touching the DB (the bundle's db is
// nil here — a 403, not a panic, proves the guard runs first), because cascade
// victim synthesis cannot honor redaction.
func TestRecoverCascade_refusedUnderRBAC(t *testing.T) {
	s := newBootServer(nil)
	s.redactCols = []query.SchemaTableColumn{{Schema: "app", Table: "child", Column: "ssn"}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover-cascade", strings.NewReader(`{"schema":"app","table":"parent"}`))
	s.handleRecoverCascade(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("code = %d, want 403 (body=%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "RBAC") {
		t.Errorf("403 body should explain the RBAC refusal, got: %s", rec.Body.String())
	}
}

// TestRecoverCascadeCapability exercises the gate the frontend reads: recover_cascade
// is true by default (free tier), false under an RBAC profile, and
// recover_cascade_baseline mirrors the per-server baseline configuration.
func TestRecoverCascadeCapability(t *testing.T) {
	get := func(s *Server) capabilitiesResponse {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/capabilities", nil)
		s.handleCapabilities(rec, req)
		var resp capabilitiesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode capabilities: %v (body=%s)", err, rec.Body.String())
		}
		return resp
	}

	t.Run("default available, no baseline", func(t *testing.T) {
		caps := get(newBootServer(nil))
		if !caps.RecoverCascade {
			t.Error("recover_cascade should be true by default (free tier)")
		}
		if caps.RecoverCascadeBaseline {
			t.Error("recover_cascade_baseline should be false without a baseline")
		}
	})

	t.Run("false under RBAC profile", func(t *testing.T) {
		s := newBootServer(nil)
		s.redactCols = []query.SchemaTableColumn{{Schema: "app", Table: "child", Column: "ssn"}}
		if get(s).RecoverCascade {
			t.Error("recover_cascade must be false while a redaction profile is active")
		}
	})

	t.Run("baseline sub-flag true with baseline + resolver", func(t *testing.T) {
		s := newBootServer(nil)
		s.cm.boot.baselineConfigured = true
		s.cm.boot.resolver = metadata.NewResolverFromTables(1, nil)
		if !get(s).RecoverCascadeBaseline {
			t.Error("recover_cascade_baseline should be true when baseline + resolver are present")
		}
	})

	t.Run("baseline sub-flag false when resolver nil (no snapshot)", func(t *testing.T) {
		// baselineConfigured can be true with resolver==nil (baseline dir set but
		// `bintrail snapshot` never run); the handler degrades to Phase-1 there, so
		// the capability must NOT over-advertise Phase-2 — it gates on resolver too.
		s := newBootServer(nil)
		s.cm.boot.baselineConfigured = true // resolver stays nil
		if get(s).RecoverCascadeBaseline {
			t.Error("recover_cascade_baseline must be false without a resolver (would over-advertise Phase-2)")
		}
	})
}
