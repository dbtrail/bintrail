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

// TestRowsContainCascadeTriggerOn locks the auto-cascade gate's precondition,
// which is matched PER REFERENTIAL ACTION (#1002): a DELETE undo routes to
// synthesis only on an ON DELETE cascade parent, an UPDATE undo only on an
// ON UPDATE one, and an INSERT undo never routes at all. Crossing the two would
// surface a misleading "0 victims" and teach the operator the signal is noise.
func TestRowsContainCascadeTriggerOn(t *testing.T) {
	ins := query.ResultRow{TableName: "orders", EventType: event.EventInsert}
	upd := query.ResultRow{TableName: "orders", EventType: event.EventUpdate}
	del := query.ResultRow{TableName: "orders", EventType: event.EventDelete}

	cases := []struct {
		name               string
		rows               []query.ResultRow
		table              string
		onDelete, onUpdate bool
		want               bool
	}{
		{"insert only never cascades", []query.ResultRow{ins}, "orders", true, true, false},
		{"delete on an ON DELETE parent", []query.ResultRow{ins, del}, "orders", true, false, true},
		{"delete on an ON UPDATE-only parent", []query.ResultRow{ins, del}, "orders", false, true, false},
		{"update on an ON UPDATE parent", []query.ResultRow{ins, upd}, "orders", false, true, true},
		{"update on an ON DELETE-only parent", []query.ResultRow{ins, upd}, "orders", true, false, false},
		{"other table", []query.ResultRow{del, upd}, "customers", true, true, false},
		{"not a cascade parent at all", []query.ResultRow{del, upd}, "orders", false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := rowsContainCascadeTriggerOn(c.rows, c.table, c.onDelete, c.onUpdate); got != c.want {
				t.Errorf("rowsContainCascadeTriggerOn = %v, want %v", got, c.want)
			}
		})
	}
}

// TestCascadeRootsOnTable pins that the auto-detect path derives its parent set
// from baseRows (never a re-fetch, #772) and now carries UPDATEs alongside
// DELETEs — an INSERT can never be a cascade root.
func TestCascadeRootsOnTable(t *testing.T) {
	rows := []query.ResultRow{
		{TableName: "orders", EventType: event.EventInsert, PKValues: "1"},
		{TableName: "orders", EventType: event.EventUpdate, PKValues: "2"},
		{TableName: "orders", EventType: event.EventDelete, PKValues: "3"},
		{TableName: "customers", EventType: event.EventDelete, PKValues: "4"},
	}
	got := cascadeRootsOnTable(rows, "orders")
	if len(got) != 2 {
		t.Fatalf("want 2 roots (the UPDATE and the DELETE on orders), got %d: %+v", len(got), got)
	}
	if got[0].PKValues != "2" || got[1].PKValues != "3" {
		t.Errorf("want roots pk 2 (UPDATE) and 3 (DELETE), got %q and %q", got[0].PKValues, got[1].PKValues)
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
