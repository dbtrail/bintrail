package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// reqWithPolicy builds a request carrying pol, as tokenMiddleware would after a
// scoped session authenticates.
func reqWithPolicy(pol *ext.AccessPolicy) *http.Request {
	r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/x", nil)
	return r.WithContext(context.WithValue(r.Context(), policyCtxKey{}, pol))
}

func TestSessionRestricted(t *testing.T) {
	cases := []struct {
		name string
		pol  *ext.AccessPolicy
		want bool
	}{
		{"no policy (OSS)", nil, false},
		{"policy, no profile", &ext.AccessPolicy{Permissions: ext.AllPermissions()}, false},
		{"policy with profile", &ext.AccessPolicy{Profile: "sensitive"}, true},
		{"policy with restrictions (#1449)", &ext.AccessPolicy{
			Restrictions: &ext.SessionRestrictions{DenyTables: []ext.TableRef{{Schema: "s", Table: "t"}}},
		}, true},
		{"policy with EMPTY restrictions struct", &ext.AccessPolicy{
			Restrictions: &ext.SessionRestrictions{},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sessionRestricted(reqWithPolicy(tc.pol)); got != tc.want {
				t.Errorf("sessionRestricted = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRbacActiveForFoldsSession pins that the per-request helpers equal the
// process-global signals in OSS (no session profile) and go true for a profiled
// session even with no startup rules.
func TestRbacActiveForFoldsSession(t *testing.T) {
	s := &Server{} // no startup deny/redact rules, profileActive false
	// OSS: no policy → identical to process-global (false).
	if s.rbacActiveFor(reqWithPolicy(nil)) || s.profileActiveFor(reqWithPolicy(nil)) {
		t.Error("with no startup rules and no session profile, both must be false (OSS unchanged)")
	}
	// Session profile → both true.
	profiled := reqWithPolicy(&ext.AccessPolicy{Profile: "sensitive"})
	if !s.rbacActiveFor(profiled) || !s.profileActiveFor(profiled) {
		t.Error("a session profile must make rbacActiveFor and profileActiveFor true")
	}
	// A permissioned session WITHOUT a profile does not restrict data.
	roleOnly := reqWithPolicy(&ext.AccessPolicy{Permissions: ext.AllPermissions()})
	if s.rbacActiveFor(roleOnly) || s.profileActiveFor(roleOnly) {
		t.Error("a role-only session (no data profile) must not restrict data")
	}
}

// TestApplySessionProfileNoOp pins that a session with no data profile leaves
// query.Options byte-for-byte as buildOptions produced them (the OSS path).
func TestApplySessionProfileNoOp(t *testing.T) {
	s := &Server{sessionProfiles: newProfileRuleCache()}
	in := query.Options{DenyTables: []query.SchemaTable{{Schema: "a", Table: "b"}}, ProfileActive: false}
	out, err := s.applySessionProfile(context.Background(), reqWithPolicy(nil), &bundle{}, in)
	if err != nil {
		t.Fatalf("applySessionProfile (no profile) err = %v", err)
	}
	if out.ProfileActive != false || len(out.DenyTables) != 1 {
		t.Errorf("no-profile session must leave opts unchanged, got %+v", out)
	}
}

// TestApplySessionPolicyRestrictions pins the #1449 path: a session whose
// policy carries direct restrictions (no profile name) gets them unioned onto
// the startup floor — converted to query types, allow lists included — with
// ProfileActive forced so query_text/query_hash stay withheld. No profile
// name means no index round trip: the bundle carries no DB here, so reaching
// for one would panic this test.
func TestApplySessionPolicyRestrictions(t *testing.T) {
	s := &Server{sessionProfiles: newProfileRuleCache()}
	pol := &ext.AccessPolicy{Restrictions: &ext.SessionRestrictions{
		DenyTables:    []ext.TableRef{{Schema: "hr", Table: "payroll"}},
		RedactColumns: []ext.TableColumnRef{{Schema: "hr", Table: "people", Column: "ssn"}},
		AllowTables:   []ext.TableRef{{Schema: "shop", Table: "orders"}},
		AllowColumns:  []ext.TableColumnRef{{Schema: "shop", Table: "orders", Column: "id"}},
	}}
	in := query.Options{DenyTables: []query.SchemaTable{{Schema: "floor", Table: "f"}}}
	out, err := s.applySessionProfile(context.Background(), reqWithPolicy(pol), &bundle{}, in)
	if err != nil {
		t.Fatalf("applySessionProfile err = %v", err)
	}
	if !out.ProfileActive {
		t.Error("restrictions must force ProfileActive (query_text/query_hash withholding)")
	}
	wantDeny := []query.SchemaTable{{Schema: "floor", Table: "f"}, {Schema: "hr", Table: "payroll"}}
	if len(out.DenyTables) != 2 || out.DenyTables[0] != wantDeny[0] || out.DenyTables[1] != wantDeny[1] {
		t.Errorf("DenyTables = %v, want startup floor + policy deny (%v)", out.DenyTables, wantDeny)
	}
	if len(out.RedactColumns) != 1 || out.RedactColumns[0] != (query.SchemaTableColumn{Schema: "hr", Table: "people", Column: "ssn"}) {
		t.Errorf("RedactColumns = %v, want the policy's converted entry", out.RedactColumns)
	}
	if len(out.AllowTables) != 1 || out.AllowTables[0] != (query.SchemaTable{Schema: "shop", Table: "orders"}) {
		t.Errorf("AllowTables = %v, want the policy's converted entry", out.AllowTables)
	}
	if len(out.AllowColumns) != 1 || out.AllowColumns[0] != (query.SchemaTableColumn{Schema: "shop", Table: "orders", Column: "id"}) {
		t.Errorf("AllowColumns = %v, want the policy's converted entry", out.AllowColumns)
	}
	// The startup floor slice the caller handed in must not have been mutated.
	if len(in.DenyTables) != 1 || in.DenyTables[0] != (query.SchemaTable{Schema: "floor", Table: "f"}) {
		t.Errorf("caller's options were mutated: %v", in.DenyTables)
	}
}

func TestWriteSessionProfileError(t *testing.T) {
	// A nonexistent profile → 403.
	rec := httptest.NewRecorder()
	writeSessionProfileError(rec, httptest.NewRequest("GET", "/api/events", nil), &profileNotFoundError{"ghost"})
	if rec.Code != http.StatusForbidden {
		t.Errorf("profileNotFoundError → %d, want 403", rec.Code)
	}
	// Any other error → 500.
	rec = httptest.NewRecorder()
	writeSessionProfileError(rec, httptest.NewRequest("GET", "/api/events", nil), errors.New("db down"))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("generic error → %d, want 500", rec.Code)
	}
}

// TestProfileRuleCacheInvalidate pins that invalidate purges exactly the target
// server's entries (so a DSN edit re-resolves against the new index), leaves
// other servers' entries, and is nil-safe.
func TestProfileRuleCacheInvalidate(t *testing.T) {
	c := newProfileRuleCache()
	c.m["srv1\x00p"] = profileRuleEntry{exists: true}
	c.m["srv1\x00q"] = profileRuleEntry{exists: true}
	c.m["srv2\x00p"] = profileRuleEntry{exists: true}
	c.invalidate("srv1")
	if _, ok := c.m["srv1\x00p"]; ok {
		t.Error("srv1 profile p not purged")
	}
	if _, ok := c.m["srv1\x00q"]; ok {
		t.Error("srv1 profile q not purged")
	}
	if _, ok := c.m["srv2\x00p"]; !ok {
		t.Error("srv2 entry must survive invalidate(srv1)")
	}
	var nilC *profileRuleCache
	nilC.invalidate("x") // must not panic
}

// scopedServer builds a token server and returns a bearer for a session that
// holds all permissions (so route-tier authz never masks the data-profile gate)
// plus the given data profile.
func scopedServer(t *testing.T, profile string) (*Server, string) {
	t.Helper()
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	tok, _, err := srv.sessions.IssueWithPolicy("test-user", &ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	return srv, tok
}

// TestRecoverCascadeRefusedForSessionProfile pins the raw-data gate that fires
// before any DB access: a profiled session is 403'd on recover-cascade.
func TestRecoverCascadeRefusedForSessionProfile(t *testing.T) {
	srv, tok := scopedServer(t, "sensitive")
	rec := postJSON(t, srv, "/api/recover-cascade", tok, `{"schema":"app","table":"orders","pk":"1"}`)
	if rec.Code != http.StatusForbidden {
		t.Errorf("profiled session POST /api/recover-cascade = %d, want 403", rec.Code)
	}
}

// TestCapabilitiesFalseForSessionProfile pins that the reconstruct/cascade
// capabilities go false for a profiled session, so the SPA hides those surfaces.
func TestCapabilitiesFalseForSessionProfile(t *testing.T) {
	srv, tok := scopedServer(t, "sensitive")
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/capabilities", tok)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/capabilities = %d", rec.Code)
	}
	var caps struct {
		Reconstruct    bool `json:"reconstruct"`
		RecoverCascade bool `json:"recover_cascade"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
		t.Fatal(err)
	}
	if caps.Reconstruct || caps.RecoverCascade {
		t.Errorf("profiled session capabilities: reconstruct=%v recover_cascade=%v, want both false", caps.Reconstruct, caps.RecoverCascade)
	}
}
