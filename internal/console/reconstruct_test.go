package console

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

func TestNewBaselineGating(t *testing.T) {
	mk := func(cfg Config) *Server {
		cfg.Listen, cfg.Token = "127.0.0.1:8090", "t"
		s, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return s
	}
	// The gates now live on the boot bundle (per-server); nil boot — no DB and
	// no baseline configured — is equivalently "not configured".
	configured := func(s *Server) bool { return s.cm.boot != nil && s.cm.boot.baselineConfigured }
	src := func(s *Server) string {
		if s.cm.boot == nil {
			return ""
		}
		return s.cm.boot.baselineSrc
	}

	if s := mk(Config{BaselineDir: "/tmp/b"}); !configured(s) || src(s) != "/tmp/b" {
		t.Errorf("baseline dir: configured=%v src=%q, want true /tmp/b", configured(s), src(s))
	}
	if s := mk(Config{BaselineS3: "s3://x/"}); !configured(s) || src(s) != "s3://x/" {
		t.Errorf("baseline s3: configured=%v src=%q, want true s3://x/", configured(s), src(s))
	}
	// Dir takes precedence over S3.
	if s := mk(Config{BaselineDir: "/tmp/b", BaselineS3: "s3://x/"}); src(s) != "/tmp/b" {
		t.Errorf("dir should win over s3: src=%q", src(s))
	}
	// No baseline → disabled.
	if s := mk(Config{}); configured(s) {
		t.Error("no baseline → reconstruct must be disabled")
	}
	// Active RBAC profile disables reconstruct even with a baseline (baseline
	// reads bypass redaction).
	if s := mk(Config{BaselineDir: "/tmp/b", RedactColumns: []query.SchemaTableColumn{{Schema: "a", Table: "b", Column: "c"}}}); configured(s) {
		t.Error("active profile must disable reconstruct")
	}
	if s := mk(Config{BaselineDir: "/tmp/b", DenyTables: []query.SchemaTable{{Schema: "a", Table: "b"}}}); configured(s) {
		t.Error("deny rules must disable reconstruct")
	}
	// --no-archive disables reconstruct: without archive access the planner can't
	// verify coverage of rotated-out hours, so AllowGaps=false can't fail loud.
	if s := mk(Config{BaselineDir: "/tmp/b", NoArchive: true}); configured(s) {
		t.Error("--no-archive must disable reconstruct (planner cannot verify archived-hour coverage)")
	}
}

func TestHandleCapabilities(t *testing.T) {
	for _, configured := range []bool{true, false} {
		s := newBootServer(nil)
		s.cm.boot.baselineConfigured = configured
		rec := httptest.NewRecorder()
		s.handleCapabilities(rec, httptest.NewRequest("GET", "/api/capabilities", nil))
		if rec.Code != 200 {
			t.Fatalf("code=%d", rec.Code)
		}
		var resp capabilitiesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Reconstruct != configured {
			t.Errorf("reconstruct=%v want %v", resp.Reconstruct, configured)
		}
	}
}

// TestHandleReconstructGatedOff: the endpoint is the boundary, not just the UI —
// it must refuse when reconstruct is not configured.
func TestHandleReconstructGatedOff(t *testing.T) {
	s := newBootServer(nil) // baselineConfigured defaults to false
	rec := httptest.NewRecorder()
	s.handleReconstruct(rec, httptest.NewRequest("GET", "/api/reconstruct?schema=app&table=users&pk=1", nil))
	if rec.Code != 404 {
		t.Errorf("gated-off reconstruct: code=%d, want 404", rec.Code)
	}
}

func TestHandleReconstructRequiresParams(t *testing.T) {
	s := newBootServer(nil)
	s.cm.boot.baselineConfigured = true
	s.cm.boot.baselineSrc = "/tmp/b"
	rec := httptest.NewRecorder()
	s.handleReconstruct(rec, httptest.NewRequest("GET", "/api/reconstruct?schema=app", nil))
	if rec.Code != 400 {
		t.Errorf("missing table/pk: code=%d, want 400", rec.Code)
	}
}

func TestBuildPKFilter(t *testing.T) {
	got, err := buildPKFilter([]string{"id"}, "42")
	if err != nil || !reflect.DeepEqual(got, map[string]string{"id": "42"}) {
		t.Errorf("single: got=%v err=%v", got, err)
	}
	got, err = buildPKFilter([]string{"order_id", "item_id"}, "7|3")
	if err != nil || !reflect.DeepEqual(got, map[string]string{"order_id": "7", "item_id": "3"}) {
		t.Errorf("composite: got=%v err=%v", got, err)
	}
	if _, err := buildPKFilter([]string{"a", "b"}, "1"); err == nil {
		t.Error("length mismatch must error")
	}
}

func TestPKColumnsNoResolver(t *testing.T) {
	b := &bundle{}
	if _, err := b.pkColumns("app", "users"); err == nil {
		t.Error("nil resolver must produce a clear error, not a panic")
	}
}
