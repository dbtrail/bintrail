package console

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/dbtrail/bintrail/internal/query"
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

	if s := mk(Config{BaselineDir: "/tmp/b"}); !s.baselineConfigured || s.baselineSrc != "/tmp/b" {
		t.Errorf("baseline dir: configured=%v src=%q, want true /tmp/b", s.baselineConfigured, s.baselineSrc)
	}
	if s := mk(Config{BaselineS3: "s3://x/"}); !s.baselineConfigured || s.baselineSrc != "s3://x/" {
		t.Errorf("baseline s3: configured=%v src=%q, want true s3://x/", s.baselineConfigured, s.baselineSrc)
	}
	// Dir takes precedence over S3.
	if s := mk(Config{BaselineDir: "/tmp/b", BaselineS3: "s3://x/"}); s.baselineSrc != "/tmp/b" {
		t.Errorf("dir should win over s3: src=%q", s.baselineSrc)
	}
	// No baseline → disabled.
	if s := mk(Config{}); s.baselineConfigured {
		t.Error("no baseline → reconstruct must be disabled")
	}
	// Active RBAC profile disables reconstruct even with a baseline (baseline
	// reads bypass redaction).
	if s := mk(Config{BaselineDir: "/tmp/b", RedactColumns: []query.SchemaTableColumn{{Schema: "a", Table: "b", Column: "c"}}}); s.baselineConfigured {
		t.Error("active profile must disable reconstruct")
	}
	if s := mk(Config{BaselineDir: "/tmp/b", DenyTables: []query.SchemaTable{{Schema: "a", Table: "b"}}}); s.baselineConfigured {
		t.Error("deny rules must disable reconstruct")
	}
	// --no-archive disables reconstruct: without archive access the planner can't
	// verify coverage of rotated-out hours, so AllowGaps=false can't fail loud.
	if s := mk(Config{BaselineDir: "/tmp/b", NoArchive: true}); s.baselineConfigured {
		t.Error("--no-archive must disable reconstruct (planner cannot verify archived-hour coverage)")
	}
}

func TestHandleCapabilities(t *testing.T) {
	for _, configured := range []bool{true, false} {
		s := &Server{baselineConfigured: configured}
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
	s := &Server{baselineConfigured: false}
	rec := httptest.NewRecorder()
	s.handleReconstruct(rec, httptest.NewRequest("GET", "/api/reconstruct?schema=app&table=users&pk=1", nil))
	if rec.Code != 404 {
		t.Errorf("gated-off reconstruct: code=%d, want 404", rec.Code)
	}
}

func TestHandleReconstructRequiresParams(t *testing.T) {
	s := &Server{baselineConfigured: true, baselineSrc: "/tmp/b"}
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
	s := &Server{}
	if _, err := s.pkColumns("app", "users"); err == nil {
		t.Error("nil resolver must produce a clear error, not a panic")
	}
}
