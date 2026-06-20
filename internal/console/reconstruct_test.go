package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
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

// TestHandleCapabilitiesMonitorSurvivesUnresolvableServer: Monitor/Auth are
// process-level and must be reported even when the SELECTED server can't be
// resolved (e.g. a monitored source whose per-source index isn't provisioned
// yet). A 502 here would make the frontend's gateCapabilities degrade to {},
// hiding the whole control plane (Start button, "+ Add server" monitor copy).
func TestHandleCapabilitiesMonitorSurvivesUnresolvableServer(t *testing.T) {
	srv, _ := newSupervisorServer(t) // MonitorCtrl set, empty registry
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/capabilities", nil)
	req.Header.Set(serverHeader, "ffffffffffffffff") // no such server → resolve fails
	srv.handleCapabilities(rec, req)
	if rec.Code != 200 {
		t.Fatalf("capabilities must still 200 for an unresolvable selection, got %d (body=%s)", rec.Code, rec.Body)
	}
	var resp capabilitiesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Monitor {
		t.Error("Monitor is process-level — must survive a broken server selection")
	}
	if resp.Reconstruct {
		t.Error("Reconstruct needs a resolvable bundle — must be false here")
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

// TestAppendStaleWarning pins the console-side surfacing of a stale-baseline
// fallback (#466): a non-stale result leaves Warnings untouched; a stale one
// appends a "stale_baseline:" entry carrying the message.
func TestAppendStaleWarning(t *testing.T) {
	if got := appendStaleWarning(nil, reconstruct.StaleWarning{}); got != nil {
		t.Errorf("non-stale must not add a warning, got %v", got)
	}
	base := []string{"gap warning"}
	got := appendStaleWarning(base, reconstruct.StaleWarning{Message: "table absent from newest"})
	if len(got) != 2 || got[0] != "gap warning" || !strings.HasPrefix(got[1], "stale_baseline: ") {
		t.Errorf("stale warning not appended correctly: %v", got)
	}
	if !strings.Contains(got[1], "table absent from newest") {
		t.Errorf("stale warning lost its message: %q", got[1])
	}
}

// TestStaleBaselineReachesWarningsDTO drives a REAL stale fallback end-to-end on
// the surfacing path: a local baseline source where the table is absent from the
// newest snapshot makes reconstruct.FindBaseline return a populated StaleWarning,
// which the console's appendStaleWarning lands in the reconstructResponse the
// Time-travel UI renders (#466). Uses a local source — findBaselineLocal and
// findBaselineS3 produce the identical StaleWarning, and handleReconstruct is
// source-agnostic, so this exercises the same plumbing the S3 path feeds.
func TestStaleBaselineReachesWarningsDTO(t *testing.T) {
	dir := t.TempDir()
	// Newest snapshot lacks "orders"; an older one has it → stale fallback.
	mkEmpty := func(parts ...string) {
		p := filepath.Join(append([]string{dir}, parts...)...)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mkEmpty("2026-01-01T00-00-00Z", "shop", "orders.parquet")
	mkEmpty("2026-02-01T00-00-00Z", "shop", "users.parquet") // newest, no orders

	at := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	_, snapTime, stale, err := reconstruct.FindBaseline(context.Background(), dir, "shop", "orders", at)
	if err != nil {
		t.Fatalf("FindBaseline: %v", err)
	}
	if !stale.Stale() {
		t.Fatal("expected a stale fallback (orders absent from the newest snapshot)")
	}

	// Build the response exactly as handleReconstruct does for the warnings.
	resp := reconstructResponse{
		Schema: "shop", Table: "orders", PK: "1",
		At:           at.Format(consoleTSFormat),
		BaselineTime: snapTime.Format(consoleTSFormat),
		Warnings:     appendStaleWarning(nil, stale),
	}
	if len(resp.Warnings) != 1 || !strings.HasPrefix(resp.Warnings[0], "stale_baseline: ") {
		t.Fatalf("Warnings DTO missing the stale_baseline entry: %v", resp.Warnings)
	}

	// And it survives JSON encoding (what the UI receives).
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "stale_baseline") {
		t.Fatalf("encoded response lacks stale_baseline: %s", b)
	}
}
