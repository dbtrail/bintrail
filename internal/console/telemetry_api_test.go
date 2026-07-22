package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

type fakeTelemetry struct {
	enabled  bool
	decision telemetry.Decision
	setCalls []bool
}

func (f *fakeTelemetry) Enabled() bool                  { return f.enabled }
func (f *fakeTelemetry) Decision() telemetry.Decision   { return f.decision }
func (f *fakeTelemetry) SetRuntimeConsent(enabled bool) { f.enabled = enabled; f.setCalls = append(f.setCalls, enabled) }

func decodeTelemetry(t *testing.T, rec *httptest.ResponseRecorder) telemetryStateDTO {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var dto telemetryStateDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return dto
}

// GET prefers the live client's own decision — the truth for a running daemon.
func TestHandleTelemetryGetUsesLiveClient(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ft := &fakeTelemetry{enabled: true, decision: telemetry.Decision{Enabled: true, Source: telemetry.SourceDefault}}
	s := &Server{telemetry: ft}
	rec := httptest.NewRecorder()
	s.handleTelemetryGet(rec, httptest.NewRequest("GET", "/api/telemetry", nil))
	dto := decodeTelemetry(t, rec)
	if !dto.Reporting || !dto.Consent {
		t.Errorf("expected reporting+consent from the live client: %+v", dto)
	}
	if dto.Overridden {
		t.Errorf("default source must not read as overridden: %+v", dto)
	}
}

// Turning it off must persist to the machine consent file AND flip the live
// daemon immediately — the whole point of the UI opt-out.
func TestHandleTelemetrySetOffPersistsAndStopsLiveClient(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")
	ft := &fakeTelemetry{enabled: true}
	s := &Server{telemetry: ft}
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":false}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if len(ft.setCalls) != 1 || ft.setCalls[0] != false {
		t.Errorf("live client not disabled: %v", ft.setCalls)
	}
	dir, _ := telemetry.ConfigDir()
	if d := telemetry.Resolve("", dir); d.Enabled || d.Source != telemetry.SourceConfig {
		t.Errorf("choice not persisted off: %+v", d)
	}
}

func TestHandleTelemetrySetOnPersists(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")
	ft := &fakeTelemetry{enabled: false}
	s := &Server{telemetry: ft}
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":true}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if len(ft.setCalls) != 1 || ft.setCalls[0] != true {
		t.Errorf("live client not enabled: %v", ft.setCalls)
	}
	dir, _ := telemetry.ConfigDir()
	if d := telemetry.Resolve("", dir); !d.Enabled {
		t.Errorf("choice not persisted on: %+v", d)
	}
}

// With no live client wired (the read-only console), the toggle still persists
// the machine-wide choice and never panics.
func TestHandleTelemetrySetWithoutLiveClient(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "")
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":false}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	dir, _ := telemetry.ConfigDir()
	if telemetry.Resolve("", dir).Enabled {
		t.Error("expected persisted off")
	}
}

// A write under a higher-precedence control must be refused server-side (409),
// never persisted, so a hand-crafted request can't flip the daemon past the
// DO_NOT_TRACK / flag / env floor.
func TestHandleTelemetrySetRefusesWhenOverridden(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "1")
	s := &Server{} // no live client → Resolve sees DO_NOT_TRACK → overridden
	rec := httptest.NewRecorder()
	s.handleTelemetrySet(rec, httptest.NewRequest("POST", "/api/telemetry", strings.NewReader(`{"enabled":true}`)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409 Conflict", rec.Code)
	}
	dir, _ := telemetry.ConfigDir()
	if telemetry.Resolve("", dir).Source == telemetry.SourceConfig {
		t.Error("wrote the config file despite a higher-precedence override")
	}
}

// An env override must be reported so the UI can disable the toggle and explain.
func TestHandleTelemetryGetOverriddenByEnv(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "1")
	s := &Server{} // no live client → Resolve path exercises the env precedence
	rec := httptest.NewRecorder()
	s.handleTelemetryGet(rec, httptest.NewRequest("GET", "/api/telemetry", nil))
	dto := decodeTelemetry(t, rec)
	if !dto.Overridden || dto.Consent {
		t.Errorf("expected overridden + off under DO_NOT_TRACK: %+v", dto)
	}
	if dto.DecidedBy != string(telemetry.SourceDoNotTrack) {
		t.Errorf("decided_by = %q, want DO_NOT_TRACK", dto.DecidedBy)
	}
}
