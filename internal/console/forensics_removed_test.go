package console

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// TestRemovedForensicsRoutesReturn404 pins the retirement of the console
// attribution surface: each removed /api/forensics/* route must be absent
// from the mux entirely (404 from the authenticated /api/ catch-all), not
// merely disabled. The requests carry a valid bearer so the pin is about
// route absence, never about auth.
func TestRemovedForensicsRoutesReturn404(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	s := newBootServer(db)

	routes := []struct{ method, path string }{
		{"GET", "/api/forensics/capabilities"},
		{"GET", "/api/forensics/users"},
		{"POST", "/api/forensics/who-changed"},
		{"POST", "/api/forensics/activity"},
	}
	for _, rt := range routes {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(rt.method, "http://127.0.0.1:8090"+rt.path, nil)
		req.Header.Set("Authorization", "Bearer t")
		s.mux.ServeHTTP(rec, req)
		if rec.Code != 404 {
			t.Errorf("%s %s = %d, want 404 (route was retired)", rt.method, rt.path, rec.Code)
		}
	}
}

// TestCapabilitiesHasNoForensicsKey pins the wire format: the retired
// `forensics` capability flag must not reappear in the /api/capabilities
// JSON — the frontend gates views on the keys present here.
func TestCapabilitiesHasNoForensicsKey(t *testing.T) {
	db, _, closer := newSQLMock(t)
	defer closer()
	s := newBootServer(db)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/capabilities", nil)
	req.Header.Set("Authorization", "Bearer t")
	s.mux.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("capabilities = %d body=%s", rec.Code, rec.Body.String())
	}
	var caps map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
		t.Fatalf("unmarshal capabilities: %v", err)
	}
	if _, ok := caps["forensics"]; ok {
		t.Errorf("capabilities JSON still carries a %q key: %s", "forensics", rec.Body.String())
	}
}
