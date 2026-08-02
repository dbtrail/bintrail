package console

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// TestVerifyHistoryEndpoint_disabledWithoutStore: a console with the verify
// controller but no history store (or neither) refuses like the other verify
// verbs — 403, not an empty 200 that reads as "no runs ever".
func TestVerifyHistoryEndpoint_disabledWithoutStore(t *testing.T) {
	srv, _ := newVerifyTriggerServer(t)
	id := addVerifyEntry(t, srv, "", "s3://b/base", "")
	rec, _ := doServersReqHeader(t, srv, "GET", "/api/servers/"+id+"/verify/history", "", id)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("history without a store: got %d, want 403", rec.Code)
	}
}

func TestVerifyHistoryEndpoint_servesRecordsNewestFirst(t *testing.T) {
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	hist, err := OpenVerifyHistory(filepath.Join(t.TempDir(), "console-verify-history.json"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: &stubVerifyCtrl{}, VerifyHistory: hist,
	})
	if err != nil {
		t.Fatal(err)
	}
	id := addVerifyEntry(t, srv, "", "s3://b/base", "")
	for _, state := range []string{"succeeded", "failed"} {
		if err := hist.Append(VerifyRunRecord{ServerID: id, Trigger: "scheduled", VerifyStatus: VerifyStatus{State: state}}); err != nil {
			t.Fatal(err)
		}
	}

	rec, body := doServersReqHeader(t, srv, "GET", "/api/servers/"+id+"/verify/history", "", id)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rec.Code, body)
	}
	var resp struct {
		History []VerifyRunRecord `json:"history"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.History) != 2 || resp.History[0].State != "failed" || resp.History[1].State != "succeeded" {
		t.Fatalf("want newest-first [failed succeeded], got %+v", resp.History)
	}

	// Unknown server id: 404, mirroring the other verify verbs.
	rec, _ = doServersReqHeader(t, srv, "GET", "/api/servers/nope/verify/history", "", id)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown server: got %d, want 404", rec.Code)
	}
}

// TestVerifyHistoryEndpoint_rbacBlocked: history carries the same per-table
// verdicts as the live status and spans restarts — unavailable under an
// active RBAC profile like the other verify verbs.
func TestVerifyHistoryEndpoint_rbacBlocked(t *testing.T) {
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	hist, err := OpenVerifyHistory(filepath.Join(t.TempDir(), "console-verify-history.json"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: &stubVerifyCtrl{}, VerifyHistory: hist,
		DenyTables: []query.SchemaTable{{Schema: "a", Table: "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := addVerifyEntry(t, srv, "", "s3://b/base", "")
	rec, _ := doServersReqHeader(t, srv, "GET", "/api/servers/"+id+"/verify/history", "", id)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("history under an active RBAC profile: got %d, want 403", rec.Code)
	}
}

// TestVerifyTrigger_recoverInputsNeedsNoBaseline: the recover-inputs check is
// index-only (the console face of `verify --check recover`), so — unlike the
// content modes — an entry with no baseline location must be accepted.
func TestVerifyTrigger_recoverInputsNeedsNoBaseline(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	id := addVerifyEntry(t, srv, "", "", "")

	rec, body := doServersReqHeader(t, srv, "POST", "/api/servers/"+id+"/verify", `{"mode":"recover-inputs"}`, id)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("recover-inputs without baseline: got %d (%s), want 202", rec.Code, body)
	}
	if len(ctrl.triggered) != 1 || ctrl.triggered[0].Mode != VerifyModeRecoverInputs {
		t.Fatalf("controller saw %+v, want one recover-inputs request", ctrl.triggered)
	}

	// The content default still requires a baseline location.
	rec, _ = doServersReqHeader(t, srv, "POST", "/api/servers/"+id+"/verify", `{"mode":"baseline-anchored"}`, id)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("baseline-anchored without baseline: got %d, want 400", rec.Code)
	}
}
