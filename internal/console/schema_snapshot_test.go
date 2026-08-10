package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// ─── Refresh schema snapshot (#1296) ──────────────────────────────────────────
//
// The capture-degraded banner named a remedy the UI could not perform; this is
// that remedy. The tests below pin the two things that make it worth having:
// the endpoint reaches a controller (not a hopeful 202), and it refuses the
// cases where taking a snapshot would be wrong or a no-op.

type stubSchemaSnapCtrl struct {
	triggered []SchemaSnapshotRequest
	err       error
	status    SchemaSnapshotStatus
}

func (c *stubSchemaSnapCtrl) Trigger(req SchemaSnapshotRequest) error {
	c.triggered = append(c.triggered, req)
	return c.err
}

func (c *stubSchemaSnapCtrl) Status(string) SchemaSnapshotStatus { return c.status }

func newSchemaSnapshotServer(t *testing.T) (*Server, *stubSchemaSnapCtrl) {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctrl := &stubSchemaSnapCtrl{status: SchemaSnapshotStatus{State: "idle"}}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, SchemaSnapshotCtrl: ctrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, ctrl
}

func addSnapshotEntry(t *testing.T, srv *Server, e ServerEntry) string {
	t.Helper()
	added, err := srv.cm.reg.Add(e)
	if err != nil {
		t.Fatal(err)
	}
	return added.ID
}

func mysqlSnapshotEntry() ServerEntry {
	return ServerEntry{
		Name:      "wp",
		DSN:       "idx:idxpw@tcp(127.0.0.1:3306)/bintrail_wp",
		SourceDSN: "u:p@tcp(127.0.0.1:3306)/",
		Schemas:   "shop, blog",
	}
}

func TestSchemaSnapshotTrigger_reachesTheController(t *testing.T) {
	srv, ctrl := newSchemaSnapshotServer(t)
	id := addSnapshotEntry(t, srv, mysqlSnapshotEntry())
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/schema-snapshot", "")
	if rec.Code != 202 {
		t.Fatalf("trigger: code=%d body=%s", rec.Code, body)
	}
	if len(ctrl.triggered) != 1 {
		t.Fatalf("controller was not asked to take a snapshot: %v", ctrl.triggered)
	}
	req := ctrl.triggered[0]
	if req.ServerID != id || req.SourceDSN != "u:p@tcp(127.0.0.1:3306)/" {
		t.Errorf("request does not carry the entry's source: %+v", req)
	}
	// The snapshot must land in the entry's OWN index database — writing it to
	// another index would leave the stream reloading the snapshot it already had.
	if req.IndexDSN != "idx:idxpw@tcp(127.0.0.1:3306)/bintrail_wp" {
		t.Errorf("request does not carry the entry's index DSN: %+v", req)
	}
	if len(req.Schemas) != 2 || req.Schemas[0] != "shop" || req.Schemas[1] != "blog" {
		t.Errorf("schema filter not carried: %v", req.Schemas)
	}
	// The DSNs are secrets and must never come back over HTTP.
	if strings.Contains(string(body), "idxpw") || strings.Contains(string(body), "u:p@") {
		t.Errorf("response leaked a DSN: %s", body)
	}
}

func TestSchemaSnapshotTrigger_readOnlyConsoleRefuses(t *testing.T) {
	reg, _ := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}})
	if err != nil {
		t.Fatal(err)
	}
	id := addSnapshotEntry(t, srv, mysqlSnapshotEntry())
	for _, method := range []string{"POST", "GET"} {
		rec, _ := doServersReq(t, srv, method, "/api/servers/"+id+"/schema-snapshot", "")
		if rec.Code != 403 {
			t.Errorf("%s with no controller: code=%d, want 403", method, rec.Code)
		}
	}
}

func TestSchemaSnapshotTrigger_requiresSource(t *testing.T) {
	srv, ctrl := newSchemaSnapshotServer(t)
	e := mysqlSnapshotEntry()
	e.SourceDSN = ""
	id := addSnapshotEntry(t, srv, e)
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/schema-snapshot", "")
	if rec.Code != 400 {
		t.Fatalf("no source: code=%d, want 400", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Error("controller must not run without a source to read the layout from")
	}
}

// Without an index database there is nowhere to write the snapshot and no
// stream to reload — a 202 here would be a button that does nothing.
func TestSchemaSnapshotTrigger_requiresIndex(t *testing.T) {
	srv, ctrl := newSchemaSnapshotServer(t)
	e := mysqlSnapshotEntry()
	e.DSN = ""
	id := addSnapshotEntry(t, srv, e)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/schema-snapshot", "")
	if rec.Code != 400 {
		t.Fatalf("no index: code=%d, want 400", rec.Code)
	}
	if !strings.Contains(string(body), "start monitoring") {
		t.Errorf("the refusal must say what to do first: %s", body)
	}
	if len(ctrl.triggered) != 0 {
		t.Error("controller must not run without an index database")
	}
}

// PostgreSQL capture resolves its own column layout; running the MySQL snapshot
// taker against it would query information_schema with MySQL's InnoDB/PK rules.
func TestSchemaSnapshotTrigger_refusesPostgres(t *testing.T) {
	srv, ctrl := newSchemaSnapshotServer(t)
	e := mysqlSnapshotEntry()
	e.SourceDSN = "postgres://u:p@127.0.0.1:5432/app"
	e.Flavor = FlavorPostgres
	id := addSnapshotEntry(t, srv, e)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/schema-snapshot", "")
	if rec.Code != 400 {
		t.Fatalf("postgres: code=%d body=%s, want 400", rec.Code, body)
	}
	if len(ctrl.triggered) != 0 {
		t.Error("a PostgreSQL source must never reach the MySQL snapshot taker")
	}
}

// The command-line entry is streamed by the daemon itself, outside the control
// plane — the monitor verbs already refuse it and so must this.
func TestSchemaSnapshotTrigger_refusesBootEntry(t *testing.T) {
	srv, ctrl := newSchemaSnapshotServer(t)
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+bootServerID+"/schema-snapshot", "")
	if rec.Code != 409 {
		t.Fatalf("boot entry: code=%d, want 409", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Error("the boot entry has no supervised stream to reload")
	}
}

func TestSchemaSnapshotTrigger_alreadyRunningIsConflict(t *testing.T) {
	srv, ctrl := newSchemaSnapshotServer(t)
	ctrl.err = ErrSchemaSnapshotRunning
	id := addSnapshotEntry(t, srv, mysqlSnapshotEntry())
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/schema-snapshot", "")
	if rec.Code != 409 {
		t.Fatalf("already running: code=%d, want 409", rec.Code)
	}
}

// The status endpoint is what the UI polls; it must report the reload outcome,
// because a snapshot whose stream did not reload has fixed nothing yet.
func TestSchemaSnapshotStatus_reportsTheReloadOutcome(t *testing.T) {
	srv, ctrl := newSchemaSnapshotServer(t)
	ctrl.status = SchemaSnapshotStatus{
		State: "succeeded", Tables: 12, SnapshotID: 7,
		StreamReloaded: false, ReloadError: "stream did not stop within 15s",
		ExcludedTables: []string{"shop.audit_raw"},
	}
	id := addSnapshotEntry(t, srv, mysqlSnapshotEntry())
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/schema-snapshot", "")
	if rec.Code != 200 {
		t.Fatalf("status: code=%d body=%s", rec.Code, body)
	}
	var got struct {
		SchemaSnapshot SchemaSnapshotStatus `json:"schema_snapshot"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaSnapshot.StreamReloaded {
		t.Error("stream_reloaded must not be reported true when the stream did not reload")
	}
	if got.SchemaSnapshot.ReloadError == "" {
		t.Error("a failed reload must be reported, or a no-op looks like a fix")
	}
	if len(got.SchemaSnapshot.ExcludedTables) != 1 {
		t.Error("tables validation excluded must be reported — they stay uncaptured after this run")
	}
}

// The UI gate: only a console with the control plane advertises the action.
func TestCapabilitySchemaSnapshotTriggerGate(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		reg, _ := LoadRegistry("")
		cfg := Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg}
		if enabled {
			cfg.SchemaSnapshotCtrl = &stubSchemaSnapCtrl{}
		}
		srv, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		srv.cm.boot = &bundle{}
		rec, body := doServersReq(t, srv, "GET", "/api/capabilities", "")
		if rec.Code != 200 {
			t.Fatalf("capabilities: code=%d body=%s", rec.Code, body)
		}
		var caps capabilitiesResponse
		if err := json.Unmarshal(body, &caps); err != nil {
			t.Fatal(err)
		}
		if caps.SchemaSnapshotTrigger != enabled {
			t.Errorf("schema_snapshot_trigger capability = %v, want %v", caps.SchemaSnapshotTrigger, enabled)
		}
	}
}
