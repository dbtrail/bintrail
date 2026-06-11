package console

import (
	"context"
	"encoding/json"
	"testing"
)

// TestDefaultIDDemoteBoot: under demoteBoot (source-less watch) the no-header
// default prefers a registry entry — first a monitored one, else the first —
// while an empty registry or demoteBoot=false keeps the boot entry as default.
func TestDefaultIDDemoteBoot(t *testing.T) {
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	cm := newConnManager(reg, false)
	cm.boot = &bundle{}
	cm.demoteBoot = true

	if got := cm.defaultID(); got != bootServerID {
		t.Fatalf("empty registry: defaultID = %q, want %q (nothing to demote to)", got, bootServerID)
	}

	viewOnly, err := reg.Add(ServerEntry{Name: "view-only", DSN: "u:p@tcp(h:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	if got := cm.defaultID(); got != viewOnly.ID {
		t.Fatalf("demoted, only view-only entries: defaultID = %q, want %q", got, viewOnly.ID)
	}

	monitored, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx2", SourceDSN: "r:p@tcp(src:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	if got := cm.defaultID(); got != monitored.ID {
		t.Fatalf("demoted: defaultID = %q, want the monitored entry %q", got, monitored.ID)
	}

	cm.demoteBoot = false
	if got := cm.defaultID(); got != bootServerID {
		t.Fatalf("not demoted: defaultID = %q, want %q", got, bootServerID)
	}
}

// TestResolveEmptyFollowsDemotedDefault: Resolve("") must land on the same
// entry defaultID() reports — the switcher renders default_id as selected, so
// resolving "" anywhere else would render one server while querying another.
func TestResolveEmptyFollowsDemotedDefault(t *testing.T) {
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	cm := newConnManager(reg, false)
	cm.boot = &bundle{}
	cm.demoteBoot = true

	entry, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx", SourceDSN: "r:p@tcp(s:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	want := &bundle{}
	cm.bundles[entry.ID] = want // pre-cached so Resolve never dials

	got, err := cm.Resolve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf(`Resolve("") did not follow the demoted default %q`, entry.ID)
	}

	// The boot entry stays selectable by id even while demoted.
	b, err := cm.Resolve(context.Background(), bootServerID)
	if err != nil {
		t.Fatal(err)
	}
	if b != cm.boot {
		t.Fatal("boot entry must stay reachable by its id while demoted")
	}
}

// TestServersAPIDemoteBootWire: the DemoteBoot wiring end to end — Config flag
// → connManager → /api/servers default_id. Guards the one-line
// s.cm.demoteBoot assignment in New(): drop it and source-less watch silently
// lands fresh tabs back on the permanently-empty boot index (the exact bug
// DemoteBoot exists to fix) while the white-box tests above keep passing.
func TestServersAPIDemoteBootWire(t *testing.T) {
	db, _, closeFn := newSQLMock(t)
	defer closeFn()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx", SourceDSN: "r:p@tcp(s:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		DB: db, DBName: "binlog_index", BootDSN: "cli:pw@tcp(127.0.0.1:3306)/binlog_index",
		DemoteBoot: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != 200 {
		t.Fatalf("list code = %d, body = %s", rec.Code, body)
	}
	var resp serversResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.DefaultID != entry.ID {
		t.Fatalf("default_id = %q, want the monitored registry entry %q (DemoteBoot wiring broken?)", resp.DefaultID, entry.ID)
	}
	hasBoot := false
	for _, sv := range resp.Servers {
		if sv.ID == bootServerID {
			hasBoot = true
		}
	}
	if !hasBoot {
		t.Fatal("the boot entry must stay listed while demoted")
	}
}
