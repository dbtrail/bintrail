package console

import (
	"context"
	"encoding/json"
	"testing"
)

// TestDefaultIDHideBoot: under hideBoot (source-less watch) the boot entry is
// never the default — the first monitored registry entry wins, else the first
// entry, else "" (a fresh install reports no default at all) — while
// hideBoot=false keeps the boot entry as default.
func TestDefaultIDHideBoot(t *testing.T) {
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	cm := newConnManager(reg, false)
	cm.boot = &bundle{}
	cm.hideBoot = true

	if got := cm.defaultID(); got != "" {
		t.Fatalf(`fresh install (empty registry): defaultID = %q, want "" (boot is hidden)`, got)
	}

	viewOnly, err := reg.Add(ServerEntry{Name: "view-only", DSN: "u:p@tcp(h:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	if got := cm.defaultID(); got != viewOnly.ID {
		t.Fatalf("hidden boot, only view-only entries: defaultID = %q, want %q", got, viewOnly.ID)
	}

	monitored, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx2", SourceDSN: "r:p@tcp(src:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	if got := cm.defaultID(); got != monitored.ID {
		t.Fatalf("hidden boot: defaultID = %q, want the monitored entry %q", got, monitored.ID)
	}

	cm.hideBoot = false
	if got := cm.defaultID(); got != bootServerID {
		t.Fatalf("boot visible: defaultID = %q, want %q", got, bootServerID)
	}

	// Registry-only `serve` (boot == nil, same registry: [view-only, sourced]):
	// the longstanding first-entry default holds — the sourced preference is a
	// hidden-boot behavior ONLY, and leaking it here would silently change
	// which server header-less tabs query on registries shared with watch.
	cmServe := newConnManager(reg, false)
	if got := cmServe.defaultID(); got != viewOnly.ID {
		t.Fatalf("registry-only serve: defaultID = %q, want the FIRST entry %q (not the sourced %q)", got, viewOnly.ID, monitored.ID)
	}
}

// TestResolveEmptyFollowsHiddenDefault: Resolve("") must land on the same
// entry defaultID() reports — the switcher renders default_id as selected, so
// resolving "" anywhere else would render one server while querying another.
// With an EMPTY registry it must quietly fall back to the hidden boot bundle
// so a fresh install renders views instead of a 404.
func TestResolveEmptyFollowsHiddenDefault(t *testing.T) {
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	cm := newConnManager(reg, false)
	cm.boot = &bundle{}
	cm.hideBoot = true

	// Fresh install: no registry entries → the hidden boot bundle backs "".
	b, err := cm.Resolve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if b != cm.boot {
		t.Fatal(`fresh install: Resolve("") must fall back to the hidden boot bundle`)
	}

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
		t.Fatalf(`Resolve("") did not follow the registry default %q`, entry.ID)
	}

	// The boot entry stays reachable by its reserved id even while hidden
	// (e.g. an old tab with "default" in sessionStorage mid-upgrade).
	b, err = cm.Resolve(context.Background(), bootServerID)
	if err != nil {
		t.Fatal(err)
	}
	if b != cm.boot {
		t.Fatal("boot entry must stay reachable by its id while hidden")
	}
}

// TestServersAPIHideBootWire: the HideBoot wiring end to end — Config flag →
// connManager → /api/servers. Guards the one-line s.cm.hideBoot assignment in
// New(): drop it and source-less watch shows the internal boot index as a
// phantom server again while the white-box tests above keep passing.
func TestServersAPIHideBootWire(t *testing.T) {
	db, _, closeFn := newSQLMock(t)
	defer closeFn()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		DB: db, DBName: "binlog_index", BootDSN: "cli:pw@tcp(127.0.0.1:3306)/binlog_index",
		HideBoot: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Fresh install: NO servers listed, no default — but header-less requests
	// still resolve (capabilities hits the hidden boot bundle, no DB queries).
	rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != 200 {
		t.Fatalf("list code = %d, body = %s", rec.Code, body)
	}
	var resp serversResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Servers) != 0 || resp.DefaultID != "" {
		t.Fatalf("fresh install: servers = %+v default_id = %q, want an empty list and no default", resp.Servers, resp.DefaultID)
	}
	if rec, body = doServersReq(t, srv, "GET", "/api/capabilities", ""); rec.Code != 200 {
		t.Fatalf("header-less capabilities on a fresh install = %d (%s), want 200 via the hidden boot fallback", rec.Code, body)
	}

	// With a monitored entry added, it becomes the default and the boot entry
	// stays hidden.
	entry, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx", SourceDSN: "r:p@tcp(s:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	_, body = doServersReq(t, srv, "GET", "/api/servers", "")
	resp = serversResponse{}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.DefaultID != entry.ID {
		t.Fatalf("default_id = %q, want the monitored registry entry %q (HideBoot wiring broken?)", resp.DefaultID, entry.ID)
	}
	for _, sv := range resp.Servers {
		if sv.ID == bootServerID {
			t.Fatal("the boot entry must stay hidden under HideBoot")
		}
	}

	// The reserved id stays addressable while hidden (a pre-upgrade tab with
	// "default" in sessionStorage fetches it directly) — hiding is a listing
	// concern, not a 404.
	rec, body = doServersReq(t, srv, "GET", "/api/servers/"+bootServerID, "")
	if rec.Code != 200 {
		t.Fatalf("GET /api/servers/default while hidden = %d (%s), want 200", rec.Code, body)
	}
}
