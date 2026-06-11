package console

import (
	"context"
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
