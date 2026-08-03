package consoleapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func TestWatchNotifier_BaselineStale(t *testing.T) {
	n, f := testNotifier()
	n.BaselineStale("wp", true, "shop.legacy: newest baseline predates coverage")
	n.BaselineStale("wp", true, "shop.legacy: newest baseline predates coverage")
	if len(f.events) != 1 || f.events[0].Event != "baseline_stale" || f.events[0].Severity != "critical" {
		t.Fatalf("want one critical baseline_stale, got %+v", f.events)
	}
	n.BaselineStale("wp", false, "")
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("recovery must resolve once, got %+v", f.events)
	}
	n.BaselineStale("wp", false, "")
	if len(f.events) != 2 {
		t.Fatalf("healthy with no prior alert must stay silent, got %+v", f.events)
	}
}

// TestStalenessWatcher_targets pins the all-or-nothing baseline fallback and
// the skip of servers with no baseline anywhere.
func TestStalenessWatcher_targets(t *testing.T) {
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "own-dir", DSN: "d1", BaselineDir: "/own"},
		console.ServerEntry{Name: "own-s3", DSN: "d2", BaselineS3: "s3://own"},
		console.ServerEntry{Name: "inherits", DSN: "d3"},
	)
	w := &stalenessWatcher{registry: reg, bootDSN: "boot-dsn", globalDir: "/global"}
	got := w.targets()
	if len(got) != 4 {
		t.Fatalf("want boot + 3 entries, got %+v", got)
	}
	bySrc := map[string]string{}
	for _, tg := range got {
		bySrc[tg.name] = tg.source
	}
	if bySrc["cli index"] != "/global" || bySrc["own-dir"] != "/own" || bySrc["own-s3"] != "s3://own" || bySrc["inherits"] != "/global" {
		t.Fatalf("fallback wrong: %+v", bySrc)
	}

	// No global baseline: boot and the inheriting entry drop out.
	w = &stalenessWatcher{registry: reg, bootDSN: "boot-dsn"}
	if got := w.targets(); len(got) != 2 {
		t.Fatalf("without a global source only own-baseline entries remain, got %+v", got)
	}
}

func TestStalenessWatcher_runCycle(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "wp", DSN: "d1", BaselineDir: "/b"})
	now := time.Now().UTC()
	oldest := now.Add(-100 * time.Hour)
	n, f := testNotifier()

	files := []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}, // superseded, broken
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour)},   // newest, fine
	}
	w := &stalenessWatcher{
		n: n, registry: reg,
		listBaselines: func(context.Context, string) ([]reconstruct.BaselineFile, error) { return files, nil },
		oldestDelta:   func(context.Context, string) (time.Time, error) { return oldest, nil },
	}

	// Newest per table is fine: no event.
	w.runCycle(context.Background())
	if len(f.events) != 0 {
		t.Fatalf("healthy newest snapshot must not alert: %+v", f.events)
	}

	// The newest snapshot for a table slides past coverage: critical fires.
	files = append(files, reconstruct.BaselineFile{Schema: "shop", Table: "legacy", SnapshotTime: oldest.Add(-2 * time.Hour)})
	w.runCycle(context.Background())
	if len(f.events) != 1 || f.events[0].Event != "baseline_stale" || f.events[0].Severity != "critical" {
		t.Fatalf("broken newest snapshot must alert: %+v", f.events)
	}

	// Unknown floor: the target is skipped whole — no fire, and crucially NO
	// resolve of the active alert.
	w.oldestDelta = func(context.Context, string) (time.Time, error) { return time.Time{}, errors.New("index down") }
	w.runCycle(context.Background())
	if len(f.events) != 1 {
		t.Fatalf("unknown floor must neither fire nor resolve: %+v", f.events)
	}

	// Fresh baseline lands: the alert resolves once.
	w.oldestDelta = func(context.Context, string) (time.Time, error) { return oldest, nil }
	files = append(files, reconstruct.BaselineFile{Schema: "shop", Table: "legacy", SnapshotTime: now.Add(-time.Minute)})
	w.runCycle(context.Background())
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("fresh baseline must resolve the alert: %+v", f.events)
	}
}
