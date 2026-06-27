package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ─── fixtures ───────────────────────────────────────────────────────────────

// makeSnapshot creates <root>/<tsDir>/<schema>/<table>.parquet for each
// "schema/table" entry and writes the completeness marker. tsDir must be a
// baseline timestamp directory name (RFC3339 with ':' → '-').
func makeSnapshot(t *testing.T, root, tsDir string, complete bool, tables ...string) {
	t.Helper()
	snapDir := filepath.Join(root, tsDir)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		schema, table, ok := strings.Cut(tbl, "/")
		if !ok {
			t.Fatalf("table fixture %q must be schema/table", tbl)
		}
		tdir := filepath.Join(snapDir, schema)
		if err := os.MkdirAll(tdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tdir, table+".parquet"), []byte("parquet-bytes"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if complete {
		if err := WriteSuccessMarker(snapDir); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := WriteIncompleteMarker(snapDir); err != nil {
			t.Fatal(err)
		}
	}
}

// ─── planPrune (pure decision) ──────────────────────────────────────────────

func TestPlanPrune(t *testing.T) {
	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	mk := func(name string, daysOld int, complete bool) localSnapshot {
		return localSnapshot{
			name:     name,
			ts:       now.Add(-time.Duration(daysOld) * 24 * time.Hour),
			complete: complete,
		}
	}

	snaps := []localSnapshot{
		mk("old-durable", 100, true),     // prunable
		mk("old-not-durable", 100, true), // kept: only copy
		mk("recent", 1, true),            // kept: within retention
		mk("keeper", 100, true),          // kept: newest for some table
		mk("incomplete", 100, false),     // kept: _INCOMPLETE
	}
	keepers := map[string]bool{"keeper": true}
	durable := map[string]bool{
		"old-durable":     true,
		"recent":          true,
		"keeper":          true,
		"old-not-durable": false,
	}

	prune, res := planPrune(snaps, keepers, durable, 7*24*time.Hour, baselinePruneMinAge, now)

	if len(prune) != 1 || prune[0] != "old-durable" {
		t.Fatalf("prune = %v, want [old-durable]", prune)
	}
	if res.KeptKeeper != 1 {
		t.Errorf("KeptKeeper = %d, want 1", res.KeptKeeper)
	}
	if res.KeptNotDurable != 1 {
		t.Errorf("KeptNotDurable = %d, want 1", res.KeptNotDurable)
	}
	if res.KeptRecent != 1 {
		t.Errorf("KeptRecent = %d, want 1", res.KeptRecent)
	}
	if res.KeptIncomplete != 1 {
		t.Errorf("KeptIncomplete = %d, want 1", res.KeptIncomplete)
	}
}

// A keeper is never pruned even when it is old AND durable — Time-travel must
// always retain the newest snapshot for every table.
func TestPlanPrune_keeperNeverPrunedEvenWhenDurable(t *testing.T) {
	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	snaps := []localSnapshot{
		{name: "k", ts: now.Add(-365 * 24 * time.Hour), complete: true},
	}
	prune, res := planPrune(snaps, map[string]bool{"k": true}, map[string]bool{"k": true},
		time.Hour, baselinePruneMinAge, now)
	if len(prune) != 0 {
		t.Fatalf("prune = %v, want empty (keeper protected)", prune)
	}
	if res.KeptKeeper != 1 {
		t.Errorf("KeptKeeper = %d, want 1", res.KeptKeeper)
	}
}

// The min-age floor keeps a snapshot that is past the (sub-floor) retention
// window but younger than baselinePruneMinAge.
func TestPlanPrune_minAgeFloor(t *testing.T) {
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	// 30 minutes old: past a hypothetical sub-hour retention, but under the 1h floor.
	snaps := []localSnapshot{
		{name: "fresh", ts: now.Add(-30 * time.Minute), complete: true},
	}
	prune, res := planPrune(snaps, nil, map[string]bool{"fresh": true},
		1*time.Minute, baselinePruneMinAge, now)
	if len(prune) != 0 {
		t.Fatalf("prune = %v, want empty (under min-age floor)", prune)
	}
	if res.KeptRecent != 1 {
		t.Errorf("KeptRecent = %d, want 1", res.KeptRecent)
	}
}

// ─── computeKeepers (per-table newest) ──────────────────────────────────────

func TestComputeKeepers_perTableNewest(t *testing.T) {
	snaps := []localSnapshot{
		{name: "2026-01-01T00-00-00Z", ts: mustParse("2026-01-01T00-00-00Z"), complete: true, tables: []string{"shop/orders", "shop/users"}},
		{name: "2026-06-01T00-00-00Z", ts: mustParse("2026-06-01T00-00-00Z"), complete: true, tables: []string{"shop/orders", "shop/users"}},
	}
	keepers := computeKeepers(snaps, keepersNow)
	if !keepers["2026-06-01T00-00-00Z"] {
		t.Error("newest snapshot must be a keeper")
	}
	if keepers["2026-01-01T00-00-00Z"] {
		t.Error("older snapshot whose tables all exist in a newer one must NOT be a keeper")
	}
}

// keepersNow is a fixed "now" comfortably after every fixture timestamp, so all
// snapshots are at-or-before now (computeKeepers excludes future-dated ones).
var keepersNow = time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)

// PARTIAL OVERLAP is the discriminating union case (disjoint tables pass even
// under a buggy "newest-for-ALL-my-tables" rule). old{orders,users} is superseded
// for `users` by new{users} but remains the only snapshot with `orders`, so it
// must stay a keeper — pruning it would strand shop/orders.
func TestComputeKeepers_partialOverlapUnion(t *testing.T) {
	snaps := []localSnapshot{
		{name: "old", ts: mustParse("2026-01-01T00-00-00Z"), complete: true, tables: []string{"shop/orders", "shop/users"}},
		{name: "new", ts: mustParse("2026-06-25T00-00-00Z"), complete: true, tables: []string{"shop/users"}},
	}
	keepers := computeKeepers(snaps, keepersNow)
	if !keepers["old"] {
		t.Error("old snapshot is the only one with shop/orders — must stay a keeper despite being superseded for shop/users")
	}
	if !keepers["new"] {
		t.Error("new snapshot is the newest with shop/users — must be a keeper")
	}
}

// A future-dated snapshot (clock skew / explicit --timestamp) is invisible to
// findBaselineLocal for at=now, so it must not shadow the real present keeper.
func TestComputeKeepers_futureSnapshotDoesNotShadowPresent(t *testing.T) {
	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	snaps := []localSnapshot{
		{name: "present", ts: mustParse("2026-01-01T00-00-00Z"), complete: true, tables: []string{"shop/orders"}},
		{name: "future", ts: mustParse("2099-01-01T00-00-00Z"), complete: true, tables: []string{"shop/orders"}},
	}
	keepers := computeKeepers(snaps, now)
	if !keepers["present"] {
		t.Error("the newest at-or-before-now snapshot with shop/orders must be a keeper")
	}
	if keepers["future"] {
		t.Error("a future-dated snapshot must not be a keeper (findBaselineLocal can't use it for at=now)")
	}
}

// An empty _SUCCESS snapshot (complete, zero tables) serves no table, so it is a
// keeper for nothing — eligible for pruning when old+durable, never phantom-kept.
func TestComputeKeepers_emptySnapshotNotAKeeper(t *testing.T) {
	snaps := []localSnapshot{
		{name: "empty", ts: mustParse("2026-01-01T00-00-00Z"), complete: true, tables: nil},
	}
	if computeKeepers(snaps, keepersNow)["empty"] {
		t.Error("an empty _SUCCESS snapshot must not be a keeper")
	}
}

// The crux (#616, agent finding from TestFindBaselineLocal_staleSnapshotWarns):
// a table present ONLY in an older snapshot makes that older snapshot a keeper,
// because reconstruct.FindBaseline falls back to it for that table. Pruning it
// would break Time-travel for `shop.orders`.
func TestComputeKeepers_staleScenario(t *testing.T) {
	snaps := []localSnapshot{
		{name: "2026-01-01T00-00-00Z", ts: mustParse("2026-01-01T00-00-00Z"), complete: true, tables: []string{"shop/orders"}},
		{name: "2026-02-01T00-00-00Z", ts: mustParse("2026-02-01T00-00-00Z"), complete: true, tables: []string{"shop/users"}},
	}
	keepers := computeKeepers(snaps, keepersNow)
	if !keepers["2026-01-01T00-00-00Z"] {
		t.Error("older snapshot is the only one with shop/orders — must be a keeper")
	}
	if !keepers["2026-02-01T00-00-00Z"] {
		t.Error("newer snapshot is the only one with shop/users — must be a keeper")
	}
}

// Incomplete snapshots never contribute a keeper (their tables don't count).
func TestComputeKeepers_incompleteIgnored(t *testing.T) {
	snaps := []localSnapshot{
		{name: "complete", ts: mustParse("2026-01-01T00-00-00Z"), complete: true, tables: []string{"shop/orders"}},
		{name: "incomplete", ts: mustParse("2026-06-01T00-00-00Z"), complete: false, tables: nil},
	}
	keepers := computeKeepers(snaps, keepersNow)
	if !keepers["complete"] {
		t.Error("the only complete snapshot with shop/orders must be a keeper")
	}
	if keepers["incomplete"] {
		t.Error("an incomplete snapshot must never be a keeper")
	}
}

func mustParse(tsDir string) time.Time {
	ts, _ := parseBaselineDirTimestamp(tsDir)
	return ts
}

// ─── pruneWithProbe (end-to-end on a temp dir, injected durability) ──────────

func TestPruneWithProbe_endToEnd(t *testing.T) {
	root := t.TempDir()
	const (
		snapA = "2026-01-01T00-00-00Z" // old, durable, non-keeper  → PRUNE
		snapB = "2026-03-01T00-00-00Z" // old, durable, non-keeper  → PRUNE
		snapC = "2026-06-25T00-00-00Z" // newest complete (keeper)  → keep
		snapD = "2026-02-01T00-00-00Z" // _INCOMPLETE               → keep, never touched
		snapE = "2026-06-20T00-00-00Z" // 6 days old (< retain)     → keep (recent)
		snapF = "2026-01-15T00-00-00Z" // old, NOT durable          → keep (only copy)
	)
	makeSnapshot(t, root, snapA, true, "shop/orders", "shop/users")
	makeSnapshot(t, root, snapB, true, "shop/orders", "shop/users")
	makeSnapshot(t, root, snapC, true, "shop/orders", "shop/users")
	makeSnapshot(t, root, snapD, false, "shop/orders")
	makeSnapshot(t, root, snapE, true, "shop/orders", "shop/users")
	makeSnapshot(t, root, snapF, true, "shop/orders", "shop/users")

	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	durableSet := map[string]bool{snapA: true, snapB: true, snapC: true, snapE: true} // snapF NOT durable
	probe := func(_ context.Context, name string) (bool, error) {
		return durableSet[name], nil
	}

	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root,
		S3URL:    "s3://bucket/prefix", // unused by the injected probe, but set so it isn't the no-op path
		Retain:   7 * 24 * time.Hour,
		Now:      now,
	}, probe)
	if err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}

	prunedSet := map[string]bool{}
	for _, n := range res.Pruned {
		prunedSet[n] = true
	}
	if !prunedSet[snapA] || !prunedSet[snapB] || len(res.Pruned) != 2 {
		t.Fatalf("pruned = %v, want exactly [%s %s]", res.Pruned, snapA, snapB)
	}
	if res.KeptKeeper != 1 || res.KeptIncomplete != 1 || res.KeptRecent != 1 || res.KeptNotDurable != 1 {
		t.Errorf("kept tally = keeper:%d incomplete:%d recent:%d notDurable:%d, want 1/1/1/1",
			res.KeptKeeper, res.KeptIncomplete, res.KeptRecent, res.KeptNotDurable)
	}
	if res.ReclaimedBytes <= 0 {
		t.Errorf("ReclaimedBytes = %d, want > 0", res.ReclaimedBytes)
	}

	// Pruned dirs are gone; kept dirs survive; no .pruning leftover.
	for _, gone := range []string{snapA, snapB} {
		if _, err := os.Stat(filepath.Join(root, gone)); !os.IsNotExist(err) {
			t.Errorf("snapshot %s should be removed, stat err = %v", gone, err)
		}
	}
	for _, kept := range []string{snapC, snapD, snapE, snapF} {
		if _, err := os.Stat(filepath.Join(root, kept)); err != nil {
			t.Errorf("snapshot %s should survive, stat err = %v", kept, err)
		}
	}
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), pruningSuffix) {
			t.Errorf("leftover staging dir not cleaned: %s", e.Name())
		}
	}
}

// A probe error is fail-safe: the snapshot is KEPT, never deleted on uncertainty.
func TestPruneWithProbe_probeErrorKeeps(t *testing.T) {
	root := t.TempDir()
	const old = "2026-01-01T00-00-00Z"
	const newest = "2026-06-25T00-00-00Z"
	makeSnapshot(t, root, old, true, "shop/orders")
	makeSnapshot(t, root, newest, true, "shop/orders")

	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	probe := func(_ context.Context, name string) (bool, error) {
		return false, context.DeadlineExceeded // every probe errors
	}
	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p", Retain: 7 * 24 * time.Hour, Now: now,
	}, probe)
	if err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned = %v, want none (probe errored → keep)", res.Pruned)
	}
	if res.ProbeErrors != 1 {
		t.Errorf("ProbeErrors = %d, want 1 (the old snapshot's probe errored)", res.ProbeErrors)
	}
	if _, err := os.Stat(filepath.Join(root, old)); err != nil {
		t.Errorf("old snapshot must survive a probe error, stat err = %v", err)
	}
}

// DryRun reports what would be pruned without deleting anything.
func TestPruneWithProbe_dryRun(t *testing.T) {
	root := t.TempDir()
	const old = "2026-01-01T00-00-00Z"
	const newest = "2026-06-25T00-00-00Z"
	makeSnapshot(t, root, old, true, "shop/orders")
	makeSnapshot(t, root, newest, true, "shop/orders")

	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	probe := func(_ context.Context, _ string) (bool, error) { return true, nil }
	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p", Retain: 7 * 24 * time.Hour, Now: now, DryRun: true,
	}, probe)
	if err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}
	if len(res.Pruned) != 1 || res.Pruned[0] != old {
		t.Fatalf("dry-run pruned = %v, want [%s]", res.Pruned, old)
	}
	if _, err := os.Stat(filepath.Join(root, old)); err != nil {
		t.Errorf("dry-run must not delete %s, stat err = %v", old, err)
	}
}

// ─── PruneLocal guards ──────────────────────────────────────────────────────

func TestPruneLocal_noS3IsLoudNoOp(t *testing.T) {
	root := t.TempDir()
	makeSnapshot(t, root, "2026-01-01T00-00-00Z", true, "shop/orders")
	res, err := PruneLocal(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "", Retain: 7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("PruneLocal no-S3: %v", err)
	}
	if len(res.Pruned) != 0 {
		t.Fatalf("no-S3 must prune nothing, got %v", res.Pruned)
	}
	if _, err := os.Stat(filepath.Join(root, "2026-01-01T00-00-00Z")); err != nil {
		t.Errorf("no-S3 must not delete the only copy, stat err = %v", err)
	}
}

func TestPruneLocal_validatesArgs(t *testing.T) {
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: "", S3URL: "s3://b/p", Retain: time.Hour}); err == nil {
		t.Error("empty LocalDir must error")
	}
	if _, err := PruneLocal(context.Background(), PruneOptions{LocalDir: t.TempDir(), S3URL: "s3://b/p", Retain: 0}); err == nil {
		t.Error("non-positive Retain must error")
	}
}

// ─── leftovers / enumeration ────────────────────────────────────────────────

func TestSweepPruningLeftovers(t *testing.T) {
	root := t.TempDir()
	leftover := filepath.Join(root, ".2026-01-01T00-00-00Z"+pruningSuffix)
	if err := os.MkdirAll(filepath.Join(leftover, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	makeSnapshot(t, root, "2026-06-25T00-00-00Z", true, "shop/orders")

	sweepPruningLeftovers(root)

	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Errorf("leftover .pruning dir should be swept, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "2026-06-25T00-00-00Z")); err != nil {
		t.Errorf("a real snapshot must survive the sweep, stat err = %v", err)
	}
}

// A staging (".pruning") dir is invisible to enumeration (it doesn't parse as a
// timestamp), so a crash mid-delete self-excludes the half-removed tree.
func TestEnumerateLocalSnapshots_skipsPruningAndNonTimestamp(t *testing.T) {
	root := t.TempDir()
	makeSnapshot(t, root, "2026-06-25T00-00-00Z", true, "shop/orders")
	if err := os.MkdirAll(filepath.Join(root, ".2026-01-01T00-00-00Z"+pruningSuffix, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "not-a-snapshot"), 0o755); err != nil {
		t.Fatal(err)
	}
	snaps, err := enumerateLocalSnapshots(root)
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}
	if len(snaps) != 1 || snaps[0].name != "2026-06-25T00-00-00Z" {
		t.Fatalf("enumerate = %+v, want only the real snapshot", snaps)
	}
}

func TestEnumerateLocalSnapshots_missingDirIsEmpty(t *testing.T) {
	snaps, err := enumerateLocalSnapshots(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("missing dir must not error: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("missing dir must yield no snapshots, got %v", snaps)
	}
}

// ─── fail-safe: unenumerable snapshot is never pruned ────────────────────────

// Pure: an unreadable snapshot is force-kept regardless of durability/age.
func TestPlanPrune_unreadableForceKept(t *testing.T) {
	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	snaps := []localSnapshot{
		{name: "u", ts: now.Add(-365 * 24 * time.Hour), complete: true, unreadable: true},
	}
	// durable + old + non-keeper would normally prune; unreadable must override.
	prune, res := planPrune(snaps, nil, map[string]bool{"u": true}, time.Hour, baselinePruneMinAge, now)
	if len(prune) != 0 {
		t.Fatalf("an unreadable snapshot must never be pruned, got %v", prune)
	}
	if res.KeptUnreadable != 1 {
		t.Errorf("KeptUnreadable = %d, want 1", res.KeptUnreadable)
	}
}

// End-to-end: a snapshot whose directory cannot be LISTED (transient ReadDir
// failure, simulated with chmod 0111 — still stat-able, so reconstruct could read
// it) must be kept, never deleted. This is the critical silent-data-loss path.
func TestPruneWithProbe_unreadableSnapshotKept(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory read permissions; the 0111 trick is a no-op")
	}
	root := t.TempDir()
	const unreadable = "2026-01-01T00-00-00Z" // old, durable — would be pruned if listable
	const keeper = "2026-06-25T00-00-00Z"
	makeSnapshot(t, root, unreadable, true, "shop/orders", "shop/users")
	makeSnapshot(t, root, keeper, true, "shop/orders", "shop/users")

	unreadablePath := filepath.Join(root, unreadable)
	if err := os.Chmod(unreadablePath, 0o111); err != nil { // search but not read
		t.Fatal(err)
	}
	defer os.Chmod(unreadablePath, 0o755) //nolint:errcheck // restore for TempDir cleanup

	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	probe := func(_ context.Context, _ string) (bool, error) { return true, nil }
	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p", Retain: 7 * 24 * time.Hour, Now: now,
	}, probe)
	if err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}
	if len(res.Pruned) != 0 {
		t.Fatalf("an unreadable snapshot must NOT be pruned, got %v", res.Pruned)
	}
	if res.KeptUnreadable != 1 {
		t.Errorf("KeptUnreadable = %d, want 1", res.KeptUnreadable)
	}
	if _, err := os.Stat(unreadablePath); err != nil {
		t.Errorf("unreadable snapshot must survive, stat err = %v", err)
	}
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), pruningSuffix) {
			t.Errorf("unreadable snapshot must not be staged for deletion: %s", e.Name())
		}
	}
}

// ─── more end-to-end keeper protections ─────────────────────────────────────

// Partial-overlap union: old{orders,users} is the only snapshot with shop/orders,
// so it survives even though shop/users is superseded by new{users}.
func TestPruneWithProbe_partialOverlapKeepsOlder(t *testing.T) {
	root := t.TempDir()
	const old = "2026-01-01T00-00-00Z"
	const newer = "2026-06-25T00-00-00Z"
	makeSnapshot(t, root, old, true, "shop/orders", "shop/users")
	makeSnapshot(t, root, newer, true, "shop/users")

	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	probe := func(_ context.Context, _ string) (bool, error) { return true, nil }
	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p", Retain: 7 * 24 * time.Hour, Now: now,
	}, probe)
	if err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}
	if len(res.Pruned) != 0 {
		t.Fatalf("old snapshot is the only one with shop/orders — must not be pruned, got %v", res.Pruned)
	}
	if _, err := os.Stat(filepath.Join(root, old)); err != nil {
		t.Errorf("old snapshot must survive (it is the shop/orders keeper), stat err = %v", err)
	}
}

// Regression for the future-snapshot data-loss edge: a future-dated snapshot must
// not shadow the real present keeper (which would otherwise be pruned).
func TestPruneWithProbe_futureSnapshotKeepsPresent(t *testing.T) {
	root := t.TempDir()
	const present = "2026-01-01T00-00-00Z" // old, durable, the real at=now keeper
	const future = "2099-01-01T00-00-00Z"  // future-dated (clock skew / --timestamp)
	makeSnapshot(t, root, present, true, "shop/orders")
	makeSnapshot(t, root, future, true, "shop/orders")

	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	probe := func(_ context.Context, _ string) (bool, error) { return true, nil }
	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p", Retain: 7 * 24 * time.Hour, Now: now,
	}, probe)
	if err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}
	if len(res.Pruned) != 0 {
		t.Fatalf("present snapshot is the at=now shop/orders keeper — must not be pruned, got %v", res.Pruned)
	}
	if _, err := os.Stat(filepath.Join(root, present)); err != nil {
		t.Errorf("present keeper must survive a future-dated sibling, stat err = %v", err)
	}
}

// Legacy marker-less snapshots are complete-by-default: the newest is a keeper,
// an old durable one is reclaimable.
func TestPruneWithProbe_legacyMarkerless(t *testing.T) {
	root := t.TempDir()
	const oldLegacy = "2026-01-01T00-00-00Z" // marker-less, old, durable, non-keeper → PRUNE
	const newLegacy = "2026-06-25T00-00-00Z" // marker-less, newest → keeper
	makeRawSnapshot(t, root, oldLegacy, "shop/orders")
	makeRawSnapshot(t, root, newLegacy, "shop/orders")

	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	probe := func(_ context.Context, _ string) (bool, error) { return true, nil }
	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p", Retain: 7 * 24 * time.Hour, Now: now,
	}, probe)
	if err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}
	if len(res.Pruned) != 1 || res.Pruned[0] != oldLegacy {
		t.Fatalf("legacy prune = %v, want [%s] (newest legacy kept as keeper)", res.Pruned, oldLegacy)
	}
	if _, err := os.Stat(filepath.Join(root, newLegacy)); err != nil {
		t.Errorf("legacy newest (keeper) must survive, stat err = %v", err)
	}
}

// ─── boundaries / misc ──────────────────────────────────────────────────────

// A snapshot exactly `retain` old is prunable (predicate is age < retain → keep).
func TestPlanPrune_retentionBoundary(t *testing.T) {
	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	retain := 7 * 24 * time.Hour
	snaps := []localSnapshot{{name: "exact", ts: now.Add(-retain), complete: true}}
	prune, _ := planPrune(snaps, nil, map[string]bool{"exact": true}, retain, baselinePruneMinAge, now)
	if len(prune) != 1 || prune[0] != "exact" {
		t.Fatalf("a snapshot exactly `retain` old must be prunable, got %v", prune)
	}
}

func TestPruneLocal_malformedS3URL(t *testing.T) {
	_, err := PruneLocal(context.Background(), PruneOptions{
		LocalDir: t.TempDir(), S3URL: "not-an-s3-url", Retain: time.Hour,
	})
	if err == nil {
		t.Error("a malformed S3URL must error before any deletion")
	}
}

// makeRawSnapshot creates a snapshot directory with NO completeness marker — a
// legacy (pre-#467) snapshot, which SnapshotComplete treats as complete-by-default.
func makeRawSnapshot(t *testing.T, root, tsDir string, tables ...string) {
	t.Helper()
	snapDir := filepath.Join(root, tsDir)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		schema, table, ok := strings.Cut(tbl, "/")
		if !ok {
			t.Fatalf("table fixture %q must be schema/table", tbl)
		}
		tdir := filepath.Join(snapDir, schema)
		if err := os.MkdirAll(tdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tdir, table+".parquet"), []byte("parquet-bytes"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The unreadable-detection wiring (`s.unreadable = !ok`) is exercised
// deterministically by faking a readDir failure — independent of uid, so it runs
// under a root CI where the chmod-0111 test (TestPruneWithProbe_unreadableSnapshotKept)
// is skipped. Covers BOTH the snapshot-dir and the schema-dir ReadDir failure
// levels (the silent-failure-hunter noted the schema-dir path lacked coverage).
func TestPruneWithProbe_unreadableDetection(t *testing.T) {
	for _, level := range []string{"snapshot-dir", "schema-dir"} {
		t.Run(level, func(t *testing.T) {
			root := t.TempDir()
			const unreadable = "2026-01-01T00-00-00Z" // old, durable — pruned if listable
			const keeper = "2026-06-25T00-00-00Z"
			makeSnapshot(t, root, unreadable, true, "shop/orders", "shop/users")
			makeSnapshot(t, root, keeper, true, "shop/orders", "shop/users")

			failPath := filepath.Join(root, unreadable)
			if level == "schema-dir" {
				failPath = filepath.Join(root, unreadable, "shop")
			}
			orig := readDir
			t.Cleanup(func() { readDir = orig })
			readDir = func(p string) ([]os.DirEntry, error) {
				if p == failPath {
					return nil, os.ErrPermission
				}
				return orig(p)
			}

			now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
			probe := func(_ context.Context, _ string) (bool, error) { return true, nil }
			res, err := pruneWithProbe(context.Background(), PruneOptions{
				LocalDir: root, S3URL: "s3://b/p", Retain: 7 * 24 * time.Hour, Now: now,
			}, probe)
			if err != nil {
				t.Fatalf("pruneWithProbe: %v", err)
			}
			if len(res.Pruned) != 0 {
				t.Fatalf("an unreadable snapshot must NOT be pruned, got %v", res.Pruned)
			}
			if res.KeptUnreadable != 1 {
				t.Errorf("KeptUnreadable = %d, want 1", res.KeptUnreadable)
			}
			if _, err := os.Stat(filepath.Join(root, unreadable)); err != nil {
				t.Errorf("unreadable snapshot must survive, stat err = %v", err)
			}
		})
	}
}

// Invariant property: after a prune, every table that had a usable (complete,
// at-or-before-now) baseline BEFORE the prune must STILL have one — the keeper
// guarantee reconstruct.FindBaseline relies on at=now. Asserted as a property
// (importing reconstruct here would be an import cycle); it catches a prune-side
// regression that deletes a table's last at=now copy across a mix of overlapping,
// disjoint, dropped-from-newer, recent, and future-dated snapshots.
func TestPruneWithProbe_everyTableStillResolvable(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 6, 26, 0, 0, 0, 0, time.UTC)
	makeSnapshot(t, root, "2026-01-01T00-00-00Z", true, "shop/orders", "shop/users") // old, both
	makeSnapshot(t, root, "2026-02-01T00-00-00Z", true, "shop/orders")               // old, orders only
	makeSnapshot(t, root, "2026-03-01T00-00-00Z", true, "shop/legacy")               // table only here
	makeSnapshot(t, root, "2026-06-25T00-00-00Z", true, "shop/orders", "shop/users") // newest
	makeSnapshot(t, root, "2099-01-01T00-00-00Z", true, "shop/orders")               // future-dated

	before := tablesResolvable(t, root, now)
	if len(before) == 0 {
		t.Fatal("test bug: no tables resolvable before prune")
	}
	probe := func(_ context.Context, _ string) (bool, error) { return true, nil }
	if _, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, S3URL: "s3://b/p", Retain: 7 * 24 * time.Hour, Now: now,
	}, probe); err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}
	after := tablesResolvable(t, root, now)
	for tbl := range before {
		if !after[tbl] {
			t.Errorf("table %q was resolvable before the prune but not after — a keeper was deleted", tbl)
		}
	}
}

// tablesResolvable returns the "schema/table" set with at least one complete,
// at-or-before-now snapshot containing them — the at=now reconstruct
// resolvability condition, computed straight from disk.
func tablesResolvable(t *testing.T, root string, now time.Time) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	snaps, err := enumerateLocalSnapshots(root)
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}
	for _, s := range snaps {
		if !s.complete || s.ts.After(now) {
			continue
		}
		for _, tbl := range s.tables {
			out[tbl] = true
		}
	}
	return out
}
