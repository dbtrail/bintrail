package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// The staged-build lifecycle (#1448): a finished .sql build leaves the disk
// when it is downloaded, when its TTL passes, when its files vanish, when
// it fails, and at boot. Every removal goes through the path guard.

// fakeClock is the injected clock: tests cross the TTL by moving it.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClockedSupervisor(t *testing.T) (*baselineSupervisor, *fakeClock, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clk := &fakeClock{t: time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)}
	sup := newBaselineSupervisor(ctx, t.TempDir(), "")
	sup.now = clk.now
	return sup, clk, cancel
}

// seedFinishedExport runs a build's real lifecycle minus the fold: Trigger's
// bookkeeping, a dump the fold would have written (with the #842 marker),
// and the success tail that stamps the deadline. Returns the build dir.
func seedFinishedExport(t *testing.T, sup *baselineSupervisor, serverID string) string {
	t.Helper()
	at := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)
	req := console.SQLExportRequest{ServerID: serverID, ServerName: "wp", IndexDSN: "dsn",
		BaselineSrc: t.TempDir(), At: at}
	dir := filepath.Join(sup.sqlExportRoot(serverID), "1")
	sup.mu.Lock()
	sup.exports[serverID] = &console.BaselineStatus{State: "running", Since: sup.clock().UTC().Format(time.RFC3339),
		At: at.Format(time.RFC3339)}
	sup.exportDirs[serverID] = dir
	sup.mu.Unlock()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop.orders.00000.sql"), []byte("INSERT INTO `orders` VALUES (1);"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatal(err)
	}
	sup.finishSQLExport(req, dir, 1, 1, 32, nil)
	return dir
}

// TestSQLExportTTL_expiresAndRemovesTheBuild: the success tail stamps a
// deadline sqlExportTTL after finishing; the build is downloadable right up
// to it and gone, with the state saying so, the moment it passes.
func TestSQLExportTTL_expiresAndRemovesTheBuild(t *testing.T) {
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")

	st := sup.SQLExportStatus("srv1")
	wantExp := clk.now().Add(sqlExportTTL).Format(time.RFC3339)
	if st.State != "succeeded" || st.ExpiresAt != wantExp {
		t.Fatalf("status = %+v, want succeeded with expires_at = %s", st, wantExp)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); !ok {
		t.Fatal("a fresh build must be downloadable")
	}

	clk.advance(sqlExportTTL - time.Second)
	if st := sup.SQLExportStatus("srv1"); st.State != "succeeded" || !onDisk(dir) {
		t.Fatalf("one second before the deadline: state = %s, dir exists = %v; want succeeded and present", st.State, onDisk(dir))
	}

	clk.advance(2 * time.Second)
	if st := sup.SQLExportStatus("srv1"); st.State != "expired" {
		t.Fatalf("past the deadline: state = %s, want expired", st.State)
	}
	if onDisk(dir) {
		t.Fatalf("past the deadline: %s still on disk", dir)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("an expired build must not be downloadable")
	}
	if staged := sup.SQLExportStaged(); len(staged.Builds) != 0 {
		t.Fatalf("staging still lists %+v after expiry", staged.Builds)
	}
	// The export slot is terminal, so the server's single-flight is free.
	sup.mu.Lock()
	busy := sup.busyLocked("srv1")
	sup.mu.Unlock()
	if busy {
		t.Fatal("an expired export must not hold the per-server single-flight")
	}
}

// TestSQLExportReaper_expiresAnUnwatchedBuild: with nobody polling, the
// background loop alone removes a build past its deadline. The test never
// reads the status until the directory is gone, so a lazy expiry on a read
// cannot be what removed it.
func TestSQLExportReaper_expiresAnUnwatchedBuild(t *testing.T) {
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	sup.exportReapEvery = 5 * time.Millisecond
	dir := seedFinishedExport(t, sup, "srv1")
	go sup.runSQLExportReaper()

	clk.advance(sqlExportTTL + time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for onDisk(dir) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if onDisk(dir) {
		t.Fatalf("the reaper never removed %s", dir)
	}
	sup.mu.Lock()
	state := sup.exports["srv1"].State
	sup.mu.Unlock()
	if state != "expired" {
		t.Fatalf("state = %s after the reaper ran, want expired", state)
	}
}

// TestSQLExportDelivered_removesTheBuild: a completed download consumes the
// build; the state records the handover and the bytes leave the disk.
func TestSQLExportDelivered_removesTheBuild(t *testing.T) {
	sup, clk, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	clk.advance(10 * time.Minute)

	sup.SQLExportDelivered("srv1", dir)
	st := sup.SQLExportStatus("srv1")
	if st.State != "downloaded" || st.DownloadedAt != clk.now().Format(time.RFC3339) {
		t.Fatalf("status = %+v, want downloaded at %s", st, clk.now().Format(time.RFC3339))
	}
	if onDisk(dir) {
		t.Fatalf("delivered build %s still on disk", dir)
	}
	if _, _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("a delivered build must not be downloadable again")
	}
	if st.At != "2026-06-10T11:00:00Z" || st.Bytes != 32 {
		t.Fatalf("status = %+v: the instant and size must survive the handover (the card names them)", st)
	}
}

// TestSQLExportDelivered_ignoresAnotherBuild: the dir pins WHICH build was
// delivered. A download of an old build that completes after a new build
// took the slot must not remove the new build, and a build that is not
// finished cannot be "delivered".
func TestSQLExportDelivered_ignoresAnotherBuild(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")

	sup.SQLExportDelivered("srv1", filepath.Join(sup.sqlExportRoot("srv1"), "0"))
	if st := sup.SQLExportStatus("srv1"); st.State != "succeeded" || !onDisk(dir) {
		t.Fatalf("delivery of a different dir: state = %s, current build present = %v; want untouched", st.State, onDisk(dir))
	}

	sup.mu.Lock()
	sup.exports["srv1"].State = "running"
	sup.mu.Unlock()
	sup.SQLExportDelivered("srv1", dir)
	if st := sup.SQLExportStatus("srv1"); st.State != "running" || !onDisk(dir) {
		t.Fatalf("delivery against a running build: state = %s, dir present = %v; want untouched", st.State, onDisk(dir))
	}
}

// TestSQLExportStatus_selfHealsARemovedBuild: an operator's rm -rf of the
// staged run used to leave the state "succeeded" behind a download that
// would 409 until the next build. Now the first read after the removal
// reports expired.
func TestSQLExportStatus_selfHealsARemovedBuild(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	dir := seedFinishedExport(t, sup, "srv1")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if st := sup.SQLExportStatus("srv1"); st.State != "expired" {
		t.Fatalf("state = %s after the build dir was removed by hand, want expired", st.State)
	}
	if staged := sup.SQLExportStaged(); len(staged.Builds) != 0 {
		t.Fatalf("staging lists %+v for a build that is gone", staged.Builds)
	}
}

// TestSQLExportFailure_removesThePartialBuild: a refused fold can have
// written gigabytes under _INCOMPLETE, and nothing will ever download them,
// so a failed run removes its directory at once rather than at the next
// build or the next boot. Drives runSQLExport itself: the empty store makes
// the fold refuse before touching the DSN.
func TestSQLExportFailure_removesThePartialBuild(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}
	dir := filepath.Join(sup.sqlExportRoot("srv1"), "1")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop.orders.00000.sql"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	seedExport(sup, "srv1", "running", dir)
	sup.runSQLExport(req, dir)
	if st := sup.SQLExportStatus("srv1"); st.State != "failed" {
		t.Fatalf("state = %s, want failed", st.State)
	}
	if onDisk(dir) {
		t.Fatalf("failed build %s still on disk", dir)
	}
}

// TestSQLExportStaged_reportsLiveSizes: the Storage page's share lists every
// build on disk (running or waiting) with the bytes it holds NOW, sorted by
// server, plus the base dir and the TTL the card quotes.
func TestSQLExportStaged_reportsLiveSizes(t *testing.T) {
	sup, _, cancel := newClockedSupervisor(t)
	defer cancel()
	finished := seedFinishedExport(t, sup, "srv2")
	running := filepath.Join(sup.sqlExportRoot("srv1"), "7")
	if err := os.MkdirAll(running, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(running, "shop.orders.00000.sql"), make([]byte, 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	seedExport(sup, "srv1", "running", running)
	seedExport(sup, "srv3", "failed", filepath.Join(sup.sqlExportRoot("srv3"), "9"))

	info := sup.SQLExportStaged()
	if info.Dir != sup.sqlExportBase() || info.TTL != sqlExportTTL {
		t.Fatalf("info = %+v, want dir %s and ttl %s", info, sup.sqlExportBase(), sqlExportTTL)
	}
	if len(info.Builds) != 2 || info.Builds[0].ServerID != "srv1" || info.Builds[1].ServerID != "srv2" {
		t.Fatalf("builds = %+v, want srv1 (running) then srv2 (succeeded)", info.Builds)
	}
	if b := info.Builds[0]; b.State != "running" || b.Bytes != 1000 {
		t.Fatalf("running build = %+v, want 1000 live bytes", b)
	}
	// The finished build's live size is what is on disk (dump + marker),
	// not what the status was told.
	want, err := dirBytes(finished)
	if err != nil {
		t.Fatal(err)
	}
	if b := info.Builds[1]; b.State != "succeeded" || b.Bytes != want || b.ExpiresAt == "" || b.At == "" {
		t.Fatalf("finished build = %+v, want succeeded, %d bytes, an instant and a deadline", b, want)
	}
}

// TestRemoveStagedBuild_guard: the one function every deletion goes through
// refuses anything outside the staging base and anything that crosses a
// symbolic link, and touches nothing when it refuses.
func TestRemoveStagedBuild_guard(t *testing.T) {
	base := filepath.Join(t.TempDir(), "sql-export")
	outside := t.TempDir()
	victim := filepath.Join(outside, "keep.sql")
	if err := os.WriteFile(victim, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}
	mkBuild := func(server, run string) string {
		dir := filepath.Join(base, server, run)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "a.sql"), []byte("1234"), 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	refuse := func(name, dir string) {
		t.Helper()
		if _, err := removeStagedBuild(base, dir); err == nil {
			t.Fatalf("%s: removeStagedBuild(%q) = nil, want a refusal", name, dir)
		}
		if !onDisk(victim) {
			t.Fatalf("%s: the refusal still removed %s", name, victim)
		}
	}

	refuse("absolute path elsewhere", outside)
	refuse("the base itself", base)
	refuse("base/.. escape", filepath.Join(base, "..", filepath.Base(outside)))
	refuse("a sibling with the base as a prefix", base+"-other")

	// A run entry that is a link to an outside directory.
	if err := os.MkdirAll(filepath.Join(base, "srv-link"), 0o700); err != nil {
		t.Fatal(err)
	}
	linkRun := filepath.Join(base, "srv-link", "1")
	if err := os.Symlink(outside, linkRun); err != nil {
		t.Fatal(err)
	}
	refuse("run dir is a symlink", linkRun)
	if _, err := os.Lstat(linkRun); err != nil {
		t.Fatalf("the refusal removed the link itself: %v", err)
	}

	// A server directory that is a link: the run below it is inside the
	// base by name and outside it on disk.
	linkSrv := filepath.Join(base, "srv-linked")
	if err := os.Symlink(outside, linkSrv); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(outside, "run"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "run", "b.sql"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	refuse("server dir is a symlink", filepath.Join(linkSrv, "run"))
	if !onDisk(filepath.Join(outside, "run", "b.sql")) {
		t.Fatal("the refusal removed the linked-through directory's contents")
	}

	// The base itself as a link (something swapped our directory out).
	linkedBase := filepath.Join(t.TempDir(), "sql-export")
	if err := os.Symlink(outside, linkedBase); err != nil {
		t.Fatal(err)
	}
	if _, err := removeStagedBuild(linkedBase, filepath.Join(linkedBase, "run")); err == nil {
		t.Fatal("a base that is a symlink must refuse")
	}
	if !onDisk(filepath.Join(outside, "run", "b.sql")) {
		t.Fatal("a base that is a symlink was followed")
	}

	// A real build is removed and its size reported; a missing one is a
	// success with nothing freed.
	real := mkBuild("srv1", "1")
	n, err := removeStagedBuild(base, real)
	if err != nil || n != 4 {
		t.Fatalf("real build: (%d, %v), want (4, nil)", n, err)
	}
	if onDisk(real) {
		t.Fatal("real build still on disk")
	}
	if n, err := removeStagedBuild(base, filepath.Join(base, "srv1", "missing")); err != nil || n != 0 {
		t.Fatalf("missing build: (%d, %v), want (0, nil)", n, err)
	}
}

// TestSQLExportBootSweep_neverFollowsSymlinks: the boot sweep removes every
// real build a previous process left, and leaves alone anything that is
// not a directory this daemon wrote: a run that is a link to elsewhere, a
// server entry that is a link, a stray file, and a base that is itself a
// link.
func TestSQLExportBootSweep_neverFollowsSymlinks(t *testing.T) {
	staging := t.TempDir()
	base := filepath.Join(staging, "sql-export")
	outside := t.TempDir()
	victim := filepath.Join(outside, "keep.sql")
	if err := os.WriteFile(victim, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(base, "srv-old", "1")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "shop.orders.00000.sql"), []byte("INSERT"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "srv-old", "2")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "srv-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "stray.txt"), []byte("?"), 0o644); err != nil {
		t.Fatal(err)
	}

	sweepSQLExportStaging(staging)

	if onDisk(stale) {
		t.Fatalf("the real stale build %s survived the sweep", stale)
	}
	if !onDisk(victim) {
		t.Fatalf("the sweep followed a link and removed %s", victim)
	}
	for _, keep := range []string{filepath.Join(base, "srv-old", "2"), filepath.Join(base, "srv-link"), filepath.Join(base, "stray.txt")} {
		if _, err := os.Lstat(keep); err != nil {
			t.Fatalf("the sweep removed %s, which it did not write: %v", keep, err)
		}
	}

	// A base that is a link: nothing under it is touched.
	staging2 := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(staging2, "sql-export")); err != nil {
		t.Fatal(err)
	}
	sweepSQLExportStaging(staging2)
	if !onDisk(victim) {
		t.Fatal("a linked base was swept through")
	}
}

// onDisk reports whether p exists at all (file, dir or dangling link), so
// the guard tests can check a victim FILE as well as a build directory.
func onDisk(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}
