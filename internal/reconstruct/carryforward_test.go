package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
)

func TestCarryForwardEligible(t *testing.T) {
	gap := &CaptureGap{Detail: "events permanently lost"}
	for _, tc := range []struct {
		name    string
		format  string
		src     string
		changes int
		gap     *CaptureGap
		want    bool
	}{
		{"parquet, no changes, local, no gap", OutputFormatParquet, "/b/2026/demo/o.parquet", 0, nil, true},
		{"a single change disqualifies it", OutputFormatParquet, "/b/2026/demo/o.parquet", 1, nil, false},
		// A mydumper run emits SQL for a human to load. There is no previous
		// artifact to carry, so the rows still have to be written.
		{"mydumper", OutputFormatMydumper, "/b/2026/demo/o.parquet", 0, nil, false},
		// The zero value resolves to mydumper, so it must NOT be treated as
		// parquet by an == "" slip.
		{"unset format is mydumper, not parquet", "", "/b/2026/demo/o.parquet", 0, nil, false},
		// S3 would have to be downloaded, which buys back the cost this avoids.
		{"s3 source", OutputFormatParquet, "s3://bucket/base/2026/demo/o.parquet", 0, nil, false},
		// The one that does not look like the others: a gap arrives as a VALUE
		// rather than an error, because --allow-gaps makes step 3c report and
		// proceed. Events were permanently lost, so an empty change map no
		// longer means the table was untouched, and the gap stamp the merge
		// path writes would be lost with the fold.
		{"a known capture gap", OutputFormatParquet, "/b/2026/demo/o.parquet", 0, gap, false},
	} {
		if got := carryForwardEligible(tc.format, tc.src, tc.changes, tc.gap); got != tc.want {
			t.Errorf("%s: carryForwardEligible = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The file must arrive with its bytes intact, and it must arrive as a LINK when
// the filesystem allows one: the entire point is to stop paying for bytes that
// did not change, and a silent fallback to copying would leave the feature
// looking like it worked while costing exactly what it was meant to avoid.
func TestCarryForward_linksWhenItCanAndTheBytesSurvive(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "old", "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	want := []byte("PAR1-not-really-but-bytes-are-bytes")
	if err := os.WriteFile(src, want, 0o644); err != nil {
		t.Fatal(err)
	}
	dstSnap := filepath.Join(root, "new")

	linked, err := carryForward(context.Background(), src, dstSnap, "demo", "orders")
	if err != nil {
		t.Fatalf("carryForward: %v", err)
	}
	if !linked {
		t.Error("fell back to a copy inside one temp dir, where a hard link should succeed; " +
			"the saving this exists for is the bytes not copied")
	}
	dst := filepath.Join(dstSnap, "demo", "orders.parquet")
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read the carried file: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("carried bytes differ: got %q want %q", got, want)
	}

	// A link means one inode, which is what makes it free. Asserted rather than
	// assumed, because os.Link succeeding and the destination being a distinct
	// copy are different facts.
	si, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	di, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	ss, ok1 := si.Sys().(*syscall.Stat_t)
	ds, ok2 := di.Sys().(*syscall.Stat_t)
	if !ok1 || !ok2 {
		t.Fatal("no inode information available, so the link claim cannot be checked; this test cannot " +
			"pass silently on the property it exists for")
	}
	if ss.Ino != ds.Ino {
		t.Errorf("carryForward reported a link but the inodes differ (%d vs %d)", ss.Ino, ds.Ino)
	}
}

// A leftover destination must be unlinked before anything is written, and the
// reason is the COPY path rather than the link path.
//
// os.Create truncates. After a carry-forward the destination may share an inode
// with a file an OLDER snapshot still references, so truncating in place would
// empty that one too. Unlinking first breaks the share before a byte is
// written. (os.Link would also fail with EEXIST, though reconstruct's leftovers
// refusal already rules that out, so EEXIST is not the stake here.)
//
// The `linked` assertion is load-bearing: without it, removing the unlink
// degrades the retry from a link to a copy, the content still comes out right,
// and the test stays green while the saving is gone.
func TestCarryForward_replacesALeftoverWithoutTruncatingASharedInode(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "old", "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte("the real snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The leftover is itself a hard link to a THIRD snapshot's file, which is
	// exactly what an earlier carry-forward leaves behind.
	older := filepath.Join(root, "older", "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(older), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(older, []byte("an older snapshot still needs these bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	dstSnap := filepath.Join(root, "new")
	dst := filepath.Join(dstSnap, "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(older, dst); err != nil {
		t.Skipf("no hard links on this filesystem: %v", err)
	}

	linked, err := carryForward(context.Background(), src, dstSnap, "demo", "orders")
	if err != nil {
		t.Fatalf("carryForward over a leftover: %v", err)
	}
	if !linked {
		t.Error("the retry fell back to a copy: without the unlink, os.Link fails on the leftover and the " +
			"saving this exists for is silently lost")
	}
	if got, err := os.ReadFile(dst); err != nil || string(got) != "the real snapshot" {
		t.Errorf("the leftover survived: %q (%v)", got, err)
	}
	// The older snapshot must be untouched. A copy into the shared inode would
	// have overwritten its bytes through the link.
	if got, err := os.ReadFile(older); err != nil || string(got) != "an older snapshot still needs these bytes" {
		t.Errorf("an older snapshot was written through a shared inode: %q (%v)", got, err)
	}
}

// Carrying a file forward is the ONE route into a new snapshot that does not
// read the file, so it is the one route that could re-certify a corrupt one
// under a fresh manifest. The merge path validates on read; this must too.
func TestCarryForward_refusesAFileItsManifestDisagreesWith(t *testing.T) {
	root := t.TempDir()
	snap := filepath.Join(root, "old")
	src := filepath.Join(snap, "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte("original contents"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A manifest that certifies the ORIGINAL bytes...
	if err := baselineintegrity.WriteManifest(snap); err != nil {
		t.Fatalf("write the fixture manifest: %v", err)
	}
	// ...and then the file is corrupted underneath it.
	if err := os.WriteFile(src, []byte("corrupted underneath"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := carryForward(context.Background(), src, filepath.Join(root, "new"), "demo", "orders")
	if err == nil {
		t.Fatal("carried a file forward that does not match its own manifest; a corrupt snapshot would " +
			"be re-certified under the new snapshot's manifest and look healthy")
	}
	if !strings.Contains(err.Error(), "carried forward") {
		t.Errorf("the refusal does not say what it was doing: %v", err)
	}
}

// copyFile is the fallback for a filesystem with no hard links, or a
// cross-device destination. Nothing reaches it in a t.TempDir(), where the link
// always wins, so it was entirely unexecuted: called directly here so a
// regression in it is not invisible until it is the only path available.
func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.parquet")
	want := []byte("bytes that must arrive intact")
	if err := os.WriteFile(src, want, 0o644); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "dst.parquet")
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("copied bytes differ: got %q want %q", got, want)
	}
	// Distinct inodes: a copy is a copy. If this ever links, the fallback has
	// stopped being a fallback and the caller's `linked` report is wrong.
	si, _ := os.Stat(src)
	di, _ := os.Stat(dst)
	ss, ok1 := si.Sys().(*syscall.Stat_t)
	ds, ok2 := di.Sys().(*syscall.Stat_t)
	if ok1 && ok2 && ss.Ino == ds.Ino {
		t.Error("copyFile produced a link, so a caller reporting linked=false would be wrong")
	}
	if err := copyFile(filepath.Join(dir, "missing.parquet"), dst); err == nil {
		t.Error("copyFile silently accepted a source that does not exist")
	}
}
