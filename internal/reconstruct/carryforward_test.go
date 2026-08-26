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
	if ok1 && ok2 && ss.Ino != ds.Ino {
		t.Errorf("carryForward reported a link but the inodes differ (%d vs %d)", ss.Ino, ds.Ino)
	}
}

// A leftover from an interrupted attempt must not be kept. os.Link fails with
// EEXIST rather than overwriting, and a copy would open the stale file and be
// happy, so the removal is what makes a retry correct.
func TestCarryForward_replacesALeftoverDestination(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "old", "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte("the real snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	dstSnap := filepath.Join(root, "new")
	dst := filepath.Join(dstSnap, "demo", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte("debris from a killed run"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := carryForward(context.Background(), src, dstSnap, "demo", "orders"); err != nil {
		t.Fatalf("carryForward over a leftover: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "the real snapshot" {
		t.Errorf("the leftover survived: %q", got)
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
