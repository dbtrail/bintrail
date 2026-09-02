package views

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestWriteSuccessMarker_publishesTheSnapshotViewsFile drives the REAL door
// (#1583): baseline.WriteSuccessMarker, whose hook this package's init()
// arms by being linked. Calling WriteSnapshotViews directly would prove the
// generator and skip the wiring, which is the half that can silently fall
// away with an import.
func TestWriteSuccessMarker_publishesTheSnapshotViewsFile(t *testing.T) {
	root := t.TempDir()
	// marked=false: WriteSuccessMarker itself is under test, marker included.
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", false, "kept")
	dir := filepath.Join(root, "2026-04-30T03-00-00Z")
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		t.Fatalf("no %s beside the views file: %v", baseline.SuccessMarker, err)
	}
	b, err := os.ReadFile(filepath.Join(dir, SnapshotFileName))
	if err != nil {
		t.Fatalf("no %s was published into the completing snapshot: %v", SnapshotFileName, err)
	}
	sqlText := string(b)

	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Pinned to its own prefix: absolute paths, so the file works from any
	// working directory on the machine that holds the snapshot.
	if want := "read_parquet('" + filepath.ToSlash(abs) + "/shop/orders.parquet')"; !strings.Contains(sqlText, want) {
		t.Errorf("views file does not read the snapshot's own absolute path %s:\n%s", want, sqlText)
	}
	// No variable, no session step, no pointer, and no claims about a
	// registry this producer never read.
	for _, gone := range []string{"getvariable", "/current/", "archive_state"} {
		if strings.Contains(sqlText, gone) {
			t.Errorf("the in-snapshot file must be pinned and snapshot-scoped; it carries %q:\n%s", gone, sqlText)
		}
	}

	// And it EXECUTES, against a DuckDB whose working directory is NOT the
	// snapshot's — the absolute spelling is what makes that work.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("DuckDB rejected the published views file:\n%v\n\n--- file ---\n%s", err, sqlText)
	}
	var status string
	if err := db.QueryRow(`SELECT "status" FROM state_shop_orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "kept" {
		t.Errorf("state view read %q; want \"kept\"", status)
	}
}

// TestWriteSuccessMarker_skipsANonSnapshotDirectory pins the same decline
// PublishCurrentPointer makes: `reconstruct --output-format mydumper`
// completes an operator-chosen dump directory through this exact call, and a
// views file has no business there.
func TestWriteSuccessMarker_skipsANonSnapshotDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "my-dump")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, SnapshotFileName)); !os.IsNotExist(err) {
		t.Fatalf("a views file was published into a non-snapshot directory (stat err = %v)", err)
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the decline must not cost the marker: %v", err)
	}
}

// TestGenerateSnapshotViews_respellsForS3 is the upload half's contract: same
// snapshot, same generator, the destination's own spelling.
func TestGenerateSnapshotViews_respellsForS3(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "kept")
	dir := filepath.Join(root, "2026-04-30T03-00-00Z")

	s3root := "s3://bkt/baselines/2026-04-30T03-00-00Z"
	sqlText, ok, err := GenerateSnapshotViews(context.Background(), dir, s3root)
	if err != nil || !ok {
		t.Fatalf("GenerateSnapshotViews: ok=%v err=%v", ok, err)
	}
	if want := "read_parquet('" + s3root + "/shop/orders.parquet')"; !strings.Contains(sqlText, want) {
		t.Errorf("respelled file does not read %s:\n%s", want, sqlText)
	}
	// An s3-spelled file needs the httpfs session setup ahead of its views.
	if !strings.Contains(sqlText, "httpfs") {
		t.Errorf("respelled file carries no S3 preamble:\n%s", sqlText)
	}
	// The local staging path must not leak into the published artifact.
	if strings.Contains(sqlText, filepath.ToSlash(root)) {
		t.Errorf("respelled file leaks the local staging path %s:\n%s", root, sqlText)
	}
}
