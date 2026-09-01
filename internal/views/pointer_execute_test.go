package views

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// pointerRoot builds a local baselines root with one published snapshot and the
// `current` pointer aimed at it, and returns the root plus the two tables'
// absolute paths as a producer would hand them over.
func pointerRoot(t *testing.T) (string, []string) {
	t.Helper()
	root := t.TempDir()
	const stamp = "2026-05-30T03-00-00Z"
	writeSnapshot(t, root, stamp, true, "here")
	// A second table, copied from the first, so a test that asserts nothing was
	// created has something that would otherwise have been created. "audit"
	// sorts ahead of "shop", which is the half that matters: a view emitted
	// AFTER the failing one would be absent for the ordinary reason.
	src := filepath.Join(root, stamp, "shop", "orders.parquet")
	dstDir := filepath.Join(root, stamp, "audit")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read snapshot table: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "events.parquet"), body, 0o644); err != nil {
		t.Fatalf("write second table: %v", err)
	}
	if err := baseline.PublishCurrentPointer(filepath.Join(root, stamp)); err != nil {
		t.Fatalf("publish pointer: %v", err)
	}
	return root, []string{
		filepath.Join(dstDir, "events.parquet"),
		src,
	}
}

// pointerFixture goes through ApplyFollow rather than setting Follow by hand.
// The pointer mode is the one that can only be reached with a real symlink on
// disk, so the golden for it has to build its Input directly; this is where the
// producer's own decision gets executed.
func pointerFixture(t *testing.T, root string, paths []string) string {
	t.Helper()
	in := Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 5, 30, 3, 0, 0, 0, time.UTC),
		Baselines: []BaselineTable{
			{Schema: "audit", Table: "events", Path: paths[0]},
			{Schema: "shop", Table: "orders", Path: paths[1]},
		},
	}
	ApplyFollow(&in, root, false)
	if in.Follow != FollowPointer {
		t.Fatalf("ApplyFollow chose %v over a root with a published pointer; want FollowPointer", in.Follow)
	}
	for _, b := range in.Baselines {
		// The preflight cannot describe a table whose tail the producer did not
		// fill, and it fails OPEN on one, so this mode losing Rel would take the
		// whole check out with no test failing anywhere near it.
		if b.Rel == "" {
			t.Fatalf("ApplyFollow left %s.%s without a Rel; the preflight silently "+
				"skips every table it cannot name", b.Schema, b.Table)
		}
	}
	return Generate(in)
}

// TestPointerStateView_readsThroughTheSymlink is the assumption the pointer
// preflight rests on: DuckDB's glob() reports paths AS ASKED rather than
// resolved, so a listing taken through `current/` can be compared against the
// `current/` paths the views use. If glob ever started returning the snapshot
// directory it points at, the comparison would match nothing and the check
// would refuse every read of a perfectly healthy file.
func TestPointerStateView_readsThroughTheSymlink(t *testing.T) {
	root, paths := pointerRoot(t)
	db := execViews(t, pointerFixture(t, root, paths))

	var status string
	if err := db.QueryRow(`SELECT "status" FROM state_shop_orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "here" {
		t.Errorf("state view read %q; want \"here\"", status)
	}
}

func TestPointerStateView_raisesWhenTheTableLeftTheSnapshot(t *testing.T) {
	root, paths := pointerRoot(t)
	sqlText := pointerFixture(t, root, paths)

	// Generated while the table was there, read after it went: a DROP at the
	// source between the download and the query.
	if err := os.RemoveAll(filepath.Dir(paths[1])); err != nil {
		t.Fatalf("remove table: %v", err)
	}

	db, err := loadViews(t, sqlText)
	if err == nil {
		t.Fatal("the generated file loaded cleanly with shop.orders gone from the " +
			"pointed-to snapshot; a table that left must fail loudly, not read as empty")
	}
	if !strings.Contains(err.Error(), "shop/orders.parquet") {
		t.Errorf("failed with %v; want the refusal naming the table that is not there", err)
	}
	if !strings.Contains(err.Error(), baseline.CurrentLinkName) {
		t.Errorf("failed with %v; the message must name where it looked", err)
	}

	var n int
	if err := db.QueryRow(
		`SELECT count(*) FROM duckdb_views() WHERE NOT internal`).Scan(&n); err != nil {
		t.Fatalf("count views: %v", err)
	}
	if n != 0 {
		t.Errorf("the refused file left %d view(s) defined; the check has to run before "+
			"the first CREATE, not at the view that happens to break", n)
	}
}

// TestPointerStateView_acceptsATableNestedDeeper pins the preflight's glob as a
// SUPERSET of whatever tails the producer hands over.
//
// snapshotRel returns everything after the snapshot directory, so the depth of a
// Rel is the layout's business, not this check's. A glob narrower than that does
// not weaken the check, it inverts it: every table looks absent and a healthy
// file is refused before it creates anything. No layout produces this tail
// today, which is exactly why the coupling would go unnoticed.
func TestPointerStateView_acceptsATableNestedDeeper(t *testing.T) {
	root, paths := pointerRoot(t)

	// Same bytes, one directory further down than any current layout puts them.
	nested := filepath.Join(root, baseline.CurrentLinkName, "shop", "archived", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(nested), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	if err := os.WriteFile(nested, body, 0o644); err != nil {
		t.Fatalf("write nested table: %v", err)
	}

	in := Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 5, 30, 3, 0, 0, 0, time.UTC),
		Follow:           FollowPointer,
		Baselines: []BaselineTable{{
			Schema: "shop", Table: "orders",
			Path: nested,
			Rel:  "shop/archived/orders.parquet",
		}},
	}

	db := execViews(t, Generate(in))
	var status string
	if err := db.QueryRow(`SELECT "status" FROM state_shop_orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "here" {
		t.Errorf("state view read %q; want \"here\"", status)
	}
}
