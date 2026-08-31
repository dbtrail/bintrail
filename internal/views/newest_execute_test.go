package views

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeSnapshot writes one baseline snapshot holding shop.orders with the given
// status values, and marks it complete unless marked is false.
func writeSnapshot(t *testing.T, root, stamp string, marked bool, statuses ...string) string {
	t.Helper()
	path := filepath.Join(root, stamp, "shop", "orders.parquet")
	cols, err := baseline.ParseSchema(writeSchemaFile(t))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	for i, st := range statuses {
		if err := w.WriteRow([]string{string(rune('1' + i)), st}, []bool{false, false}); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if marked {
		marker := filepath.Join(root, stamp, baseline.SuccessMarker)
		if err := os.WriteFile(marker, nil, 0o644); err != nil {
			t.Fatalf("write marker: %v", err)
		}
	}
	return path
}

// newestFixture renders the FollowNewest state view over a LOCAL root.
//
// Follow and Rel are set directly instead of through ApplyFollow, which reaches
// this mode only for an s3:// root. What is under test here is the SQL the mode
// EMITS, and DuckDB reads a local glob and an S3 one through the same code path;
// that ApplyFollow reaches the mode at all is what the golden covers.
func newestFixture(t *testing.T, root, tableRel string) string {
	t.Helper()
	return Generate(Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow:           FollowNewest,
		Baselines: []BaselineTable{{
			Schema: "shop", Table: "orders",
			Path: filepath.Join(root, "2026-04-30T03-00-00Z", tableRel),
			Rel:  tableRel,
		}},
	})
}

func execViews(t *testing.T, sqlText string) *sql.DB {
	t.Helper()
	db, err := loadViews(t, sqlText)
	if err != nil {
		t.Fatalf("DuckDB rejected the generated views:\n%v\n\n--- generated ---\n%s", err, sqlText)
	}
	return db
}

// loadViews runs the generated file and HANDS BACK the error, for the cases
// where refusing to load is the behaviour under test.
func loadViews(t *testing.T, sqlText string) (*sql.DB, error) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(sqlText)
	return db, err
}

// TestNewestStateView_readsTheNewestMarkedSnapshot runs the emitted SQL, which
// the golden cannot do: a file that pins byte-for-byte can still be invalid SQL,
// or valid SQL that selects the wrong file.
//
// The two snapshots hold DIFFERENT rows on purpose. A view that ignored the
// filter and unioned both, or that picked the older one, returns a row count or
// a value this asserts against, so the test cannot pass by reading whatever
// happens to be there.
func TestNewestStateView_readsTheNewestMarkedSnapshot(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "old-a", "old-b")
	writeSnapshot(t, root, "2026-05-30T03-00-00Z", true, "new-a")

	db := execViews(t, newestFixture(t, root, filepath.Join("shop", "orders.parquet")))

	var n int
	var status string
	if err := db.QueryRow(`SELECT count(*), min("status") FROM state_shop_orders`).Scan(&n, &status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if n != 1 || status != "new-a" {
		t.Errorf("state view read %d row(s) with status %q; want 1 row of \"new-a\" (the newest marked snapshot)", n, status)
	}
}

// TestNewestStateView_ignoresAnUnmarkedNewerSnapshot pins the anchor. A snapshot
// directory with a later name but no marker is a run that failed or is still
// writing, and reading it would serve a half-published table as the current one.
func TestNewestStateView_ignoresAnUnmarkedNewerSnapshot(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "complete")
	writeSnapshot(t, root, "2026-05-30T03-00-00Z", false, "half-written")

	db := execViews(t, newestFixture(t, root, filepath.Join("shop", "orders.parquet")))

	var status string
	if err := db.QueryRow(`SELECT "status" FROM state_shop_orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "complete" {
		t.Errorf("state view read %q; want \"complete\" (the unmarked newer snapshot must not win)", status)
	}
}

// TestNewestStateView_raisesWhenTheTableLeftTheNewestSnapshot is the guarantee
// this mechanism exists to keep, and the one it is easiest to lose.
//
// The shape this replaced returned ZERO ROWS with no error when its filter
// matched nothing, and an empty table that should have had rows is a worse
// answer than a stale one. Reading one named file cannot fail that way: the
// path is either there or DuckDB refuses it. This asserts the refusal.
func TestNewestStateView_raisesWhenTheTableLeftTheNewestSnapshot(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "here")
	// A newer complete snapshot that does NOT carry shop.orders. Another table
	// is what makes it a real snapshot rather than an empty directory.
	writeSnapshot(t, root, "2026-05-30T03-00-00Z", true, "elsewhere")
	if err := os.Rename(filepath.Join(root, "2026-05-30T03-00-00Z", "shop"),
		filepath.Join(root, "2026-05-30T03-00-00Z", "warehouse")); err != nil {
		t.Fatalf("rename: %v", err)
	}

	_, err := loadViews(t, newestFixture(t, root, filepath.Join("shop", "orders.parquet")))
	if err == nil {
		t.Fatal("the generated file loaded cleanly with shop.orders absent from the newest " +
			"snapshot; a table that left must fail loudly, not read as empty")
	}
	// DuckDB's own refusal, which already names the exact path it looked for:
	// the snapshot the variable resolved to, plus the table. Wrapping that in a
	// message of our own would restate it and could fall out of step with it.
	if !strings.Contains(err.Error(), "No files found") {
		t.Errorf("failed with %v; want the refusal naming the path that is not there", err)
	}
	if !strings.Contains(err.Error(), "2026-05-30T03-00-00Z") {
		t.Errorf("failed with %v; the message must name the snapshot it looked in", err)
	}
}

// TestNewestStateView_refusesToLoadWhenNothingIsMarked pins the CASE in the
// variable lookup, and it asserts a refusal to LOAD rather than to query.
//
// With no marker under the root, max() is NULL and the variable is NULL, which
// read_parquet reports as "cannot take NULL list as parameter". That is true and
// says nothing about baselines, so the lookup raises first and names the root
// and the marker. Failing at load also beats failing per query: the operator
// learns at the moment they open the file, not at the first thing they ask it.
func TestNewestStateView_refusesToLoadWhenNothingIsMarked(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", false, "unmarked")

	_, err := loadViews(t, newestFixture(t, root, filepath.Join("shop", "orders.parquet")))
	if err == nil {
		t.Fatalf("the generated file loaded cleanly with no %s marker under the root; "+
			"there is no snapshot for its state views to read", baseline.SuccessMarker)
	}
	if !strings.Contains(err.Error(), "no completed snapshot under") {
		t.Errorf("failed with %v; want the message naming the root", err)
	}
	if strings.Contains(err.Error(), "NULL list as parameter") {
		t.Errorf("failed with DuckDB's raw NULL refusal, which says nothing about baselines: %v", err)
	}
}
