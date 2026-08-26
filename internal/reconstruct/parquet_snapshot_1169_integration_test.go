//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // parquet_scan, to read the emitted snapshot back

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const ordersCreateSQL = "CREATE TABLE `orders` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `status` varchar(32) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB;\n"

// seedSourceBaseline writes a complete, discoverable snapshot of orders under
// root, anchored at binlog.000001:4, through the real baseline writer.
func seedSourceBaseline(t *testing.T, root string, at time.Time, schema string) {
	t.Helper()
	snapDir := filepath.Join(root, strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-"))
	path := filepath.Join(snapDir, schema, "orders.parquet")
	cols, err := baseline.ParseSchemaText(ordersCreateSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: ordersCreateSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      "4",
			"bintrail.snapshot_timestamp":  at.UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}, {"3", "shipped"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
}

// seedOrdersSnapshot registers the table's schema so the resolver can find its
// primary key.
func seedOrdersSnapshot(t *testing.T, db *sql.DB, schema string, at time.Time) {
	t.Helper()
	ts := at.UTC().Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, ts, schema, "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, schema, "orders", "status", 2, "", "varchar", "YES")
}

// readOrders reads a baseline Parquet back as sorted "id=status" strings.
func readOrders(t *testing.T, path string) []string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer ddb.Close()
	rows, err := ddb.Query(fmt.Sprintf("SELECT id, status FROM parquet_scan('%s')", path))
	if err != nil {
		t.Fatalf("parquet_scan %s: %v", path, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int32
		var status sql.NullString
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, fmt.Sprintf("%d=%s", id, status.String))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	sort.Strings(out)
	return out
}

// TestReconstructParquet_refreshChainMatchesSingleFold is the #1169 acceptance
// test: a snapshot emitted by reconstruct must be a valid anchor, meaning a
// SECOND reconstruct anchored on it produces exactly what ONE reconstruct over
// the whole combined window produces.
//
// The fixture is built specifically to break a timestamp-derived anchor. Event
// B carries a timestamp PAST the first run's target while committing BEFORE
// event C, whose timestamp is inside it — the execution-time/commit-time skew
// #797 documents, here straddling the cut. Under any anchor derived from the
// time cut (say "the newest position among the events at or before --at",
// which is event C's), the first fold takes A and C, and the second fold —
// which can only bound from below by POSITION — starts after C and never sees
// B. B's update to id=2 is then absent from the chain forever, while the
// single-fold reference has it. The chain's final state diverging from the
// reference is precisely that silent loss.
//
// Events are inserted directly rather than captured from a real binlog because
// the subject is the seam arithmetic over binlog coordinates, and a real server
// gives no way to manufacture the straddle deterministically.
func TestReconstructParquet_refreshChainMatchesSingleFold(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	// The production DDL, not testutil's single-p_future stand-in: the query
	// planner derives an hour's coverage from the PARTITION list, so a table
	// with no hourly partitions reads as "every hour is a gap" and the strict
	// fetch refuses before the anchoring this test is about is ever exercised.
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	dsn := testutil.BaseDSN() + "/" + dbName
	const schema = "shop"

	// The whole timeline lives inside ONE hour on purpose: binlog_events is
	// partitioned hourly and the planner reports any hour in the window with
	// neither live rows nor an archive as a coverage gap, which a sparse
	// synthetic index spanning several hours would trip on — a fixture artifact
	// that has nothing to do with what this test asserts.
	base := time.Now().UTC().Truncate(time.Hour)
	t0 := base
	cut1 := base.Add(30 * time.Second)
	cut2 := base.Add(60 * time.Second)

	seedOrdersSnapshot(t, db, schema, t0)

	fmtTS := func(d time.Duration) string {
		return base.Add(d).Format("2006-01-02 15:04:05")
	}
	ins := func(file string, start, end uint64, ts string, evType uint8, pk, after string) {
		testutil.InsertEvent(t, db, file, start, end, ts, nil, schema, "orders", evType, pk, nil, nil, []byte(after))
	}
	// INSERTION ORDER IS THE FIXTURE: event_id ascending is commit order, and B
	// (timestamp past cut1) commits before C (timestamp inside cut1).
	ins("binlog.000001", 100, 200, fmtTS(10*time.Second), 2, "1", `{"id":1,"status":"A"}`)
	ins("binlog.000001", 200, 300, fmtTS(40*time.Second), 2, "2", `{"id":2,"status":"B"}`)
	ins("binlog.000001", 300, 400, fmtTS(20*time.Second), 2, "3", `{"id":3,"status":"C"}`)
	ins("binlog.000001", 400, 500, fmtTS(50*time.Second), 1, "4", `{"id":4,"status":"D"}`)

	// ── Chain: fold to cut1, then fold the emitted snapshot to cut2 ──────────
	chainRoot := t.TempDir()
	seedSourceBaseline(t, chainRoot, t0, schema)

	run := func(root string, at time.Time) {
		t.Helper()
		if _, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
			IndexDSN:     dsn,
			BaselineSrc:  root,
			Tables:       []string{schema + ".orders"},
			At:           at,
			OutputDir:    root,
			OutputFormat: reconstruct.OutputFormatParquet,
		}); err != nil {
			t.Fatalf("ReconstructTables(at=%s): %v", at.Format(time.RFC3339), err)
		}
	}
	run(chainRoot, cut1)

	// The emitted snapshot must be what discovery picks for the second fold —
	// asserted here rather than assumed, because a snapshot the chain cannot
	// find would silently re-fold from the original and pass the comparison
	// below for the wrong reason.
	mid, midTime, _, err := reconstruct.FindBaseline(ctx, chainRoot, schema, "orders", cut2)
	if err != nil {
		t.Fatalf("FindBaseline after the first fold: %v", err)
	}
	if !midTime.Equal(cut1) {
		t.Fatalf("second fold would anchor on the %s snapshot, not the emitted %s one", midTime, cut1)
	}
	midMeta, err := baseline.ReadParquetMetadata(mid)
	if err != nil {
		t.Fatalf("ReadParquetMetadata(emitted): %v", err)
	}
	if midMeta.BinlogFile != "binlog.000001" || midMeta.BinlogPos != 200 {
		t.Errorf("emitted anchor = %s:%d, want binlog.000001:200 — the start of the first event past the cut",
			midMeta.BinlogFile, midMeta.BinlogPos)
	}
	if got, want := readOrders(t, mid), []string{"1=A", "2=paid", "3=shipped"}; !equalStrings(got, want) {
		t.Errorf("intermediate snapshot = %v, want %v (only the event that COMMITTED before the cut applies)", got, want)
	}

	run(chainRoot, cut2)
	chainFinal, _, _, err := reconstruct.FindBaseline(ctx, chainRoot, schema, "orders", cut2.Add(time.Second))
	if err != nil {
		t.Fatalf("FindBaseline after the second fold: %v", err)
	}

	// ── Reference: one fold over the whole window, from the original baseline ─
	refRoot := t.TempDir()
	seedSourceBaseline(t, refRoot, t0, schema)
	run(refRoot, cut2)
	refFinal, _, _, err := reconstruct.FindBaseline(ctx, refRoot, schema, "orders", cut2.Add(time.Second))
	if err != nil {
		t.Fatalf("FindBaseline on the reference root: %v", err)
	}

	got, want := readOrders(t, chainFinal), readOrders(t, refFinal)
	if !equalStrings(got, want) {
		t.Fatalf("a refreshed chain and a single fold disagree — events were lost or double-applied across the seam:\n"+
			" chain: %v\n  once: %v", got, want)
	}
	if want := []string{"1=A", "2=B", "3=C", "4=D"}; !equalStrings(got, want) {
		t.Fatalf("final state = %v, want %v", got, want)
	}
}

// TestResolveSnapshotCut covers the two branches that decide where a snapshot
// is anchored, including the empty index.
func TestResolveSnapshotCut(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	base := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)

	// Empty index: nothing to fold, and the caller keeps the source anchor.
	cut, err := reconstruct.ResolveSnapshotCut(ctx, db, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("ResolveSnapshotCut on an empty index: %v", err)
	}
	if cut != nil {
		t.Fatalf("empty index returned a cut %+v, want nil", cut)
	}

	fmtTS := func(d time.Duration) string { return base.Add(d).Format("2006-01-02 15:04:05") }
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, fmtTS(10*time.Minute), nil, "shop", "orders", 2, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, fmtTS(40*time.Minute), nil, "shop", "orders", 2, "2", nil, nil, []byte(`{"id":2}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, fmtTS(20*time.Minute), nil, "shop", "orders", 2, "3", nil, nil, []byte(`{"id":3}`))

	// A cut inside the window is the START of the first event past it in COMMIT
	// order — not the newest position among the events at or before it (400),
	// which would strand the second event forever.
	cut, err = reconstruct.ResolveSnapshotCut(ctx, db, base.Add(30*time.Minute))
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.File != "binlog.000001" || cut.Pos != 200 {
		t.Fatalf("cut = %+v, want binlog.000001:200", cut)
	}

	// Past every event: fold everything, resume after the newest one's END.
	cut, err = reconstruct.ResolveSnapshotCut(ctx, db, base.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("ResolveSnapshotCut past the newest event: %v", err)
	}
	if cut == nil || cut.Pos != 400 {
		t.Fatalf("cut = %+v, want binlog.000001:400 (the newest event's end_pos)", cut)
	}
}

// TestReconstructParquet_refusesTableWithoutBaseline: the #766 binlog-only
// degrade must not produce a snapshot, because one built from deltas alone
// omits every row the window never touched.
func TestReconstructParquet_refusesTableWithoutBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	seedOrdersSnapshot(t, db, schema, base)
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		base.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		schema, "orders", 1, "1", nil, nil, []byte(`{"id":1,"status":"A"}`))

	root := t.TempDir() // no baseline at all
	out := t.TempDir()
	_, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + ".orders"},
		At:           base.Add(30 * time.Minute),
		OutputDir:    out,
		OutputFormat: reconstruct.OutputFormatParquet,
	})
	if err == nil || !strings.Contains(err.Error(), "cannot be re-emitted as one") {
		t.Fatalf("error = %v, want a refusal to publish a binlog-only snapshot", err)
	}
	// And the run must stay marked incomplete, so nothing discovers it.
	entries, _ := os.ReadDir(out)
	for _, e := range entries {
		if e.IsDir() && baseline.SnapshotComplete(filepath.Join(out, e.Name())) {
			t.Errorf("snapshot dir %s was published despite the refusal", e.Name())
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// carriedInode returns the inode of a published snapshot table, so a test can
// tell a hard link from a copy. A link is what makes carrying a table forward
// free; a copy would still write every byte.
func carriedInode(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no inode information on this platform")
	}
	return st.Ino
}

// A table with no events in the window is published by carrying its previous
// file forward, not by folding an empty change map over it and re-emitting the
// same rows.
//
// This is the whole point of the change: every cycle used to rewrite every
// table in full, however little of it changed, and that rewrite cost is what
// puts a floor under how often a refresh can run.
func TestReconstructParquet_carriesForwardATableWithNoEvents(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	// The production DDL rather than testutil's single-p_future stand-in, and
	// one hour for the whole timeline: the planner derives an hour's coverage
	// from the PARTITION list, so without hourly partitions every hour reads as
	// a gap and the strict fetch refuses before this test's subject is reached.
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, schema, base)

	root := t.TempDir()
	seedSourceBaseline(t, root, base, schema)

	// No events for shop.orders, but the window still has to be COVERED, and
	// the distinction is the realistic one: a cold table does not leave the
	// index empty, its neighbours are still being written. Without this the
	// planner sees an hour with no data at all and refuses for a coverage gap
	// before the carry-forward branch is ever reached, which is correct
	// behaviour on a genuinely empty index and not the case under test.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		base.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		schema, "customers", 1, "1", nil, nil, []byte(`{"id":1,"name":"n"}`))

	out := root // refresh publishes into the same baselines root
	reports, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + ".orders"},
		At:           base.Add(30 * time.Minute),
		OutputDir:    out,
		OutputFormat: reconstruct.OutputFormatParquet,
	})
	if err != nil {
		t.Fatalf("ReconstructTables: %v", err)
	}
	if len(reports) != 1 {
		t.Fatalf("got %d reports, want 1", len(reports))
	}
	if !reports[0].CarriedForward {
		t.Fatal("a table with no events in the window was folded and re-emitted; the rewrite it did not " +
			"need is exactly the cost this avoids")
	}
	if len(reports[0].Files) != 1 {
		t.Fatalf("a carried-forward table published %d files, want 1", len(reports[0].Files))
	}

	src := filepath.Join(root, base.Format("2006-01-02T15-04-05Z"), schema, "orders.parquet")
	if _, err := os.Stat(src); err != nil {
		// The source snapshot's directory name is produced by seedSourceBaseline;
		// find it rather than guess if the format ever moves.
		t.Skipf("could not locate the source snapshot to compare inodes: %v", err)
	}
	if a, b := carriedInode(t, src), carriedInode(t, reports[0].Files[0]); a != b {
		t.Errorf("the carried file is a copy, not a link (inodes %d vs %d): the bytes were written again", a, b)
	}
}

// TestReconstructParquet_destructiveDDLRefusesEvenWithNoRowEvents is the
// ordering guard, and it protects an invariant that is easy to invert on
// purpose.
//
// A TRUNCATE emits no row-level events, so the change map for a truncated
// table is EMPTY — indistinguishable, on its own, from a table nobody touched.
// Carrying the previous file forward would then republish rows the truncate
// deleted, under a fresh snapshot, silently.
//
// What makes an empty change map mean "untouched" is CheckDestructiveDDL
// running first. Someone optimizing this path might reasonably think the DDL
// check can be skipped when nothing changed; it is precisely backwards, and
// this test is what says so.
func TestReconstructParquet_destructiveDDLRefusesEvenWithNoRowEvents(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	// The production DDL rather than testutil's single-p_future stand-in, and
	// one hour for the whole timeline: the planner derives an hour's coverage
	// from the PARTITION list, so without hourly partitions every hour reads as
	// a gap and the strict fetch refuses before this test's subject is reached.
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	const schema = "shop"
	base := time.Now().UTC().Truncate(time.Hour)
	seedOrdersSnapshot(t, db, schema, base)

	root := t.TempDir()
	seedSourceBaseline(t, root, base, schema)

	// Same as above: a neighbour keeps the window covered, so the run reaches
	// the destructive-DDL check rather than stopping at a coverage gap.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		base.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		schema, "customers", 1, "1", nil, nil, []byte(`{"id":1,"name":"n"}`))

	// A TRUNCATE inside the window, and no row events for orders at all.
	if _, err := db.Exec(
		`INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		 VALUES (?, 'binlog.000001', 400, ?, ?, 'TRUNCATE TABLE', 'TRUNCATE TABLE orders')`,
		base.Add(10*time.Minute), schema, "orders"); err != nil {
		t.Fatalf("seed the truncate: %v", err)
	}

	_, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + dbName,
		BaselineSrc:  root,
		Tables:       []string{schema + ".orders"},
		At:           base.Add(30 * time.Minute),
		OutputDir:    root,
		OutputFormat: reconstruct.OutputFormatParquet,
	})
	if err == nil {
		t.Fatal("a truncated table was published from its pre-truncate file: no row events does NOT mean " +
			"nothing happened, and the destructive-DDL refusal is what makes an empty change map safe")
	}
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Errorf("refused for the wrong reason: %v", err)
	}
}
