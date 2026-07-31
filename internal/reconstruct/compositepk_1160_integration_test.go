//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestCompositeIntBinaryPKBaselineJoin_endToEnd closes the third gap of #1160:
// every #1155 fixture is single-column, while the realistic tenant-id + uuid
// shape is a mixed (INT, BINARY(16)) primary key that puts a pass-through
// component and a trimmed component through event.BuildPKValues' pipe-joining
// in ONE call — on both sides of the join (the indexer at capture, the
// baseline canonicalization at merge).
//
// Like the single-column sibling (TestBinaryPKBaselineJoin_endToEnd), this
// runs the production chain end to end against a real server — source DDL+DML
// → real ROW binlog → parser → indexer → query engine → baseline merge —
// because the whole premise is WHICH bytes MySQL puts in the ROW image, and a
// hand-built fixture would assert the test's own assumption instead.
//
// The discriminating fixture: the SAME binary key value under two different
// tenants, only one of which is updated. If the integer component were
// dropped, reordered, or re-spelled anywhere along the chain, the join either
// misses (row-count divergence: stale baseline row + event appended as a new
// PK) or folds the update onto the wrong tenant's row.
func TestCompositeIntBinaryPKBaselineJoin_endToEnd(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// kZ carries trailing 0x00 bytes and is invalid UTF-8 once stripped, so
	// its pk_values component is the 0x-hex spelling; kA's surviving bytes are
	// printable ASCII ("AB"), so formatPKValue's content gate stores it
	// VERBATIM — the composite must mix both spellings with the plain integer.
	const (
		kZHex = "11223344556677889900AABB00000000"
		kAHex = "41420000000000000000000000000000"
	)
	testutil.MustExec(t, sourceDB, `CREATE TABLE cbp (
		tenant INT NOT NULL,
		k      BINARY(16) NOT NULL,
		val    VARCHAR(32) NOT NULL,
		PRIMARY KEY (tenant, k)
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	// Seed the baseline state: the same binary key under tenants 7 and 8, plus
	// the ASCII-payload key under tenant 7.
	testutil.MustExec(t, sourceDB, fmt.Sprintf(`INSERT INTO cbp (tenant, k, val) VALUES
		(7, UNHEX('%s'), 'a7'),
		(8, UNHEX('%s'), 'a8'),
		(7, UNHEX('%s'), 'b7')`, kZHex, kZHex, kAHex))

	// Baseline Parquet the way mydumper --hex-blob dumps it: the integer as a
	// decimal literal, the binary column as 0x<hex> at its FULL stored width —
	// read back from the server so the fixture is MySQL's own rendering.
	baselineDir := t.TempDir()
	baselinePath := writeCompositeBaseline(t, sourceDB, baselineDir, sourceName, "cbp")

	// Delta window: update ONLY tenant 7's rows through a real ROW binlog.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	testutil.MustExec(t, sourceDB, `UPDATE cbp SET val = CONCAT(val, '-updated') WHERE tenant = 7`)
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	// Parse the real binlog and index it — where composite pk_values is minted.
	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, err := cp.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, err, out)
	}
	p := parser.New(tmpDir, res, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	evCh := make(chan event.Event, 100)
	parseErr := make(chan error, 1)
	go func() {
		defer close(evCh)
		parseErr <- p.ParseFile(ctx, currentBinlog, evCh)
	}()
	var events []event.Event
	for ev := range evCh {
		events = append(events, ev)
	}
	if err := <-parseErr; err != nil {
		t.Fatalf("ParseFile: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events parsed — the delta window is empty and nothing below would be proving anything")
	}
	idx := indexer.New(indexDB, 1000)
	if _, err := idx.InsertBatch(events); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	// Premise, read back from the index rather than asserted in prose: the
	// composite pk_values the REAL parser+indexer minted must be tenant-first,
	// pipe-joined, with the binary component in its ROW-image spelling —
	// stripped 0x-hex for kZ, verbatim ASCII for kA — and the integer
	// component untouched. Every downstream spelling consumer (the change-map
	// key, canonicalizePKMap's re-encode, the CLI's --pk re-spell) rests on
	// exactly these strings.
	rows, err := indexDB.Query(`SELECT pk_values FROM binlog_events WHERE table_name = 'cbp'`)
	if err != nil {
		t.Fatalf("read pk_values: %v", err)
	}
	var stored []string
	for rows.Next() {
		var pv string
		if err := rows.Scan(&pv); err != nil {
			t.Fatalf("scan pk_values: %v", err)
		}
		stored = append(stored, pv)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pk_values: %v", err)
	}
	rows.Close()
	sort.Strings(stored)
	wantPK := []string{"7|0x11223344556677889900AABB", "7|AB"}
	if len(stored) != len(wantPK) || stored[0] != wantPK[0] || stored[1] != wantPK[1] {
		t.Fatalf("indexed pk_values = %v, want %v — the composite spelling premise every downstream join relies on", stored, wantPK)
	}

	tm, err := res.Resolve(sourceName, "cbp")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) != 2 {
		t.Fatalf("resolved %d PK columns, want 2 (tenant, k)", len(pkCols))
	}
	for _, c := range pkCols {
		if !reconstruct.SupportedPKType(c.DataType) {
			t.Fatalf("SupportedPKType(%q) = false for composite component %q", c.DataType, c.Name)
		}
	}

	// Full-table merge: the change map keyed by the stored pk_values, exactly
	// as the production fold keys it; the baseline side re-derives the same
	// key via canonicalizePKMap → event.BuildPKValues in one call per row.
	engine := query.New(indexDB)
	fetched, err := engine.Fetch(ctx, query.Options{Schema: sourceName, Table: "cbp", Limit: 100})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(fetched) != 2 {
		t.Fatalf("indexed %d events, want 2 (the two tenant-7 updates)", len(fetched))
	}
	changes := make(map[string]*query.ResultRow, len(fetched))
	for i := range fetched {
		changes[fetched[i].PKValues] = &fetched[i]
	}

	var vals []string
	err = reconstruct.SnapshotFullTableImages(ctx, reconstruct.SnapshotFullTableInput{
		BaselinePath: baselinePath,
		Schema:       sourceName,
		Table:        "cbp",
		PKCols:       pkCols,
		Changes:      changes,
		Events:       fetched,
	}, func(row map[string]any) error {
		vals = append(vals, fmt.Sprint(row["val"]))
		return nil
	})
	if err != nil {
		t.Fatalf("SnapshotFullTableImages: %v", err)
	}
	sort.Strings(vals)
	// Row count first: a composite key that fails to join emits the stale
	// baseline rows AND appends both events as new PKs (5 rows, not 3). Then
	// the values: tenant 7's rows folded, tenant 8's row — same binary key
	// bytes, different integer component — untouched.
	want := []string{"a7-updated", "a8", "b7-updated"}
	if len(vals) != len(want) {
		t.Fatalf("merge emitted %d rows %v, want %d %v — a composite PK that fails to join duplicates every "+
			"changed row", len(vals), vals, len(want), want)
	}
	for i := range want {
		if vals[i] != want[i] {
			t.Fatalf("merged vals = %v, want %v — tenant 8's row shares kZ's bytes and must NOT absorb tenant 7's update", vals, want)
		}
	}

	// Single-row lookup with a composite filter: both components in one WHERE.
	// The padded spelling resolves; the tenant component alone decides which
	// of the two kZ rows answers.
	t.Run("composite baseline filter is scoped by both components", func(t *testing.T) {
		for _, tc := range []struct {
			tenant, wantVal string
		}{
			{"7", "a7"},
			{"8", "a8"},
		} {
			row, err := reconstruct.ReadBaselineRow(ctx, baselinePath,
				map[string]string{"tenant": tc.tenant, "k": "0x" + kZHex}, nil)
			if err != nil {
				t.Fatalf("ReadBaselineRow(tenant=%s): %v", tc.tenant, err)
			}
			if row == nil {
				t.Fatalf("no baseline row for tenant=%s k=0x%s — the padded composite filter must resolve", tc.tenant, kZHex)
			}
			if got := fmt.Sprint(row["val"]); got != tc.wantVal {
				t.Errorf("tenant=%s resolved val=%q, want %q — the integer component must scope the binary match", tc.tenant, got, tc.wantVal)
			}
		}

		// The stripped (pk_values) spelling of the binary component does not
		// match the padded Parquet bytes when no PK metas are supplied (nil
		// disables the width retry) — that documented miss is exactly why
		// callers pass the metas so ReadBaselineRow retries at the storage
		// width (covered at command level by internal/cli's
		// TestRunReconstruct_singleRow_compositeIntBinaryPK).
		row, err := reconstruct.ReadBaselineRow(ctx, baselinePath,
			map[string]string{"tenant": "7", "k": "0x11223344556677889900AABB"}, nil)
		if err != nil {
			t.Fatalf("ReadBaselineRow (stripped): %v", err)
		}
		if row != nil {
			t.Log("note: the stripped spelling resolved directly — the padding retry would be redundant for this key")
		}
	})
}

// writeCompositeBaseline dumps cbp into a baseline Parquet the way mydumper
// --hex-blob would: the integer as its decimal literal, the binary column as
// 0x<hex> at its FULL stored width — read straight from the server so the
// fixture is MySQL's own rendering. Returns the Parquet path.
func writeCompositeBaseline(t *testing.T, sourceDB *sql.DB, dir, dbName, table string) string {
	t.Helper()
	outDir := filepath.Join(dir, dbName)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cols := []baseline.Column{
		{Name: "tenant", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	path := filepath.Join(outDir, table+".parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	rows, err := sourceDB.Query(fmt.Sprintf(
		"SELECT tenant, CONCAT('0x', HEX(k)), val FROM %s ORDER BY tenant, k", table))
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var tenant, k, val string
		if err := rows.Scan(&tenant, &k, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if err := w.WriteRow([]string{tenant, k, val}, []bool{false, false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if n == 0 {
		t.Fatalf("baseline for %s is empty — the merge would have nothing to join against", table)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("baseline Close: %v", err)
	}
	return path
}
