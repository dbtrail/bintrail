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

// TestBinaryPKBaselineJoin_endToEnd is the discriminating test for #1155.
//
// A BINARY/VARBINARY/blob-prefix primary key captures fine since #1132, but the
// baseline side could not resolve it: the indexer stores the ROW image's bytes
// under formatPKValue's "0x"+hex spelling while the baseline Parquet holds the
// raw source bytes, so the two never joined and `verify` reported the table
// inconclusive rather than checking it.
//
// Why this has to run against a real server rather than a hand-built fixture:
// the whole fix turns on WHICH bytes MySQL puts in the ROW image, and that is
// not the same as what the column stores. Fixed BINARY(n) arrives with every
// trailing 0x00 byte stripped, VARBINARY arrives intact — so a key that ends in
// a zero byte is the ONLY input that can tell a correct canonicalization from
// an inverted one. Both directions pass on a key without trailing zeros, which
// is why every case below has them. Hand-writing the event bytes would assert
// this test's own assumption instead of MySQL's behaviour.
//
// The chain driven here is the production one end to end: source DDL+DML → real
// ROW binlog → parser → indexer (real pk_values in a real VARCHAR column) →
// query engine → the baseline merge.
func TestBinaryPKBaselineJoin_endToEnd(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Three PK shapes, all with trailing zero bytes at the source:
	//   b16  — BINARY(16), the UUID idiom the issue reports
	//   b16a — BINARY(16) whose surviving bytes are printable ASCII, so
	//          formatPKValue stores them VERBATIM (no 0x prefix): the
	//          content-gating that a type-driven fix would get wrong
	//   vb   — VARBINARY(16), which keeps its trailing zeros
	testutil.MustExec(t, sourceDB, `CREATE TABLE bp (
		k  BINARY(16) PRIMARY KEY,
		val VARCHAR(32) NOT NULL
	)`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE vbp (
		k  VARBINARY(16) PRIMARY KEY,
		val VARCHAR(32) NOT NULL
	)`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	// Seed the rows that the baseline snapshot will capture, BEFORE the binlog
	// window opens — a baseline is a snapshot of prior state.
	testutil.MustExec(t, sourceDB, `INSERT INTO bp (k, val) VALUES
		(0x11223344556677889900AABB00000000, 'baseline-a'),
		(0x41420000000000000000000000000000, 'baseline-b')`)
	testutil.MustExec(t, sourceDB, `INSERT INTO vbp (k, val) VALUES
		(0xAABB0000, 'baseline-c')`)

	// The baseline: mydumper renders a binary column as 0x<hex> under
	// --hex-blob, and always dumps the FULL stored width (MySQL pads a short
	// BINARY(16) with 0x00 itself). Read the values back from the source so
	// the fixture is the server's own rendering, not this test's idea of it.
	baselineDir := t.TempDir()
	writeBinaryBaseline(t, sourceDB, baselineDir, sourceName, "bp", "binary")
	writeBinaryBaseline(t, sourceDB, baselineDir, sourceName, "vbp", "varbinary")

	// Now the delta window: update every row through a real ROW binlog.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	testutil.MustExec(t, sourceDB, `UPDATE bp SET val = CONCAT(val, '-updated')`)
	testutil.MustExec(t, sourceDB, `UPDATE vbp SET val = CONCAT(val, '-updated')`)
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	// Parse the real binlog and index it — this is where pk_values is minted.
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

	// Premise check, read back from MySQL rather than asserted in prose: a
	// fixed BINARY(16) key must be SHORTER on the event side than at the
	// source, and a VARBINARY key must not be. If MySQL ever stopped stripping
	// the padding, the canonicalizer's trim would be the wrong direction and
	// every assertion below would still pass on the padding-free rows.
	assertPaddingStripped(t, sourceDB, indexDB)

	engine := query.New(indexDB)
	for _, tc := range []struct {
		table    string
		pkType   string
		wantRows int
	}{
		{"bp", "binary", 2},
		{"vbp", "varbinary", 1},
	} {
		t.Run(tc.table, func(t *testing.T) {
			tm, err := res.Resolve(sourceName, tc.table)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			pkCols := tm.PKColumnMetas()
			if !reconstruct.SupportedPKType(pkCols[0].DataType) {
				t.Fatalf("SupportedPKType(%q) = false — verify reports this table inconclusive (#1155)", pkCols[0].DataType)
			}

			rows, err := engine.Fetch(ctx, query.Options{Schema: sourceName, Table: tc.table, Limit: 100})
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			if len(rows) != tc.wantRows {
				t.Fatalf("indexed %d events, want %d", len(rows), tc.wantRows)
			}
			// The change map is keyed by the stored pk_values, exactly as the
			// production fold keys it.
			changes := make(map[string]*query.ResultRow, len(rows))
			for i := range rows {
				changes[rows[i].PKValues] = &rows[i]
			}

			var emitted []map[string]any
			err = reconstruct.SnapshotFullTableImages(ctx, reconstruct.SnapshotFullTableInput{
				BaselinePath: filepath.Join(baselineDir, sourceName, tc.table+".parquet"),
				Schema:       sourceName,
				Table:        tc.table,
				PKCols:       pkCols,
				Changes:      changes,
				Events:       rows,
			}, func(row map[string]any) error {
				emitted = append(emitted, row)
				return nil
			})
			if err != nil {
				t.Fatalf("SnapshotFullTableImages: %v", err)
			}

			// A failed join does NOT lose rows — it emits the stale baseline
			// row AND appends the event as a brand-new PK. So the row COUNT is
			// the primary tell, and the values are the confirmation.
			if len(emitted) != tc.wantRows {
				t.Fatalf("merge emitted %d rows, want %d — a PK that fails to join duplicates every changed row "+
					"(stale baseline row + the event appended as a new PK)", len(emitted), tc.wantRows)
			}
			var vals []string
			for _, r := range emitted {
				vals = append(vals, fmt.Sprint(r["val"]))
			}
			sort.Strings(vals)
			for _, v := range vals {
				if len(v) < len("-updated") || v[len(v)-len("-updated"):] != "-updated" {
					t.Errorf("emitted row still carries the baseline value %q — the binlog UPDATE did not fold onto its baseline row", v)
				}
			}
		})
	}

	// The single-row lookup half of #1155 (`reconstruct --pk`, the console, the
	// MCP tool). The key an operator copies out of binlog_events.pk_values is
	// the ROW image's stripped spelling; the baseline holds the padded value.
	t.Run("single-row lookup by the stored pk_values spelling", func(t *testing.T) {
		var stored string
		if err := indexDB.QueryRow(
			`SELECT pk_values FROM binlog_events WHERE table_name='bp' AND pk_values LIKE '0x%' LIMIT 1`).
			Scan(&stored); err != nil {
			t.Fatalf("read pk_values: %v", err)
		}
		path := filepath.Join(baselineDir, sourceName, "bp.parquet")

		tm, err := res.Resolve(sourceName, "bp")
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		pkMetas := tm.PKColumnMetas()

		// The stripped spelling cannot match the padded baseline value exactly
		// — with nil metas (no declared width) the lookup stays a miss.
		row, err := reconstruct.ReadBaselineRow(ctx, path, map[string]string{"k": stored}, nil)
		if err != nil {
			t.Fatalf("ReadBaselineRow: %v", err)
		}
		if row != nil {
			t.Log("note: the stored spelling resolved directly — this key had no trailing padding")
		}

		// With the PK metas, ReadBaselineRow's own pad-and-retry (#1157) must
		// resolve the stripped spelling — the same reconciliation every
		// surface (CLI, console, MCP) now gets.
		row, err = reconstruct.ReadBaselineRow(ctx, path, map[string]string{"k": stored}, pkMetas)
		if err != nil {
			t.Fatalf("ReadBaselineRow (metas): %v", err)
		}
		if row == nil {
			t.Fatalf("no baseline row for the stored pk_values spelling %s with PK metas — the pad-and-retry did not run (#1157)", stored)
		}

		// Padded to the storage width it must resolve too. Before #1155 this
		// failed as well: the 0x… string was bound as text against a BLOB column.
		var padded string
		if err := sourceDB.QueryRow(`SELECT CONCAT('0x', HEX(k)) FROM bp
			WHERE CONCAT('0x', UPPER(HEX(k))) LIKE CONCAT(?, '%')`, stored).Scan(&padded); err != nil {
			t.Fatalf("read padded key from source: %v", err)
		}
		row, err = reconstruct.ReadBaselineRow(ctx, path, map[string]string{"k": padded}, pkMetas)
		if err != nil {
			t.Fatalf("ReadBaselineRow (padded): %v", err)
		}
		if row == nil {
			t.Fatalf("no baseline row for the padded key %s — the 0x… spelling is still being bound as text (#1155)", padded)
		}
	})
}

// writeBinaryBaseline dumps one table into a baseline Parquet the way mydumper
// --hex-blob would: binary columns as 0x<hex> at their FULL stored width, read
// straight from the server so the fixture is MySQL's own rendering.
func writeBinaryBaseline(t *testing.T, sourceDB *sql.DB, dir, dbName, table, pkType string) {
	t.Helper()
	outDir := filepath.Join(dir, dbName)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cols := []baseline.Column{
		{Name: "k", MySQLType: pkType, ParquetType: baseline.MysqlToParquetNode(pkType)},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(filepath.Join(outDir, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	rows, err := sourceDB.Query(fmt.Sprintf("SELECT CONCAT('0x', HEX(k)), val FROM %s ORDER BY k", table))
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var k, val string
		if err := rows.Scan(&k, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if err := w.WriteRow([]string{k, val}, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if n == 0 {
		t.Fatalf("baseline for %s is empty — the merge below would have nothing to join against", table)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("baseline Close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
}

// assertPaddingStripped verifies against the live server that the ROW image
// really does strip a fixed BINARY(n)'s trailing 0x00 padding and really does
// not strip a VARBINARY's. This is the premise the canonicalizer's trim
// direction rests on; asserting it here means a future MySQL that changed it
// fails loudly instead of silently inverting the fix.
func assertPaddingStripped(t *testing.T, sourceDB, indexDB *sql.DB) {
	t.Helper()

	var srcLen, evLen int
	if err := sourceDB.QueryRow(
		`SELECT LENGTH(k) FROM bp WHERE HEX(k) LIKE '11223344%'`).Scan(&srcLen); err != nil {
		t.Fatalf("source key length: %v", err)
	}
	if err := indexDB.QueryRow(
		`SELECT (CHAR_LENGTH(pk_values) - 2) DIV 2 FROM binlog_events
		 WHERE table_name = 'bp' AND pk_values LIKE '0x11223344%'`).Scan(&evLen); err != nil {
		t.Fatalf("indexed key length: %v", err)
	}
	if srcLen != 16 {
		t.Fatalf("source BINARY(16) value is %d bytes, want 16 — MySQL is not padding as assumed", srcLen)
	}
	if evLen >= srcLen {
		t.Fatalf("the BINARY(16) ROW image is %d bytes and the stored value %d: this server does NOT strip the "+
			"trailing 0x00 padding, so canonicalizePKValue's trim is the wrong direction for it", evLen, srcLen)
	}

	var vbSrcLen, vbEvLen int
	if err := sourceDB.QueryRow(`SELECT LENGTH(k) FROM vbp`).Scan(&vbSrcLen); err != nil {
		t.Fatalf("source varbinary length: %v", err)
	}
	if err := indexDB.QueryRow(
		`SELECT (CHAR_LENGTH(pk_values) - 2) DIV 2 FROM binlog_events WHERE table_name = 'vbp'`).Scan(&vbEvLen); err != nil {
		t.Fatalf("indexed varbinary length: %v", err)
	}
	if vbSrcLen != vbEvLen {
		t.Fatalf("VARBINARY key is %d bytes at the source but %d in the ROW image — trailing zeros are being "+
			"stripped from a variable-width column, which would make the pass-through canonicalization wrong",
			vbSrcLen, vbEvLen)
	}
}
