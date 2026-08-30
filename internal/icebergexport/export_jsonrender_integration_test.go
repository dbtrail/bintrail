//go:build integration

package icebergexport

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationExport_jsonColumnRendersOneWayOnBothPaths pins #1508: a
// JSON column reads as ONE text whether the row came from the baseline
// (MySQL's rendering: keys in MySQL's order, a space after every comma) or
// from a row image. Rows 1 and 2 hold the same document; row 2 is then
// rewritten by an UPDATE carrying that same document in its row image, so
// the two rows must read byte for byte the same. Rows 3 and 5 are the same
// string scalar from each side, rows 4 and 8 the same JSON null.
//
// The row images are written the way the indexer writes them: a document
// (object or array) embedded as JSON, a top-level scalar stored as base64 of
// the text go-mysql rendered (#736), which the export's epoch decoder turns
// back into that text.
func TestIntegrationExport_jsonColumnRendersOneWayOnBothPaths(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Hour)
	const schema, tbl = "shop", "docs"
	insertSnapshotTyped(t, db, 1, base, schema, tbl, "id", 1, "PRI", "int", "int", "NO")
	insertSnapshotTyped(t, db, 1, base, schema, tbl, "meta", 2, "", "json", "json", "YES")
	createSQL := "CREATE TABLE `docs` (\n  `id` int NOT NULL,\n  `meta` json DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"

	// What `SELECT meta` prints on MySQL 8, and therefore what the dump
	// holds: keys in MySQL's order, `", "` and `": "` separators, `<` as is,
	// a 20-digit unsigned integer (what a real dump carries, and what a
	// float64 round trip would round).
	const mysqlDoc = `{"b": 1, "a": [1, 2], "s": "<x>&y", "n": 1.5, "i": 12345678901234567890}`
	// The same document as a row image carries it (whatever spacing the
	// binlog decoder used; the text is parsed either way). The index stores
	// row images in a MySQL JSON column, which rewrites a number the way it
	// parsed it (`1.50` comes back `1.5`; the source does the same in its
	// own JSON column), so the fixture uses a number MySQL keeps as written.
	const imageDoc = `{"b":1,"a":[1,2],"s":"<x>&y","n":1.5,"i":12345678901234567890}`
	const canon = `{"a":[1,2],"b":1,"i":12345678901234567890,"n":1.5,"s":"<x>&y"}`
	scalar := func(text string) string { return `"` + base64.StdEncoding.EncodeToString([]byte(text)) + `"` }

	baseDir := t.TempDir()
	writeBaseline(t, baseDir, base, schema, tbl, createSQL, [][]string{
		{"1", mysqlDoc},
		{"2", mysqlDoc},
		{"3", `"abc"`},
		{"4", `null`},
	}, map[string]string{baseline.MetaKeyBinlogFile: "binlog.000001", baseline.MetaKeyBinlogPos: "100"})

	at := func(offset time.Duration) string { return base.Add(offset).Format("2006-01-02 15:04:05") }
	// UPDATE row 2 with the same document (a no-op on meta); INSERT a string
	// scalar, a document with a null member and a bool, a bool scalar and a
	// JSON null scalar.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, at(10*time.Second), nil, schema, tbl, 2, "2", nil,
		[]byte(`{"id":2,"meta":`+imageDoc+`}`), []byte(`{"id":2,"meta":`+imageDoc+`}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, at(20*time.Second), nil, schema, tbl, 1, "5", nil,
		nil, []byte(`{"id":5,"meta":`+scalar(`"abc"`)+`}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, at(30*time.Second), nil, schema, tbl, 1, "6", nil,
		nil, []byte(`{"id":6,"meta":{"z":true,"k":null}}`))
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, at(40*time.Second), nil, schema, tbl, 1, "7", nil,
		nil, []byte(`{"id":7,"meta":`+scalar(`true`)+`}`))
	testutil.InsertEvent(t, db, "binlog.000001", 500, 600, at(50*time.Second), nil, schema, tbl, 1, "8", nil,
		nil, []byte(`{"id":8,"meta":`+scalar(`null`)+`}`))

	warehouse := t.TempDir()
	o := runOne(t, Config{
		IndexDSN:       testutil.IntegrationDSN(dbName),
		BaselineSrc:    baseDir,
		Warehouse:      warehouse,
		Tables:         []string{schema + "." + tbl},
		At:             base.Add(20 * time.Minute),
		ArchiveFetcher: parquetquery.Fetch,
	})
	if o.Verdict != VerdictLoaded || o.RowsLoaded != 4 || o.Events != 5 || o.Upserts != 5 {
		t.Fatalf("run = %+v, want loaded: 4 rows, 5 events, 5 upserts (%s)", o, o.Detail)
	}

	icetbl := loadExported(t, warehouse, schema, tbl)
	got := scanByID(t, icetbl, "meta")
	want := map[string]string{
		"1": canon,   // loaded: MySQL's text, re-encoded
		"2": canon,   // rewritten by the delta with the same document
		"3": `"abc"`, // loaded string scalar keeps its quotes
		"4": `null`,  // loaded JSON null keeps its literal
		"5": `"abc"`, // delta string scalar: the decoded text, re-emitted
		"6": `{"k":null,"z":true}`,
		"7": `true`, // delta bool scalar: the decoded text, re-emitted
		"8": `null`, // delta JSON null: a value, not SQL NULL
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %d", got, len(want))
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("row %s meta = %q, want %q", id, got[id], w)
		}
	}

	// The load recorded which columns it rendered as JSON, in the table.
	props := icetbl.Properties()
	if props[propJSONColumns] != "meta" {
		t.Fatalf("%s = %q, want meta", propJSONColumns, props[propJSONColumns])
	}
}

// loadExported opens the warehouse (taking the single-writer lock for the
// rest of the test, so call it once) and loads the exported table.
func loadExported(t *testing.T, warehouse, schema, tbl string) *table.Table {
	t.Helper()
	ctx := context.Background()
	cat, release, err := openWarehouse(ctx, warehouse)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	icetbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(schema, tbl))
	if err != nil {
		t.Fatalf("load %s.%s: %v", schema, tbl, err)
	}
	return icetbl
}

// scanByID reads one string column through iceberg-go, keyed by the id
// column's text.
func scanByID(t *testing.T, icetbl *table.Table, col string) map[string]string {
	t.Helper()
	at, err := icetbl.Scan().ToArrowTable(context.Background())
	if err != nil {
		t.Fatalf("iceberg-go scan: %v", err)
	}
	defer at.Release()
	out := map[string]string{}
	tr := array.NewTableReader(at, 1024)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idIdx, colIdx := -1, -1
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "id":
				idIdx = i
			case col:
				colIdx = i
			}
		}
		if idIdx < 0 || colIdx < 0 {
			t.Fatalf("schema %v lacks id or %s", rec.Schema().Fields(), col)
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			v := "NULL"
			if !rec.Column(colIdx).IsNull(i) {
				v = rec.Column(colIdx).ValueStr(i)
			}
			out[rec.Column(idIdx).ValueStr(i)] = v
		}
	}
	return out
}
