//go:build integration

package icebergexport

import (
	"context"
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
// from a row image (decoded, then re-encoded). Rows 1 and 2 hold the same
// document; row 2 is then rewritten by an UPDATE carrying that same document
// in its row image, so the two rows must read byte for byte the same. Row 3
// and row 5 are the same string scalar from each side.
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
	// holds: keys in MySQL's order, `", "` and `": "` separators, `<` as is.
	const mysqlDoc = `{"b": 1, "a": [1, 2], "s": "<x>&y", "n": 1.5}`
	// The same document as a row image carries it (whatever spacing the
	// binlog decoder used; the text is parsed either way). The index stores
	// row images in a MySQL JSON column, which rewrites a number the way it
	// parsed it (`1.50` comes back `1.5`; the source does the same in its
	// own JSON column), so the fixture uses a number MySQL keeps as written.
	const imageDoc = `{"b":1,"a":[1,2],"s":"<x>&y","n":1.5}`
	const canon = `{"a":[1,2],"b":1,"n":1.5,"s":"<x>&y"}`

	baseDir := t.TempDir()
	writeBaseline(t, baseDir, base, schema, tbl, createSQL, [][]string{
		{"1", mysqlDoc},
		{"2", mysqlDoc},
		{"3", `"abc"`},
		{"4", `null`},
	}, map[string]string{baseline.MetaKeyBinlogFile: "binlog.000001", baseline.MetaKeyBinlogPos: "100"})

	at := func(offset time.Duration) string { return base.Add(offset).Format("2006-01-02 15:04:05") }
	// UPDATE row 2 with the same document (a no-op on meta), INSERT a string
	// scalar and a document with a null member and a bool.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, at(10*time.Second), nil, schema, tbl, 2, "2", nil,
		[]byte(`{"id":2,"meta":`+imageDoc+`}`), []byte(`{"id":2,"meta":`+imageDoc+`}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, at(20*time.Second), nil, schema, tbl, 1, "5", nil,
		nil, []byte(`{"id":5,"meta":"abc"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, at(30*time.Second), nil, schema, tbl, 1, "6", nil,
		nil, []byte(`{"id":6,"meta":{"z":true,"k":null}}`))

	warehouse := t.TempDir()
	o := runOne(t, Config{
		IndexDSN:       testutil.IntegrationDSN(dbName),
		BaselineSrc:    baseDir,
		Warehouse:      warehouse,
		Tables:         []string{schema + "." + tbl},
		At:             base.Add(20 * time.Minute),
		ArchiveFetcher: parquetquery.Fetch,
	})
	if o.Verdict != VerdictLoaded || o.RowsLoaded != 4 || o.Events != 3 || o.Upserts != 3 {
		t.Fatalf("run = %+v, want loaded: 4 rows, 3 events, 3 upserts (%s)", o, o.Detail)
	}

	got := scanColumnByID(t, warehouse, schema, tbl, "meta")
	want := map[string]string{
		"1": canon,   // loaded: MySQL's text, re-encoded
		"2": canon,   // rewritten by the delta with the same document
		"3": `"abc"`, // loaded string scalar keeps its quotes
		"4": `null`,  // loaded JSON null keeps its literal
		"5": `"abc"`, // delta string scalar is quoted, not copied bare
		"6": `{"k":null,"z":true}`,
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %d", got, len(want))
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("row %s meta = %q, want %q", id, got[id], w)
		}
	}
}

// scanColumnByID reads one string column of the exported table through
// iceberg-go, keyed by the id column's text.
func scanColumnByID(t *testing.T, warehouse, schema, tbl, col string) map[string]string {
	t.Helper()
	ctx := context.Background()
	cat, release, err := openWarehouse(ctx, warehouse)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	icetbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(schema, tbl))
	if err != nil {
		t.Fatalf("load %s.%s: %v", schema, tbl, err)
	}
	return scanByID(t, icetbl, col)
}

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
