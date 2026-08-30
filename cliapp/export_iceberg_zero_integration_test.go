//go:build integration

package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationAuditContract_ExportIceberg_zeroRowLoad: a first load of a
// baseline with zero rows commits the table and its cursor and no data
// file, so the table has no Iceberg snapshot. The audit event still fires
// (something durable was written) with rows 0 and NO snapshot_id, and the
// JSON outcome is a loaded table with a cursor and a location and no
// snapshot_id. Before #1509 both carried snapshot_id 0, an id no snapshot
// has.
func TestIntegrationAuditContract_ExportIceberg_zeroRowLoad(t *testing.T) {
	rec := audittest.Install(t)
	resetExportGlobals(t)

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Hour)
	ts := base.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, ts, "shop", "empty", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "shop", "empty", "status", 2, "", "varchar", "YES")
	createSQL := "CREATE TABLE `empty` (\n  `id` int NOT NULL,\n  `status` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	baseDir := t.TempDir()
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	// The schema and no rows: what a dump of an empty table produces.
	w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", "empty.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: createSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      "100",
			"bintrail.snapshot_timestamp":  base.Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}

	eiIndexDSN = testutil.IntegrationDSN(dbName)
	eiBaselineDir = baseDir
	eiWarehouse = t.TempDir()
	eiTables = "shop.empty"
	eiAt = base.Add(20 * time.Minute).Format(time.RFC3339)
	eiFormat = "json"
	exportIcebergCmd.SetContext(context.Background())
	var out bytes.Buffer
	exportIcebergCmd.SetOut(&out)
	if err := runExportIceberg(exportIcebergCmd, nil); err != nil {
		t.Fatalf("export iceberg: %v\n%s", err, out.String())
	}

	var got exportJSON
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if len(got.Tables) != 1 {
		t.Fatalf("tables = %+v, want one", got.Tables)
	}
	tb := got.Tables[0]
	// The cursor of a zero-row load is the BASELINE anchor, not the --at
	// instant: it is where the next run's deltas start.
	wantCursor := "binlog.000001:100 at " + base.Format(time.RFC3339)
	wantLocation := filepath.Join(eiWarehouse, "shop", "empty")
	if tb.Verdict != "loaded" || tb.RowsLoaded != 0 || tb.Cursor != wantCursor || tb.Location != wantLocation {
		t.Fatalf("table = %+v, want loaded with 0 rows, cursor %q and location %q", tb, wantCursor, wantLocation)
	}
	if tb.SnapshotID != nil {
		t.Fatalf("table = %+v, want no snapshot_id", tb)
	}
	if strings.Contains(out.String(), "snapshot_id") {
		t.Fatalf("json output names a snapshot_id for a table without a snapshot:\n%s", out.String())
	}

	evs := rec.Events()
	if len(evs) != 1 {
		t.Fatalf("audit events = %d, want one (the load): %+v", len(evs), evs)
	}
	ev := evs[0]
	if ev.Surface != "cli" || ev.Action != "export.iceberg" || ev.Schema != "shop" || ev.Table != "empty" {
		t.Fatalf("event = %+v", ev)
	}
	if ev.Detail["commit"] != "load" || ev.Detail["rows"] != "0" || ev.Detail["cursor"] != wantCursor || ev.Detail["location"] != wantLocation {
		t.Fatalf("event detail = %v, want a load of 0 rows with cursor %q and location %q", ev.Detail, wantCursor, wantLocation)
	}
	if v, ok := ev.Detail["snapshot_id"]; ok {
		t.Fatalf("event detail carries snapshot_id %q for a table without a snapshot", v)
	}

	// The next thing an operator hits: a delta run against the table that
	// has no snapshot yet. The first INSERT gives it one, and the audit event
	// and the JSON outcome must name the SAME id.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, base.Add(25*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"shop", "empty", 1, "1", nil, nil, []byte(`{"id":1,"status":"new"}`))
	rec.Reset()
	out.Reset()
	eiAt = base.Add(30 * time.Minute).Format(time.RFC3339)
	if err := runExportIceberg(exportIcebergCmd, nil); err != nil {
		t.Fatalf("second export: %v\n%s", err, out.String())
	}
	got = exportJSON{}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if len(got.Tables) != 1 {
		t.Fatalf("tables = %+v, want one", got.Tables)
	}
	tb = got.Tables[0]
	if tb.Verdict != "exported" || tb.Events != 1 || tb.Upserts != 1 || tb.SnapshotID == nil || *tb.SnapshotID == 0 {
		t.Fatalf("second run table = %+v, want exported with 1 event, 1 upsert and a snapshot id", tb)
	}
	if tb.Cursor != "binlog.000001:200 at "+base.Add(30*time.Minute).Format(time.RFC3339) {
		t.Fatalf("second run cursor = %q, want the cut at binlog.000001:200", tb.Cursor)
	}
	evs = rec.Events()
	if len(evs) != 1 || evs[0].Detail["commit"] != "delta" || evs[0].Detail["rows"] != "1" {
		t.Fatalf("second run events = %+v, want one delta commit of 1 row", evs)
	}
	if want := strconv.FormatInt(*tb.SnapshotID, 10); evs[0].Detail["snapshot_id"] != want {
		t.Fatalf("audit snapshot_id = %q, JSON snapshot_id = %s; they must name the same snapshot", evs[0].Detail["snapshot_id"], want)
	}
}
