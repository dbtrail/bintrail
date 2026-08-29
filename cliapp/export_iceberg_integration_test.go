//go:build integration

package cliapp

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationAuditContract_ExportIceberg is the OwnerExportCLI contract
// case: the real command, against the integration MySQL and a baseline
// Parquet, must emit cli/export.iceberg once per table whose data was
// written, and nothing for a run that wrote nothing.
func TestIntegrationAuditContract_ExportIceberg(t *testing.T) {
	rec := audittest.Install(t)
	resetExportGlobals(t)

	db, dbName := testutil.CreateTestDB(t)
	// The production DDL, not testutil's single-p_future stand-in: the query
	// planner derives an hour's coverage from the PARTITION list, so a table
	// with no hourly partitions reads as "every hour is a gap".
	if err := indexer.CreateIndexTables(context.Background(), db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Hour) // inside the current hour: the test index only has p_future
	ts := base.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, ts, "shop", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "shop", "orders", "status", 2, "", "varchar", "YES")
	createSQL := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	baseDir := t.TempDir()
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", "orders.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: createSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			// The anchor is where the first indexed event STARTS: a dump records
			// the position its next transaction begins at, and the export reads
			// a first event past the anchor as unproven coverage (#781).
			baseline.MetaKeyBinlogPos:     "100",
			"bintrail.snapshot_timestamp": base.Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "new"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, base.Add(10*time.Second).Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 2, "2", nil, []byte(`{"id":2,"status":"new"}`), []byte(`{"id":2,"status":"paid"}`))

	eiIndexDSN = testutil.IntegrationDSN(dbName)
	eiBaselineDir = baseDir
	eiWarehouse = t.TempDir()
	eiTables = "shop.orders"
	eiAt = base.Add(20 * time.Minute).Format(time.RFC3339)
	eiFormat = "json"
	exportIcebergCmd.SetContext(context.Background())
	var out bytes.Buffer
	exportIcebergCmd.SetOut(&out)

	if err := runExportIceberg(exportIcebergCmd, nil); err != nil {
		t.Fatalf("export iceberg: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), `"verdict": "loaded"`) {
		t.Fatalf("json output lacks the loaded verdict:\n%s", out.String())
	}

	// One event per COMMIT: the load and the delta window are two.
	evs := rec.Events()
	if len(evs) != 2 {
		t.Fatalf("audit events = %d, want two (load, delta): %+v", len(evs), evs)
	}
	for _, ev := range evs {
		if ev.Surface != "cli" || ev.Action != "export.iceberg" || ev.Schema != "shop" || ev.Table != "orders" {
			t.Fatalf("event = %+v", ev)
		}
		if ev.Actor == "" || ev.Time.IsZero() {
			t.Fatalf("event lacks actor or time: %+v", ev)
		}
		if ev.Detail["snapshot_id"] == "" || ev.Detail["snapshot_id"] == "0" || ev.Detail["cursor"] == "" || ev.Detail["location"] == "" {
			t.Fatalf("event detail = %v, want a snapshot id, a cursor and a location", ev.Detail)
		}
	}
	if evs[0].Detail["commit"] != "load" || evs[0].Detail["rows"] != "2" {
		t.Fatalf("first event = %v, want the load of 2 rows", evs[0].Detail)
	}
	if evs[1].Detail["commit"] != "delta" || evs[1].Detail["events"] != "1" || evs[1].Detail["rows"] != "1" {
		t.Fatalf("second event = %v, want the delta of 1 event / 1 upsert", evs[1].Detail)
	}

	// A second run with nothing new writes nothing and must not emit.
	rec.Reset()
	out.Reset()
	eiAt = base.Add(30 * time.Minute).Format(time.RFC3339)
	if err := runExportIceberg(exportIcebergCmd, nil); err != nil {
		t.Fatalf("second export: %v\n%s", err, out.String())
	}
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("an unchanged run emitted %d audit event(s); nothing was written", n)
	}

	// The steady state: one more event, one delta commit, one audit event.
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, base.Add(35*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 3, "1", nil, []byte(`{"id":1,"status":"new"}`), nil)
	rec.Reset()
	out.Reset()
	eiAt = base.Add(40 * time.Minute).Format(time.RFC3339)
	if err := runExportIceberg(exportIcebergCmd, nil); err != nil {
		t.Fatalf("third export: %v\n%s", err, out.String())
	}
	evs = rec.Events()
	if len(evs) != 1 || evs[0].Detail["commit"] != "delta" || evs[0].Detail["deletes"] != "1" {
		t.Fatalf("third run events = %+v, want one delta commit with one delete", evs)
	}

	audittest.CheckCoverage(t, audittest.OwnerExportCLI, []audittest.Pair{{Surface: evs[0].Surface, Action: evs[0].Action}})
}
