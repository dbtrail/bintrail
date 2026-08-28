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
			baseline.MetaKeyBinlogPos:      "4",
			"bintrail.snapshot_timestamp":  base.Format(time.RFC3339),
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

	evs := rec.Events()
	if len(evs) != 1 {
		t.Fatalf("audit events = %d, want exactly one for the one exported table: %+v", len(evs), evs)
	}
	ev := evs[0]
	if ev.Surface != "cli" || ev.Action != "export.iceberg" || ev.Schema != "shop" || ev.Table != "orders" {
		t.Fatalf("event = %+v", ev)
	}
	if ev.Actor == "" || ev.Time.IsZero() {
		t.Fatalf("event lacks actor or time: %+v", ev)
	}
	if ev.Detail["rows_loaded"] != "2" || ev.Detail["events"] != "1" || ev.Detail["snapshot_id"] == "" || ev.Detail["snapshot_id"] == "0" {
		t.Fatalf("event detail = %v, want the committed counts and a snapshot id", ev.Detail)
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

	audittest.CheckCoverage(t, audittest.OwnerExportCLI, []audittest.Pair{{Surface: ev.Surface, Action: ev.Action}})
}

func resetExportGlobals(t *testing.T) {
	t.Helper()
	sDSN, sDir, sS3, sWh, sTables, sAt, sBatch, sFormat := eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat
	t.Cleanup(func() {
		eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat = sDSN, sDir, sS3, sWh, sTables, sAt, sBatch, sFormat
		exportIcebergCmd.SetOut(nil)
	})
	eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat = "", "", "", "", "", "", 0, "text"
}
