package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/icebergexport"
)

func resetExportGlobals(t *testing.T) {
	t.Helper()
	sDSN, sDir, sS3, sWh, sTables, sAt, sBatch, sFormat := eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat
	t.Cleanup(func() {
		eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat = sDSN, sDir, sS3, sWh, sTables, sAt, sBatch, sFormat
		exportIcebergCmd.SetOut(nil)
	})
	eiIndexDSN, eiBaselineDir, eiBaselineS3, eiWarehouse, eiTables, eiAt, eiFetchBatch, eiFormat = "", "", "", "", "", "", 0, "text"
}

// TestRunExportIceberg_flagValidation: each refusal fires before anything
// is opened, and the positive control proves the checks were PASSED and the
// run reached the index (an unreachable DSN fails on connect, not on a flag).
func TestRunExportIceberg_flagValidation(t *testing.T) {
	valid := func() {
		eiIndexDSN = "u:p@tcp(127.0.0.1:1)/db?timeout=200ms"
		eiBaselineDir = t.TempDir()
		eiBaselineS3 = ""
		eiWarehouse = t.TempDir()
		eiTables = "shop.orders"
		eiAt = ""
		eiFetchBatch = 0
		eiFormat = "text"
	}
	cases := []struct {
		name string
		mut  func()
		want string
	}{
		{"no dsn", func() { eiIndexDSN = "" }, "--index-dsn is required"},
		{"no baseline", func() { eiBaselineDir = "" }, "one of --baseline-dir or --baseline-s3"},
		{"both baselines", func() { eiBaselineS3 = "s3://b/p/" }, "mutually exclusive"},
		{"no warehouse", func() { eiWarehouse = "" }, "--warehouse is required"},
		{"bad format", func() { eiFormat = "yaml" }, "--format must be text or json"},
		{"negative batch", func() { eiFetchBatch = -1 }, "--fetch-batch-size must be >= 0"},
		{"bad at", func() { eiAt = "yesterday-ish" }, "--at"},
		{"table without schema", func() { eiTables = "orders" }, "must be schema.table"},
		{"tables all blank", func() { eiTables = " , " }, "no entries"},
		{"positive control: connect refused", func() {}, "connect to index"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetExportGlobals(t)
			valid()
			tc.mut()
			exportIcebergCmd.SetContext(context.Background())
			exportIcebergCmd.SetOut(&bytes.Buffer{})
			err := runExportIceberg(exportIcebergCmd, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestWriteExportJSON_shape(t *testing.T) {
	at := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	err := writeExportJSON(&buf, []icebergexport.Outcome{
		{Schema: "shop", Table: "orders", Verdict: icebergexport.VerdictExported, Detail: "3 events", Events: 3, Upserts: 2, Deletes: 1, SnapshotID: 42, Cursor: "binlog.000001:700 at 2026-08-28T12:00:00Z", Location: "/w/shop/orders"},
		{Schema: "shop", Table: "customers", Verdict: icebergexport.VerdictRefusedGap, Detail: "gap"},
	}, "/w", at)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	if got["warehouse"] != "/w" || got["at"] != "2026-08-28T12:00:00Z" {
		t.Fatalf("top level = %v", got)
	}
	tables := got["tables"].([]any)
	if len(tables) != 2 {
		t.Fatalf("tables = %d", len(tables))
	}
	first := tables[0].(map[string]any)
	for _, k := range []string{"schema", "table", "verdict", "detail", "rows_loaded", "events", "upserts", "deletes", "snapshot_id", "cursor", "location"} {
		if _, ok := first[k]; !ok {
			t.Errorf("exported table lacks key %q: %v", k, first)
		}
	}
	if first["verdict"] != "exported" || first["events"].(float64) != 3 || first["snapshot_id"].(float64) != 42 {
		t.Fatalf("first = %v", first)
	}
	second := tables[1].(map[string]any)
	if second["verdict"] != "refused-gap" {
		t.Fatalf("second = %v", second)
	}
	if _, ok := second["snapshot_id"]; ok {
		t.Fatal("a refused table must not carry a snapshot_id")
	}
}

func TestExportIceberg_envReachesWarehouseFlag(t *testing.T) {
	resetExportGlobals(t)
	dir := t.TempDir()
	t.Setenv("BINTRAIL_ICEBERG_WAREHOUSE", dir)
	bindCommandEnv(exportIcebergCmd)
	if eiWarehouse != dir {
		t.Fatalf("eiWarehouse = %q, want the env value %q", eiWarehouse, dir)
	}
}
