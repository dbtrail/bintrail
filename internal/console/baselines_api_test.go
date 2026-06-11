package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// newBaselineServer builds a Server whose boot bundle points at a baseline
// source, without a DB (the baselines endpoint never touches the index).
func newBaselineServer(t *testing.T, src string, configured bool) *Server {
	t.Helper()
	s := &Server{token: "t", cm: newConnManager(nil, false)}
	s.cm.boot = &bundle{baselineSrc: src, baselineConfigured: configured}
	s.mux = s.buildHandler()
	return s
}

// writeBaselineFixture creates <dir>/<ts>/<schema>/<table>.parquet as an empty
// file — the lister is path-derived, and the (local-only) metadata enrichment
// must tolerate an unreadable Parquet footer by omitting the coordinates.
func writeBaselineFixture(t *testing.T, dir string, parts ...string) {
	t.Helper()
	p := filepath.Join(append([]string{dir}, parts...)...)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestBaselinesAPI_listsGroupedSnapshots(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-01T00-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "users.parquet")
	writeBaselineFixture(t, dir, "not-a-timestamp", "shop", "junk.parquet") // skipped
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "README")  // not .parquet

	srv := newBaselineServer(t, dir, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Configured || got.Kind != "dir" || got.Source != dir || !got.Reconstruct {
		t.Fatalf("header = %+v, want configured dir source with reconstruct on", got)
	}
	if len(got.Snapshots) != 2 {
		t.Fatalf("snapshots = %d (%+v), want 2", len(got.Snapshots), got.Snapshots)
	}
	newest, oldest := got.Snapshots[0], got.Snapshots[1]
	if newest.Time != "2026-06-10 12:00:00" || oldest.Time != "2026-06-01 00:00:00" {
		t.Fatalf("order = %s, %s — want newest first", newest.Time, oldest.Time)
	}
	if len(newest.Tables) != 2 || newest.Tables[0] != "shop.orders" || newest.Tables[1] != "shop.users" {
		t.Fatalf("newest tables = %v, want [shop.orders shop.users]", newest.Tables)
	}
	if len(oldest.Tables) != 1 || oldest.Tables[0] != "shop.orders" {
		t.Fatalf("oldest tables = %v, want [shop.orders]", oldest.Tables)
	}
	if newest.AgeHours <= 0 {
		t.Fatalf("age_hours = %v, want > 0", newest.AgeHours)
	}
	// Empty fixture files have no readable Parquet footer — coordinates omitted.
	if newest.BinlogFile != "" || newest.BinlogPos != 0 {
		t.Fatalf("binlog coords = %s:%d, want omitted for unreadable footers", newest.BinlogFile, newest.BinlogPos)
	}
}

func TestBaselinesAPI_notConfigured(t *testing.T) {
	srv := newBaselineServer(t, "", false)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Configured || got.Reconstruct || got.Snapshots == nil || len(got.Snapshots) != 0 {
		t.Fatalf("got %+v, want unconfigured with an empty (non-null) snapshot list", got)
	}
}

func TestBaselinesAPI_missingDirFailsLoud(t *testing.T) {
	srv := newBaselineServer(t, filepath.Join(t.TempDir(), "nope"), true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 502 {
		t.Fatalf("code = %d (body %s), want 502 for an unreadable configured source", rec.Code, body)
	}
}

// TestBaselinesAPI_truncation: the listing is a recency view — beyond the cap
// it must keep the NEWEST snapshots and say so, and exactly-at-cap must not
// claim truncation.
func TestBaselinesAPI_truncation(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mkDir := func(n int) string {
		dir := t.TempDir()
		for i := range n {
			name := base.Add(time.Duration(i) * time.Hour).Format("2006-01-02T15-04-05Z")
			writeBaselineFixture(t, dir, name, "shop", "orders.parquet")
		}
		return dir
	}

	srv := newBaselineServer(t, mkDir(baselinesMaxSnapshots+1), true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Snapshots) != baselinesMaxSnapshots || !got.Truncated {
		t.Fatalf("snapshots = %d truncated = %v, want exactly %d truncated", len(got.Snapshots), got.Truncated, baselinesMaxSnapshots)
	}
	wantNewest := base.Add(time.Duration(baselinesMaxSnapshots) * time.Hour).Format("2006-01-02 15:04:05")
	if got.Snapshots[0].Time != wantNewest {
		t.Fatalf("snapshots[0] = %s, want the NEWEST %s — truncation must drop the oldest", got.Snapshots[0].Time, wantNewest)
	}

	srv = newBaselineServer(t, mkDir(baselinesMaxSnapshots), true)
	_, body = doServersReq(t, srv, "GET", "/api/baselines", "")
	// Fresh struct: truncated is omitempty, so a false response would leave
	// the first subtest's stale true in place through Unmarshal.
	got = baselinesResponse{}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Snapshots) != baselinesMaxSnapshots || got.Truncated {
		t.Fatalf("at exactly the cap: snapshots = %d truncated = %v, want %d untruncated", len(got.Snapshots), got.Truncated, baselinesMaxSnapshots)
	}
}

// TestBaselinesAPI_metadataEnrichment: a real Parquet footer's binlog
// coordinates must surface in the DTO (the happy half of the best-effort
// enrichment — the error half is covered by the empty-file fixtures above).
func TestBaselinesAPI_metadataEnrichment(t *testing.T) {
	dir := t.TempDir()
	snap := filepath.Join(dir, "2026-06-10T12-00-00Z", "shop")
	if err := os.MkdirAll(snap, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(snap, "orders.parquet"),
		[]baseline.Column{{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)}},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyBinlogFile: "binlog.000042",
			baseline.MetaKeyBinlogPos:  "12345",
			baseline.MetaKeyGTIDSet:    "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100",
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	srv := newBaselineServer(t, dir, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Snapshots) != 1 {
		t.Fatalf("snapshots = %+v, want 1", got.Snapshots)
	}
	sn := got.Snapshots[0]
	if sn.BinlogFile != "binlog.000042" || sn.BinlogPos != 12345 || sn.GTIDSet != "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-100" {
		t.Fatalf("coords = %s:%d gtid=%s, want the written footer metadata", sn.BinlogFile, sn.BinlogPos, sn.GTIDSet)
	}
}
