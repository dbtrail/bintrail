package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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
