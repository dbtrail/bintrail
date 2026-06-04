//go:build integration

package console

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/testutil"
)

// writeBaselineParquet writes a one-row baseline snapshot in the layout
// FindBaseline expects: <baseDir>/<RFC3339-with-hyphens>/<schema>/<table>.parquet.
func writeBaselineParquet(t *testing.T, baseDir, schema, table string, at time.Time, idVal, nameVal string) {
	t.Helper()
	tsDir := strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-")
	dir := filepath.Join(baseDir, tsDir, schema)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(filepath.Join(dir, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{idVal, nameVal}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// seedReconstruct builds an index with a snapshot (id is PK), a baseline row
// (id=1, name=alice at 00:00), and three deltas: UPDATE→alicia (12:00),
// UPDATE→alex (13:00), DELETE (14:00).
func seedReconstruct(t *testing.T) *Server {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "name", 2, "", "varchar", "YES")

	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil, "app", "users", 2, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"alicia"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 13:00:00", nil, "app", "users", 2, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alicia"}`), []byte(`{"id":1,"name":"alex"}`))
	testutil.InsertEvent(t, db, "bin.000001", 80, 120, "2026-06-01 14:00:00", nil, "app", "users", 3, "1",
		nil, []byte(`{"id":1,"name":"alex"}`), nil)

	baseDir := t.TempDir()
	writeBaselineParquet(t, baseDir, "app", "users", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "1", "alice")

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	if !srv.baselineConfigured {
		t.Fatal("expected reconstruct to be enabled with a baseline dir")
	}
	return srv
}

func reconstructAt(t *testing.T, srv *Server, qs string) reconstructResponse {
	t.Helper()
	rec, body := doReq(t, srv, "GET", "/api/reconstruct?"+qs, "")
	if rec.Code != 200 {
		t.Fatalf("reconstruct %q: code=%d body=%s", qs, rec.Code, body)
	}
	var resp reconstructResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	return resp
}

// allow_gaps=true is required in these tests because InitIndexTables creates
// only p_future, so the planner classifies the whole window as a coverage gap.
// A real deployment with hourly partitions would not need it. The gap-refusal
// behavior itself is asserted separately below.
func TestIntegrationReconstructValueAsOf(t *testing.T) {
	srv := seedReconstruct(t)

	// Just after the baseline, before any delta → the baseline value.
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2000:00:01&allow_gaps=true")
	if !r.Found || r.Deleted || fmt.Sprint(r.State["name"]) != "alice" {
		t.Errorf("at 00:00:01: found=%v deleted=%v name=%v, want alice", r.Found, r.Deleted, r.State["name"])
	}

	// After the first UPDATE → alicia.
	r = reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2012:30:00&allow_gaps=true")
	if fmt.Sprint(r.State["name"]) != "alicia" {
		t.Errorf("at 12:30: name=%v, want alicia", r.State["name"])
	}

	// After the second UPDATE → alex.
	r = reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true")
	if fmt.Sprint(r.State["name"]) != "alex" {
		t.Errorf("at 13:30: name=%v, want alex", r.State["name"])
	}

	// After the DELETE → deleted as of T (distinct from not-found).
	r = reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2015:00:00&allow_gaps=true")
	if !r.Found || !r.Deleted || r.State != nil {
		t.Errorf("at 15:00: found=%v deleted=%v state=%v, want found+deleted", r.Found, r.Deleted, r.State)
	}
}

func TestIntegrationReconstructHistory(t *testing.T) {
	srv := seedReconstruct(t)
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&history=true&allow_gaps=true")
	if len(r.History) != 3 {
		t.Fatalf("history len=%d, want 3 (baseline + 2 updates): %+v", len(r.History), r.History)
	}
	want := []struct{ source, name string }{
		{"baseline", "alice"},
		{"UPDATE", "alicia"},
		{"UPDATE", "alex"},
	}
	for i, w := range want {
		e := r.History[i]
		if e.Source != w.source || fmt.Sprint(e.State["name"]) != w.name {
			t.Errorf("history[%d]: source=%q name=%v, want %q %q", i, e.Source, e.State["name"], w.source, w.name)
		}
	}
}

// TestIntegrationReconstructGapRefused: without allow_gaps, a coverage gap
// between baseline and target must abort (422) rather than reconstruct a
// silently-incomplete row state. This is the safety default that distinguishes
// reconstruct from events/recover browsing.
func TestIntegrationReconstructGapRefused(t *testing.T) {
	srv := seedReconstruct(t)
	rec, body := doReq(t, srv, "GET", "/api/reconstruct?schema=app&table=users&pk=1&at=2026-06-01%2012:30:00", "")
	if rec.Code != 422 {
		t.Errorf("reconstruct over a gap without allow_gaps: code=%d, want 422 (body=%s)", rec.Code, body)
	}
}

func TestIntegrationReconstructUnknownPK(t *testing.T) {
	srv := seedReconstruct(t)
	// No baseline row for pk=999 → clean found=false, not a 500.
	r := reconstructAt(t, srv, "schema=app&table=users&pk=999&allow_gaps=true")
	if r.Found {
		t.Errorf("unknown pk: found=%v, want false", r.Found)
	}
}

func TestIntegrationReconstructCapability(t *testing.T) {
	srv := seedReconstruct(t)
	rec, body := doReq(t, srv, "GET", "/api/capabilities", "")
	if rec.Code != 200 {
		t.Fatalf("capabilities code=%d", rec.Code)
	}
	var caps capabilitiesResponse
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	if !caps.Reconstruct {
		t.Error("capabilities.reconstruct=false, want true (baseline configured)")
	}
}
