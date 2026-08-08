//go:build integration

package console

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// writeBaselineParquet writes a one-row baseline snapshot in the layout
// FindBaseline expects: <baseDir>/<RFC3339-with-hyphens>/<schema>/<table>.parquet.
func writeBaselineParquet(t *testing.T, baseDir, schema, table string, at time.Time, idVal, nameVal string, meta ...map[string]string) {
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
	cfg := baseline.WriterConfig{Compression: "none", RowGroupSize: 100}
	if len(meta) > 0 {
		cfg.Metadata = meta[0]
	}
	w, err := baseline.NewWriter(filepath.Join(dir, table+".parquet"), cols, cfg)
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
func seedReconstruct(t *testing.T) *Server { return seedReconstructMeta(t, nil) }

// seedReconstructMeta is seedReconstruct with optional key-value metadata
// stamped into the baseline Parquet footer (#921: LSN + render-GUCs stamp).
func seedReconstructMeta(t *testing.T, baselineMeta map[string]string) *Server {
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
	// id=2 has NO baseline row — it is INSERTed after the baseline. Reconstruct
	// must still return its state (the post-baseline-INSERT case).
	testutil.InsertEvent(t, db, "bin.000001", 120, 160, "2026-06-01 11:00:00", nil, "app", "users", 1, "2",
		nil, nil, []byte(`{"id":2,"name":"bob"}`))
	// id=3 carries a residual unchanged-TOAST marker (#592) in its row image —
	// used ONLY by TestIntegrationReconstructToastMarkerRefused (every other test
	// queries pk=1/2/999, so this row is invisible to them).
	testutil.InsertEvent(t, db, "bin.000001", 160, 200, "2026-06-01 11:30:00", nil, "app", "users", 1, "3",
		nil, nil, []byte(`{"id":3,"name":{"__bintrail_unchanged_toast__":true}}`))

	baseDir := t.TempDir()
	writeBaselineParquet(t, baseDir, "app", "users", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "1", "alice", baselineMeta)

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	if srv.cm.boot == nil || !srv.cm.boot.baselineConfigured {
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
// #921: the render_gucs_mismatch warning must reach the response through the
// REAL handler path (handleReconstruct → ReadParquetMetadataAny →
// appendRenderGUCsWarning) — the unit test pins only the helper contract, so
// unwiring the append in handleReconstruct must go red HERE.
func TestIntegrationReconstructRenderGUCsWarning(t *testing.T) {
	srv := seedReconstructMeta(t, map[string]string{
		baseline.MetaKeyLSN:        "42",
		baseline.MetaKeyRenderGUCs: "TimeZone=America/New_York;DateStyle=SQL;extra_float_digits=0;bytea_output=escape;IntervalStyle=sql_standard",
	})
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2000:00:01&allow_gaps=true")
	found := false
	for _, wmsg := range r.Warnings {
		if strings.HasPrefix(wmsg, "render_gucs_mismatch: ") {
			found = true
		}
	}
	if !found {
		t.Fatalf("render_gucs_mismatch warning missing from the handler response: %v", r.Warnings)
	}

	// A MySQL baseline (no LSN anchor) must not raise it.
	srv2 := seedReconstruct(t)
	r2 := reconstructAt(t, srv2, "schema=app&table=users&pk=1&at=2026-06-01%2000:00:01&allow_gaps=true")
	for _, wmsg := range r2.Warnings {
		if strings.HasPrefix(wmsg, "render_gucs_mismatch") {
			t.Fatalf("MySQL baseline must not raise the GUC warning: %v", r2.Warnings)
		}
	}
}

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
	// The allow_gaps override surfaces the skipped-coverage warning so the
	// operator knows the result may be incomplete (the window is all gap here).
	if len(r.Warnings) == 0 {
		t.Error("at 12:30 with allow_gaps: expected a coverage-gap warning in the response")
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
	// The refusal must name the non-lossy remedy, not just the checkbox
	// override (#1275) — deleting the hint leaves the user with only the
	// lossy exit.
	if !strings.Contains(string(body), "archive reconcile --repair") {
		t.Errorf("gap 422 must name the reconcile remedy, got body: %s", body)
	}
}

func TestIntegrationReconstructUnknownPK(t *testing.T) {
	srv := seedReconstruct(t)
	// pk=999 has no baseline row AND no deltas in the window → genuinely never
	// existed → clean found=false, not a 500. (allow_gaps=true because the
	// handler now fetches deltas before deciding, and the window is all gap.)
	r := reconstructAt(t, srv, "schema=app&table=users&pk=999&allow_gaps=true")
	if r.Found {
		t.Errorf("unknown pk: found=%v, want false", r.Found)
	}
}

// TestIntegrationReconstructPostBaselineInsert: a row created AFTER the baseline
// has no baseline entry but still exists as of T — it must reconstruct from the
// deltas, not be mislabeled "not found". (Regression guard for the review fix.)
func TestIntegrationReconstructPostBaselineInsert(t *testing.T) {
	srv := seedReconstruct(t)
	r := reconstructAt(t, srv, "schema=app&table=users&pk=2&at=2026-06-01%2012:00:00&allow_gaps=true")
	if !r.Found || r.Deleted {
		t.Fatalf("post-baseline INSERT (pk=2): found=%v deleted=%v, want found+not-deleted", r.Found, r.Deleted)
	}
	if got := fmt.Sprint(r.State["name"]); got != "bob" {
		t.Errorf("post-baseline INSERT (pk=2): name=%v, want bob", got)
	}
}

// TestIntegrationReconstructHistoryDeleted exercises the history path through a
// DELETE: the final entry must be a DELETE transition with deleted=true / state
// nil, while the baseline entry is NOT mislabeled deleted.
func TestIntegrationReconstructHistoryDeleted(t *testing.T) {
	srv := seedReconstruct(t)
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2015:00:00&history=true&allow_gaps=true")
	if len(r.History) != 4 {
		t.Fatalf("history len=%d, want 4 (baseline + 2 updates + delete)", len(r.History))
	}
	if r.History[0].Deleted {
		t.Error("baseline entry must not be labeled deleted")
	}
	last := r.History[len(r.History)-1]
	if last.Source != "DELETE" || !last.Deleted || last.State != nil {
		t.Errorf("last entry: source=%q deleted=%v state=%v, want DELETE/true/nil", last.Source, last.Deleted, last.State)
	}
}

// TestIntegrationReconstructEventCap: more events than the cap in [baseline, at]
// must refuse (422) rather than fold from a truncated prefix.
func TestIntegrationReconstructEventCap(t *testing.T) {
	srv := seedReconstruct(t)
	orig := reconstructMaxEvents
	reconstructMaxEvents = 2 // pk=1 has 3 deltas through 15:00
	defer func() { reconstructMaxEvents = orig }()

	rec, body := doReq(t, srv, "GET", "/api/reconstruct?schema=app&table=users&pk=1&at=2026-06-01%2015:00:00&allow_gaps=true", "")
	if rec.Code != 422 {
		t.Errorf("event cap exceeded: code=%d, want 422 (body=%s)", rec.Code, body)
	}
}

// TestIntegrationReconstructToastMarkerRefused: a residual unchanged-TOAST
// marker (#592) in a folded event must refuse with 422 (the captured history is
// not usable — same class as the gap/cap refusals above), NOT 500, in both
// state and history modes. The body must carry the marker message so the 422 is
// attributable to the capture-invariant violation, not the coverage gap
// (allow_gaps=true rules the gap out).
func TestIntegrationReconstructToastMarkerRefused(t *testing.T) {
	srv := seedReconstruct(t)
	for _, mode := range []string{"", "&history=true"} {
		rec, body := doReq(t, srv, "GET",
			"/api/reconstruct?schema=app&table=users&pk=3&at=2026-06-01%2012:00:00&allow_gaps=true"+mode, "")
		if rec.Code != 422 {
			t.Errorf("toast marker (mode=%q): code=%d, want 422 (body=%s)", mode, rec.Code, body)
			continue
		}
		for _, want := range []string{"unresolved unchanged-TOAST marker", "capture invariant violated", "name"} {
			if !strings.Contains(string(body), want) {
				t.Errorf("toast marker (mode=%q): body missing %q: %s", mode, want, body)
			}
		}
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

// TestIntegrationReconstructEnumLabels pins #476 on the console surface:
// the Time-travel response must render ENUM deltas as labels — decoded
// with the snapshot in effect at the event's time (#475) — matching the
// representation baseline rows already carry. Pre-#476 the same row
// answered status=3 here and status='shipped' through the shim.
func TestIntegrationReconstructEnumLabels(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Typed snapshot rows (testutil.InsertSnapshot predates column_type).
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable)
		VALUES (1, '2026-06-01 00:00:00', 'app', 'orders', 'id', 1, 'PRI', 'int', 'int', 'NO'),
		       (1, '2026-06-01 00:00:00', 'app', 'orders', 'status', 2, '', 'enum', 'enum(''pending'',''processing'',''shipped'')', 'NO')`)

	// Post-baseline UPDATE: ordinals, exactly as the binlog stores them.
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil, "app", "orders", 2, "1",
		[]byte(`["status"]`), []byte(`{"id":1,"status":1}`), []byte(`{"id":1,"status":3}`))

	// Baseline row carries the LABEL (the mydumper dump shape).
	baseDir := t.TempDir()
	tsDir := strings.ReplaceAll(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), ":", "-")
	dir := filepath.Join(baseDir, tsDir, "app")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "enum", ParquetType: baseline.MysqlToParquetNode("enum")},
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "pending"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}

	// Before the delta: the baseline label passes through untouched.
	r := reconstructAt(t, srv, "schema=app&table=orders&pk=1&at=2026-06-01%2000:30:00&allow_gaps=true")
	if !r.Found || fmt.Sprint(r.State["status"]) != "pending" {
		t.Errorf("at 00:30: found=%v status=%v, want baseline label 'pending'", r.Found, r.State["status"])
	}

	// After the delta: the ordinal 3 must arrive as 'shipped', not 3.
	r = reconstructAt(t, srv, "schema=app&table=orders&pk=1&at=2026-06-01%2012:30:00&allow_gaps=true")
	if !r.Found || fmt.Sprint(r.State["status"]) != "shipped" {
		t.Errorf("at 12:30: found=%v status=%v, want mapped label 'shipped'", r.Found, r.State["status"])
	}

	// History: both entries carry labels (baseline + mapped delta).
	r = reconstructAt(t, srv, "schema=app&table=orders&pk=1&at=2026-06-01%2012:30:00&history=true&allow_gaps=true")
	if len(r.History) != 2 {
		t.Fatalf("history len=%d, want 2: %+v", len(r.History), r.History)
	}
	if fmt.Sprint(r.History[0].State["status"]) != "pending" || fmt.Sprint(r.History[1].State["status"]) != "shipped" {
		t.Errorf("history statuses = [%v, %v], want [pending, shipped]",
			r.History[0].State["status"], r.History[1].State["status"])
	}
}

// TestIntegrationReconstructBlobText pins #666 on the console Time-travel
// surface — the literal sibling of the ENUM test above, and the surface the
// issue names. A TEXT column is stored base64; the console reconstruct must
// return it decoded in both State and History, not as base64. Guards against a
// one-line deletion of the DecodeEventBinaries wiring silently reintroducing the
// bug. TEXT (not BLOB): a decoded BLOB []byte re-base64-encodes in the JSON
// response and would assert vacuously.
func TestIntegrationReconstructBlobText(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "docs", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "docs", "body", 2, "", "text", "YES")

	// Post-baseline UPDATE: body stored base64, as marshalRow encodes the []byte
	// go-mysql delivers for a TEXT column.
	b64 := base64.StdEncoding.EncodeToString([]byte("updated bio ☃"))
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil, "app", "docs", 2, "1",
		[]byte(`["body"]`),
		[]byte(`{"id":1,"body":"`+base64.StdEncoding.EncodeToString([]byte("baseline-bio"))+`"}`),
		[]byte(`{"id":1,"body":"`+b64+`"}`))

	baseDir := t.TempDir()
	tsDir := strings.ReplaceAll(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), ":", "-")
	dir := filepath.Join(baseDir, tsDir, "app")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "body", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "docs.parquet"), cols,
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "baseline-bio"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}

	// After the delta: the TEXT value must come back decoded, not base64.
	r := reconstructAt(t, srv, "schema=app&table=docs&pk=1&at=2026-06-01%2012:30:00&allow_gaps=true")
	if !r.Found || fmt.Sprint(r.State["body"]) != "updated bio ☃" {
		t.Errorf("at 12:30: found=%v body=%v, want decoded 'updated bio ☃'", r.Found, r.State["body"])
	}
	if fmt.Sprint(r.State["body"]) == b64 {
		t.Errorf("body came back as base64 %q (decode did not run)", b64)
	}

	// History: the delta entry carries the decoded value too (the toStateEntryDTOs
	// path the CLI E2E does not exercise).
	r = reconstructAt(t, srv, "schema=app&table=docs&pk=1&at=2026-06-01%2012:30:00&history=true&allow_gaps=true")
	if len(r.History) != 2 {
		t.Fatalf("history len=%d, want 2: %+v", len(r.History), r.History)
	}
	if fmt.Sprint(r.History[1].State["body"]) != "updated bio ☃" {
		t.Errorf("history[1] body = %v, want decoded 'updated bio ☃'", r.History[1].State["body"])
	}
}

// TestIntegrationReconstructBinaryPK pins #1157 on the console surface: a fixed
// BINARY(16) key whose stored value ends in 0x00 must resolve from the baseline
// by its trailing-0x00-stripped pk_values spelling — the spelling the events
// view displays and an operator copies. Before the fix the pad-and-retry lived
// only in the CLI, so this endpoint proceeded with baselineRow==nil into
// ApplyAt(nil, deltas, at) and answered found=false ("the row did not exist at
// that time") while `bintrail reconstruct` answered correctly.
//
// The delta half is the sharper edge: the event fetch matches pk_values, which
// wants the key spelled the OPPOSITE way from the baseline (stripped+uppercase
// vs padded). A lowercase or full-width hex key that resolves the baseline but
// fetches zero events would silently present baseline-era state as the state
// at `at` — so those spellings are asserted against the DELTA-applied value,
// which only comes back when both lookups resolve.
func TestIntegrationReconstructBinaryPK(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Typed snapshot rows: the declared binary(16) width is what the
	// pad-and-retry pads to (testutil.InsertSnapshot predates column_type).
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable)
		VALUES (1, '2026-06-01 00:00:00', 'app', 'vault', 'k',   1, 'PRI', 'binary',  'binary(16)',  'NO'),
		       (1, '2026-06-01 00:00:00', 'app', 'vault', 'val', 2, '',    'varchar', 'varchar(32)', 'YES')`)

	// Post-baseline UPDATE, keyed and imaged exactly as the indexer stores a
	// binary PK: pk_values carries the stripped+uppercased 0x spelling, the
	// row images carry the []byte value base64-encoded (marshalRow).
	kStripped, err := hex.DecodeString("11223344556677889900AABB")
	if err != nil {
		t.Fatal(err)
	}
	kB64 := base64.StdEncoding.EncodeToString(kStripped)
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil, "app", "vault", 2,
		"0x11223344556677889900AABB",
		[]byte(`["val"]`),
		[]byte(`{"k":"`+kB64+`","val":"sealed"}`),
		[]byte(`{"k":"`+kB64+`","val":"resealed"}`))

	// Baseline: the FULL storage width, padding included — what mydumper
	// --hex-blob dumps for a BINARY(16) column (the writer decodes the 0x
	// literal to raw bytes).
	baseDir := t.TempDir()
	tsDir := strings.ReplaceAll(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), ":", "-")
	dir := filepath.Join(baseDir, tsDir, "app")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "vault.parquet"), cols,
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"0x11223344556677889900AABB00000000", "sealed"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}

	// Before the delta, by the stored (stripped, uppercase) spelling: the
	// baseline value, via the pad-and-retry.
	r := reconstructAt(t, srv, "schema=app&table=vault&pk=0x11223344556677889900AABB&at=2026-06-01%2011:00:00&allow_gaps=true")
	if !r.Found || r.Deleted {
		t.Fatalf("stripped BINARY(16) spelling: found=%v deleted=%v, want the baseline row (#1157)", r.Found, r.Deleted)
	}
	if got := fmt.Sprint(r.State["val"]); got != "sealed" {
		t.Errorf("at 11:00: val=%v, want the baseline value sealed", got)
	}

	// After the delta, every legitimate spelling of the same key must return
	// the DELTA-applied state. "sealed" here means the baseline resolved but
	// the event fetch matched nothing — the silent fail-loud-to-fail-silent
	// regression this test exists to catch: both lookups must respell, each in
	// its own direction.
	for name, pk := range map[string]string{
		"stored spelling":     "0x11223344556677889900AABB",
		"lowercase spelling":  "0x11223344556677889900aabb",
		"full-width spelling": "0x11223344556677889900AABB00000000",
	} {
		r := reconstructAt(t, srv, "schema=app&table=vault&pk="+pk+"&at=2026-06-01%2013:00:00&allow_gaps=true")
		if !r.Found || r.Deleted {
			t.Errorf("%s at 13:00: found=%v deleted=%v, want the row", name, r.Found, r.Deleted)
			continue
		}
		if got := fmt.Sprint(r.State["val"]); got != "resealed" {
			t.Errorf("%s at 13:00: val=%v, want the delta-applied resealed (a baseline-era answer means the event fetch silently matched nothing)", name, got)
		}
	}
}
