//go:build integration

package mcptools

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// writeReconstructBaseline writes a one-row baseline snapshot in the layout
// FindBaseline expects: <baseDir>/<RFC3339-with-hyphens>/<schema>/<table>.parquet.
func writeReconstructBaseline(t *testing.T, baseDir, schema, table string, at time.Time, idVal, nameVal string) {
	t.Helper()
	dir := filepath.Join(baseDir, strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-"), schema)
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

// seedReconstructIndex builds an index with a schema snapshot (id is PK), and
// three deltas on app.users pk=1: UPDATE→alicia (12:00), UPDATE→alex (13:00),
// DELETE (14:00). A baseline at 00:00 holds id=1/name=alice.
func seedReconstructIndex(t *testing.T) (*sql.DB, string, string) {
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
	writeReconstructBaseline(t, baseDir, "app", "users",
		time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "1", "alice")
	return db, dbName, baseDir
}

// reconstructSession wires the standalone posture (baseline parameters
// accepted) over the seeded index and connects an in-memory MCP client — the
// same transport pattern the other MCP integration tests use.
func reconstructSession(t *testing.T, db *sql.DB, dbName string) *mcp.ClientSession {
	t.Helper()
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	cfg := Config{
		Version:             "test",
		Reconstruct:         true,
		AllowBaselineParams: true,
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{DB: db, DBName: dbName, Resolver: resolver, ResolverLoaded: true}, nil
		},
	}

	ctx := context.Background()
	clientT, serverT := mcp.NewInMemoryTransports()
	ss, err := NewServer(cfg).Connect(ctx, serverT, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "2025-06-18"}, nil)
	cs, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func callReconstructTool(t *testing.T, cs *mcp.ClientSession, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "reconstruct", Arguments: args})
	if err != nil {
		t.Fatalf("CallTool reconstruct: %v", err)
	}
	return res
}

func decodeReconstruct(t *testing.T, res *mcp.CallToolResult) reconstructResult {
	t.Helper()
	text := resultText(res)
	if res.IsError {
		t.Fatalf("reconstruct returned a tool error: %s", text)
	}
	var out reconstructResult
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decode reconstruct payload: %v (payload=%s)", err, text)
	}
	return out
}

// TestIntegrationReconstructToolPointInTime walks the fold across the window.
// allow_gaps is required because InitIndexTables creates only p_future, so the
// planner classifies the whole window as a coverage gap — the refusal itself is
// asserted separately below.
func TestIntegrationReconstructToolPointInTime(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	cs := reconstructSession(t, db, dbName)

	base := func(at string) map[string]any {
		return map[string]any{
			"schema": "app", "table": "users", "pk": "1",
			"at": at, "baseline_dir": baseDir, "allow_gaps": true,
		}
	}

	// Just after the baseline, before any delta → the baseline value. A row
	// never touched in the window is exactly what `recover` cannot resolve.
	r := decodeReconstruct(t, callReconstructTool(t, cs, base("2026-06-01 00:00:01")))
	if !r.Found || r.Deleted || r.State["name"] != "alice" {
		t.Errorf("at 00:00:01: found=%v deleted=%v name=%v, want alice", r.Found, r.Deleted, r.State["name"])
	}
	// The opted-into gap must still be reported, never swallowed.
	if len(r.Warnings) == 0 {
		t.Error("allow_gaps=true must still surface the coverage-gap warning")
	}

	// After the first UPDATE, and after the second.
	if r := decodeReconstruct(t, callReconstructTool(t, cs, base("2026-06-01 12:30:00"))); r.State["name"] != "alicia" {
		t.Errorf("at 12:30: name=%v, want alicia", r.State["name"])
	}
	if r := decodeReconstruct(t, callReconstructTool(t, cs, base("2026-06-01 13:30:00"))); r.State["name"] != "alex" {
		t.Errorf("at 13:30: name=%v, want alex", r.State["name"])
	}

	// After the DELETE: existed, then gone — distinct from "never existed".
	r = decodeReconstruct(t, callReconstructTool(t, cs, base("2026-06-01 15:00:00")))
	if !r.Found || !r.Deleted || r.State != nil {
		t.Errorf("at 15:00: found=%v deleted=%v state=%v, want found+deleted with no state", r.Found, r.Deleted, r.State)
	}

	// A pk with no baseline row and no deltas: never present in the window.
	never := base("2026-06-01 15:00:00")
	never["pk"] = "999"
	r = decodeReconstruct(t, callReconstructTool(t, cs, never))
	if r.Found || r.Deleted {
		t.Errorf("pk=999: found=%v deleted=%v, want neither", r.Found, r.Deleted)
	}
}

// TestIntegrationReconstructToolHistory pins history mode: the baseline entry
// plus every transition, with the DELETE flagged.
func TestIntegrationReconstructToolHistory(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	cs := reconstructSession(t, db, dbName)

	r := decodeReconstruct(t, callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 15:00:00", "baseline_dir": baseDir,
		"history": true, "allow_gaps": true,
	}))

	if !r.Found {
		t.Fatal("history: expected found=true")
	}
	if r.EventCount != 3 {
		t.Errorf("history: event_count=%d, want 3", r.EventCount)
	}
	if len(r.History) != 4 {
		t.Fatalf("history: got %d entries, want 4 (baseline + 3 deltas): %+v", len(r.History), r.History)
	}
	if r.History[0].Source != "baseline" || r.History[0].State["name"] != "alice" {
		t.Errorf("history[0]: source=%q name=%v, want baseline/alice", r.History[0].Source, r.History[0].State["name"])
	}
	if r.History[1].State["name"] != "alicia" || r.History[2].State["name"] != "alex" {
		t.Errorf("history middle: %v then %v, want alicia then alex",
			r.History[1].State["name"], r.History[2].State["name"])
	}
	last := r.History[3]
	if !last.Deleted || last.State != nil {
		t.Errorf("history[3]: deleted=%v state=%v, want a flagged DELETE with no state", last.Deleted, last.State)
	}
	// The baseline entry is not a DELETE even though its predecessor is absent.
	if r.History[0].Deleted {
		t.Error("history[0]: the baseline entry must never be flagged deleted")
	}
}

// TestIntegrationReconstructToolGapRefusedByDefault pins the strict default: a
// coverage gap aborts with an actionable error instead of returning a silently
// partial row state, and the remediation names the allow_gaps parameter (not
// the CLI's --allow-gaps flag, which an MCP client cannot pass).
func TestIntegrationReconstructToolGapRefusedByDefault(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	cs := reconstructSession(t, db, dbName)

	res := callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 13:30:00", "baseline_dir": baseDir,
		// allow_gaps deliberately omitted — false is the default.
	})
	if !res.IsError {
		t.Fatalf("expected a gap refusal, got: %s", resultText(res))
	}
	text := resultText(res)
	if !strings.Contains(text, "allow_gaps: true") {
		t.Errorf("gap error should point at the allow_gaps parameter; got %q", text)
	}
	if strings.Contains(text, "--allow-gaps") {
		t.Errorf("gap error must not hand an MCP client a CLI flag; got %q", text)
	}
}

// TestIntegrationReconstructToolMissingBaseline pins that a baseline location
// with no snapshot for the table refuses with a remediation, rather than
// folding the deltas onto nothing and reporting a partial state as complete.
func TestIntegrationReconstructToolMissingBaseline(t *testing.T) {
	db, dbName, _ := seedReconstructIndex(t)
	cs := reconstructSession(t, db, dbName)

	res := callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 13:30:00", "baseline_dir": t.TempDir(), "allow_gaps": true,
	})
	if !res.IsError {
		t.Fatalf("expected a missing-baseline refusal, got: %s", resultText(res))
	}
	if text := resultText(res); !strings.Contains(text, "bintrail baseline") {
		t.Errorf("missing-baseline error should name `bintrail baseline`; got %q", text)
	}

	// No baseline source at all is a different, equally actionable refusal.
	t.Setenv("BINTRAIL_BASELINE_DIR", "")
	t.Setenv("BINTRAIL_BASELINE_S3", "")
	res = callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1", "at": "2026-06-01 13:30:00",
	})
	if !res.IsError {
		t.Fatalf("expected a no-baseline-source refusal, got: %s", resultText(res))
	}
	if text := resultText(res); !strings.Contains(text, "baseline_dir") {
		t.Errorf("no-source error should name the baseline_dir parameter; got %q", text)
	}
}

// TestIntegrationReconstructToolEventCapRefused pins that an over-cap window is
// refused rather than folded from a truncated event prefix — wrong state, not
// merely incomplete.
func TestIntegrationReconstructToolEventCapRefused(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	cs := reconstructSession(t, db, dbName)

	orig := MaxReconstructEvents
	MaxReconstructEvents = 2
	t.Cleanup(func() { MaxReconstructEvents = orig })

	res := callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 15:00:00", "baseline_dir": baseDir, "allow_gaps": true,
	})
	if !res.IsError {
		t.Fatalf("expected an over-cap refusal for 3 events against a cap of 2, got: %s", resultText(res))
	}
	if text := resultText(res); !strings.Contains(text, "too many events") {
		t.Errorf("over-cap error text = %q", text)
	}
}

// stampCaptureGap records a PERMANENT capture loss (#765) in stream_state — the
// state `bintrail status` renders as "GAP LOST": events that no longer exist in
// live MySQL, in an archive, or anywhere else.
func stampCaptureGap(t *testing.T, db *sql.DB, at, detail string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, last_checkpoint, server_id, gap_lost_at, gap_lost_detail)
		VALUES (1, 'position', 'bin.000001', 120, '2026-06-01 15:00:00', 1, ?, ?)`, at, detail); err != nil {
		t.Fatalf("stamp capture gap: %v", err)
	}
}

// TestIntegrationReconstructToolCaptureGapRefusedByDefault pins the refusal an
// MCP client gets over a permanently-lost window: it must name the tool
// parameter that overrides it, never the CLI's `--allow-gaps`, which is the
// only wording the shared reconstruct helper knows.
func TestIntegrationReconstructToolCaptureGapRefusedByDefault(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	stampCaptureGap(t, db, "2026-06-01 12:30:00", "binlogs purged before the stream caught up")
	cs := reconstructSession(t, db, dbName)

	res := callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 13:30:00", "baseline_dir": baseDir,
	})
	if !res.IsError {
		t.Fatalf("expected a capture-gap refusal, got: %s", resultText(res))
	}
	text := resultText(res)
	if !strings.Contains(text, "capture gap") {
		t.Errorf("the refusal should say what it refused on; got %q", text)
	}
	if !strings.Contains(text, "allow_gaps: true") {
		t.Errorf("the refusal must name the tool parameter that overrides it; got %q", text)
	}
	if strings.Contains(text, "--allow-gaps") {
		t.Errorf("the refusal must not hand an MCP client the CLI flag; got %q", text)
	}
}

// TestIntegrationReconstructToolCaptureGapOverrideWarns is the other half, and
// the one that was actually broken: with allow_gaps the fold proceeds, and the
// permanent loss it folded over MUST come back in the payload. The CLI's
// operator sees the equivalent slog.Warn on stderr; an MCP client sees only
// this JSON, so a missing warning reports a known-incomplete state as clean.
func TestIntegrationReconstructToolCaptureGapOverrideWarns(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	stampCaptureGap(t, db, "2026-06-01 12:30:00", "binlogs purged before the stream caught up")
	cs := reconstructSession(t, db, dbName)

	res := callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 13:30:00", "baseline_dir": baseDir, "allow_gaps": true,
	})
	out := decodeReconstruct(t, res)
	if !out.Found {
		t.Fatalf("expected the fold to proceed under allow_gaps, got: %+v", out)
	}
	var captureWarning string
	for _, w := range out.Warnings {
		if strings.HasPrefix(w, "capture_gap: ") {
			captureWarning = w
		}
	}
	if captureWarning == "" {
		t.Fatalf("an overridden capture gap must surface as a capture_gap warning, got warnings: %v", out.Warnings)
	}
	if !strings.Contains(captureWarning, "2026-06-01T12:30:00Z") {
		t.Errorf("the warning must say when capture was lost; got %q", captureWarning)
	}
	if !strings.Contains(captureWarning, "binlogs purged") {
		t.Errorf("the warning must carry the recorded detail; got %q", captureWarning)
	}
}

// TestIntegrationReconstructToolLegacyIndexGapUnevaluable covers the index the
// console actually serves: registry servers are never migrated (EnsureSchema is
// false there), so an index predating the gap_lost_* columns reaches this tool
// with gap state that was never evaluated. Reading that as "no gap" made the
// guard inert on precisely those servers; it must refuse instead, and say why.
func TestIntegrationReconstructToolLegacyIndexGapUnevaluable(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	if _, err := db.Exec(`ALTER TABLE stream_state DROP COLUMN gap_lost_at, DROP COLUMN gap_lost_detail`); err != nil {
		t.Fatalf("simulate a legacy index: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, 'position', 'bin.000001', 120, '2026-06-01 15:00:00', 1)`); err != nil {
		t.Fatalf("seed legacy stream_state: %v", err)
	}
	cs := reconstructSession(t, db, dbName)

	res := callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 13:30:00", "baseline_dir": baseDir,
	})
	if !res.IsError {
		t.Fatalf("expected a refusal when gap state is not evaluable, got: %s", resultText(res))
	}
	if text := resultText(res); !strings.Contains(text, "NOT EVALUABLE") {
		t.Errorf("the refusal must say the gap state could not be evaluated; got %q", text)
	}

	// The override still works, and still reports what it overrode.
	res = callReconstructTool(t, cs, map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 13:30:00", "baseline_dir": baseDir, "allow_gaps": true,
	})
	out := decodeReconstruct(t, res)
	var found bool
	for _, w := range out.Warnings {
		if strings.HasPrefix(w, "capture_gap: ") {
			found = true
		}
	}
	if !found {
		t.Errorf("an overridden unevaluable verdict must still warn, got warnings: %v", out.Warnings)
	}
}
