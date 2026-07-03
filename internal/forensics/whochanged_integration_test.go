//go:build integration

package forensics

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// insertWhoChangedEvent seeds one binlog_events row with the forensic columns
// (connection_id, query_text) that testutil.InsertEvent predates.
func insertWhoChangedEvent(t *testing.T, db *sql.DB, ts string, schema, table string, pk string, connID int64, queryText string) {
	t.Helper()
	var qt any
	if queryText != "" {
		qt = queryText
	}
	testutil.MustExec(t, db, `INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, gtid, connection_id,
		 schema_name, table_name, event_type, pk_values, row_before, row_after, query_text)
		VALUES ('binlog.000001', 100, 200, ?, NULL, ?, ?, ?, 2, ?, '{"id":1}', '{"id":1}', ?)`,
		ts, connID, schema, table, pk, qt)
}

// fetchVia adapts a query.Engine to the WhoChanged fetch seam (the CLI wires
// query.FetchMerged on top; archive behavior is that pipeline's tested
// concern, not this engine's).
func fetchVia(eng *query.Engine) func(context.Context, query.Options) ([]query.ResultRow, error) {
	return func(ctx context.Context, opts query.Options) ([]query.ResultRow, error) {
		return eng.Fetch(ctx, opts)
	}
}

// TestIntegrationWhoChanged_LiveThenCacheCascade is the acceptance path for
// the tier cascade against a real server:
//
//	(a) an event whose connection is still live attributes from
//	    performance_schema (exact), with #712 query_text surfaced;
//	(b) after the session dies, the SAME event attributes from
//	    connection_cache (corroborated) — identity outliving the session;
//	(c) an event with an unknown connection id degrades to binlog-only with
//	    an explanatory note and fallback SQL, never an error.
//
// The audit tier is unit-fixture territory (the test container runs no audit
// plugin), and the GTID tier disables itself unless gtid_mode=ON.
func TestIntegrationWhoChanged_LiveThenCacheCascade(t *testing.T) {
	indexDB, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB := integrationSourceDB(t)

	// A dedicated victim session pinned to one connection, so its
	// PROCESSLIST_ID is a stable, known connection id.
	victim, err := sql.Open("mysql", sourceDSN()+"?parseTime=true")
	if err != nil {
		t.Fatalf("open victim connection: %v", err)
	}
	victimClosed := false
	defer func() {
		if !victimClosed {
			victim.Close()
		}
	}()
	victim.SetMaxOpenConns(1)
	victim.SetMaxIdleConns(1)
	var victimID int64
	if err := victim.QueryRow("SELECT CONNECTION_ID()").Scan(&victimID); err != nil {
		t.Fatalf("read victim connection id: %v", err)
	}

	// Two events "captured" now: one by the victim session (with the original
	// statement, as if binlog_rows_query_log_events had been ON), one by a
	// connection id that never existed.
	const ghostID = int64(4_000_000_000)
	const stmt = "UPDATE orders SET status = 'shipped' WHERE id = 42"
	ts := time.Now().UTC().Format("2006-01-02 15:04:05")
	insertWhoChangedEvent(t, indexDB, ts, dbName, "orders", "42", victimID, stmt)
	insertWhoChangedEvent(t, indexDB, ts, dbName, "orders", "43", ghostID, "")

	deps := WhoChangedDeps{
		Fetch:    fetchVia(query.New(indexDB)),
		SourceDB: sourceDB,
		IndexDB:  indexDB,
	}
	params := WhoChangedParams{Schema: dbName, Table: "orders"}
	ctx := context.Background()

	// ── (a) live attribution from performance_schema ────────────────────────
	res, err := WhoChanged(ctx, deps, params)
	if err != nil {
		t.Fatalf("WhoChanged (live): %v", err)
	}
	if res.TotalCount != 2 {
		t.Fatalf("TotalCount = %d, want 2", res.TotalCount)
	}
	if !res.AppliedDefaultWindow {
		t.Error("AppliedDefaultWindow = false for an unbounded call")
	}

	byPK := map[string]WhoChangedEvent{}
	for _, ev := range res.Events {
		byPK[ev.PKValues] = ev
	}

	live := byPK["42"]
	if live.Attribution == nil {
		t.Fatalf("live event not attributed; result: %+v", res)
	}
	if live.Attribution.Source != AttributionSourcePerfSchema {
		t.Errorf("live source = %q, want %q", live.Attribution.Source, AttributionSourcePerfSchema)
	}
	// Corroborated, not exact: the live performance_schema tier reports the
	// current holder of the connection id, whose session lifetime is not bounded
	// against the event (a reused id could be a different actor). Only the audit
	// CONNECT..DISCONNECT and GTID tiers earn "exact".
	if live.Attribution.Confidence != ConfidenceCorroborated {
		t.Errorf("live confidence = %q, want %q", live.Attribution.Confidence, ConfidenceCorroborated)
	}
	if live.Attribution.User != "root" {
		t.Errorf("live user = %q, want root", live.Attribution.User)
	}
	if live.QueryText == nil || *live.QueryText != stmt {
		t.Errorf("query_text not surfaced on the live event: %v", live.QueryText)
	}

	// ── (c) binlog-only degradation for the ghost id ────────────────────────
	ghost := byPK["43"]
	if ghost.Attribution != nil {
		t.Errorf("ghost event attributed to %+v; want binlog-only", ghost.Attribution)
	}
	joined := strings.Join(res.Notes, "\n")
	if !strings.Contains(joined, "1 of 2") {
		t.Errorf("degradation note missing '1 of 2': %v", res.Notes)
	}
	if len(res.FallbackQueries) == 0 {
		t.Error("no fallback SQL for the unresolved ghost id")
	}

	// ── (b) cache fallback after the session dies ────────────────────────────
	pollerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := StartConnCachePoller(pollerCtx, ConnCacheConfig{
		SourceDSN: sourceDSN(),
		IndexDSN:  testutil.IntegrationDSN(dbName),
		Retention: DefaultRetention,
	})
	waitForCachedThread(t, indexDB, victimID, 20*time.Second)

	victim.Close() // performance_schema forgets the session immediately
	victimClosed = true

	res, err = WhoChanged(ctx, deps, params)
	if err != nil {
		t.Fatalf("WhoChanged (cache): %v", err)
	}
	byPK = map[string]WhoChangedEvent{}
	for _, ev := range res.Events {
		byPK[ev.PKValues] = ev
	}
	cachedEv := byPK["42"]
	if cachedEv.Attribution == nil {
		t.Fatal("dead session's event lost attribution — the cache tier did not fire")
	}
	if cachedEv.Attribution.Source != AttributionSourceConnCache {
		t.Errorf("post-disconnect source = %q, want %q", cachedEv.Attribution.Source, AttributionSourceConnCache)
	}
	if cachedEv.Attribution.Confidence != ConfidenceCorroborated {
		t.Errorf("post-disconnect confidence = %q, want %q", cachedEv.Attribution.Confidence, ConfidenceCorroborated)
	}
	if cachedEv.Attribution.User != "root" {
		t.Errorf("cached user = %q, want root", cachedEv.Attribution.User)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("poller did not shut down after cancel")
	}
}

// TestIntegrationWhoChanged_IndexOnlyMode: without a source DB, only the
// index-side tiers run. A cached identity still resolves (deterministically
// seeded), an unknown id degrades to binlog-only, and nothing errors.
func TestIntegrationWhoChanged_IndexOnlyMode(t *testing.T) {
	indexDB, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	// Seed connection_cache directly — the deterministic equivalent of the
	// poller having seen a session that later disconnected.
	testutil.MustExec(t, indexDB, `INSERT INTO connection_cache
		(connection_id, user, host, db, command, connection_attributes, cached_at, last_seen)
		VALUES (777, 'app_rw', '10.9.8.7:55123', 'shop', 'Sleep', '{"program_name":"payroll-svc"}', NOW(), NOW())`)

	ts := time.Now().UTC().Format("2006-01-02 15:04:05")
	insertWhoChangedEvent(t, indexDB, ts, dbName, "orders", "1", 777, "")
	insertWhoChangedEvent(t, indexDB, ts, dbName, "orders", "2", 4_000_000_001, "")

	res, err := WhoChanged(context.Background(), WhoChangedDeps{
		Fetch:   fetchVia(query.New(indexDB)),
		IndexDB: indexDB,
	}, WhoChangedParams{Schema: dbName, Table: "orders"})
	if err != nil {
		t.Fatalf("WhoChanged (index-only): %v", err)
	}

	byPK := map[string]WhoChangedEvent{}
	for _, ev := range res.Events {
		byPK[ev.PKValues] = ev
	}
	cached := byPK["1"]
	if cached.Attribution == nil {
		t.Fatal("cached identity did not resolve in index-only mode")
	}
	if cached.Attribution.Source != AttributionSourceConnCache ||
		cached.Attribution.User != "app_rw" ||
		cached.Attribution.ClientProgram != "payroll-svc" {
		t.Errorf("cache attribution wrong: %+v", cached.Attribution)
	}
	if unknown := byPK["2"]; unknown.Attribution != nil {
		t.Errorf("unknown id attributed in index-only mode: %+v", unknown.Attribution)
	}
	if len(res.FallbackQueries) == 0 {
		t.Error("no fallback SQL for the unresolved id")
	}
}
