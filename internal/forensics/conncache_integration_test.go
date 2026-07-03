//go:build integration

package forensics

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// sourceDSN returns the test container's server-level DSN. The container is
// both the SOURCE (its performance_schema sees every test connection) and the
// index server, mirroring the smallest real deployment.
func sourceDSN() string {
	return testutil.BaseDSN() + "/"
}

// TestIntegrationConnCachePoller_IdentityOutlivesDisconnect is the headline
// acceptance path (#703): the poller caches a live session's identity, the
// session is killed, and the identity still resolves from connection_cache —
// exactly the window where performance_schema has already forgotten it.
func TestIntegrationConnCachePoller_IdentityOutlivesDisconnect(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// A dedicated "victim" session pinned to a single connection.
	victim, err := sql.Open("mysql", sourceDSN()+"?parseTime=true")
	if err != nil {
		t.Fatalf("open victim connection: %v", err)
	}
	victim.SetMaxOpenConns(1)
	victim.SetMaxIdleConns(1)
	var victimID int64
	if err := victim.QueryRow("SELECT CONNECTION_ID()").Scan(&victimID); err != nil {
		victim.Close()
		t.Fatalf("read victim connection id: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := StartConnCachePoller(ctx, ConnCacheConfig{
		SourceDSN: sourceDSN(),
		IndexDSN:  testutil.IntegrationDSN(dbName),
		Retention: DefaultRetention,
	})

	// The 500ms cadence should see the victim within a couple of ticks.
	waitForCachedThread(t, db, victimID, 20*time.Second)

	// Kill the session. performance_schema forgets it immediately; the cache
	// must not.
	victim.Close()

	cached, err := LookupCachedThreads(context.Background(), db, []int64{victimID})
	if err != nil {
		t.Fatalf("LookupCachedThreads after disconnect: %v", err)
	}
	ct, ok := cached[victimID]
	if !ok {
		t.Fatalf("connection %d no longer resolvable after disconnect — the cache did not outlive the session", victimID)
	}
	if ct.User != "root" {
		t.Errorf("cached user = %q, want %q", ct.User, "root")
	}
	if ct.Host == "" {
		t.Error("cached host is empty, want the client endpoint")
	}
	if ct.ConnectionID != victimID {
		t.Errorf("cached connection id = %d, want %d", ct.ConnectionID, victimID)
	}

	// An id that never existed stays absent (no phantom identities).
	absent, err := LookupCachedThreads(context.Background(), db, []int64{victimID + 1_000_000})
	if err != nil {
		t.Fatalf("LookupCachedThreads for absent id: %v", err)
	}
	if len(absent) != 0 {
		t.Errorf("lookup of a never-seen id returned %v, want empty", absent)
	}

	// Deterministic shutdown: cancelling the context closes done.
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("poller did not stop on context cancel")
	}
}

// TestIntegrationConnCacheRetentionSweep pins the D2 parameterized sweep: rows
// unseen for longer than the retention are deleted; fresh rows survive.
func TestIntegrationConnCacheRetentionSweep(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.MustExec(t, db, `INSERT INTO connection_cache
		(connection_id, user, host, db, command, cached_at, last_seen) VALUES
		(101, 'olduser', 'oldhost:1', NULL, 'Sleep', NOW() - INTERVAL 2 DAY, NOW() - INTERVAL 2 DAY),
		(102, 'newuser', 'newhost:2', 'app', 'Query', NOW(), NOW())`)

	if err := cleanupConnectionCache(context.Background(), db, 24*time.Hour); err != nil {
		t.Fatalf("cleanupConnectionCache: %v", err)
	}

	cached, err := LookupCachedThreads(context.Background(), db, []int64{101, 102})
	if err != nil {
		t.Fatalf("LookupCachedThreads: %v", err)
	}
	if _, ok := cached[101]; ok {
		t.Error("row unseen for 2 days survived a 24h retention sweep")
	}
	if _, ok := cached[102]; !ok {
		t.Error("fresh row was deleted by the retention sweep")
	}

	// A longer retention keeps everything: the window is a parameter, not the
	// SaaS's hardcoded 24h.
	testutil.MustExec(t, db, `INSERT INTO connection_cache
		(connection_id, user, host, db, command, cached_at, last_seen) VALUES
		(103, 'olduser2', 'oldhost:3', NULL, 'Sleep', NOW() - INTERVAL 2 DAY, NOW() - INTERVAL 2 DAY)`)
	if err := cleanupConnectionCache(context.Background(), db, 72*time.Hour); err != nil {
		t.Fatalf("cleanupConnectionCache (72h): %v", err)
	}
	cached, err = LookupCachedThreads(context.Background(), db, []int64{103})
	if err != nil {
		t.Fatalf("LookupCachedThreads: %v", err)
	}
	if _, ok := cached[103]; !ok {
		t.Error("2-day-old row was deleted under a 72h retention")
	}
}

// TestIntegrationHasAuditPlugin_NoneInstalled pins the probe against a real
// server with no audit plugin: it must report false so the poller runs.
func TestIntegrationHasAuditPlugin_NoneInstalled(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, err := sql.Open("mysql", sourceDSN()+"?parseTime=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if hasAuditPlugin(context.Background(), db) {
		t.Fatal("test container reports an active audit plugin; expected none (did the container image change?)")
	}
}

// TestIntegrationAuditPluginSkipsPoller pins the skip branch: with an audit
// plugin "present" (probe stubbed — the container has none to install), the
// poller must exit on its own without ever writing to connection_cache.
func TestIntegrationAuditPluginSkipsPoller(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	old := auditProbe
	auditProbe = func(context.Context, *sql.DB) bool { return true }
	defer func() { auditProbe = old }()

	done := StartConnCachePoller(context.Background(), ConnCacheConfig{
		SourceDSN: sourceDSN(),
		IndexDSN:  testutil.IntegrationDSN(dbName),
		Retention: DefaultRetention,
	})
	select {
	case <-done: // exits on its own — no context cancel involved
	case <-time.After(30 * time.Second):
		t.Fatal("poller did not exit after audit-plugin detection")
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM connection_cache").Scan(&n); err != nil {
		t.Fatalf("count connection_cache: %v", err)
	}
	if n != 0 {
		t.Fatalf("connection_cache has %d rows, want 0 — an audit plugin must suppress polling", n)
	}
}

// TestIntegrationConnCachePoller_RefreshesLastSeen pins the upsert half of
// the ON DUPLICATE KEY UPDATE: a connection seen across consecutive polls
// keeps ONE row whose last_seen advances (the property the retention sweep
// relies on to spare active sessions).
func TestIntegrationConnCachePoller_RefreshesLastSeen(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// The test's own index connection is a foreground session on the source
	// (same container), so the poller always has at least one row to upsert.
	var selfID int64
	if err := db.QueryRow("SELECT CONNECTION_ID()").Scan(&selfID); err != nil {
		t.Fatalf("read own connection id: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := StartConnCachePoller(ctx, ConnCacheConfig{
		SourceDSN: sourceDSN(),
		IndexDSN:  testutil.IntegrationDSN(dbName),
		Retention: DefaultRetention,
	})
	waitForCachedThread(t, db, selfID, 20*time.Second)

	var firstSeen time.Time
	if err := db.QueryRow(
		"SELECT last_seen FROM connection_cache WHERE connection_id = ?", selfID).
		Scan(&firstSeen); err != nil {
		t.Fatalf("read last_seen: %v", err)
	}

	// TIMESTAMP has 1s granularity; wait past it plus a few poll ticks.
	time.Sleep(2500 * time.Millisecond)

	var laterSeen time.Time
	var rowCount int
	if err := db.QueryRow(
		"SELECT last_seen, (SELECT COUNT(*) FROM connection_cache WHERE connection_id = ?) "+
			"FROM connection_cache WHERE connection_id = ?", selfID, selfID).
		Scan(&laterSeen, &rowCount); err != nil {
		t.Fatalf("re-read last_seen: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("connection %d has %d rows, want exactly 1 (upsert, not append)", selfID, rowCount)
	}
	if !laterSeen.After(firstSeen) {
		t.Errorf("last_seen did not advance across polls: first %v, later %v", firstSeen, laterSeen)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("poller did not stop on context cancel")
	}
}

// waitForCachedThread polls connection_cache until the given connection id
// appears or the deadline passes.
func waitForCachedThread(t *testing.T, indexDB *sql.DB, connID int64, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		cached, err := LookupCachedThreads(context.Background(), indexDB, []int64{connID})
		if err == nil {
			if _, ok := cached[connID]; ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection %d never appeared in connection_cache within %v (last err: %v)", connID, wait, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
