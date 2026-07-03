package forensics

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func TestEnrichSourceString(t *testing.T) {
	// The source string depends on how many identities came from live
	// performance_schema vs the connection_cache fallback.
	tests := []struct {
		name       string
		liveCount  int
		cacheCount int
		want       string
	}{
		{"all live", 5, 0, "performance_schema"},
		{"all cached", 0, 3, "connection_cache"},
		{"mixed", 2, 1, "performance_schema+connection_cache"},
		{"none found", 0, 0, "performance_schema"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := enrichSourceString(tc.liveCount, tc.cacheCount)
			if got != tc.want {
				t.Errorf("enrichSourceString(%d, %d) = %q, want %q",
					tc.liveCount, tc.cacheCount, got, tc.want)
			}
		})
	}
}

func TestLookupCachedThreads_EmptyIDs(t *testing.T) {
	// Empty ids must return (nil, nil) before any DB use — a nil *sql.DB
	// proves the fast path never touches the handle.
	result, err := LookupCachedThreads(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result for empty ids, got %v", result)
	}
}

func TestLookupCachedThreads_UnreachableDB(t *testing.T) {
	// A valid-looking but unreachable DB must surface an error, not nil.
	db := openLazyDB(t, "user:pass@tcp(127.0.0.1:39999)/db")
	result, err := LookupCachedThreads(context.Background(), db, []int64{42, 99})
	if err == nil {
		t.Fatal("expected error for unreachable DB")
	}
	if result != nil {
		t.Fatalf("expected nil result on error, got %v", result)
	}
}

func TestPollOnce_SourceUnreachable(t *testing.T) {
	// pollOnce must return an error (not panic) when the source is
	// unreachable. sql.Open doesn't connect; the error surfaces at query time.
	sourceDB := openLazyDB(t, "user:pass@tcp(127.0.0.1:39998)/db")
	indexDB := openLazyDB(t, "user:pass@tcp(127.0.0.1:39999)/db")

	if err := pollOnce(context.Background(), sourceDB, indexDB); err == nil {
		t.Fatal("expected error when source is unreachable")
	}
}

func TestUpsertBatch_Empty(t *testing.T) {
	// upsertBatch with an empty slice is a no-op — nil DB proves it never
	// reaches the handle.
	if err := upsertBatch(context.Background(), nil, nil, nil); err != nil {
		t.Fatalf("unexpected error for empty batch: %v", err)
	}
}

func TestHasAuditPlugin_Unreachable(t *testing.T) {
	// An unreachable source must return false (poller starts), never panic:
	// "could not check" degrades to capturing, not to an attribution gap.
	db := openLazyDB(t, "user:pass@tcp(127.0.0.1:39997)/db")
	if hasAuditPlugin(context.Background(), db) {
		t.Fatal("expected false for unreachable source")
	}
}

func TestStartConnCachePoller_DisabledRetention(t *testing.T) {
	// Retention <= 0 disables the poller entirely: the done channel is closed
	// before StartConnCachePoller returns and no connection is ever attempted
	// (the DSNs here would not even parse).
	for _, retention := range []time.Duration{0, -time.Hour} {
		done := StartConnCachePoller(context.Background(), ConnCacheConfig{
			SourceDSN: "not-a-dsn",
			IndexDSN:  "also-not-a-dsn",
			Retention: retention,
		})
		select {
		case <-done:
		default:
			t.Fatalf("retention %v: done channel not closed synchronously for a disabled poller", retention)
		}
	}
}

func TestStartConnCachePoller_BadSourceDSN(t *testing.T) {
	// An unparseable source DSN must be non-fatal: the goroutine logs and
	// exits, closing done — the caller (the stream) is never taken down.
	done := StartConnCachePoller(context.Background(), ConnCacheConfig{
		SourceDSN: "not-a-dsn",
		IndexDSN:  "also-not-a-dsn",
		Retention: DefaultRetention,
	})
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("poller did not exit on an unparseable source DSN")
	}
}

// openLazyDB opens a sql.DB without connecting (lazy connect) and registers
// cleanup. Errors surface at first use, which is what these tests exercise.
func openLazyDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open(%q): %v", dsn, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
