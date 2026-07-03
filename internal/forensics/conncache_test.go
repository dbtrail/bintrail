package forensics

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

// TestCleanupConnectionCache_SubSecondRetention guards #8: a positive
// sub-second --attribution-retention must not truncate to INTERVAL 0 SECOND
// (which would DELETE every row, including live sessions). It is rounded up to
// the minimum meaningful window of 1 second (last_seen is second-precision).
func TestCleanupConnectionCache_SubSecondRetention(t *testing.T) {
	tests := []struct {
		name      string
		retention time.Duration
		wantSecs  int64
	}{
		{"half a second rounds up to 1", 500 * time.Millisecond, 1},
		{"one nanosecond rounds up to 1", time.Nanosecond, 1},
		{"exactly one second", time.Second, 1},
		{"24h is exact", 24 * time.Hour, 86400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			mock.ExpectExec("DELETE FROM connection_cache").
				WithArgs(tt.wantSecs).
				WillReturnResult(sqlmock.NewResult(0, 0))

			if err := cleanupConnectionCache(context.Background(), db, tt.retention); err != nil {
				t.Fatalf("cleanupConnectionCache: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("retention interval mismatch — sub-second must not become INTERVAL 0: %v", err)
			}
		})
	}
}

// signalingArg is a sqlmock argument matcher that closes fired (once) when the
// expected bound value is matched, giving a test a happens-before hook to know
// a query has reached the driver — without racing on sqlmock's internals.
type signalingArg struct {
	want  int64
	fired chan struct{}
	once  sync.Once
}

func (s *signalingArg) Match(v driver.Value) bool {
	if n, ok := v.(int64); ok && n == s.want {
		s.once.Do(func() { close(s.fired) })
		return true
	}
	return false
}

// TestSweepLoop_ImmediateSweepThenExitsOnCancel guards the fast-lane coverage
// of #3's audit-present branch (the end-to-end behavior is pinned by the
// integration test, which needs a MySQL container). sweepLoop must run one
// retention sweep immediately — pruning the pre-audit backlog without waiting a
// full cleanupInterval — and then exit promptly when the context is cancelled.
func TestSweepLoop_ImmediateSweepThenExitsOnCancel(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Exactly one DELETE is expected: the immediate sweep. cleanupInterval is an
	// hour, so the ticker cannot fire within the test window.
	sig := &signalingArg{want: 86400, fired: make(chan struct{})} // 24h in seconds
	mock.ExpectExec("DELETE FROM connection_cache").
		WithArgs(sig).
		WillReturnResult(sqlmock.NewResult(0, 0))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sweepLoop(ctx, db, 24*time.Hour)
		close(done)
	}()

	select {
	case <-sig.fired: // the immediate sweep issued its DELETE
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("sweepLoop did not run an immediate sweep before blocking on its ticker")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sweepLoop did not exit promptly on context cancel")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected sweep queries — only the immediate sweep should run: %v", err)
	}
}
