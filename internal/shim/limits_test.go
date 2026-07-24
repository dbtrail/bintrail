package shim

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/query"
)

// newLimitsHandler builds a Handler wired like the production one but
// with an inert archive fetcher, for the #823 timeout/cancel/gate
// tests. db may be nil for paths that never reach the index.
func newLimitsHandler(db *sql.DB, cfg Config) *Handler {
	return &Handler{
		indexDB: db,
		cfg:     cfg,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
			return nil, nil
		},
	}
}

// expectDelayedFetch registers the two queries FetchMerged issues on
// the NoArchive path — the planner's partition inspection and the
// binlog_events fetch — delaying the fetch so the query outlives the
// deadline / cancellation the test then applies.
func expectDelayedFetch(mock sqlmock.Sqlmock, fetchPattern string, delay time.Duration) {
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	mock.ExpectQuery(fetchPattern).
		WillDelayFor(delay).
		WillReturnRows(emptyBinlogEventsRows())
}

func TestGateSemantics(t *testing.T) {
	t.Run("nil_gate_is_unlimited", func(t *testing.T) {
		for _, n := range []int{0, -1} {
			if g := NewGate(n); g != nil {
				t.Errorf("NewGate(%d) = %v, want nil (unlimited)", n, g)
			}
		}
		var g *Gate
		if err := g.Acquire(context.Background()); err != nil {
			t.Errorf("nil Gate.Acquire = %v, want nil", err)
		}
		g.Release() // must not panic
		if got := g.Cap(); got != 0 {
			t.Errorf("nil Gate.Cap() = %d, want 0", got)
		}
	})

	t.Run("acquire_release_cycle", func(t *testing.T) {
		g := NewGate(1)
		if got := g.Cap(); got != 1 {
			t.Fatalf("Cap() = %d, want 1", got)
		}
		if err := g.Acquire(context.Background()); err != nil {
			t.Fatalf("first Acquire: %v", err)
		}
		// Saturated: a waiter must give up when its context dies.
		expired, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := g.Acquire(expired); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("saturated Acquire = %v, want DeadlineExceeded", err)
		}
		canceled, cancel2 := context.WithCancel(context.Background())
		cancel2()
		if err := g.Acquire(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("saturated Acquire (canceled ctx) = %v, want Canceled", err)
		}
		g.Release()
		if err := g.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire after Release: %v", err)
		}
	})
}

// TestQueryContextDefaults pins the backward-compat contract (#823): a
// Handler with no bound connection context and no QueryTimeout behaves
// exactly like the pre-#823 context.WithCancel(context.Background()).
func TestQueryContextDefaults(t *testing.T) {
	h := NewHandler(nil, nil)
	ctx, cancel := h.queryContext()
	defer cancel()
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		t.Error("default queryContext must carry no deadline")
	}
	select {
	case <-ctx.Done():
		t.Error("default queryContext must not be done")
	default:
	}

	h.cfg.QueryTimeout = time.Minute
	tctx, tcancel := h.queryContext()
	defer tcancel()
	if _, hasDeadline := tctx.Deadline(); !hasDeadline {
		t.Error("queryContext must carry a deadline when QueryTimeout is set")
	}

	connCtx, connCancel := context.WithCancel(context.Background())
	connCancel()
	h.BindConnContext(connCtx)
	cctx, ccancel := h.queryContext()
	defer ccancel()
	select {
	case <-cctx.Done():
	default:
		t.Error("queryContext must inherit cancellation from the bound connection context")
	}
}

// TestQueryTimeoutInterruptsPointLookup proves cfg.QueryTimeout reaps a
// slow fetch (#823): the index query is delayed far beyond the
// deadline, and the handler must return ER_QUERY_INTERRUPTED (1317)
// within the deadline's order of magnitude instead of blocking for the
// full fetch. Fails on the pre-#823 code (no deadline: the call blocks
// for the whole 10s delay and returns success).
func TestQueryTimeoutInterruptsPointLookup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectDelayedFetch(mock, "pk_hash = SHA2", 10*time.Second)

	h := newLimitsHandler(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: "bintrail_index",
		QueryTimeout: 100 * time.Millisecond,
	})

	q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "orders",
		PKColumn: "id", PKValue: "1", AsOf: time.Now().UTC()}
	start := time.Now()
	_, qerr := h.runPointInTime(q)
	elapsed := time.Since(start)

	if qerr == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	var myErr *gomysql.MyError
	if !errors.As(qerr, &myErr) || myErr.Code != gomysql.ER_QUERY_INTERRUPTED {
		t.Fatalf("want ER_QUERY_INTERRUPTED (1317), got %v", qerr)
	}
	if !strings.Contains(myErr.Message, "--query-timeout") {
		t.Errorf("timeout error must point the operator at --query-timeout; got %q", myErr.Message)
	}
	if elapsed > 5*time.Second {
		t.Errorf("query returned after %v; the deadline should have reaped it within ~100ms", elapsed)
	}
}

// TestConnContextCancelAbortsInFlightQuery proves BindConnContext wires
// client-disconnect cancellation into a running fetch (#823): canceling
// the bound context mid-flight must abort the delayed index query with
// ER_QUERY_INTERRUPTED naming the disconnect, long before the fetch's
// own duration.
func TestConnContextCancelAbortsInFlightQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectDelayedFetch(mock, "pk_hash = SHA2", 10*time.Second)

	h := newLimitsHandler(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: "bintrail_index",
	})
	connCtx, cancel := context.WithCancel(context.Background())
	h.BindConnContext(connCtx)
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "orders",
		PKColumn: "id", PKValue: "1", AsOf: time.Now().UTC()}
	start := time.Now()
	_, qerr := h.runPointInTime(q)
	elapsed := time.Since(start)

	var myErr *gomysql.MyError
	if !errors.As(qerr, &myErr) || myErr.Code != gomysql.ER_QUERY_INTERRUPTED {
		t.Fatalf("want ER_QUERY_INTERRUPTED (1317), got %v", qerr)
	}
	if !strings.Contains(myErr.Message, "client disconnected") {
		t.Errorf("cancellation error must name the disconnect; got %q", myErr.Message)
	}
	if elapsed > 5*time.Second {
		t.Errorf("query returned after %v; the conn-context cancel should have reaped it within ~100ms", elapsed)
	}
}

// TestFullTableGateSaturation proves the full-table concurrency cap
// (#823): with the single slot held, a full-table query must give up at
// its deadline with ER_TOO_MANY_USER_CONNECTIONS (1203, distinct from
// the 1317 a slow fetch gets) and an actionable message; once the slot
// frees, the same query must run — and release its slot afterwards.
func TestFullTableGateSaturation(t *testing.T) {
	gate := NewGate(1)
	if err := gate.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "orders",
		AsOf: time.Now().UTC()} // no PKColumn → full-table path

	// Saturated: the gate is consulted before any DB work, so a nil
	// indexDB proves the refusal happens pre-fetch.
	h := newLimitsHandler(nil, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: "bintrail_index",
		QueryTimeout: 100 * time.Millisecond, FullTableGate: gate,
	})
	_, qerr := h.runPointInTime(q)
	var myErr *gomysql.MyError
	if !errors.As(qerr, &myErr) || myErr.Code != gomysql.ER_TOO_MANY_USER_CONNECTIONS {
		t.Fatalf("want ER_TOO_MANY_USER_CONNECTIONS (1203), got %v", qerr)
	}
	if !strings.Contains(myErr.Message, "--max-fulltable-queries") || !strings.Contains(myErr.Message, "cap 1") {
		t.Errorf("saturation error must name the cap and the flag; got %q", myErr.Message)
	}

	// Freed: the query proceeds and releases the slot on completion.
	gate.Release()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(emptyBinlogEventsRows())

	h2 := newLimitsHandler(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: "bintrail_index",
		QueryTimeout: 2 * time.Second, FullTableGate: gate,
	})
	if _, err := h2.runPointInTime(q); err != nil {
		t.Fatalf("query after Release: %v", err)
	}
	// The deferred Release must have freed the slot again.
	reacquire, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.Acquire(reacquire); err != nil {
		t.Fatalf("slot not released after a successful full-table query: %v", err)
	}
}
