package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// realWriteDeadlineError produces the ACTUAL error `watch` died on in #1482, by
// driving indexer.InsertBatch against a TCP listener that accepts and then never
// speaks MySQL, with WriteTimeout shrunk. Hand-building an error here would test
// this file's idea of what the indexer returns; a restart policy keyed on the
// wrong shape is exactly the bug, so the fixture has to come from the real path.
//
// Mutates the shared indexer.WriteTimeout global, so callers must not
// t.Parallel().
func realWriteDeadlineError(t *testing.T) error {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		var held []net.Conn
		for {
			c, err := ln.Accept()
			if err != nil {
				for _, h := range held {
					h.Close()
				}
				return
			}
			held = append(held, c) // hold open, never respond
		}
	}()

	prev := indexer.WriteTimeout
	indexer.WriteTimeout = 250 * time.Millisecond
	t.Cleanup(func() { indexer.WriteTimeout = prev })

	db, err := sql.Open("mysql", "root:x@tcp("+ln.Addr().String()+")/db?timeout=30s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = indexer.New(db, 1).InsertBatch(nil)
	if err == nil {
		t.Fatal("setup: expected the stalled write to fail")
	}
	if !errors.Is(err, indexer.ErrWriteDeadline) {
		t.Fatalf("setup: expected a write-deadline error from the real path, got: %v", err)
	}
	return err
}

// shrinkMonitorBackoff makes the restart policy run in milliseconds.
func shrinkMonitorBackoff(t *testing.T) {
	t.Helper()
	giveUp, base, cap_, healthy := monitorGiveUpAfter, monitorBackoffBase, monitorBackoffCap, monitorHealthyReset
	t.Cleanup(func() {
		monitorGiveUpAfter, monitorBackoffBase, monitorBackoffCap, monitorHealthyReset = giveUp, base, cap_, healthy
	})
	monitorGiveUpAfter = time.Hour
	monitorBackoffBase = time.Millisecond
	monitorBackoffCap = 2 * time.Millisecond
	monitorHealthyReset = time.Hour // no run in this test is "healthy"
}

// Every behavioural test below drives the policy through the mainStreamFn seam,
// which proves the policy works and says NOTHING about whether the daemon uses
// it. The regression that reopens #1482 is one line: watch.go going back to
// calling streamrun.One directly. All four tests stay green through that, so
// pin the call site.
//
// What this guard CANNOT see, on purpose: whether the wrapper's policy is
// correct (the tests below), or a caller added somewhere other than watch.go.
// It sees exactly one thing — that watch's main source goes through the
// wrapper and not around it.
func TestWatch_mainSourceGoesThroughTheRestartPolicy(t *testing.T) {
	src, err := os.ReadFile("watch.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(src, []byte("runMainStreamWithWriteDeadlineRetry(ctx, streamCfg)")) {
		t.Error("watch.go must run its main source through runMainStreamWithWriteDeadlineRetry; " +
			"without it a slow index write ends the daemon again (#1482)")
	}
	if bytes.Contains(src, []byte("streamrun.One(")) {
		t.Error("watch.go calls streamrun.One directly again; the main source must go through " +
			"runMainStreamWithWriteDeadlineRetry (#1482)")
	}
}

// #1482: a single index write-deadline failure must not end capture. Before the
// restart policy, `watch`'s main source was a bare one-shot streamrun.One call,
// so this returned the error on the first attempt and the daemon exited — the
// 41-minute outage in the issue. The assertion is that CAPTURE SURVIVED (nil
// error, stream re-entered), not that some counter reached a number.
//
// Must not t.Parallel(): mutates indexer.WriteTimeout and the monitor policy
// globals.
func TestRunMainStream_restartsOnWriteDeadline(t *testing.T) {
	shrinkMonitorBackoff(t)
	deadlineErr := realWriteDeadlineError(t)

	prev := mainStreamFn
	t.Cleanup(func() { mainStreamFn = prev })

	calls := 0
	mainStreamFn = func(ctx context.Context, cfg streamrun.Config) error {
		calls++
		if calls == 1 {
			return deadlineErr // the index was starved; the link is fine
		}
		return nil // restarted, resumed from the checkpoint, ran to a clean stop
	}

	if err := runMainStreamWithWriteDeadlineRetry(context.Background(), streamrun.Config{}); err != nil {
		t.Fatalf("a write-deadline failure ended capture instead of restarting it: %v", err)
	}
	if calls != 2 {
		t.Fatalf("stream ran %d time(s); want the failed run plus exactly one restart", calls)
	}
}

// The incident's own shape: a daemon that had been capturing happily for a long
// time, then hit ONE write deadline. It must restart.
//
// This exercises the healthy-reset branch and the breaker clock TOGETHER, which
// is the combination the other tests here deliberately avoid (they pin
// monitorHealthyReset above every run, or monitorGiveUpAfter at zero). With the
// breaker clock seeded from the failed run's START, the run's own 60ms of uptime
// is charged to it, exceeds the 20ms give-up threshold, and capture stops on
// failure number one having restarted nothing. Scaled constants, same ordering
// as 35 hours of uptime against a 6h breaker.
//
// Must not t.Parallel(): mutates indexer.WriteTimeout and the policy globals.
func TestRunMainStream_longHealthyRunThenOneDeadlineStillRestarts(t *testing.T) {
	shrinkMonitorBackoff(t)
	monitorHealthyReset = 10 * time.Millisecond // the first run outlives this
	monitorGiveUpAfter = 20 * time.Millisecond  // ...and outlives this too
	deadlineErr := realWriteDeadlineError(t)

	prev := mainStreamFn
	t.Cleanup(func() { mainStreamFn = prev })

	calls := 0
	mainStreamFn = func(ctx context.Context, cfg streamrun.Config) error {
		calls++
		if calls == 1 {
			time.Sleep(60 * time.Millisecond) // a long, healthy run
			return deadlineErr
		}
		return nil
	}

	if err := runMainStreamWithWriteDeadlineRetry(context.Background(), streamrun.Config{}); err != nil {
		t.Fatalf("a healthy daemon's first write deadline ended capture instead of restarting it: %v", err)
	}
	if calls != 2 {
		t.Fatalf("stream ran %d time(s); want the failed run plus one restart. The breaker charged the "+
			"daemon's uptime to a crash loop that had not started yet (#1482)", calls)
	}
}

// The narrowing is load-bearing, not incidental: an un-indexable event must
// still abort loudly rather than spin (#652), so anything that is not a write
// deadline has to return on the first failure, untouched.
func TestRunMainStream_doesNotRetryOtherErrors(t *testing.T) {
	shrinkMonitorBackoff(t)

	prev := mainStreamFn
	t.Cleanup(func() { mainStreamFn = prev })

	want := errors.New("batch INSERT of 1000 events failed: Error 1406: Data too long")
	calls := 0
	mainStreamFn = func(ctx context.Context, cfg streamrun.Config) error {
		calls++
		return want
	}

	if err := runMainStreamWithWriteDeadlineRetry(context.Background(), streamrun.Config{}); !errors.Is(err, want) {
		t.Fatalf("a non-deadline error must surface unchanged, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("stream ran %d times; a non-deadline error must not be retried", calls)
	}
}

// A daemon that is shutting down must not be dragged through the backoff loop:
// One's final checkpoint flush can hit the very same deadline, and that is a
// stopping daemon, not a stall to ride out. The error still surfaces — a daemon
// that stopped without capturing does not exit 0.
func TestRunMainStream_cancelledContextDoesNotRetry(t *testing.T) {
	shrinkMonitorBackoff(t)
	deadlineErr := realWriteDeadlineError(t)

	prev := mainStreamFn
	t.Cleanup(func() { mainStreamFn = prev })

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	mainStreamFn = func(ctx context.Context, cfg streamrun.Config) error {
		calls++
		cancel()
		return deadlineErr
	}

	if err := runMainStreamWithWriteDeadlineRetry(ctx, streamrun.Config{}); !errors.Is(err, indexer.ErrWriteDeadline) {
		t.Fatalf("the failure that stopped a shutting-down daemon must still surface, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("stream ran %d times; a cancelled daemon must not restart capture", calls)
	}
}

// Exhausting the budget must be no quieter than exiting on the first failure
// was: the SAME error, with its three remedies intact, ends the daemon.
// Must not t.Parallel().
func TestRunMainStream_givesUpWithTheOriginalError(t *testing.T) {
	shrinkMonitorBackoff(t)
	monitorGiveUpAfter = 0 // any continuous crash-looping trips it immediately
	deadlineErr := realWriteDeadlineError(t)

	prev := mainStreamFn
	t.Cleanup(func() { mainStreamFn = prev })

	calls := 0
	mainStreamFn = func(ctx context.Context, cfg streamrun.Config) error {
		calls++
		return deadlineErr
	}

	err := runMainStreamWithWriteDeadlineRetry(context.Background(), streamrun.Config{})
	if err == nil {
		t.Fatal("a permanently stalled index must still stop the daemon")
	}
	if err.Error() != deadlineErr.Error() {
		t.Fatalf("the give-up path must return the original message verbatim (it names --write-timeout,\n"+
			"--batch-size and max_allowed_packet); got:\n%v", err)
	}
	if calls < 1 {
		t.Fatal("the stream never ran")
	}
}
