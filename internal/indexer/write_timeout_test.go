package indexer

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// The whole point of WriteTimeout is to bound a stalled index write. A zero (or
// negative) value silently disables the deadline and reintroduces #959; an
// absurdly long value defeats the purpose (must stay below the kernel's
// ~13-16 min TCP give-up). Pin a sane window.
func TestWriteTimeout_default(t *testing.T) {
	if WriteTimeout < time.Minute || WriteTimeout > 10*time.Minute {
		t.Fatalf("WriteTimeout = %v; want above a healthy batch INSERT and below the kernel ~13-16m give-up", WriteTimeout)
	}
}

// InsertBatch is the hot-path index write (#959: the "capturing nothing" site).
// Its ExecContext deadline must cut a mid-statement network stall, or the daemon
// freezes ~13-16 min invisibly to `watch`. A TCP listener that accepts but never
// speaks MySQL reproduces the stall; an empty batch still reaches the same
// ExecContext, so this directly guards the wiring at this site (a copy-paste
// back to db.Exec would silently reintroduce the freeze here). Must NOT call
// t.Parallel(): it mutates the shared WriteTimeout global.
func TestInsertBatch_boundedByWriteTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
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

	prev := WriteTimeout
	WriteTimeout = 750 * time.Millisecond
	t.Cleanup(func() { WriteTimeout = prev })

	db, err := sql.Open("mysql", "root:x@tcp("+ln.Addr().String()+")/db?timeout=30s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	idx := New(db, 1)
	start := time.Now()
	_, err = idx.InsertBatch(nil) // reaches ExecContext; the handshake stall is cut by WriteTimeout
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from the stalled write")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("InsertBatch blocked %v — the WriteTimeout deadline is not wired at the hot-path site", elapsed)
	}
	// The stall must surface as the deadline SPECIFICALLY (not merely "some fast
	// error"), and carry the operator hint — otherwise a large-batch-over-slow-link
	// false-fire looks like a phantom network fault instead of a tunable knob (#959).
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the write bounded by the WriteTimeout deadline, got %v", err)
	}
	if !strings.Contains(err.Error(), "write deadline") || !strings.Contains(err.Error(), "--write-timeout") {
		t.Fatalf("DeadlineExceeded error must carry the operator hint (write deadline / --write-timeout), got: %v", err)
	}
}
