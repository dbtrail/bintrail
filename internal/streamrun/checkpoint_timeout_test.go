package streamrun

import (
	"context"
	"database/sql"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// A mid-statement network stall must surface as an error within
// indexer.WriteTimeout, not block on kernel TCP retransmission (~13-16 min)
// while `watch` sees a healthy daemon (#959). A TCP listener that accepts but
// never speaks the MySQL protocol reproduces the mid-handshake freeze; the
// ExecContext deadline in saveCheckpoint must cut it short.
func TestSaveCheckpoint_boundedByWriteTimeout(t *testing.T) {
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

	prev := indexer.WriteTimeout
	indexer.WriteTimeout = 750 * time.Millisecond
	t.Cleanup(func() { indexer.WriteTimeout = prev })

	// Connect timeout is generous so it does not preempt the ExecContext deadline
	// under test — the dial succeeds (listener accepts); the stall is the read.
	db, err := sql.Open("mysql", "root:x@tcp("+ln.Addr().String()+")/db?timeout=30s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	start := time.Now()
	err = saveCheckpoint(db, &streamState{mode: "gtid"})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from the stalled write")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("saveCheckpoint blocked %v — the WriteTimeout deadline is not wired", elapsed)
	}
}

// The <=0 guard in One() rejects a non-positive --write-timeout BEFORE opening any
// connection: a zero/negative deadline would make every index write's
// context.WithTimeout fire immediately, turning each write into an instant failure
// rather than bounding a genuine stall (#959). Mutates the shared WriteTimeout
// global, so no t.Parallel(). Format+GapTimeout are the only checks that precede
// the guard, so a minimal Config reaches it without touching the network.
func TestOne_rejectsNonPositiveWriteTimeout(t *testing.T) {
	prev := indexer.WriteTimeout
	indexer.WriteTimeout = 0
	t.Cleanup(func() { indexer.WriteTimeout = prev })

	err := One(context.Background(), Config{Format: "text", GapTimeout: 30})
	if err == nil || !strings.Contains(err.Error(), "invalid --write-timeout") {
		t.Fatalf("want an 'invalid --write-timeout' rejection before connecting, got %v", err)
	}
}
