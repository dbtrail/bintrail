package pgstreamrun

import (
	"context"
	"database/sql"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// A mid-statement network stall on the PostgreSQL daemon's checkpoint write must
// surface as an error within indexer.WriteTimeout, not block on kernel TCP
// retransmission (~13-16 min) while `watch` shows a healthy stream (#959). This
// mirrors the MySQL streamrun test: the PG loop reaches the same index over the
// same driver, so it shares the freeze exposure and must share the deadline. A
// TCP listener that accepts but never speaks the MySQL protocol reproduces the
// mid-handshake freeze; saveCheckpointPG's ExecContext deadline must cut it. Must
// NOT call t.Parallel(): it mutates the shared WriteTimeout global.
func TestSaveCheckpointPG_boundedByWriteTimeout(t *testing.T) {
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

	db, err := sql.Open("mysql", "root:x@tcp("+ln.Addr().String()+")/db?timeout=30s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	start := time.Now()
	err = saveCheckpointPG(db, &pgStreamState{}, 0x1A2B3C4)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from the stalled write")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("saveCheckpointPG blocked %v — the WriteTimeout deadline is not wired", elapsed)
	}
}

// The <=0 guard in One() rejects a non-positive --write-timeout BEFORE opening any
// connection (#959). The guard sits immediately after the required-field check, so
// a Config with all five fields present reaches it without touching the network.
// Mutates the shared WriteTimeout global, so no t.Parallel().
func TestOne_rejectsNonPositiveWriteTimeout(t *testing.T) {
	prev := indexer.WriteTimeout
	indexer.WriteTimeout = -1
	t.Cleanup(func() { indexer.WriteTimeout = prev })

	err := One(context.Background(), Config{
		IndexDSN: "x", ReplDSN: "x", QueryDSN: "x", SlotName: "x", Publication: "x",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid --write-timeout") {
		t.Fatalf("want an 'invalid --write-timeout' rejection before connecting, got %v", err)
	}
}
