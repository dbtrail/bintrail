package config

import (
	"crypto/tls"
	"database/sql"
	"net"
	"testing"
	"time"
)

// silentPeer is a TCP listener that ACCEPTS and then never speaks MySQL. It is
// the frozen-VM / idle-dropped-NLB shape: the dial succeeds, so cfg.Timeout
// (the DIAL timeout, and the only timeout normalizeDSN sets) is already spent,
// and the driver then blocks in readHandshakePacket with no read deadline
// anywhere.
func silentPeer(t *testing.T) string {
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
	return ln.Addr().String()
}

// Opening a connection must not be able to block forever (#1482).
//
// This is not a tidiness issue. The write-deadline restart policy in
// consoleapp counts attempts and gives up after a bounded number of them, and
// an attempt re-enters through this function. A connect that never returns
// makes that bound unenforceable: the daemon stays alive, /api/healthz stays
// green, and capture is dead with one log line to show for it. Before the
// bounded ping this test does not fail fast, it HANGS, which is why the guard
// is a wall-clock ceiling rather than an error assertion alone.
//
// Both entry points are covered because they do NOT share a body: Connect
// builds its own DSN string, ConnectWithTLS keeps the *mysql.Config. Bounding
// only one leaves the other able to hang.
func TestConnect_boundedAgainstASilentPeer(t *testing.T) {
	addr := silentPeer(t)
	// timeout=1s is the DSN's own connect budget, shrunk so the test is quick.
	dsn := "root:x@tcp(" + addr + ")/db?timeout=1s"

	for _, tc := range []struct {
		name string
		open func() (*sql.DB, error)
	}{
		{"Connect", func() (*sql.DB, error) { return Connect(dsn) }},
		{"ConnectWithTLS", func() (*sql.DB, error) { return ConnectWithTLS(dsn, (*tls.Config)(nil)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				db, err := tc.open()
				if db != nil {
					db.Close()
				}
				done <- err
			}()

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("a peer that never speaks MySQL must not yield a usable connection")
				}
			case <-time.After(15 * time.Second):
				// The goroutine is parked in the driver's handshake read and will
				// stay there; the test process exiting is what releases it.
				t.Fatalf("%s blocked past its own connect budget — the ping is unbounded, "+
					"so a stream restart can hang here forever and no supervisor can count the attempt (#1482)", tc.name)
			}
		})
	}
}
