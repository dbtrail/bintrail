package main

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/telemetry/telemetrytest"
)

// TestPGStreamDaemonWiringEmitsBeacon is the #1362 wiring guard for
// `bintrail-pg stream`: it drives the REAL runPGStream and passes only once a
// real daemon_beacon carrying its command name is delivered. The daemon is
// held alive in its very first connection — pgstreamrun.One dials the MySQL
// index before it ever touches PostgreSQL, and the hanging endpoint accepts
// without answering, so the handshake read blocks while the shortened daemon
// tick fires. Delete the `go tel.Client().RunDaemon(...)` line and this fails.
func TestPGStreamDaemonWiringEmitsBeacon(t *testing.T) {
	c, bodies := telemetrytest.CollectingClient(t)
	defer tel.SetClientForTest(c)()

	// pgStreamConfigFromFlags fills EMPTY flag vars from BINTRAIL_PG_*; make
	// sure a developer shell's values cannot leak into the run.
	for _, v := range []string{"BINTRAIL_PG_REPL_DSN", "BINTRAIL_PG_QUERY_DSN",
		"BINTRAIL_PG_SLOT", "BINTRAIL_PG_PUBLICATION", "BINTRAIL_PG_START_LSN"} {
		t.Setenv(v, "")
	}

	addr, sever := telemetrytest.HangingTCPAddr(t)
	origIndex, origRepl, origQuery := pgIndexDSN, pgReplDSN, pgQueryDSN
	origSlot, origPub, origServerID := pgSlot, pgPublication, pgServerID
	defer func() {
		pgIndexDSN, pgReplDSN, pgQueryDSN = origIndex, origRepl, origQuery
		pgSlot, pgPublication, pgServerID = origSlot, origPub, origServerID
	}()
	pgIndexDSN = "root:x@tcp(" + addr + ")/bintrail_index"
	pgReplDSN = "postgres://u:p@127.0.0.1:1/db?replication=database"
	pgQueryDSN = "postgres://u:p@127.0.0.1:1/db"
	pgSlot, pgPublication, pgServerID = "wiring", "wiring", 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamCmd.SetContext(ctx)

	done := make(chan error, 1)
	go func() { done <- runPGStream(streamCmd, nil) }()

	telemetrytest.WaitForBeacon(t, bodies, "stream")

	cancel()
	sever()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runPGStream did not return after cancel — daemon shutdown would hang")
	}
}
