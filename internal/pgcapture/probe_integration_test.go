//go:build integration

package pgcapture_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestProbeHealth_Integration drives ProbeHealth (slot health + REPLICA IDENTITY
// coverage, connect-per-call) against a live PostgreSQL — the source-side read the
// streaming daemon persists for the console health panel (#599). It also exercises the
// report-only QueryReplicaIdentityNotFull entry point and the shared replicaIdentityNotFull
// helper end to end: a publication with one FULL and one default-identity table must
// report exactly the non-FULL one.
//
// Requires BINTRAIL_TEST_PG_DSN (e.g. postgres://postgres:testpg@localhost:15533/pgtest).
func TestProbeHealth_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_probe_it"
	const pub = "bintrail_probe_pub"

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	cleanup := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS probe_full, probe_nofull")
	}
	cleanup()
	t.Cleanup(cleanup)

	mustExec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec("CREATE TABLE probe_full (id int PRIMARY KEY)")
	mustExec("CREATE TABLE probe_nofull (id int PRIMARY KEY)")
	mustExec("ALTER TABLE probe_full REPLICA IDENTITY FULL")
	// probe_nofull keeps the default identity (relreplident='d') → reported not-FULL.
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE probe_full, probe_nofull")
	if _, err := conn.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot); err != nil {
		t.Fatalf("create slot: %v", err)
	}

	snap, err := pgcapture.ProbeHealth(ctx, dsn, slot, pub)
	if err != nil {
		t.Fatalf("ProbeHealth: %v", err)
	}
	if !snap.Slot.Exists {
		t.Errorf("ProbeHealth slot Exists=false for a created slot: %+v", snap.Slot)
	}
	if len(snap.ReplicaIdentityNotFull) != 1 || !strings.Contains(snap.ReplicaIdentityNotFull[0], "probe_nofull") {
		t.Errorf("ReplicaIdentityNotFull = %v, want exactly the one default-identity table", snap.ReplicaIdentityNotFull)
	}
	for _, s := range snap.ReplicaIdentityNotFull {
		if strings.Contains(s, "probe_full") {
			t.Errorf("a REPLICA IDENTITY FULL table must not be reported not-FULL: %v", snap.ReplicaIdentityNotFull)
		}
	}
}
