//go:build integration

package pgcapture_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestQuerySlotHealth_Integration drives QuerySlotHealth against a live PostgreSQL:
// an absent slot reports Exists=false (no error), a present logical slot reports its
// real retention state, and a dropped slot reverts to Exists=false. This is the
// foundation query for #532 WAL-retention monitoring (doctor + ensureSlot).
//
// Requires BINTRAIL_TEST_PG_DSN (e.g. postgres://postgres:testpg@localhost:15533/pgtest);
// the MySQL CI jobs leave it unset and skip cleanly (Postgres CI is #534).
func TestQuerySlotHealth_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const slot = "bintrail_health_it"

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	dropSlot := func() {
		_, _ = conn.Exec(context.Background(),
			"SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropSlot()
	t.Cleanup(dropSlot)

	// Absent slot: Exists=false, nil error (an expected state, not a failure).
	h, err := pgcapture.QuerySlotHealth(ctx, conn, slot)
	if err != nil {
		t.Fatalf("QuerySlotHealth(absent): %v", err)
	}
	if h.Exists {
		t.Fatalf("absent slot reported Exists=true: %+v", h)
	}

	// Create a logical slot, write a little WAL, then read its health.
	if _, err := conn.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot); err != nil {
		t.Fatalf("create slot: %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_switch_wal()"); err != nil {
		t.Fatalf("switch wal: %v", err)
	}

	h, err = pgcapture.QuerySlotHealth(ctx, conn, slot)
	if err != nil {
		t.Fatalf("QuerySlotHealth(present): %v", err)
	}
	if !h.Exists {
		t.Fatal("present slot reported Exists=false")
	}
	if h.WalStatus != "reserved" {
		// A brand-new slot under the default (unlimited) retention is always reserved.
		t.Errorf("WalStatus = %q, want reserved", h.WalStatus)
	}
	if h.Active {
		t.Error("slot reported Active=true with no consumer holding it")
	}
	if h.CurrentWalLSN == 0 {
		t.Error("CurrentWalLSN is 0; expected the live server WAL head")
	}
	if h.RetainedBytes < 0 {
		t.Errorf("RetainedBytes = %d, want >= 0", h.RetainedBytes)
	}
	// safe_wal_size is NULL on the dev/CI container (max_slot_wal_keep_size=-1); just
	// assert QuerySlotHealth handled the NULL without erroring (it returned above).

	// Drop the slot: back to Exists=false.
	dropSlot()
	h, err = pgcapture.QuerySlotHealth(ctx, conn, slot)
	if err != nil {
		t.Fatalf("QuerySlotHealth(after drop): %v", err)
	}
	if h.Exists {
		t.Errorf("dropped slot reported Exists=true: %+v", h)
	}
}
