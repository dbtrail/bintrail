//go:build integration

package pgstreamrun_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func findCheck(t *testing.T, r *doctor.Report, name string) doctor.CheckResult {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("report has no check named %q; checks=%v", name, r.Checks)
	return doctor.CheckResult{}
}

// TestBuildPGReport_Integration drives the doctor report against a live PostgreSQL:
// a healthy source (slot + publication + REPLICA IDENTITY FULL) passes the required
// checks, and a wrong publication name surfaces as a publication-coverage FAIL.
//
// Requires BINTRAIL_TEST_PG_DSN; the MySQL CI jobs leave it unset and skip cleanly.
func TestBuildPGReport_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const slot = "bintrail_doctor_it"
	const pub = "bintrail_doctor_it_pub"
	const tbl = "doctor_it_t"

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	mustExec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	dropAll := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec("CREATE TABLE " + tbl + " (id int PRIMARY KEY, v text)")
	mustExec("ALTER TABLE " + tbl + " REPLICA IDENTITY FULL")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + tbl)
	mustExec("SELECT pg_create_logical_replication_slot('" + slot + "', 'pgoutput')")

	t.Run("healthy", func(t *testing.T) {
		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN: dsn, SlotName: slot, Publication: pub,
		})
		for _, name := range []string{"Source PostgreSQL connection", "wal_level = logical", "Publication coverage", "REPLICA IDENTITY FULL"} {
			if c := findCheck(t, r, name); c.Status != doctor.StatusPass {
				t.Errorf("%s = %s (%s), want pass", name, c.Status, c.Detail)
			}
		}
		// The slot is reserved (healthy) under the default unlimited retention.
		if c := findCheck(t, r, "Replication slot health"); c.Status != doctor.StatusPass {
			t.Errorf("slot health = %s (%s), want pass", c.Status, c.Detail)
		}
		// max_slot_wal_keep_size runs and reads the real setting: the dev/CI container
		// leaves it at the -1 default, so it WARNs (the production red line). A bounded
		// server would PASS; either way it must not FAIL or SKIP in a healthy run.
		if c := findCheck(t, r, "max_slot_wal_keep_size"); c.Status != doctor.StatusWarn && c.Status != doctor.StatusPass {
			t.Errorf("max_slot_wal_keep_size = %s (%s), want warn (unlimited) or pass (bounded)", c.Status, c.Detail)
		}
		if r.Failed != 0 {
			t.Errorf("healthy source has %d failed checks, want 0", r.Failed)
		}
		if err := r.Err(); err != nil {
			t.Errorf("Err() = %v, want nil for a healthy source", err)
		}
	})

	t.Run("missing-publication", func(t *testing.T) {
		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN: dsn, SlotName: slot, Publication: "no_such_pub",
		})
		if c := findCheck(t, r, "Publication coverage"); c.Status != doctor.StatusFail {
			t.Errorf("Publication coverage = %s, want fail for a non-existent publication", c.Status)
		}
		if r.Err() == nil {
			t.Error("Err() = nil, want non-nil when a required check failed")
		}
	})
}

// TestBuildPGReport_LostSlot_Integration is the #532 linchpin: it drives a real slot
// to wal_status='lost' and asserts the doctor reports it as a loud FAIL with the
// recovery path. max_slot_wal_keep_size is a server-wide SIGHUP setting; the CI runs
// PG packages with -p 1 (serial) and these tests are non-parallel, so the keep-size
// window is isolated. t.Cleanup RESETs it even on failure.
func TestBuildPGReport_LostSlot_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const slot = "bintrail_lostslot_it"
	const tbl = "lostslot_it_churn"

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	mustExec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	// Restore the server-wide setting and clean up regardless of outcome.
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "ALTER SYSTEM RESET max_slot_wal_keep_size")
		_, _ = conn.Exec(bg, "SELECT pg_reload_conf()")
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	})
	// Start clean.
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl)
	_, _ = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)

	// Bound WAL retention tightly, create an (idle) slot, then churn WAL past the
	// bound so the next checkpoint invalidates the slot.
	mustExec("ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
	mustExec("SELECT pg_reload_conf()")
	mustExec("SELECT pg_create_logical_replication_slot('" + slot + "', 'pgoutput')")
	mustExec("CREATE TABLE " + tbl + " (id serial, pad text)")

	lost := false
	for i := 0; i < 10 && !lost; i++ {
		mustExec("INSERT INTO " + tbl + " (pad) SELECT repeat('x', 900) FROM generate_series(1, 4000)")
		mustExec("SELECT pg_switch_wal()")
		mustExec("CHECKPOINT")
		var status string
		if err := conn.QueryRow(ctx, "SELECT wal_status FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&status); err != nil {
			t.Fatalf("read wal_status: %v", err)
		}
		lost = status == "lost"
	}
	if !lost {
		t.Skip("could not drive the slot to wal_status=lost on this server; skipping the lost-slot assertion")
	}

	r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
		QueryDSN: dsn, SlotName: slot, Publication: "irrelevant_for_slot_check",
	})
	c := findCheck(t, r, "Replication slot health")
	if c.Status != doctor.StatusFail {
		t.Fatalf("lost slot reported %s, want fail; detail=%q", c.Status, c.Detail)
	}
	if r.Err() == nil {
		t.Error("Err() = nil, want non-nil when the slot is lost")
	}
	// The recovery path must be present and reassure that recovery is unaffected.
	for _, want := range []string{"re-baseline", "recovery never needs the slot"} {
		if !strings.Contains(c.Remediation, want) {
			t.Errorf("lost-slot remediation missing %q:\n%s", want, c.Remediation)
		}
	}
}
