//go:build integration

package pgstreamrun_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOne_TruncateAudited is the #827 proof: a PostgreSQL TRUNCATE on a captured
// table lands a durable schema_changes audit row (ddl_type='TRUNCATE TABLE',
// one per truncated table), and reconstruct.CheckDestructiveDDL then REFUSES to
// cross it — the guard that stops a baseline+delta reconstruct from silently
// resurrecting the truncated rows. Before #827 a PG TRUNCATE produced only a
// transient Warn and nothing was persisted, so the destructive DDL was invisible
// to the read plane and reconstruct would have resurrected the rows.
//
// CI-only: needs a live PostgreSQL source (BINTRAIL_TEST_PG_DSN) + MySQL index.
func TestOne_TruncateAudited(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgsr_trunc"
	const pub = "bintrail_pgsr_trunc_pub"
	const tbl = "pgsr_trunc_t"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	// A default publication publishes truncate too (publish = insert,update,delete,truncate).
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 54,
		Tables: "public." + tbl, BatchSize: 100, Checkpoint: 200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()

	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tbl))
	waitFor(t, 15*time.Second, func() bool {
		var n int
		_ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ?", tbl).Scan(&n)
		return n >= 2
	}, "INSERTs indexed")

	mustExec(fmt.Sprintf("TRUNCATE TABLE %s", tbl))
	waitFor(t, 15*time.Second, func() bool {
		var n int
		_ = indexDB.QueryRow("SELECT COUNT(*) FROM schema_changes WHERE table_name = ? AND ddl_type = 'TRUNCATE TABLE'", tbl).Scan(&n)
		return n >= 1
	}, "TRUNCATE audited into schema_changes")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	// The audit row carries schema_name='public' (CheckDestructiveDDL matches on it).
	var schemaName string
	if err := indexDB.QueryRow(
		"SELECT schema_name FROM schema_changes WHERE table_name = ? AND ddl_type = 'TRUNCATE TABLE'", tbl,
	).Scan(&schemaName); err != nil {
		t.Fatalf("read schema_changes row: %v", err)
	}
	if schemaName != "public" {
		t.Errorf("schema_name = %q, want public", schemaName)
	}

	// TRUNCATE is an audit record, not a row event — it must NOT land in binlog_events.
	var truncRows int
	if err := indexDB.QueryRow(
		"SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND event_type = 4", tbl,
	).Scan(&truncRows); err != nil {
		t.Fatalf("count EventDDL rows in binlog_events: %v", err)
	}
	if truncRows != 0 {
		t.Errorf("found %d EventDDL rows in binlog_events, want 0 (TRUNCATE is a schema_changes audit, not a row event)", truncRows)
	}

	// The guard: reconstruct refuses to cross the truncate. A wide, clock-skew-proof
	// window brackets the commit-time detected_at.
	window := time.Hour
	err = reconstruct.CheckDestructiveDDL(ctx, indexDB, "public", tbl,
		time.Now().Add(-window), time.Now().Add(window))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("CheckDestructiveDDL over the truncate window = %v, want ErrDestructiveDDL", err)
	}
}
