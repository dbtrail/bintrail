//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedPGVerifyIndex marks the test index as a PostgreSQL capture and
// publishes the given relations — one snapshot_id each, the real
// WritePGSnapshot shape.
func seedPGVerifyIndex(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	for _, tbl := range tables {
		if _, err := metadata.WritePGSnapshot(context.Background(), db, &metadata.PGRelationSchema{
			Schema: "public", Table: tbl,
			Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true}},
		}); err != nil {
			t.Fatalf("WritePGSnapshot(%s): %v", tbl, err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', '0/0', 0, '0/0', 'postgres', UTC_TIMESTAMP(), 1)`); err != nil {
		t.Fatalf("seed stream_state: %v", err)
	}
}

// TestIntegrationVerifySupervisorPGLiveSourceDispatch is the mutation guard
// for the supervisor's flavor dispatch: a live-source run against a
// PG-flavored index must route to runLiveSourcePG (the pinned pgx connect —
// its failure text is pgx's "failed to connect"), never fall into the MySQL
// runLiveSource, whose config.Connect would reject the postgres:// DSN with a
// MySQL-driver "invalid DSN" parse error. Deleting the flavor branch in
// run()'s VerifyModeLiveSource case flips this test's observed error.
func TestIntegrationVerifySupervisorPGLiveSourceDispatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	seedPGVerifyIndex(t, db, "orders")

	s := newVerifySupervisor(context.Background(), nil, nil)
	if err := s.RunScheduled(console.VerifyRequest{
		ServerID: "pg1", ServerName: "pg", Mode: console.VerifyModeLiveSource,
		IndexDSN:  testutil.IntegrationDSN(dbName),
		SourceDSN: "postgres://bintrail:nope@127.0.0.1:9/appdb",
		// port 9 (discard) — the connect must fail fast and land in LastError
		BaselineDir: t.TempDir(),
	}); err != nil {
		t.Fatalf("RunScheduled: %v", err)
	}
	st := s.Status("pg1")
	if st.State != console.VerifyStateFailed {
		t.Fatalf("state = %q, want failed (results=%v)", st.State, st.Results)
	}
	if !strings.Contains(st.LastError, "connect source:") || !strings.Contains(st.LastError, "failed to connect") {
		t.Fatalf("LastError = %q, want the PG path's pgx connect failure — a MySQL-driver 'invalid DSN' here means the flavor dispatch fell into runLiveSource", st.LastError)
	}
	if strings.Contains(st.LastError, "invalid DSN") {
		t.Fatalf("LastError = %q — the postgres:// DSN reached the MySQL driver: flavor dispatch is broken", st.LastError)
	}
}

// TestIntegrationVerifySupervisorPGRecoverInputsEnumeration mirrors the CLI's
// enumeration guard on the console path: recover-inputs on a PG index must
// enumerate EVERY relation via the resolver, not the one relation the
// MAX(snapshot_id) lookup names.
func TestIntegrationVerifySupervisorPGRecoverInputsEnumeration(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	seedPGVerifyIndex(t, db, "orders", "items")

	s := newVerifySupervisor(context.Background(), nil, nil)
	if err := s.RunScheduled(console.VerifyRequest{
		ServerID: "pg1", ServerName: "pg", Mode: console.VerifyModeRecoverInputs,
		IndexDSN: testutil.IntegrationDSN(dbName),
	}); err != nil {
		t.Fatalf("RunScheduled: %v", err)
	}
	st := s.Status("pg1")
	if st.State != console.VerifyStateSucceeded {
		t.Fatalf("state = %q (lastError=%q), want succeeded", st.State, st.LastError)
	}
	if len(st.Results) != 2 {
		t.Fatalf("results = %d tables (%v), want both published relations — one means the enumeration fell back to MAX(snapshot_id)", len(st.Results), st.Results)
	}
}
