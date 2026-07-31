//go:build integration

package pgshim

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationAuditContract_PGShim is the PostgreSQL front-end's half of
// the shim audit contract (#1123): this serving layer answers time-travel
// queries through the exported resolve seam (ResolveFlashbackRow /
// ResolveSnapshotRow), BYPASSING Handler.HandleQuery — so the MySQL command
// loop's contract test (internal/shim/audit_contract_test.go) proves nothing
// about it, and before #1123 it served row images with zero emissions and an
// unbound actor.
//
// Real code path end-to-end: a REAL pgx client over the wire, against a real
// seeded index, with a recording sink installed. The actor must be the
// authenticated per-tenant credential — pgshim authenticates a real user
// (unlike the console flashback port, whose username is a routing key).
//
// No t.Parallel(): ext's sink is process-wide (audittest.Install).
func TestIntegrationAuditContract_PGShim(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	rec := audittest.Install(t)

	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{hour})
	snapTS := hour.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, "public", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, "public", "users", "name", 2, "", "varchar", "YES")
	t0 := hour.Add(5 * time.Minute)
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, t0.Format("2006-01-02 15:04:05"), nil,
		"public", "users", uint8(event.EventInsert), "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))

	addr := serveAddrWithDB(t, Config{
		IndexDB:    db,
		ShimConfig: shim.Config{NoArchive: true, IndexDBName: dbName},
		Auth:       testAuth(t),
	})
	conn, err := connectPGWire(t, addr, testUser, testPass)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	asOf := t0.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	cases := []struct {
		name      string
		virtual   string
		pk        string
		wantRows  string
		wantFound bool
	}{
		{name: "flashback single row", virtual: "_flashback", pk: "1", wantRows: "1", wantFound: true},
		{name: "snapshot single row", virtual: "_snapshot", pk: "1", wantRows: "1", wantFound: true},
		// A row absent at AsOf is still a served time-travel read (a real
		// zero-row resultset went to the client), so it is audited with rows=0
		// — same posture as the MySQL front-end's empty resultsets.
		{name: "row absent at AsOf", virtual: "_flashback", pk: "999", wantRows: "0", wantFound: false},
	}

	var observed []audittest.Pair
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			rows, err := conn.Query(ctx, "SELECT * FROM "+tc.virtual+".users AS OF '"+asOf+"' WHERE id = "+tc.pk)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			got := rows.Next()
			rows.Close()
			if err := rows.Err(); err != nil {
				t.Fatalf("rows err: %v", err)
			}
			if got != tc.wantFound {
				t.Fatalf("row found = %v, want %v (seed problem?)", got, tc.wantFound)
			}
			evs := rec.Events()
			if len(evs) != 1 {
				t.Fatalf("recorded %d audit events, want exactly 1: %+v", len(evs), evs)
			}
			ev := evs[0]
			if ev.Surface != "shim" || ev.Action != "timetravel.query" {
				t.Errorf("event = %s/%s, want shim/timetravel.query", ev.Surface, ev.Action)
			}
			// The authenticated tenant, not the daemon's process owner and not
			// the unbound sentinel: pgshim.handleConn must BindActor post-auth.
			if ev.Actor != testUser {
				t.Errorf("actor = %q, want the authenticated tenant %q", ev.Actor, testUser)
			}
			if ev.Schema != "public" || ev.Table != "users" {
				t.Errorf("schema/table = %q/%q, want public/users", ev.Schema, ev.Table)
			}
			if gotType := ev.Detail["query_type"]; gotType != tc.virtual {
				t.Errorf("detail[query_type] = %q, want %q", gotType, tc.virtual)
			}
			if gotRows := ev.Detail["rows"]; gotRows != tc.wantRows {
				t.Errorf("detail[rows] = %q, want %q", gotRows, tc.wantRows)
			}
			if ev.Detail["scope"] != "single_row" {
				t.Errorf("detail[scope] = %q, want single_row", ev.Detail["scope"])
			}
			observed = append(observed, audittest.Pair{Surface: ev.Surface, Action: ev.Action})
		})
	}

	// Refusals read no rows and must record nothing: a full-table AS OF (the
	// PG front-end refuses it) and a non-time-travel statement.
	rec.Reset()
	if _, err := conn.Exec(ctx, "SELECT * FROM _flashback.users AS OF '"+asOf+"'"); err == nil {
		t.Fatal("full-table AS OF must be refused on the PG front-end")
	}
	if _, err := conn.Exec(ctx, "SELECT 1"); err == nil {
		t.Fatal("a non-time-travel query must be refused")
	}
	if evs := rec.Events(); len(evs) != 0 {
		t.Errorf("refused queries recorded %d audit events, want 0: %+v", len(evs), evs)
	}

	audittest.CheckCoverage(t, audittest.OwnerPGShim, observed)
}
