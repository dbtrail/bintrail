//go:build integration

package pgshim

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestPGWire_SingleRowRoundTrip is the acceptance path against a real seeded
// index driven by a REAL pgx client (default sslmode=prefer, so the SSLRequest
// negotiation runs): a single-row _flashback / _snapshot AS OF returns the
// correct row state at the queried instant, and full-table AS OF is refused.
// Version-independent (it exercises the wire → shared-engine → row-state path
// over the MySQL index, no live PostgreSQL), so it runs in the MySQL integration
// matrix. The PostgreSQL-source, version-dependent E2E lives in
// TestPGWire_PostgresSourceRoundTrip.
func TestPGWire_SingleRowRoundTrip(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{hour})

	// PK snapshot so PKColumnCheck admits `WHERE id = 1`.
	snapTS := hour.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, "public", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, "public", "users", "name", 2, "", "varchar", "YES")

	// Row 1: INSERT alice at t0, UPDATE to bob at t1.
	t0 := hour.Add(5 * time.Minute)
	t1 := hour.Add(15 * time.Minute)
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, t0.Format("2006-01-02 15:04:05"), nil,
		"public", "users", uint8(event.EventInsert), "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, t1.Format("2006-01-02 15:04:05"), nil,
		"public", "users", uint8(event.EventUpdate), "1", []byte(`["name"]`),
		[]byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"bob"}`))

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

	// _flashback AS OF between the two events → the pre-update image (alice).
	between := t0.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	if got := queryName(t, ctx, conn, "_flashback", between); got != "alice" {
		t.Errorf("_flashback AS OF %s: name=%q, want alice", between, got)
	}
	// _flashback AS OF after the update → the post-update image (bob).
	after := t1.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	if got := queryName(t, ctx, conn, "_flashback", after); got != "bob" {
		t.Errorf("_flashback AS OF %s: name=%q, want bob", after, got)
	}
	// _snapshot with no baseline configured degrades to the same binlog-only
	// answer — proves the _snapshot route reaches the shared engine.
	if got := queryName(t, ctx, conn, "_snapshot", between); got != "alice" {
		t.Errorf("_snapshot AS OF %s: name=%q, want alice", between, got)
	}

	// Full-table AS OF (no WHERE) is refused with actionable remediation.
	_, err = conn.Exec(ctx, "SELECT * FROM _flashback.users AS OF '"+after+"'")
	pgErr := requirePgError(t, err)
	if pgErr.Code != "0A000" || !contains(pgErr.Message, "full-table") {
		t.Errorf("full-table refusal: code=%s msg=%s", pgErr.Code, pgErr.Message)
	}
}

// queryName runs a single-row AS OF for id=1 against the given virtual schema and
// returns the `name` column as text. Columns come back as text (OID 25), so we
// scan into strings — the conservative all-text encoding the front-end uses.
func queryName(t *testing.T, ctx context.Context, conn *pgx.Conn, virtualSchema, asOf string) string {
	t.Helper()
	sqlText := "SELECT * FROM " + virtualSchema + ".users AS OF '" + asOf + "' WHERE id = 1"
	rows, err := conn.Query(ctx, sqlText)
	if err != nil {
		t.Fatalf("query %s: %v", sqlText, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatalf("query %s: %v", sqlText, err)
		}
		t.Fatalf("query %s: no row returned", sqlText)
	}
	var id, name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("scan %s: %v", sqlText, err)
	}
	if id != "1" {
		t.Errorf("id = %q, want 1", id)
	}
	return name
}

func serveAddrWithDB(t *testing.T, cfg Config) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if cfg.Logger == nil {
		cfg.Logger = discardLogger()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = Serve(ctx, ln, cfg) }()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		<-done
	})
	return ln.Addr().String()
}
