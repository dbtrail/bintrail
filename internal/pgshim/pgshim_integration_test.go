//go:build integration

package pgshim

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
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
	if got := queryName(t, ctx, conn, "_flashback", between, "1"); got != "alice" {
		t.Errorf("_flashback AS OF %s: name=%q, want alice", between, got)
	}
	// _flashback AS OF after the update → the post-update image (bob).
	after := t1.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	if got := queryName(t, ctx, conn, "_flashback", after, "1"); got != "bob" {
		t.Errorf("_flashback AS OF %s: name=%q, want bob", after, got)
	}
	// _snapshot with no baseline configured degrades to the same binlog-only
	// answer — proves the _snapshot route reaches the shared engine. (The
	// baseline-materialisation path is TestPGWire_SnapshotBaselineRow.)
	if got := queryName(t, ctx, conn, "_snapshot", between, "1"); got != "alice" {
		t.Errorf("_snapshot AS OF %s: name=%q, want alice", between, got)
	}

	// A PK that never existed at AsOf → a real resultset with column headers but
	// zero rows and a "SELECT 0" tag (not an empty/errored reply).
	absent, err := conn.Query(ctx, "SELECT * FROM _flashback.users AS OF '"+after+"' WHERE id = 999")
	if err != nil {
		t.Fatalf("row-absent query: %v", err)
	}
	if len(absent.FieldDescriptions()) == 0 {
		t.Error("row-absent resultset must still carry column headers")
	}
	if absent.Next() {
		t.Error("expected zero rows for a never-existent PK")
	}
	absent.Close()
	if err := absent.Err(); err != nil {
		t.Fatalf("row-absent rows err: %v", err)
	}
	if tag := absent.CommandTag().String(); tag != "SELECT 0" {
		t.Errorf("row-absent command tag = %q, want SELECT 0", tag)
	}

	// Explicit projection is emitted verbatim; a column absent from the image
	// (never present / dropped since AsOf) renders as SQL NULL — the "column
	// dropped after AS OF" semantic.
	proj, err := conn.Query(ctx, "SELECT name, ghost FROM _flashback.users AS OF '"+after+"' WHERE id = 1")
	if err != nil {
		t.Fatalf("projection query: %v", err)
	}
	fds := proj.FieldDescriptions()
	if len(fds) != 2 || fds[0].Name != "name" || fds[1].Name != "ghost" {
		t.Fatalf("projection columns = %v, want [name ghost]", fieldNames(fds))
	}
	if !proj.Next() {
		t.Fatal("projection: no row returned")
	}
	vals, err := proj.Values()
	proj.Close()
	if err != nil {
		t.Fatalf("projection values: %v", err)
	}
	if fmt.Sprint(vals[0]) != "bob" {
		t.Errorf("projected name = %v, want bob", vals[0])
	}
	if vals[1] != nil {
		t.Errorf("projected missing column ghost = %v, want SQL NULL (nil)", vals[1])
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
func queryName(t *testing.T, ctx context.Context, conn *pgx.Conn, virtualSchema, asOf, pk string) string {
	t.Helper()
	sqlText := "SELECT * FROM " + virtualSchema + ".users AS OF '" + asOf + "' WHERE id = " + pk
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
	if id != pk {
		t.Errorf("id = %q, want %s", id, pk)
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

// TestPGWire_SnapshotBaselineRow distinguishes _snapshot from _flashback OVER THE
// WIRE: a baseline row whose PK has ZERO binlog events resolves under _snapshot
// (baseline-aware) but is absent under _flashback (binlog-only). The no-baseline
// _snapshot case in TestPGWire_SingleRowRoundTrip only proves routing; this
// proves the baseline fold is actually delivered over the PG protocol.
func TestPGWire_SnapshotBaselineRow(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
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

	// Baseline anchored inside the partitioned hour (so the Since→AsOf delta scan
	// has no coverage gap), carrying id=42 which has NO binlog events — only
	// _snapshot's baseline fold can surface it.
	baselineDir := writePGWireBaseline(t, hour.Add(time.Minute), "public", "users",
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		},
		[][]string{{"42", "ghost"}})

	addr := serveAddrWithDB(t, Config{
		IndexDB:    db,
		ShimConfig: shim.Config{NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir},
		Auth:       testAuth(t),
	})
	conn, err := connectPGWire(t, addr, testUser, testPass)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	asOf := hour.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	if got := queryName(t, ctx, conn, "_snapshot", asOf, "42"); got != "ghost" {
		t.Errorf("_snapshot baseline-only row: name=%q, want ghost", got)
	}
	// _flashback is binlog-only: id=42 has no events → zero rows.
	fb, err := conn.Query(ctx, "SELECT * FROM _flashback.users AS OF '"+asOf+"' WHERE id = 42")
	if err != nil {
		t.Fatalf("_flashback query: %v", err)
	}
	got := fb.Next()
	fb.Close()
	if err := fb.Err(); err != nil {
		t.Fatalf("_flashback rows err: %v", err)
	}
	if got {
		t.Error("_flashback must return zero rows for a baseline-only PK (binlog-only)")
	}
}

// writePGWireBaseline writes a minimal `bintrail baseline` Parquet snapshot at
// the FindBaseline directory layout (<root>/<name>Z/<schema>/<table>.parquet) so
// a _snapshot query can fold it. Mirrors internal/shim's writeBaselineSnapshot.
func writePGWireBaseline(t *testing.T, snapTime time.Time, schema, table string, cols []baseline.Column, rows [][]string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, snapTime.UTC().Format("2006-01-02T15-04-05")+"Z", schema)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir baseline layout: %v", err)
	}
	w, err := baseline.NewWriter(filepath.Join(dir, table+".parquet"), cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 100,
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	nulls := make([]bool, len(cols))
	for _, r := range rows {
		if err := w.WriteRow(r, nulls); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("baseline writer close: %v", err)
	}
	return root
}

func fieldNames(fds []pgconn.FieldDescription) []string {
	out := make([]string, len(fds))
	for i, f := range fds {
		out[i] = f.Name
	}
	return out
}
