//go:build integration

package console

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedSchemaChanges provisions an index whose schema_changes table holds a
// same-second DDL burst plus an older row and a row on a second table.
//
// The burst is the #1441 shape: three statements detected in ONE second,
// inserted in an order that disagrees with their binlog position both ways —
// id ASC gives 200,300,100 and id DESC gives 100,300,200, while the binlog
// order is 300,200,100. Whatever tie order the storage engine happens to
// walk with detected_at alone, it is wrong for at least one pair, so the
// ordering assertion below cannot pass by accident.
func seedSchemaChanges(t *testing.T) *Server {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	insert := func(at, file string, pos int, schema, table, ddlType, stmt string) {
		t.Helper()
		testutil.MustExec(t, db, `INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, at, file, pos, schema, table, ddlType, stmt)
	}
	// Unqualified DDL: the parser derives schema_name from the statement
	// text alone, so `USE app; TRUNCATE TABLE secrets` lands with an EMPTY
	// schema. A deny on app.secrets has to withhold it too.
	insert("2026-06-01 11:59:00", "bin.000001", 800, "", "secrets", "TRUNCATE TABLE", "TRUNCATE TABLE secrets")
	insert("2026-06-01 12:00:00", "bin.000001", 900, "app", "users", "CREATE TABLE", "CREATE TABLE users (id INT PRIMARY KEY)")
	insert("2026-06-01 12:00:05", "bin.000001", 200, "app", "users", "ALTER TABLE", "ALTER TABLE users ADD COLUMN email VARCHAR(255)")
	insert("2026-06-01 12:00:05", "bin.000001", 300, "app", "users", "DROP TABLE", "DROP TABLE users")
	insert("2026-06-01 12:00:05", "bin.000001", 100, "app", "users", "ALTER TABLE", "ALTER TABLE users ADD COLUMN name VARCHAR(64)")
	insert("2026-06-01 12:00:05", "bin.000001", 250, "app", "secrets", "TRUNCATE TABLE", "TRUNCATE TABLE secrets")
	// A later binlog FILE at a lower position, same second: file order beats
	// position order, so this one lists first.
	insert("2026-06-01 12:00:05", "bin.000002", 50, "app", "users", "ALTER TABLE", "ALTER TABLE users DROP COLUMN name")
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: "static-tok"})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func getSchemaChanges(t *testing.T, srv *Server, qs, bearer string) schemaChangesResponse {
	t.Helper()
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/schema-changes"+qs, bearer)
	if rec.Code != 200 {
		t.Fatalf("GET /api/schema-changes%s = %d, body = %s", qs, rec.Code, rec.Body.String())
	}
	var resp schemaChangesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func positions(resp schemaChangesResponse) []uint64 {
	out := make([]uint64, len(resp.Changes))
	for i, c := range resp.Changes {
		out[i] = c.BinlogPos
	}
	return out
}

// TestIntegrationSchemaChangesOrdering proves the tiebreak against a real
// MySQL: newest second first, within a second the newest binlog file first,
// within a file the position descending. Removing the binlog_file/binlog_pos/
// id tail from the ORDER BY turns this red (verified while writing it).
func TestIntegrationSchemaChangesOrdering(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := seedSchemaChanges(t)
	resp := getSchemaChanges(t, srv, "?schema=app&table=users", "static-tok")
	got := positions(resp)
	want := []uint64{50, 300, 200, 100, 900}
	if len(got) != len(want) {
		t.Fatalf("positions = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("positions = %v, want %v (same-second DDLs must list newest binlog position first)", got, want)
		}
	}
	if resp.Changes[0].BinlogFile != "bin.000002" || resp.Changes[1].DDLType != "DROP TABLE" || resp.Changes[4].DDLType != "CREATE TABLE" {
		t.Errorf("order = %+v, want the bin.000002 row first, DROP TABLE second and CREATE TABLE last", resp.Changes)
	}
	// Same rows, same order, when the cap cuts inside the burst: the cut is
	// stable rather than an arbitrary subset of the tied group.
	capped := getSchemaChanges(t, srv, "?schema=app&table=users&limit=2", "static-tok")
	if p := positions(capped); len(p) != 2 || p[0] != 50 || p[1] != 300 || !capped.HasMore {
		t.Errorf("limit=2: positions = %v has_more = %v, want [50 300] true", p, capped.HasMore)
	}
}

// TestIntegrationSchemaChangesFilters: schema, table, ddl_type (prefix) and
// the time window each narrow the listing.
func TestIntegrationSchemaChangesFilters(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := seedSchemaChanges(t)
	if resp := getSchemaChanges(t, srv, "", "static-tok"); resp.Count != 7 {
		t.Errorf("unfiltered count = %d, want 7", resp.Count)
	}
	if resp := getSchemaChanges(t, srv, "?ddl_type=alter", "static-tok"); resp.Count != 3 ||
		resp.Changes[0].DDLType != "ALTER TABLE" || resp.Changes[1].DDLType != "ALTER TABLE" || resp.Changes[2].DDLType != "ALTER TABLE" {
		t.Errorf("ddl_type=alter: %+v, want the three ALTER TABLE rows", resp.Changes)
	}
	if resp := getSchemaChanges(t, srv, "?table=secrets", "static-tok"); resp.Count != 2 || resp.Changes[0].Table != "secrets" || resp.Changes[1].Schema != "" {
		t.Errorf("table=secrets: %+v, want both TRUNCATE rows, the unqualified one last", resp.Changes)
	}
	if resp := getSchemaChanges(t, srv, "?schema=nope", "static-tok"); resp.Count != 0 {
		t.Errorf("schema=nope: count = %d, want 0", resp.Count)
	}
	if resp := getSchemaChanges(t, srv, "?until=2026-06-01%2012:00:00", "static-tok"); resp.Count != 2 || resp.Changes[0].BinlogPos != 900 {
		t.Errorf("until=12:00:00: %+v, want the CREATE and the earlier unqualified TRUNCATE", resp.Changes)
	}
	if resp := getSchemaChanges(t, srv, "?since=2026-06-01%2012:00:01", "static-tok"); resp.Count != 5 {
		t.Errorf("since=12:00:01: count = %d, want 5", resp.Count)
	}
	// The wire time is the exact index value, so it pastes back into a filter.
	if resp := getSchemaChanges(t, srv, "?table=secrets", "static-tok"); resp.Changes[0].DetectedAt != "2026-06-01 12:00:05" {
		t.Errorf("detected_at = %q, want the stored second", resp.Changes[0].DetectedAt)
	}
}

// TestIntegrationSchemaChangesRestrictedSession: a session whose policy
// withholds a table never sees rows attributed to that table — by deny, and
// by an allow list that does not name it — and, because an active policy
// also withholds statement text, never sees any statement either; the
// policy-less static token on the same server still reads both
// (per-session, not process-global).
func TestIntegrationSchemaChangesRestrictedSession(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := seedSchemaChanges(t)

	denied := restrictedBearer(t, srv, &ext.SessionRestrictions{
		DenyTables: []ext.TableRef{{Schema: "app", Table: "secrets"}},
	})
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/schema-changes", denied)
	if rec.Code != 200 {
		t.Fatalf("denied session: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secrets") {
		t.Errorf("policy-denied table app.secrets leaked its DDL to a restricted session (the qualified row, or the unqualified one with an empty schema_name): %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "ADD COLUMN") || strings.Contains(rec.Body.String(), "PRIMARY KEY") {
		t.Errorf("statement text leaked to a restricted session: %s", rec.Body.String())
	}
	var scoped schemaChangesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &scoped); err != nil {
		t.Fatal(err)
	}
	if !scoped.StatementWithheld || len(scoped.Warnings) != 2 {
		t.Errorf("restricted read must flag withheld statements and carry both notices: %s", rec.Body.String())
	}
	if scoped.Count != 5 || scoped.Changes[0].Table != "users" || scoped.Changes[0].DDLType != "ALTER TABLE" || scoped.Changes[0].Statement != "" {
		t.Errorf("the un-denied table's rows must remain, statement empty: %+v", scoped.Changes)
	}
	// Asking for the denied table by name is not a way around the scope.
	rec = getPath(t, srv, "127.0.0.1:8090", "/api/schema-changes?schema=app&table=secrets", denied)
	if rec.Code != 200 || strings.Contains(rec.Body.String(), "secrets") {
		t.Errorf("explicit table filter must not bypass the deny: %d %s", rec.Code, rec.Body.String())
	}

	allowed := restrictedBearer(t, srv, &ext.SessionRestrictions{
		AllowTables: []ext.TableRef{{Schema: "app", Table: "users"}},
	})
	rec = getPath(t, srv, "127.0.0.1:8090", "/api/schema-changes", allowed)
	if rec.Code != 200 || strings.Contains(rec.Body.String(), "secrets") {
		t.Errorf("allow-list session must not see the unlisted table's DDL: %d %s", rec.Code, rec.Body.String())
	}

	rec = getPath(t, srv, "127.0.0.1:8090", "/api/schema-changes", "static-tok")
	if !strings.Contains(rec.Body.String(), "TRUNCATE TABLE secrets") || strings.Contains(rec.Body.String(), "statement_withheld") {
		t.Errorf("policy-less credential should see the raw listing with statements; per-session restriction leaked to the process: %s", rec.Body.String())
	}
}

// TestIntegrationSchemaChangesMissingTable: an index without the table (one
// provisioned before DDL tracking) gets the actionable 422, from a real 1146.
func TestIntegrationSchemaChangesMissingTable(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, "DROP TABLE schema_changes")
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: "static-tok"})
	if err != nil {
		t.Fatal(err)
	}
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/schema-changes", "static-tok")
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "bintrail init") {
		t.Errorf("code = %d body = %s, want 422 naming init", rec.Code, rec.Body.String())
	}
}
