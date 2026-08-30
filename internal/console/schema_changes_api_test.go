package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// schemaChangesOrderBy is the ordering contract of GET /api/schema-changes
// (#1443, mirrored on the MCP tool by #1441): detected_at is second-granular
// and DDL arrives in same-second bursts, so the binlog coordinate must break
// the tie and id must make the cut deterministic. The integration tier proves
// the ORDER against a real MySQL; this constant pins the SQL text, so a
// dropped tiebreak fails here even when no database is available.
const schemaChangesOrderBy = "ORDER BY detected_at DESC, binlog_file DESC, binlog_pos DESC, id DESC LIMIT ?"

var schemaChangesCols = []string{"id", "detected_at", "schema_name", "table_name", "ddl_type", "ddl_query", "binlog_file", "binlog_pos"}

func schemaChangeRow(rows *sqlmock.Rows, id int64, at time.Time, schema, table, ddlType, stmt, file string, pos uint64) *sqlmock.Rows {
	return rows.AddRow(id, at, schema, table, ddlType, stmt, file, pos)
}

// TestBuildSchemaChangesQuery pins the query shape for every filter at once:
// clause spelling, argument ORDER (the mock below matches positionally, so a
// reordered append would pass through sqlmock with the wrong value in the
// wrong slot), the prefix LIKE for ddl_type, and the ordering tiebreak.
func TestBuildSchemaChangesQuery(t *testing.T) {
	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	q, args := buildSchemaChangesQuery(schemaChangesFilter{
		Schema:  "app",
		Table:   "users",
		DDLType: "ALTER",
		Since:   &since,
		Until:   &until,
		Allow:   []query.SchemaTable{{Schema: "app", Table: "users"}, {Schema: "app", Table: "orders"}},
		Deny:    []query.SchemaTable{{Schema: "app", Table: "secrets"}},
		Fetch:   101,
	})
	wantClauses := []string{
		"schema_name = ?",
		"table_name = ?",
		"ddl_type LIKE ?",
		"detected_at >= ?",
		"detected_at <= ?",
		"((BINARY schema_name = ? AND BINARY table_name = ?) OR (BINARY schema_name = ? AND BINARY table_name = ?))",
		// Deny also catches the unqualified row (schema_name = '') whose
		// table matches: see buildSchemaChangesQuery.
		"NOT (table_name = ? AND (schema_name = ? OR schema_name = ''))",
	}
	for _, c := range wantClauses {
		if !strings.Contains(q, c) {
			t.Errorf("query lacks clause %q:\n%s", c, q)
		}
	}
	if !strings.HasSuffix(q, schemaChangesOrderBy) {
		t.Errorf("query does not end with the ordering tiebreak %q:\n%s", schemaChangesOrderBy, q)
	}
	wantArgs := []any{"app", "users", "ALTER%", since, until, "app", "users", "app", "orders", "secrets", "app", 101}
	if len(args) != len(wantArgs) {
		t.Fatalf("args = %v, want %v", args, wantArgs)
	}
	for i := range wantArgs {
		if args[i] != wantArgs[i] {
			t.Errorf("args[%d] = %v, want %v", i, args[i], wantArgs[i])
		}
	}

	// No filters: no WHERE at all, only the cap.
	q, args = buildSchemaChangesQuery(schemaChangesFilter{Fetch: 11})
	if strings.Contains(q, "WHERE") {
		t.Errorf("unfiltered query must carry no WHERE: %s", q)
	}
	if len(args) != 1 || args[0] != 11 {
		t.Errorf("unfiltered args = %v, want [11]", args)
	}
}

// TestSchemaChangesHandler drives the REAL handler over sqlmock: the request
// parameters land in the SQL in order, the probe row is asked for and never
// serialized, and the DTO carries the wire names the MCP tool uses.
func TestSchemaChangesHandler(t *testing.T) {
	db, mock, closer := newSQLMock(t)
	defer closer()
	at := time.Date(2026, 6, 1, 12, 0, 5, 0, time.UTC)
	rows := sqlmock.NewRows(schemaChangesCols)
	schemaChangeRow(rows, 7, at, "app", "users", "ALTER TABLE", "ALTER TABLE users ADD COLUMN x INT", "bin.000002", 300)
	mock.ExpectQuery(regexp.QuoteMeta(schemaChangesOrderBy)).
		WithArgs("app", "users", "ALTER%", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), 101).
		WillReturnRows(rows)

	s := newBootServer(db)
	req := httptest.NewRequest("GET", "/api/schema-changes?schema=app&table=users&ddl_type=alter&since=2026-06-01", nil)
	rec := httptest.NewRecorder()
	s.handleSchemaChanges(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp schemaChangesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Count != 1 || resp.Limit != 100 || resp.HasMore {
		t.Errorf("count/limit/has_more = %d/%d/%v, want 1/100/false", resp.Count, resp.Limit, resp.HasMore)
	}
	want := schemaChangeDTO{ID: 7, DetectedAt: "2026-06-01 12:00:05", Schema: "app", Table: "users",
		DDLType: "ALTER TABLE", Statement: "ALTER TABLE users ADD COLUMN x INT", BinlogFile: "bin.000002", BinlogPos: 300}
	if len(resp.Changes) != 1 || resp.Changes[0] != want {
		t.Errorf("changes = %+v, want [%+v]", resp.Changes, want)
	}
	for _, key := range []string{`"detected_at"`, `"schema_name"`, `"table_name"`, `"ddl_type"`, `"statement"`, `"binlog_file"`, `"binlog_pos"`} {
		if !strings.Contains(rec.Body.String(), key) {
			t.Errorf("wire body lacks %s: %s", key, rec.Body.String())
		}
	}
	// Open-core line: no attribution field rides on this free surface.
	if strings.Contains(rec.Body.String(), "connection_id") {
		t.Errorf("schema-changes must not carry connection_id: %s", rec.Body.String())
	}
	// An unrestricted session: nothing withheld, nothing to warn about.
	if resp.StatementWithheld || len(resp.Warnings) != 0 || strings.Contains(rec.Body.String(), "statement_withheld") {
		t.Errorf("unrestricted read must carry no withheld flag or warnings: %s", rec.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestSchemaChangesCaps pins the Events caps model: an absent limit is 100,
// an oversized one clamps to 1000, the database is asked for one row more,
// and that probe row sets has_more without being serialized.
func TestSchemaChangesCaps(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		db, mock, closer := newSQLMock(t)
		defer closer()
		mock.ExpectQuery("FROM schema_changes").WithArgs(101).WillReturnRows(sqlmock.NewRows(schemaChangesCols))
		s := newBootServer(db)
		rec := httptest.NewRecorder()
		s.handleSchemaChanges(rec, httptest.NewRequest("GET", "/api/schema-changes", nil))
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"changes":[]`) {
			t.Errorf("empty result must serialize as [], not null: %s", rec.Body.String())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("clamped with probe", func(t *testing.T) {
		db, mock, closer := newSQLMock(t)
		defer closer()
		rows := sqlmock.NewRows(schemaChangesCols)
		at := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		for i := 0; i < 1001; i++ {
			schemaChangeRow(rows, int64(1001-i), at, "app", "t", "CREATE TABLE", "CREATE TABLE t (id INT)", "bin.000001", uint64(2000-i))
		}
		mock.ExpectQuery("FROM schema_changes").WithArgs(1001).WillReturnRows(rows)
		s := newBootServer(db)
		rec := httptest.NewRecorder()
		s.handleSchemaChanges(rec, httptest.NewRequest("GET", "/api/schema-changes?limit=5000", nil))
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, rec.Body.String())
		}
		var resp schemaChangesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Limit != 1000 || resp.Count != 1000 || len(resp.Changes) != 1000 || !resp.HasMore {
			t.Errorf("limit/count/len/has_more = %d/%d/%d/%v, want 1000/1000/1000/true",
				resp.Limit, resp.Count, len(resp.Changes), resp.HasMore)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

// TestSchemaChangesBadInput: malformed filters are a 400 before any query
// runs — never a silent fall back to an unfiltered read.
func TestSchemaChangesBadInput(t *testing.T) {
	for _, tc := range []struct{ name, qs, want string }{
		{"unknown ddl_type", "ddl_type=GRANT", "invalid ddl_type"},
		{"bad since", "since=yesterday-ish", "invalid since"},
		{"bad until", "until=2026-13-45", "invalid until"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, closer := newSQLMock(t)
			defer closer()
			s := newBootServer(db)
			rec := httptest.NewRecorder()
			s.handleSchemaChanges(rec, httptest.NewRequest("GET", "/api/schema-changes?"+tc.qs, nil))
			if rec.Code != 400 || !strings.Contains(rec.Body.String(), tc.want) {
				t.Errorf("code = %d body = %s, want 400 containing %q", rec.Code, rec.Body.String(), tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestSchemaChangesMissingTable: an index provisioned before DDL tracking has
// no schema_changes table (ER_NO_SUCH_TABLE, 1146). That is an actionable
// 422 naming the fix, not a bare 500.
func TestSchemaChangesMissingTable(t *testing.T) {
	db, mock, closer := newSQLMock(t)
	defer closer()
	mock.ExpectQuery("FROM schema_changes").WillReturnError(&mysql.MySQLError{Number: 1146, Message: "Table 'idx.schema_changes' doesn't exist"})
	s := newBootServer(db)
	rec := httptest.NewRecorder()
	s.handleSchemaChanges(rec, httptest.NewRequest("GET", "/api/schema-changes", nil))
	if rec.Code != 422 || !strings.Contains(rec.Body.String(), "bintrail init") {
		t.Errorf("code = %d body = %s, want 422 naming init", rec.Code, rec.Body.String())
	}
}

// TestSchemaChangesRestrictedSessionScope pins that a session's direct
// restrictions (#1449) reach the SQL: the deny and allow clauses are emitted
// with the session's tables, in the spelling buildQuery uses for row events.
// The body-level proof (the denied table's DDL is absent from the response)
// runs against a real MySQL in the integration tier, where the database
// actually applies the clause.
func TestSchemaChangesRestrictedSessionScope(t *testing.T) {
	db, mock, closer := newSQLMock(t)
	defer closer()
	mock.ExpectQuery(regexp.QuoteMeta("(BINARY schema_name = ? AND BINARY table_name = ?)) AND NOT (table_name = ? AND (schema_name = ? OR schema_name = ''))")).
		WithArgs("app", "users", "secrets", "app", 101).
		WillReturnRows(sqlmock.NewRows(schemaChangesCols))
	s := newBootServer(db)
	req := httptest.NewRequest("GET", "/api/schema-changes", nil)
	req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, &ext.AccessPolicy{
		Permissions: ext.AllPermissions(),
		Restrictions: &ext.SessionRestrictions{
			AllowTables: []ext.TableRef{{Schema: "app", Table: "users"}},
			DenyTables:  []ext.TableRef{{Schema: "app", Table: "secrets"}},
		},
	}))
	rec := httptest.NewRecorder()
	s.handleSchemaChanges(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	// Direct restrictions are an active profile too (applySessionProfile
	// sets ProfileActive for them), so the statement column is withheld and
	// both notices ride the response.
	assertSchemaChangesWithheld(t, rec.Body.Bytes())
}

// assertSchemaChangesWithheld checks the withheld shape: the flag set, both
// warnings present, and no statement text anywhere in the body.
func assertSchemaChangesWithheld(t *testing.T, body []byte) {
	t.Helper()
	var resp schemaChangesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.StatementWithheld {
		t.Errorf("statement_withheld must be true under an active profile: %s", body)
	}
	if len(resp.Warnings) != 2 || resp.Warnings[0] != schemaChangesScopeWarning || resp.Warnings[1] != schemaChangesWithheldWarning {
		t.Errorf("warnings = %q, want the scope and withheld notices", resp.Warnings)
	}
	for _, c := range resp.Changes {
		if c.Statement != "" {
			t.Errorf("statement leaked under an active profile: %q", c.Statement)
		}
	}
}

// TestSchemaChangesStatementWithheld pins the #699-style posture for DDL
// text: with the startup profile active (the process-wide flag, no session
// needed), the statement is absent from the WHOLE body — the canary literal
// the mock returns must not appear anywhere — while the coordinates stay.
func TestSchemaChangesStatementWithheld(t *testing.T) {
	db, mock, closer := newSQLMock(t)
	defer closer()
	const canary = "ALTER TABLE users ADD COLUMN token VARCHAR(16) DEFAULT 'canary-literal-9f3'"
	rows := sqlmock.NewRows(schemaChangesCols)
	schemaChangeRow(rows, 3, time.Date(2026, 6, 1, 12, 0, 5, 0, time.UTC), "app", "users", "ALTER TABLE", canary, "bin.000001", 200)
	mock.ExpectQuery("FROM schema_changes").WillReturnRows(rows)
	s := newBootServer(db)
	s.profileActive = true
	rec := httptest.NewRecorder()
	s.handleSchemaChanges(rec, httptest.NewRequest("GET", "/api/schema-changes", nil))
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "canary-literal") || strings.Contains(rec.Body.String(), "ADD COLUMN") {
		t.Errorf("statement text reached the body under an active profile: %s", rec.Body.String())
	}
	assertSchemaChangesWithheld(t, rec.Body.Bytes())
	var resp schemaChangesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Changes) != 1 || resp.Changes[0].Table != "users" || resp.Changes[0].DDLType != "ALTER TABLE" || resp.Changes[0].BinlogPos != 200 {
		t.Errorf("coordinates must survive withholding: %+v", resp.Changes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
