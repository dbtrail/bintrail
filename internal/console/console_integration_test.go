//go:build integration

package console

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dbtrail/bintrail/internal/query"
	"github.com/dbtrail/bintrail/internal/recovery"
	"github.com/dbtrail/bintrail/internal/testutil"
)

const intToken = "integration-token"

func seedConsoleData(t *testing.T) (*Server, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// INSERT then UPDATE on app.users pk=1.
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"name":"alice"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 12:05:00", nil,
		"app", "users", 2 /*UPDATE*/, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"alicia"}`))

	srv, err := New(Config{
		DB:        db,
		DBName:    dbName,
		Listen:    "127.0.0.1:8090",
		Token:     intToken,
		NoArchive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, dbName
}

func doReq(t *testing.T, srv *Server, method, path, body string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, r)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer "+intToken)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec, rec.Body.Bytes()
}

func TestIntegrationEventsAPI(t *testing.T) {
	srv, _ := seedConsoleData(t)

	rec, body := doReq(t, srv, "GET", "/api/events?schema=app&table=users", "")
	if rec.Code != 200 {
		t.Fatalf("events code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), "connection_id") {
		t.Errorf("events response must not contain connection_id: %s", body)
	}
	var resp eventsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Count != 2 {
		t.Errorf("event count = %d, want 2", resp.Count)
	}
}

func TestIntegrationSchemasAPI(t *testing.T) {
	srv, _ := seedConsoleData(t)

	_, body := doReq(t, srv, "GET", "/api/schemas", "")
	var sr schemasResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatal(err)
	}
	if !contains(sr.Schemas, "app") {
		t.Errorf("schemas = %v, want to include app", sr.Schemas)
	}

	_, body = doReq(t, srv, "GET", "/api/schemas?schema=app", "")
	var tr tablesResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		t.Fatal(err)
	}
	if !contains(tr.Tables, "users") {
		t.Errorf("tables = %v, want to include users", tr.Tables)
	}
}

func TestIntegrationStatusAPI(t *testing.T) {
	srv, _ := seedConsoleData(t)
	rec, body := doReq(t, srv, "GET", "/api/status", "")
	if rec.Code != 200 {
		t.Fatalf("status code = %d, body = %s", rec.Code, body)
	}
	var generic map[string]any
	if err := json.Unmarshal(body, &generic); err != nil {
		t.Fatalf("status is not valid JSON: %v", err)
	}
}

// TestIntegrationRecoverMatchesGenerator is the issue's manual check, automated:
// the console's undo SQL must match what the recovery generator produces for
// the same filters.
func TestIntegrationRecoverMatchesGenerator(t *testing.T) {
	srv, _ := seedConsoleData(t)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"app","table":"users"}`)
	if rec.Code != 200 {
		t.Fatalf("recover code = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatementCount != 2 {
		t.Errorf("statement_count = %d, want 2", resp.StatementCount)
	}
	// Order correctness: the newest event (the UPDATE) must be undone first,
	// then the INSERT. This anchors the fix for the DESC-vs-ASC ordering bug —
	// a buggy DESC fetch would invert these two statements.
	stmts := statements(resp.SQL)
	if len(stmts) != 2 {
		t.Fatalf("expected 2 undo statements, got %d: %v", len(stmts), stmts)
	}
	if !strings.HasPrefix(stmts[0], "UPDATE") {
		t.Errorf("first undo statement should reverse the newest (UPDATE) event, got: %s", stmts[0])
	}
	if !strings.HasPrefix(stmts[1], "DELETE") {
		t.Errorf("second undo statement should reverse the INSERT event, got: %s", stmts[1])
	}

	// Equivalence with the generator. Recovery always fetches oldest-first
	// (ASC, the zero value), which is exactly what the console forces for the
	// recover path — so the two scripts must match statement-for-statement.
	var buf bytes.Buffer
	opts := query.Options{Schema: "app", Table: "users", Order: "", Limit: recoverDefaultLimit}
	if _, err := recovery.New(srv.db, srv.resolver).GenerateSQL(context.Background(), opts, &buf); err != nil {
		t.Fatal(err)
	}
	if got, want := stmts, statements(buf.String()); !equalStrings(got, want) {
		t.Errorf("console recover SQL differs from generator:\nconsole: %v\ngenerator: %v", got, want)
	}
}

// TestIntegrationRecoverWithTimeRangeSurfacesGap exercises the planner-active
// recover path. testutil.InitIndexTables creates only the p_future partition,
// so loadLivePartitionHours is empty and every hour in a bounded query range is
// classified as a gap. Recover must still SUCCEED (200) with the undo
// statements and surface the gap as a warning — never hard-fail. This locks in
// AllowGaps=true for recover: a former AllowGaps=false would have returned 422
// here, diverging from the CLI `recover` the issue requires it to match.
func TestIntegrationRecoverWithTimeRangeSurfacesGap(t *testing.T) {
	srv, _ := seedConsoleData(t)

	body := `{"schema":"app","table":"users","since":"2026-06-01 00:00:00","until":"2026-06-02 00:00:00"}`
	rec, raw := doReq(t, srv, "POST", "/api/recover", body)
	if rec.Code != 200 {
		t.Fatalf("recover with time range code = %d, body = %s", rec.Code, raw)
	}
	var resp recoverResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatementCount != 2 {
		t.Errorf("statement_count = %d, want 2 (undo still generated despite gap)", resp.StatementCount)
	}
	if len(resp.Warnings) == 0 {
		t.Error("expected a coverage-gap warning (only p_future exists, so the bounded range is all gap)")
	}
}

// TestIntegrationProfileRBAC verifies end-to-end that profile RBAC rules are
// enforced on the console's events surface: a denied table never appears, and a
// redacted column's value is nulled while its siblings remain. It also confirms
// New forces NoArchive when RBAC rules are present (archives don't apply RBAC).
func TestIntegrationProfileRBAC(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"email":"alice@x","ssn":"111-22-3333"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 12:01:00", nil,
		"app", "secrets", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"value":"topsecret"}`))

	srv, err := New(Config{
		DB:            db,
		DBName:        dbName,
		Listen:        "127.0.0.1:8090",
		Token:         intToken,
		DenyTables:    []query.SchemaTable{{Schema: "app", Table: "secrets"}},
		RedactColumns: []query.SchemaTableColumn{{Schema: "app", Table: "users", Column: "ssn"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !srv.noArchive {
		t.Error("New must force noArchive when RBAC rules are present (archives don't enforce RBAC)")
	}

	// Denied table: querying it returns nothing and never leaks its data.
	_, body := doReq(t, srv, "GET", "/api/events?schema=app&table=secrets", "")
	if strings.Contains(string(body), "topsecret") {
		t.Errorf("denied table app.secrets leaked into the events response: %s", body)
	}

	// Redacted column nulled; non-redacted sibling still present.
	_, body = doReq(t, srv, "GET", "/api/events?schema=app&table=users", "")
	if strings.Contains(string(body), "111-22-3333") {
		t.Errorf("redacted column ssn leaked: %s", body)
	}
	if !strings.Contains(string(body), "alice@x") {
		t.Errorf("non-redacted column email should remain present: %s", body)
	}
}

// statements extracts the executable SQL lines (ignoring comments, blanks, and
// the BEGIN/COMMIT wrapper) so two scripts can be compared without their
// timestamped header comment.
func statements(sql string) []string {
	var out []string
	for _, line := range strings.Split(sql, "\n") {
		l := strings.TrimSpace(line)
		if l == "" || strings.HasPrefix(l, "--") || l == "BEGIN;" || l == "COMMIT;" {
			continue
		}
		out = append(out, l)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
