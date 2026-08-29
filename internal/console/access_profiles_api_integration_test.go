//go:build integration

package console

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// accessProfilesFixture is an index with two tables' events: a customers
// row whose email a pii flag will redact, and an invoices row a billing
// flag will withhold.
func accessProfilesFixture(t *testing.T) *Server {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "customers", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"email":"ann@example.com","name":"Ann"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 12:01:00", nil,
		"app", "invoices", 1 /*INSERT*/, "9",
		nil, nil, []byte(`{"id":9,"amount":5}`))
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// fetchAsProfile runs the query engine the way `bintrail query --profile
// <name>` does (internal/cli/query.go): resolve the profile's rules from the
// index, then Fetch with them applied.
func fetchAsProfile(t *testing.T, srv *Server, profile string) []query.ResultRow {
	t.Helper()
	ctx := context.Background()
	db := srv.cm.boot.db
	deny, redact, err := query.LoadProfileRules(ctx, db, profile)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := query.New(db).Fetch(ctx, query.Options{
		Schema: "app", Limit: 100,
		DenyTables: deny, RedactColumns: redact, ProfileActive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func mustAccessOK(t *testing.T, srv *Server, path, body string) accessProfilesDoc {
	t.Helper()
	rec, raw := doReq(t, srv, "POST", path, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST %s %s = %d body=%s", path, body, rec.Code, raw)
	}
	var doc accessProfilesDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// TestIntegrationAccessProfilesEnforcedByQueryEngine is the end-to-end pin:
// a profile authored through the API is what the real query engine enforces
// under --profile. Deny on a table-level flag withholds the table; deny on a
// column-level flag nulls the column; removing the rule from the API lifts
// the redaction; removing the profile removes its rules with it.
func TestIntegrationAccessProfilesEnforcedByQueryEngine(t *testing.T) {
	srv := accessProfilesFixture(t)

	// Before any rule: both rows, email in the clear (a profile with no
	// rules still counts as active, so query_text is blank, but that is not
	// what this test is about).
	mustAccessOK(t, srv, "/api/access-profiles/profiles", `{"name":"marketing","description":"Marketing analysts"}`)
	rows := fetchAsProfile(t, srv, "marketing")
	if len(rows) != 2 {
		t.Fatalf("before rules: %d rows, want 2", len(rows))
	}

	mustAccessOK(t, srv, "/api/access-profiles/flags", `{"flag":"pii","schema":"app","table":"customers","column":"email"}`)
	mustAccessOK(t, srv, "/api/access-profiles/flags", `{"flag":"billing","schema":"app","table":"invoices"}`)
	mustAccessOK(t, srv, "/api/access-profiles/rules", `{"profile":"marketing","flag":"pii","permission":"deny"}`)
	doc := mustAccessOK(t, srv, "/api/access-profiles/rules", `{"profile":"marketing","flag":"billing","permission":"deny"}`)
	if len(doc.Flags) != 2 || len(doc.Profiles) != 1 || len(doc.Rules) != 2 {
		t.Fatalf("document after authoring = %+v", doc)
	}

	rows = fetchAsProfile(t, srv, "marketing")
	if len(rows) != 1 || rows[0].TableName != "customers" {
		t.Fatalf("with both denies: rows = %+v, want only the customers row (invoices withheld)", rows)
	}
	if v, ok := rows[0].RowAfter["email"]; !ok || v != nil {
		t.Errorf("email = %v, want redacted to NULL", v)
	}
	if rows[0].RowAfter["name"] != "Ann" {
		t.Errorf("name = %v, want left in the clear", rows[0].RowAfter["name"])
	}

	// Lift the column redaction from the page; the table deny stays.
	mustAccessOK(t, srv, "/api/access-profiles/rules/remove", `{"profile":"marketing","flag":"pii"}`)
	rows = fetchAsProfile(t, srv, "marketing")
	if len(rows) != 1 || rows[0].RowAfter["email"] != "ann@example.com" {
		t.Errorf("after removing the pii rule: rows = %+v, want email visible and invoices still withheld", rows)
	}

	// Removing the profile takes its remaining rule with it.
	doc = mustAccessOK(t, srv, "/api/access-profiles/profiles/remove", `{"name":"marketing"}`)
	if len(doc.Profiles) != 0 || len(doc.Rules) != 0 || len(doc.Flags) != 2 {
		t.Errorf("after removing the profile: %+v, want no profiles, no rules, both flags", doc)
	}
	var n int
	if err := srv.cm.boot.db.QueryRow(`SELECT COUNT(*) FROM access_rules`).Scan(&n); err != nil || n != 0 {
		t.Errorf("access_rules rows after profile remove = %d (err %v), want 0 (the FK cascades)", n, err)
	}
}

// TestIntegrationAccessProfilesRefusalsOnRealIndex: the refusals a real
// database produces (not a mocked RowsAffected), with the shared package's
// words on the wire.
func TestIntegrationAccessProfilesRefusalsOnRealIndex(t *testing.T) {
	srv := accessProfilesFixture(t)
	cases := []struct {
		path, body, want string
		code             int
	}{
		{"/api/access-profiles/rules", `{"profile":"ghost","flag":"pii","permission":"deny"}`, `profile "ghost" not found`, http.StatusNotFound},
		{"/api/access-profiles/rules", `{"profile":"ghost","flag":"pii","permission":"maybe"}`, `permission must be "allow" or "deny", got "maybe"`, http.StatusBadRequest},
		{"/api/access-profiles/flags/remove", `{"flag":"pii","schema":"app","table":"customers"}`, `flag "pii" not found on app.customers`, http.StatusNotFound},
		{"/api/access-profiles/profiles/remove", `{"name":"ghost"}`, `profile "ghost" not found`, http.StatusNotFound},
		{"/api/access-profiles/rules/remove", `{"profile":"ghost","flag":"pii"}`, `access rule not found: profile="ghost" flag="pii"`, http.StatusNotFound},
	}
	for _, tc := range cases {
		rec, raw := doReq(t, srv, "POST", tc.path, tc.body)
		if rec.Code != tc.code {
			t.Errorf("POST %s %s = %d body=%s, want %d", tc.path, tc.body, rec.Code, raw, tc.code)
			continue
		}
		var body map[string]string
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		if body["error"] != tc.want {
			t.Errorf("POST %s %s error = %q, want %q", tc.path, tc.body, body["error"], tc.want)
		}
	}
}
