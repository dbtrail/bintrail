//go:build integration

package console

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
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

	// Removing the flag a rule references leaves the rule in place (a rule
	// names a flag by text; nothing cascades), the API still lists it, and
	// the deny now covers no table: invoices come back.
	doc = mustAccessOK(t, srv, "/api/access-profiles/flags/remove", `{"flag":"billing","schema":"app","table":"invoices"}`)
	if len(doc.Flags) != 1 || doc.Flags[0].Flag != "pii" {
		t.Errorf("after removing the billing flag: flags = %+v, want only pii", doc.Flags)
	}
	if len(doc.Rules) != 1 || doc.Rules[0].Flag != "billing" || doc.Rules[0].Permission != "deny" {
		t.Errorf("after removing the billing flag: rules = %+v, want the deny on billing still listed", doc.Rules)
	}
	if rows = fetchAsProfile(t, srv, "marketing"); len(rows) != 2 {
		t.Errorf("after removing the billing flag: %d rows, want 2 (the deny covers no table now)", len(rows))
	}

	// Removing the profile takes its remaining rule with it.
	doc = mustAccessOK(t, srv, "/api/access-profiles/profiles/remove", `{"name":"marketing"}`)
	if len(doc.Profiles) != 0 || len(doc.Rules) != 0 || len(doc.Flags) != 1 {
		t.Errorf("after removing the profile: %+v, want no profiles, no rules, the pii flag", doc)
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

// eventsAs GETs /api/events for schema app as the given bearer and returns
// the page, so a profiled session's view is read through the console's own
// applySessionProfile path (cache included), not a hand-rolled fetch.
func eventsAs(t *testing.T, srv *Server, bearer string) []eventDTO {
	t.Helper()
	w := accessReq(t, srv, "GET", "/api/events?schema=app&limit=100", bearer, "")
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/events = %d body=%s", w.Code, w.Body.String())
	}
	var resp eventsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp.Events
}

// TestIntegrationAccessProfilesChangeReachesProfiledSession: a session that
// carries the marketing profile has its rules cached per server for 30
// seconds. A rule authored from the API must reach that session's NEXT
// request, through the console's own enforcement path: the mutation drops
// the cache entry, so the session is not under-redacted for half a minute.
func TestIntegrationAccessProfilesChangeReachesProfiledSession(t *testing.T) {
	srv := accessProfilesFixture(t)
	// The profile and the flags exist before the priming read, so the ONLY
	// mutation between the two reads below is the rule add: this test is
	// about that verb's invalidation, not the flag verbs' (each verb
	// invalidates; a rule add that did not would be hidden behind a flag
	// add that did).
	mustAccessOK(t, srv, "/api/access-profiles/profiles", `{"name":"marketing"}`)
	mustAccessOK(t, srv, "/api/access-profiles/flags", `{"flag":"pii","schema":"app","table":"customers","column":"email"}`)
	mustAccessOK(t, srv, "/api/access-profiles/flags", `{"flag":"billing","schema":"app","table":"invoices"}`)
	profiled, _, err := srv.sessions.IssueWithPolicy("sam@example.com",
		&ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: "marketing"})
	if err != nil {
		t.Fatal(err)
	}

	// First read: no rules, both rows, email in the clear. This primes the
	// (server, marketing) cache entry.
	evs := eventsAs(t, srv, profiled)
	if len(evs) != 2 {
		t.Fatalf("before rules: %d events, want 2", len(evs))
	}
	if _, cached := srv.sessionProfiles.m[srv.cm.defaultID()+"\x00marketing"]; !cached {
		t.Fatal("the first profiled read did not cache the profile's rules; the assertion below would be vacuous")
	}

	// Author the deny rules from the API (as the operator, not the profiled
	// session). Well inside the TTL.
	mustAccessOK(t, srv, "/api/access-profiles/rules", `{"profile":"marketing","flag":"pii","permission":"deny"}`)
	mustAccessOK(t, srv, "/api/access-profiles/rules", `{"profile":"marketing","flag":"billing","permission":"deny"}`)

	evs = eventsAs(t, srv, profiled)
	if len(evs) != 1 || evs[0].TableName != "customers" {
		t.Fatalf("after the deny rules: events = %+v, want only the customers row (invoices withheld) on the very next request", evs)
	}
	if v, ok := evs[0].RowAfter["email"]; !ok || v != nil {
		t.Errorf("email = %v, want redacted to NULL on the very next request", v)
	}

	// And the other way: removing a deny lifts it on the next request.
	mustAccessOK(t, srv, "/api/access-profiles/rules/remove", `{"profile":"marketing","flag":"billing"}`)
	if evs = eventsAs(t, srv, profiled); len(evs) != 2 {
		t.Errorf("after removing the billing rule: %d events, want 2", len(evs))
	}
}

// TestIntegrationAccessProfilesTargetsSelectedServer: the verbs write to the
// server the X-Bintrail-Server header names, not to the boot index, and the
// cache invalidation is that server's.
func TestIntegrationAccessProfilesTargetsSelectedServer(t *testing.T) {
	srv := accessProfilesFixture(t)
	t.Cleanup(srv.cm.CloseAll)
	db2, dbName2 := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db2)
	rec, body := doReq(t, srv, "POST", "/api/servers", `{"name":"second","dsn":"`+testutil.IntegrationDSN(dbName2)+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create server: code=%d body=%s", rec.Code, body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}

	// Seed a cached resolution on both servers under the key shape
	// applySessionProfile uses (server id, NUL, profile).
	bootKey := srv.cm.defaultID() + "\x00marketing"
	secondKey := created.ID + "\x00marketing"
	srv.sessionProfiles.m[bootKey] = profileRuleEntry{exists: true, loadedAt: time.Now()}
	srv.sessionProfiles.m[secondKey] = profileRuleEntry{exists: true, loadedAt: time.Now()}

	rec, body = doReqOn(t, srv, created.ID, "POST", "/api/access-profiles/flags",
		`{"flag":"pii","schema":"shop","table":"orders","column":"email"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("flag add on the registry server: code=%d body=%s", rec.Code, body)
	}
	var doc accessProfilesDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Flags) != 1 || doc.Flags[0].Table != "orders" {
		t.Errorf("readback after the write = %+v, want the one flag", doc.Flags)
	}

	var n int
	if err := db2.QueryRow(`SELECT COUNT(*) FROM table_flags`).Scan(&n); err != nil || n != 1 {
		t.Errorf("registry server table_flags = %d (err %v), want 1: the row must land on the selected server", n, err)
	}
	if err := srv.cm.boot.db.QueryRow(`SELECT COUNT(*) FROM table_flags`).Scan(&n); err != nil || n != 0 {
		t.Errorf("boot table_flags = %d (err %v), want 0: the row must not land on the boot index", n, err)
	}
	if _, still := srv.sessionProfiles.m[secondKey]; still {
		t.Error("the selected server's cached profile rules survived the write")
	}
	if _, kept := srv.sessionProfiles.m[bootKey]; !kept {
		t.Error("the boot server's cached rules were dropped; invalidation must target the selected server")
	}

	// The two GETs see two different indexes.
	rec, body = doReqOn(t, srv, created.ID, "GET", "/api/access-profiles", "")
	if err := json.Unmarshal(body, &doc); rec.Code != http.StatusOK || err != nil || len(doc.Flags) != 1 {
		t.Errorf("GET on the registry server: code=%d flags=%+v body=%s", rec.Code, doc.Flags, body)
	}
	rec, body = doReq(t, srv, "GET", "/api/access-profiles", "")
	if err := json.Unmarshal(body, &doc); rec.Code != http.StatusOK || err != nil || len(doc.Flags) != 0 {
		t.Errorf("GET on the boot index: code=%d flags=%+v body=%s", rec.Code, doc.Flags, body)
	}
}
