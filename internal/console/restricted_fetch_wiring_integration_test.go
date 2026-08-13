//go:build integration

package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The unit test proves restrictedFetchWarnings returns the right strings. This
// proves the string reaches the WIRE: the handler is driven end to end with a
// real profiled session, no time filter (the case that used to warn about
// nothing at all), and the response is read as a client reads it.
//
// Mutating fetchRestricted to drop the exclusion, or handleEvents to call the
// old gapWarnings, leaves the unit test passing and breaks this one.
func TestIntegrationProfiledSessionDeclaresItsScope(t *testing.T) {
	// A server that DOES read archives, so the only exclusion under test is
	// the session's own profile. seedConsoleData sets NoArchive, which would
	// make the unprofiled control fail for the server-wide reason and prove
	// nothing about profiles.
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))
	// The profile must EXIST in the index or the request is refused before
	// any of this is reached — which is how the first draft of this test
	// silently proved nothing.
	if _, err := db.Exec(`INSERT INTO profiles (name) VALUES ('analyst')`); err != nil {
		t.Fatalf("seed profile: %v", err)
	}
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken})
	if err != nil {
		t.Fatal(err)
	}

	get := func(t *testing.T, profile string) []string {
		t.Helper()
		// No since/until on purpose: the default browse is exactly where the
		// planner does not run, so before #1311 the response carried no
		// warning at all while the session read half the index.
		r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/events?schema=app&table=users", nil)
		r.Host = "127.0.0.1:8090"
		if profile != "" {
			r = r.WithContext(context.WithValue(r.Context(),
				policyCtxKey{}, &ext.AccessPolicy{Profile: profile, Permissions: ext.AllPermissions()}))
		}
		w := httptest.NewRecorder()
		srv.handleEvents(w, r)
		if w.Code != 200 {
			t.Fatalf("events code = %d, body = %s", w.Code, w.Body.String())
		}
		var resp struct {
			Count    int      `json:"count"`
			Warnings []string `json:"warnings"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v\n%s", err, w.Body.String())
		}
		// Rows must still come back — this is a warning, not a refusal.
		if resp.Count == 0 {
			t.Fatal("the profiled read returned no rows; the fixture or the fetch is broken, and the warning assertion below would be vacuous")
		}
		return resp.Warnings
	}

	if w := get(t, ""); len(w) != 0 {
		t.Errorf("an unprofiled session must warn about nothing, got %#v", w)
	}

	// /api/recover is the endpoint whose warning actually reaches a human
	// today (the recover view renders response warnings; the events view did
	// not until this change). It had no coverage at all, which meant the one
	// working half of the feature was the untested half.
	{
		body := strings.NewReader(`{"schema":"app","table":"users","dry_run":true}`)
		r := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/recover", body)
		r.Host = "127.0.0.1:8090"
		r = r.WithContext(context.WithValue(r.Context(),
			policyCtxKey{}, &ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
		w := httptest.NewRecorder()
		srv.handleRecover(w, r)
		if w.Code != 200 {
			t.Fatalf("recover code = %d, body = %s", w.Code, w.Body.String())
		}
		var resp struct {
			Warnings []string `json:"warnings"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode recover: %v\n%s", err, w.Body.String())
		}
		if !strings.Contains(strings.Join(resp.Warnings, "\n"), "LIVE INDEX ONLY") {
			t.Errorf("/api/recover does not declare its scope to a profiled session: %#v", resp.Warnings)
		}
	}

	got := strings.Join(get(t, "analyst"), "\n")
	if !strings.Contains(got, "LIVE INDEX ONLY") {
		t.Fatalf("the profiled response does not declare its scope — a short result still reads as an answer about the data:\n%s", got)
	}
	if !strings.Contains(got, "does not mean nothing happened") {
		t.Errorf("the response does not deny the wrong inference:\n%s", got)
	}
}
