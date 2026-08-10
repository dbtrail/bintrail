//go:build integration

package console

import (
	"encoding/json"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationProfilesAPI drives GET /api/profiles through the real
// handler + query.ListProfiles against a live index: sorted names, then the
// legacy-index case (profiles table absent) degrading to an empty list
// rather than a 500.
func TestIntegrationProfilesAPI(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
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

	for _, name := range []string{"ops_redact", "dev"} {
		if _, err := db.Exec(`INSERT INTO profiles (name) VALUES (?)`, name); err != nil {
			t.Fatal(err)
		}
	}

	rec, body := doReq(t, srv, "GET", "/api/profiles", "")
	if rec.Code != 200 {
		t.Fatalf("profiles code = %d, body = %s", rec.Code, body)
	}
	var resp profilesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Profiles) != 2 || resp.Profiles[0] != "dev" || resp.Profiles[1] != "ops_redact" {
		t.Fatalf("profiles = %v, want sorted [dev ops_redact]", resp.Profiles)
	}

	// Legacy index: access_rules references profiles, so drop it first.
	if _, err := db.Exec(`DROP TABLE access_rules`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE profiles`); err != nil {
		t.Fatal(err)
	}
	rec, body = doReq(t, srv, "GET", "/api/profiles", "")
	if rec.Code != 200 {
		t.Fatalf("profiles (no table) code = %d, body = %s (a legacy index must list empty, not error)", rec.Code, body)
	}
	resp = profilesResponse{}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Profiles == nil || len(resp.Profiles) != 0 {
		t.Fatalf("profiles (no table) = %#v, want empty non-nil list", resp.Profiles)
	}
}
