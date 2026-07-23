//go:build integration

package console

import (
	"net/http"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedSensitiveProfile creates a "sensitive" profile in the index that denies
// the app.secrets table and redacts app.users.ssn — real profiles/table_flags/
// access_rules rows, so query.ProfileExists/LoadProfileRules resolve it at
// request time (the per-session enforcement path #1075 exercises).
func seedSensitiveProfile(t *testing.T, srv *Server) {
	t.Helper()
	db := srv.cm.boot.db
	testutil.MustExec(t, db, `INSERT INTO profiles (name) VALUES ('sensitive')`)
	testutil.MustExec(t, db, `INSERT INTO table_flags (schema_name, table_name, column_name, flag) VALUES ('app','secrets','','f_secrets')`)
	testutil.MustExec(t, db, `INSERT INTO table_flags (schema_name, table_name, column_name, flag) VALUES ('app','users','ssn','f_ssn')`)
	testutil.MustExec(t, db, `INSERT INTO access_rules (profile_id, flag, permission) SELECT id, 'f_secrets', 'deny' FROM profiles WHERE name='sensitive'`)
	testutil.MustExec(t, db, `INSERT INTO access_rules (profile_id, flag, permission) SELECT id, 'f_ssn', 'deny' FROM profiles WHERE name='sensitive'`)
}

// newProfileIndexServer builds a console over a fresh index seeded with an
// app.users (with an ssn) and an app.secrets event, NO startup RBAC profile.
func newProfileIndexServer(t *testing.T) *Server {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"email":"alice@example","ssn":"ssn-12345"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 12:01:00", nil,
		"app", "secrets", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"value":"topsecret"}`))
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: "static-tok"})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// scopedBearer mints a session holding all permissions (so route-tier authz
// never masks the data-profile gate) plus the given data profile.
func scopedBearer(t *testing.T, srv *Server, profile string) string {
	t.Helper()
	tok, _, err := srv.sessions.IssueWithPolicy(&ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// TestIntegrationSessionProfileRedaction pins the core of #1075: a session
// carrying a data profile sees redacted results, resolved per request against
// the selected server's index — while a policy-less credential on the SAME
// server sees the raw data (proving it is per-session, not process-global).
func TestIntegrationSessionProfileRedaction(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedSensitiveProfile(t, srv)
	scoped := scopedBearer(t, srv, "sensitive")

	// Denied table: the profiled session gets nothing from app.secrets.
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=secrets", scoped); strings.Contains(rec.Body.String(), "topsecret") {
		t.Errorf("denied table app.secrets leaked to a profiled session: %s", rec.Body.String())
	}
	// The static token (no profile) still sees it — proves the redaction is the
	// SESSION's, not the process's.
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=secrets", "static-tok"); !strings.Contains(rec.Body.String(), "topsecret") {
		t.Errorf("policy-less credential should see the raw table; per-session redaction leaked to the process: %s", rec.Body.String())
	}
	// Redacted column: ssn nulled for the profiled session, email intact.
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=users", scoped)
	if strings.Contains(rec.Body.String(), "ssn-12345") {
		t.Errorf("redacted column ssn leaked to a profiled session: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "alice@example") {
		t.Errorf("non-redacted column email must remain: %s", rec.Body.String())
	}
}

// TestIntegrationSessionProfileNonexistent pins the fail-loud path: a session
// whose profile does not exist on the selected server is refused (403), never
// silently served unredacted.
func TestIntegrationSessionProfileNonexistent(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	// No profile seeded named "ghost".
	scoped := scopedBearer(t, srv, "ghost")
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=users", scoped)
	if rec.Code != http.StatusForbidden {
		t.Errorf("session with a nonexistent profile GET /api/events = %d, want 403: %s", rec.Code, rec.Body.String())
	}
}

// TestIntegrationSessionProfileGatesRawSurfaces pins that a profiled session is
// refused the baseline-reading surfaces (reconstruct, baselines) whose reads
// bypass redaction.
func TestIntegrationSessionProfileGatesRawSurfaces(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedSensitiveProfile(t, srv)
	scoped := scopedBearer(t, srv, "sensitive")

	for _, path := range []string{
		"/api/reconstruct?schema=app&table=users&pk=1",
		"/api/baselines",
	} {
		if rec := getPath(t, srv, "127.0.0.1:8090", path, scoped); rec.Code != http.StatusForbidden {
			t.Errorf("profiled session GET %s = %d, want 403: %s", path, rec.Code, rec.Body.String())
		}
	}
}
