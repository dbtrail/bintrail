//go:build integration

package console

import (
	"net/http"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// restrictedBearer mints a session holding all permissions plus the given
// direct restrictions (#1449) — and deliberately NO profile name, because the
// point of the seam is that nothing needs to exist in the index for the
// restriction to enforce.
func restrictedBearer(t *testing.T, srv *Server, rest *ext.SessionRestrictions) string {
	t.Helper()
	tok, _, err := srv.sessions.IssueWithPolicy("test-user",
		&ext.AccessPolicy{Permissions: ext.AllPermissions(), Restrictions: rest})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// TestIntegrationSessionRestrictionsRedaction pins the label-free half of the
// redaction contract: a session whose POLICY carries deny/redact entries is
// enforced against an index holding no profiles, no table_flags and no
// access_rules rows — while the static token on the same server still reads
// raw (per-session, not process-global).
func TestIntegrationSessionRestrictionsRedaction(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		DenyTables:    []ext.TableRef{{Schema: "app", Table: "secrets"}},
		RedactColumns: []ext.TableColumnRef{{Schema: "app", Table: "users", Column: "ssn"}},
	})

	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=secrets", scoped); strings.Contains(rec.Body.String(), "topsecret") {
		t.Errorf("policy-denied table app.secrets leaked to a restricted session: %s", rec.Body.String())
	}
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=secrets", "static-tok"); !strings.Contains(rec.Body.String(), "topsecret") {
		t.Errorf("policy-less credential should see the raw table; per-session restriction leaked to the process: %s", rec.Body.String())
	}
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=users", scoped)
	if strings.Contains(rec.Body.String(), "ssn-12345") {
		t.Errorf("policy-redacted column ssn leaked to a restricted session: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "alice@example") {
		t.Errorf("non-redacted column email must remain: %s", rec.Body.String())
	}
}

// TestIntegrationSessionRestrictionsAllowList pins allow-list mode end to end:
// AllowTables withholds every table it does not name (no deny entry needed for
// app.secrets), and AllowColumns nulls the columns it does not name for the
// tables it covers.
func TestIntegrationSessionRestrictionsAllowList(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		AllowTables:  []ext.TableRef{{Schema: "app", Table: "users"}},
		AllowColumns: []ext.TableColumnRef{{Schema: "app", Table: "users", Column: "id"}, {Schema: "app", Table: "users", Column: "email"}},
	})

	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=secrets", scoped); strings.Contains(rec.Body.String(), "topsecret") {
		t.Errorf("table outside the allow list leaked to a restricted session: %s", rec.Body.String())
	}
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=users", scoped)
	if strings.Contains(rec.Body.String(), "ssn-12345") {
		t.Errorf("column outside the allow list leaked: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "alice@example") {
		t.Errorf("allowed column email must remain: %s", rec.Body.String())
	}
}

// TestIntegrationSessionRestrictionsGatesRawSurfaces pins that a restrictions-
// carrying session is refused the baseline-reading surfaces exactly as a
// profiled one is: those reads bypass redaction, so serving them would make
// the restriction a fiction.
func TestIntegrationSessionRestrictionsGatesRawSurfaces(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		RedactColumns: []ext.TableColumnRef{{Schema: "app", Table: "users", Column: "ssn"}},
	})

	for _, path := range []string{
		"/api/reconstruct?schema=app&table=users&pk=1",
		"/api/baselines",
	} {
		if rec := getPath(t, srv, "127.0.0.1:8090", path, scoped); rec.Code != http.StatusForbidden {
			t.Errorf("restricted session GET %s = %d, want 403: %s", path, rec.Code, rec.Body.String())
		}
	}
}
