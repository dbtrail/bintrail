//go:build integration

package cliapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// resetAccessGlobals saves and clears the package globals the flag, profile
// and access verbs read, restoring them when the test ends.
func resetAccessGlobals(t *testing.T) {
	t.Helper()
	sFlgDSN, sFlgSchema, sFlgTable, sFlgColumn := flgIndexDSN, flgSchema, flgTable, flgColumn
	sProDSN, sProDesc := proIndexDSN, proDescription
	sAclDSN, sAclProfile, sAclFlag, sAclPerm := aclIndexDSN, aclProfile, aclFlag, aclPermission
	t.Cleanup(func() {
		flgIndexDSN, flgSchema, flgTable, flgColumn = sFlgDSN, sFlgSchema, sFlgTable, sFlgColumn
		proIndexDSN, proDescription = sProDSN, sProDesc
		aclIndexDSN, aclProfile, aclFlag, aclPermission = sAclDSN, sAclProfile, sAclFlag, sAclPerm
	})
	flgIndexDSN, flgSchema, flgTable, flgColumn = "", "", "", ""
	proIndexDSN, proDescription = "", ""
	aclIndexDSN, aclProfile, aclFlag, aclPermission = "", "", "", ""
}

// accessRows reads the three RBAC tables in a shape that ignores ids and
// timestamps: what a profile IS, on either surface.
func accessRows(t *testing.T, db *sql.DB) [][]string {
	t.Helper()
	var out [][]string
	collect := func(q string, n int) {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			vals := make([]string, n)
			ptrs := make([]any, n)
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			out = append(out, vals)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	collect(`SELECT 'flag', schema_name, table_name, column_name, flag FROM table_flags ORDER BY schema_name, table_name, column_name, flag`, 5)
	collect(`SELECT 'profile', name, COALESCE(description, ''), '', '' FROM profiles ORDER BY name`, 5)
	collect(`SELECT 'rule', p.name, ar.flag, ar.permission, '' FROM access_rules ar JOIN profiles p ON ar.profile_id = p.id ORDER BY p.name, ar.flag`, 5)
	return out
}

// consolePost drives one access-profile verb through the console's real
// handler chain (token auth, route table, handler) and returns the status
// and the error text, if any.
func consolePost(t *testing.T, srv *console.Server, path, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090"+path, strings.NewReader(body))
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer parity-token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var parsed struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &parsed)
	return rec.Code, parsed.Error
}

// TestIntegrationAccessProfilesCLIConsoleParity pins #1445's contract: a
// profile authored through the console API is the SAME ROWS the CLI verbs
// write, and both surfaces refuse the same input with the same words. Index
// A is authored by the real cobra RunE functions, index B by the console's
// real HTTP handlers; the two must be indistinguishable afterwards.
func TestIntegrationAccessProfilesCLIConsoleParity(t *testing.T) {
	resetAccessGlobals(t)
	dbA, nameA := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, dbA)
	dbB, nameB := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, dbB)

	// ── CLI on A ──
	ctx := context.Background()
	for _, c := range []interface{ SetContext(context.Context) }{flagAddCmd, profileAddCmd, accessAddCmd} {
		c.SetContext(ctx)
	}
	flgIndexDSN = testutil.IntegrationDSN(nameA)
	flgSchema, flgTable, flgColumn = "app", "customers", "email"
	if err := runFlagAdd(flagAddCmd, []string{"pii"}); err != nil {
		t.Fatalf("flag add: %v", err)
	}
	flgSchema, flgTable, flgColumn = "app", "invoices", ""
	if err := runFlagAdd(flagAddCmd, []string{"billing"}); err != nil {
		t.Fatalf("flag add: %v", err)
	}
	proIndexDSN = testutil.IntegrationDSN(nameA)
	proDescription = "Marketing analysts"
	if err := runProfileAdd(profileAddCmd, []string{"marketing"}); err != nil {
		t.Fatalf("profile add: %v", err)
	}
	aclIndexDSN = testutil.IntegrationDSN(nameA)
	aclProfile, aclFlag, aclPermission = "marketing", "pii", "deny"
	if err := runAccessAdd(accessAddCmd, nil); err != nil {
		t.Fatalf("access add: %v", err)
	}
	aclProfile, aclFlag, aclPermission = "marketing", "billing", "allow"
	if err := runAccessAdd(accessAddCmd, nil); err != nil {
		t.Fatalf("access add: %v", err)
	}

	// ── Console on B ──
	srv, err := console.New(console.Config{DB: dbB, DBName: nameB, Listen: "127.0.0.1:8090", Token: "parity-token", NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []struct{ path, body string }{
		{"/api/access-profiles/flags", `{"flag":"pii","schema":"app","table":"customers","column":"email"}`},
		{"/api/access-profiles/flags", `{"flag":"billing","schema":"app","table":"invoices"}`},
		{"/api/access-profiles/profiles", `{"name":"marketing","description":"Marketing analysts"}`},
		{"/api/access-profiles/rules", `{"profile":"marketing","flag":"pii","permission":"deny"}`},
		{"/api/access-profiles/rules", `{"profile":"marketing","flag":"billing","permission":"allow"}`},
	} {
		if code, msg := consolePost(t, srv, m.path, m.body); code != http.StatusOK {
			t.Fatalf("console POST %s %s = %d (%s)", m.path, m.body, code, msg)
		}
	}

	cli, api := accessRows(t, dbA), accessRows(t, dbB)
	if len(cli) != 5 {
		t.Fatalf("CLI wrote %d rows, want 5: %v", len(cli), cli)
	}
	if !reflect.DeepEqual(cli, api) {
		t.Errorf("the console authored different rows than the CLI:\n cli=%v\n api=%v", cli, api)
	}

	// ── Refusals ──
	aclProfile, aclFlag, aclPermission = "ghost", "pii", "deny"
	cliErr := runAccessAdd(accessAddCmd, nil)
	code, apiErr := consolePost(t, srv, "/api/access-profiles/rules", `{"profile":"ghost","flag":"pii","permission":"deny"}`)
	if cliErr == nil || code != http.StatusNotFound || cliErr.Error() != apiErr {
		t.Errorf("unknown profile: cli=%v api=(%d, %q), want the same words", cliErr, code, apiErr)
	}
	aclProfile, aclFlag, aclPermission = "marketing", "pii", "readwrite"
	cliErr = runAccessAdd(accessAddCmd, nil)
	code, apiErr = consolePost(t, srv, "/api/access-profiles/rules", `{"profile":"marketing","flag":"pii","permission":"readwrite"}`)
	// The one difference by design: the CLI names the field as its flag.
	if cliErr == nil || code != http.StatusBadRequest || cliErr.Error() != "--"+apiErr {
		t.Errorf("bad permission: cli=%v api=(%d, %q), want the same words behind the CLI's dashes", cliErr, code, apiErr)
	}

	// ── Removal parity: the CLI remove verbs delete what the console wrote ──
	aclIndexDSN = testutil.IntegrationDSN(nameB)
	aclProfile, aclFlag = "marketing", "billing"
	accessRemoveCmd.SetContext(ctx)
	if err := runAccessRemove(accessRemoveCmd, nil); err != nil {
		t.Fatalf("access remove on the console-authored index: %v", err)
	}
	if code, msg := consolePost(t, srv, "/api/access-profiles/rules/remove", `{"profile":"marketing","flag":"billing"}`); code != http.StatusNotFound {
		t.Errorf("rule removed by the CLI still visible to the console: %d %q", code, msg)
	}
}
