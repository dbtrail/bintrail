//go:build integration

package cliapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"

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
	sListSchema, sListTable, sListProfile := flgListSchema, flgListTable, aclListProfile
	t.Cleanup(func() {
		flgIndexDSN, flgSchema, flgTable, flgColumn = sFlgDSN, sFlgSchema, sFlgTable, sFlgColumn
		proIndexDSN, proDescription = sProDSN, sProDesc
		aclIndexDSN, aclProfile, aclFlag, aclPermission = sAclDSN, sAclProfile, sAclFlag, sAclPerm
		flgListSchema, flgListTable, aclListProfile = sListSchema, sListTable, sListProfile
	})
	flgIndexDSN, flgSchema, flgTable, flgColumn = "", "", "", ""
	proIndexDSN, proDescription = "", ""
	aclIndexDSN, aclProfile, aclFlag, aclPermission = "", "", "", ""
	flgListSchema, flgListTable, aclListProfile = "", "", ""
}

// runCapturing runs one verb's RunE with its output captured.
func runCapturing(t *testing.T, cmd *cobra.Command, run func(*cobra.Command, []string) error, args []string) string {
	t.Helper()
	var out bytes.Buffer
	cmd.SetOut(&out)
	defer cmd.SetOut(nil)
	if err := run(cmd, args); err != nil {
		t.Fatalf("%s: %v", cmd.Name(), err)
	}
	return out.String()
}

// consoleGet reads the console's document for the parity assertions.
func consoleGet(t *testing.T, srv *console.Server) (flags, profiles, rules []map[string]string) {
	t.Helper()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/access-profiles", nil)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer parity-token")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/access-profiles = %d body=%s", rec.Code, rec.Body.String())
	}
	var doc struct {
		Flags    []map[string]string `json:"flags"`
		Profiles []map[string]string `json:"profiles"`
		Rules    []map[string]string `json:"rules"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Flags, doc.Profiles, doc.Rules
}

// hasLine reports whether some line of out carries every one of the words.
func hasLine(out string, words ...string) bool {
	for line := range strings.SplitSeq(out, "\n") {
		ok := true
		for _, w := range words {
			if !strings.Contains(line, w) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
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

	// ── Trimming and case parity: both surfaces refuse or fold the same ──
	// "marketing " is marketing (no second profile), and "Marketing" is
	// refused as the existing row under another spelling.
	proDescription = "trailing space"
	if err := runProfileAdd(profileAddCmd, []string{"marketing "}); err != nil {
		t.Fatalf("profile add with a trailing space: %v", err)
	}
	if code, msg := consolePost(t, srv, "/api/access-profiles/profiles", `{"name":"marketing ","description":"trailing space"}`); code != http.StatusOK {
		t.Fatalf("console profile add with a trailing space = %d (%s)", code, msg)
	}
	cliErr = runProfileAdd(profileAddCmd, []string{"Marketing"})
	code, apiErr = consolePost(t, srv, "/api/access-profiles/profiles", `{"name":"Marketing"}`)
	if cliErr == nil || code != http.StatusConflict || cliErr.Error() != apiErr ||
		apiErr != `a profile named "marketing" already exists (the index compares names without regard to case or accents)` {
		t.Errorf("case collision: cli=%v api=(%d, %q), want the same refusal", cliErr, code, apiErr)
	}
	// The same for a flag, on the real collation: "PII" beside "pii", and an
	// accented spelling beside the plain one, are the stored row.
	flgSchema, flgTable, flgColumn = "app", "customers", "email"
	cliErr = runFlagAdd(flagAddCmd, []string{"PII"})
	code, apiErr = consolePost(t, srv, "/api/access-profiles/flags", `{"flag":"PII","schema":"app","table":"customers","column":"email"}`)
	if cliErr == nil || code != http.StatusConflict || cliErr.Error() != apiErr ||
		apiErr != `flag "pii" already exists on app.customers (email) (the index compares names without regard to case or accents)` {
		t.Errorf("flag case collision: cli=%v api=(%d, %q), want the same refusal naming the stored row", cliErr, code, apiErr)
	}
	flgSchema, flgTable, flgColumn = "app", "invoices", ""
	cliErr = runFlagAdd(flagAddCmd, []string{"bílling"})
	code, apiErr = consolePost(t, srv, "/api/access-profiles/flags", `{"flag":"bílling","schema":"app","table":"invoices"}`)
	if cliErr == nil || code != http.StatusConflict || cliErr.Error() != apiErr ||
		apiErr != `flag "billing" already exists on app.invoices (the index compares names without regard to case or accents)` {
		t.Errorf("flag accent collision: cli=%v api=(%d, %q), want the same refusal", cliErr, code, apiErr)
	}
	flgSchema, flgTable, flgColumn = strings.Repeat("s", 65), "t", ""
	cliErr = runFlagAdd(flagAddCmd, []string{"pii"})
	code, apiErr = consolePost(t, srv, "/api/access-profiles/flags", `{"flag":"pii","schema":"`+strings.Repeat("s", 65)+`","table":"t"}`)
	if cliErr == nil || code != http.StatusBadRequest || cliErr.Error() != apiErr ||
		apiErr != "schema is too long (65 characters); the limit is 64 characters" {
		t.Errorf("long schema: cli=%v api=(%d, %q), want the same refusal naming the limit", cliErr, code, apiErr)
	}
	cli, api = accessRows(t, dbA), accessRows(t, dbB)
	if len(cli) != 5 || !reflect.DeepEqual(cli, api) {
		t.Errorf("after the trimming and case cases the surfaces diverged (or a second profile appeared):\n cli=%v\n api=%v", cli, api)
	}

	// ── The list verbs read what the console wrote (index B) ──
	flgIndexDSN, proIndexDSN, aclIndexDSN = testutil.IntegrationDSN(nameB), testutil.IntegrationDSN(nameB), testutil.IntegrationDSN(nameB)
	for _, c := range []interface{ SetContext(context.Context) }{flagListCmd, profileListCmd, accessListCmd, flagRemoveCmd, profileRemoveCmd, accessRemoveCmd} {
		c.SetContext(ctx)
	}
	out := runCapturing(t, flagListCmd, runFlagList, nil)
	if !hasLine(out, "app", "customers", "email", "pii") || !hasLine(out, "app", "invoices", "(table)", "billing") {
		t.Errorf("flag list does not show the console-authored flags:\n%s", out)
	}
	flgListSchema, flgListTable = "app", "invoices"
	out = runCapturing(t, flagListCmd, runFlagList, nil)
	if !hasLine(out, "billing") || hasLine(out, "pii") {
		t.Errorf("flag list --schema app --table invoices did not narrow to the billing flag:\n%s", out)
	}
	out = runCapturing(t, profileListCmd, runProfileList, nil)
	if !hasLine(out, "marketing", "trailing space") {
		t.Errorf("profile list does not show the console-authored profile (with the description the last add set):\n%s", out)
	}
	out = runCapturing(t, accessListCmd, runAccessList, nil)
	if !hasLine(out, "marketing", "pii", "deny") || !hasLine(out, "marketing", "billing", "allow") {
		t.Errorf("access list does not show the console-authored rules:\n%s", out)
	}
	aclListProfile = "nobody"
	if out = runCapturing(t, accessListCmd, runAccessList, nil); !strings.Contains(out, "No access rules found.") {
		t.Errorf("access list --profile nobody:\n%s", out)
	}

	// ── Removal parity: the CLI remove verbs delete what the console wrote ──
	aclProfile, aclFlag = "marketing", "billing"
	if out = runCapturing(t, accessRemoveCmd, runAccessRemove, nil); !strings.Contains(out, `Access rule removed: profile="marketing" flag="billing"`) {
		t.Errorf("access remove output:\n%s", out)
	}
	if code, msg := consolePost(t, srv, "/api/access-profiles/rules/remove", `{"profile":"marketing","flag":"billing"}`); code != http.StatusNotFound {
		t.Errorf("rule removed by the CLI still visible to the console: %d %q", code, msg)
	}
	// The column-level flag: the key is four values, and the column is one
	// of them. Removing pii on customers(email) must take exactly that row.
	flgSchema, flgTable, flgColumn = "app", "customers", "email"
	if out = runCapturing(t, flagRemoveCmd, runFlagRemove, []string{"pii"}); !strings.Contains(out, `Flag "pii" removed from app.customers (email)`) {
		t.Errorf("flag remove output:\n%s", out)
	}
	flags, _, _ := consoleGet(t, srv)
	if len(flags) != 1 || flags[0]["flag"] != "billing" {
		t.Errorf("after the CLI removed pii on customers(email) the console lists %+v, want only billing", flags)
	}
	// The profile: its remaining rule goes with it.
	if out = runCapturing(t, profileRemoveCmd, runProfileRemove, []string{"marketing"}); !strings.Contains(out, `Profile "marketing" removed.`) {
		t.Errorf("profile remove output:\n%s", out)
	}
	_, profiles, rules := consoleGet(t, srv)
	if len(profiles) != 0 || len(rules) != 0 {
		t.Errorf("after the CLI removed the profile the console lists profiles=%+v rules=%+v, want none", profiles, rules)
	}
	// And a second remove of each is the exit-0 "not found" line.
	if out = runCapturing(t, profileRemoveCmd, runProfileRemove, []string{"marketing"}); !strings.Contains(out, `Profile "marketing" not found.`) {
		t.Errorf("second profile remove output:\n%s", out)
	}
}
