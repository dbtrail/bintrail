package mcptools

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const testDigest = "3f2a1b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708"

// TestQueryTool_queryHashRefusedWhenStatementTextIsWithheld pins the surface
// gate. A surface that strips query_text/query_hash from every event it returns
// (the console's /mcp) must not let a client filter on the digest either:
// answering that filter hands back the withheld association one candidate at a
// time, which is what the stripping exists to prevent.
//
// The Target's DB points at an unreachable address: a refusal must land before
// any query runs, while the control case (no digest) is free to fail on the
// connection instead — which is what tells the two apart.
func TestQueryTool_queryHashRefusedWhenStatementTextIsWithheld(t *testing.T) {
	for _, tc := range []struct {
		name      string
		redact    bool
		queryHash string
		// wantMsg is asserted on the refusal text, not merely on IsError: the
		// call fails anyway once it reaches the unreachable DB, so a test that
		// only checked IsError would pass with the gate deleted (verified by
		// mutation).
		wantMsg string
	}{
		{"withholding surface refuses the filter", true, testDigest, "query_hash filtering is unavailable"},
		{"withholding surface still serves unfiltered queries", true, "", ""},
		{"malformed digest is rejected everywhere", false, "not-a-digest", "statement digest must be"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("mysql", "root:none@tcp(127.0.0.1:1)/none")
			if err != nil {
				t.Fatalf("open placeholder db: %v", err)
			}
			t.Cleanup(func() { db.Close() })

			resolved := false
			cfg := Config{
				Resolve: func(context.Context, string) (*Target, error) {
					resolved = true
					return &Target{DB: db, RedactStatementText: tc.redact, NoArchive: true}, nil
				},
			}
			res, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, QueryArgs{
				Schema:    "mydb",
				Table:     "orders",
				QueryHash: tc.queryHash,
			})
			if err != nil {
				t.Fatalf("handler error: %v", err)
			}
			if !resolved {
				t.Fatal("target was never resolved; the test is not exercising the handler")
			}
			if tc.wantMsg != "" {
				if !res.IsError {
					t.Fatalf("call succeeded, want a refusal")
				}
				if got := resultText(res); !strings.Contains(got, tc.wantMsg) {
					t.Fatalf("refusal text = %q, want it to contain %q — the call also fails at the DB, so only the message proves the gate fired", got, tc.wantMsg)
				}
				return
			}
			// The no-filter case must fail for a DIFFERENT reason (the nil DB),
			// never for the digest gate — otherwise this test would pass even if
			// the gate rejected every query.
			if res.IsError && strings.Contains(resultText(res), "query_hash") {
				t.Fatalf("unfiltered query refused by the digest gate: %s", resultText(res))
			}
		})
	}
}

// TestRecoverArgs_hasNoQueryHashParam enforces on the MCP surface the rule the
// CLI test enforces on cobra: a digest names a statement SHAPE, so a reversal
// scoped to one would undo every execution of that shape in the window — none
// of which the operator named.
//
// The realistic regression is not the shared BuildQueryOptions (its positional
// signature makes leakage hard) but someone adding QueryHash to RecoverArgs
// "for symmetry" with QueryArgs. The blast radius of that mistake is generated
// reversal SQL, so it gets its own assertion rather than a comment.
func TestRecoverArgs_hasNoQueryHashParam(t *testing.T) {
	rt := reflect.TypeOf(RecoverArgs{})
	for i := range rt.NumField() {
		if tag := rt.Field(i).Tag.Get("json"); strings.HasPrefix(tag, "query_hash") {
			t.Fatalf("RecoverArgs.%s exposes %q: recover must never be scoped by statement digest", rt.Field(i).Name, tag)
		}
	}
}
