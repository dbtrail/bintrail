package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/query"
)

// resultText extracts the text of a tool result's first content item.
func resultText(res *mcp.CallToolResult) string {
	if res == nil || len(res.Content) == 0 {
		return ""
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		return ""
	}
	return tc.Text
}

// makeRowsWithStatement builds two rows: one carrying captured statement
// text, one without (the nil case must stay nil after stripping).
func makeRowsWithStatement(qt, qh string) []query.ResultRow {
	return []query.ResultRow{
		{EventID: 1, QueryText: &qt, QueryHash: &qh},
		{EventID: 2},
	}
}

// rejectingConfig is the console-style posture: DSN/profile parameters
// refused. Resolve fails the test if a handler reaches it despite the
// rejection — the whole point is that the surface never dereferences a
// client-supplied DSN.
func rejectingConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Resolve: func(ctx context.Context, argDSN string) (*Target, error) {
			t.Fatalf("Resolve must not be called for a rejected parameter (argDSN=%q)", argDSN)
			return nil, nil
		},
		AllowDSNParam:     false,
		AllowProfileParam: false,
	}
}

func TestIndexDSNParamRejected(t *testing.T) {
	cfg := rejectingConfig(t)
	ctx := context.Background()

	cases := []struct {
		tool string
		call func() (isErr bool, text string)
	}{
		{"query", func() (bool, string) {
			res, _, _ := MakeQueryTool(cfg)(ctx, nil, QueryArgs{IndexDSN: "u:p@tcp(1.2.3.4:3306)/x"})
			return res.IsError, resultText(res)
		}},
		{"recover", func() (bool, string) {
			res, _, _ := MakeRecoverTool(cfg)(ctx, nil, RecoverArgs{IndexDSN: "u:p@tcp(1.2.3.4:3306)/x"})
			return res.IsError, resultText(res)
		}},
		{"status", func() (bool, string) {
			res, _, _ := MakeStatusTool(cfg)(ctx, nil, StatusArgs{IndexDSN: "u:p@tcp(1.2.3.4:3306)/x"})
			return res.IsError, resultText(res)
		}},
		{"list_schema_changes", func() (bool, string) {
			res, _, _ := MakeSchemaChangesTool(cfg)(ctx, nil, SchemaChangesArgs{IndexDSN: "u:p@tcp(1.2.3.4:3306)/x"})
			return res.IsError, resultText(res)
		}},
	}
	for _, tc := range cases {
		isErr, text := tc.call()
		if !isErr {
			t.Errorf("%s: index_dsn must be rejected when AllowDSNParam is false", tc.tool)
		}
		if !strings.Contains(text, "index_dsn is not accepted") {
			t.Errorf("%s: rejection must be explicit, got: %s", tc.tool, text)
		}
	}
}

func TestProfileParamRejected(t *testing.T) {
	cfg := rejectingConfig(t)
	ctx := context.Background()

	res, _, _ := MakeQueryTool(cfg)(ctx, nil, QueryArgs{Profile: "auditor"})
	if !res.IsError || !strings.Contains(resultText(res), "profile is not accepted") {
		t.Errorf("query: profile must be rejected when AllowProfileParam is false, got: %s", resultText(res))
	}
	res, _, _ = MakeRecoverTool(cfg)(ctx, nil, RecoverArgs{Profile: "auditor"})
	if !res.IsError || !strings.Contains(resultText(res), "profile is not accepted") {
		t.Errorf("recover: profile must be rejected when AllowProfileParam is false, got: %s", resultText(res))
	}
}

func TestParamsAcceptedOnStandalonePosture(t *testing.T) {
	// With both params allowed, rejection must not fire; the call proceeds to
	// Resolve (which errors here, proving the gate was passed).
	cfg := Config{
		Resolve: func(ctx context.Context, argDSN string) (*Target, error) {
			if argDSN != "dsn-under-test" {
				t.Errorf("argDSN = %q, want it passed through", argDSN)
			}
			return nil, context.Canceled // any error: stops before touching a DB
		},
		AllowDSNParam:     true,
		AllowProfileParam: true,
	}
	res, _, _ := MakeQueryTool(cfg)(context.Background(), nil, QueryArgs{IndexDSN: "dsn-under-test", Profile: "p"})
	if !res.IsError {
		t.Fatal("expected the resolver error to surface")
	}
	if strings.Contains(resultText(res), "not accepted") {
		t.Errorf("params must not be rejected on the standalone posture: %s", resultText(res))
	}
}

func TestNewServerRegistersTools(t *testing.T) {
	s := NewServer(Config{Version: "test"})
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
}

func TestStripStatementText(t *testing.T) {
	qt, qh := "UPDATE t SET secret = 1", "abc123"
	rows := makeRowsWithStatement(qt, qh)

	off := &Target{RedactStatementText: false}
	off.stripStatementText(rows)
	if rows[0].QueryText == nil {
		t.Fatal("strip must be a no-op when RedactStatementText is false")
	}

	on := &Target{RedactStatementText: true}
	on.stripStatementText(rows)
	for i, r := range rows {
		if r.QueryText != nil || r.QueryHash != nil {
			t.Errorf("row %d: query_text/query_hash must be blanked, got %v/%v", i, r.QueryText, r.QueryHash)
		}
	}
}
