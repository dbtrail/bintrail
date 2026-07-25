package mcpext

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// swapProviders isolates a test from the package-global registry (and from
// whatever any other test registered).
func swapProviders(t *testing.T) {
	t.Helper()
	orig := providers
	providers = nil
	t.Cleanup(func() { providers = orig })
}

func TestRunProvidersWithNothingRegisteredIsNoop(t *testing.T) {
	swapProviders(t)
	// The stock binaries take this path on every server they build: it must
	// not panic and must not require a usable resolver.
	RunProviders(mcp.NewServer(&mcp.Implementation{Name: "t"}, nil), nil)
}

func TestRunProvidersPassesServerAndResolver(t *testing.T) {
	swapProviders(t)

	want := ToolContext{DBName: "bintrail_index", SourceDSN: "u:p@tcp(src:3306)/", Close: func() {}}
	resolve := func(context.Context, string) (ToolContext, error) { return want, nil }

	var gotServer *mcp.Server
	var gotCtx ToolContext
	calls := 0
	Register(func(s *mcp.Server, r ToolContextFunc) {
		calls++
		gotServer = s
		gotCtx, _ = r(context.Background(), "")
	})
	// A second provider must also run: registration is additive, and a
	// distribution may split its tools across packages.
	second := 0
	Register(func(*mcp.Server, ToolContextFunc) { second++ })

	server := mcp.NewServer(&mcp.Implementation{Name: "t"}, nil)
	RunProviders(server, resolve)

	if calls != 1 || second != 1 {
		t.Fatalf("providers ran %d and %d times, want 1 each", calls, second)
	}
	if gotServer != server {
		t.Error("provider received a different server than the one being built")
	}
	if gotCtx.DBName != want.DBName || gotCtx.SourceDSN != want.SourceDSN {
		t.Errorf("provider's resolver returned %+v, want the surface's context (%+v)", gotCtx, want)
	}
}

// A nil provider is a programming error at startup. Failing there beats a nil
// dereference on the first tool call, which would surface as a dead MCP
// endpoint in production.
func TestRegisterNilPanics(t *testing.T) {
	swapProviders(t)
	defer func() {
		if recover() == nil {
			t.Error("Register(nil) did not panic")
		}
	}()
	Register(nil)
}
