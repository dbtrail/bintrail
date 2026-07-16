package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// The ext agent-command registry has no unregister API (startup-only
// contract), so the command types registered by these tests are unique to
// this file — a leftover registration cannot collide with any other test's
// dispatch.

// TestDispatchExtensionCommand pins the registry consultation in dispatch's
// default branch: a command type with no builtin case routes to the
// registered handler, which receives the channel deps and the raw payload
// and whose return value lands in the response data.
func TestDispatchExtensionCommand(t *testing.T) {
	ext.RegisterAgentCommand("ext_test_ping", func(_ context.Context, deps ext.AgentDeps, payload json.RawMessage) (any, error) {
		var req struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, err
		}
		return map[string]any{"n": req.N + 1, "source_host": deps.SourceHost}, nil
	})

	deps := ext.AgentDeps{SourceDSN: "user:pass@tcp(h:3306)/db", SourceHost: "h:3306"}
	resp := dispatch(context.Background(), &DefaultHandler{},
		Command{ID: "x1", Type: "ext_test_ping", Data: json.RawMessage(`{"n":41}`)}, deps)

	if resp.ID != "x1" || resp.Type != "ext_test_ping" {
		t.Fatalf("response envelope mangled: %+v", resp)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	got, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("resp.Data = %T, want map", resp.Data)
	}
	if got["n"] != 42 || got["source_host"] != "h:3306" {
		t.Errorf("resp.Data = %v, want n=42 source_host=h:3306", got)
	}
}

func TestDispatchExtensionCommandError(t *testing.T) {
	ext.RegisterAgentCommand("ext_test_boom", func(context.Context, ext.AgentDeps, json.RawMessage) (any, error) {
		return nil, errors.New("boom")
	})

	resp := dispatch(context.Background(), &DefaultHandler{},
		Command{ID: "x2", Type: "ext_test_boom"}, ext.AgentDeps{})
	if resp.Error != "boom" || resp.Data != nil {
		t.Fatalf("resp = %+v, want Error=boom with nil Data", resp)
	}
}

// TestDispatchUnregisteredTypeStillUnknown pins the OSS default: with nothing
// registered for the type, dispatch keeps failing exactly as before.
func TestDispatchUnregisteredTypeStillUnknown(t *testing.T) {
	resp := dispatch(context.Background(), &DefaultHandler{},
		Command{ID: "x3", Type: "ext_test_unregistered"}, ext.AgentDeps{})
	if !strings.Contains(resp.Error, "unknown command type") {
		t.Fatalf("error = %q, want unknown command type", resp.Error)
	}
}

// TestDispatchBuiltinWinsOverRegistry pins the precedence contract: the
// registry is consulted only in the default branch, so registering a builtin
// type can never shadow the builtin handler.
func TestDispatchBuiltinWinsOverRegistry(t *testing.T) {
	ext.RegisterAgentCommand("resolve_pk", func(context.Context, ext.AgentDeps, json.RawMessage) (any, error) {
		return "ext-shadowed", nil
	})

	// A DefaultHandler with no data sources: the builtin resolve_pk path
	// errors, proving the registry handler above never ran.
	resp := dispatch(context.Background(), &DefaultHandler{},
		Command{ID: "x4", Type: "resolve_pk", Data: json.RawMessage(`{"items":[]}`)}, ext.AgentDeps{})
	if resp.Data == "ext-shadowed" {
		t.Fatal("registry handler shadowed the builtin resolve_pk case")
	}
	if !strings.Contains(resp.Error, "no data sources configured") {
		t.Errorf("error = %q, want the builtin no-data-sources error", resp.Error)
	}
}
