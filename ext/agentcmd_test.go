package ext

import (
	"context"
	"encoding/json"
	"maps"
	"testing"
)

func TestLookupAgentCommandUnregistered(t *testing.T) {
	if _, ok := LookupAgentCommand("ext_test_never_registered"); ok {
		t.Fatal("unregistered command type found in registry")
	}
}

func TestRegisterAgentCommandLastWins(t *testing.T) {
	orig := agentCommands
	agentCommands = maps.Clone(orig)
	t.Cleanup(func() { agentCommands = orig })

	RegisterAgentCommand("ext_test_dup", func(context.Context, AgentDeps, json.RawMessage) (any, error) {
		return "first", nil
	})
	RegisterAgentCommand("ext_test_dup", func(context.Context, AgentDeps, json.RawMessage) (any, error) {
		return "second", nil
	})

	h, ok := LookupAgentCommand("ext_test_dup")
	if !ok {
		t.Fatal("registered command not found")
	}
	got, err := h(context.Background(), AgentDeps{}, nil)
	if err != nil || got != "second" {
		t.Fatalf("handler = (%v, %v), want (second, nil): duplicate registration must be last-wins", got, err)
	}
}

func TestRegisteredAgentCommandReceivesDepsAndPayload(t *testing.T) {
	orig := agentCommands
	agentCommands = maps.Clone(orig)
	t.Cleanup(func() { agentCommands = orig })

	RegisterAgentCommand("ext_test_echo", func(_ context.Context, deps AgentDeps, payload json.RawMessage) (any, error) {
		return deps.SourceDSN + "|" + string(payload), nil
	})

	h, ok := LookupAgentCommand("ext_test_echo")
	if !ok {
		t.Fatal("registered command not found")
	}
	got, err := h(context.Background(), AgentDeps{SourceDSN: "sdsn"}, json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != `sdsn|{"x":1}` {
		t.Errorf("handler saw %q", got)
	}
}
