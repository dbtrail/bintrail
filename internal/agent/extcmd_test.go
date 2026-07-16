package agent

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/dbtrail/dbtrail/ext"
)

// The ext agent-command registry has no unregister API (startup-only
// contract), so the command types registered by these tests are unique to
// this file — a leftover registration cannot collide with any other test's
// dispatch.

// TestDispatchExtensionCommand pins the registry consultation in dispatch's
// default branch: a command type with no builtin case routes to the
// registered handler, which receives the channel deps and the raw payload
// and whose return value lands — pre-marshalled — in the response data.
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
	// The result is pre-marshalled at dispatch time (so writeJSON can never
	// fail on handler output): Data carries the raw JSON bytes.
	raw, ok := resp.Data.(json.RawMessage)
	if !ok {
		t.Fatalf("resp.Data = %T, want json.RawMessage", resp.Data)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal resp.Data: %v", err)
	}
	if got["n"] != float64(42) || got["source_host"] != "h:3306" {
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

// TestDispatchExtensionCommandPanic pins the panic containment: a registered
// handler that panics yields a per-command error response and a normal
// return — the process (and with it the in-memory BYOS buffer) survives.
func TestDispatchExtensionCommandPanic(t *testing.T) {
	ext.RegisterAgentCommand("ext_test_panic", func(context.Context, ext.AgentDeps, json.RawMessage) (any, error) {
		panic("handler exploded")
	})

	resp := dispatch(context.Background(), &DefaultHandler{},
		Command{ID: "x5", Type: "ext_test_panic"}, ext.AgentDeps{})

	if resp.ID != "x5" || resp.Type != "ext_test_panic" {
		t.Fatalf("response envelope mangled: %+v", resp)
	}
	if !strings.Contains(resp.Error, `command handler for "ext_test_panic" panicked`) ||
		!strings.Contains(resp.Error, "handler exploded") {
		t.Errorf("error = %q, want the panic-containment message with the panic value", resp.Error)
	}
	if resp.Data != nil {
		t.Errorf("resp.Data = %v, want nil after a panic", resp.Data)
	}
}

// TestDispatchExtensionCommandUnmarshalableResult pins the response
// pre-marshaling: a handler returning a value json.Marshal rejects (NaN here)
// yields a per-command error — and the resulting Response envelope itself
// still marshals cleanly, so writeJSON cannot fail and tear the connection
// down into a reconnect death spiral.
func TestDispatchExtensionCommandUnmarshalableResult(t *testing.T) {
	ext.RegisterAgentCommand("ext_test_nan", func(context.Context, ext.AgentDeps, json.RawMessage) (any, error) {
		return map[string]any{"bad": math.NaN()}, nil
	})

	resp := dispatch(context.Background(), &DefaultHandler{},
		Command{ID: "x6", Type: "ext_test_nan"}, ext.AgentDeps{})

	if !strings.Contains(resp.Error, `marshal response for "ext_test_nan"`) {
		t.Errorf("error = %q, want the marshal-response error", resp.Error)
	}
	if resp.Data != nil {
		t.Errorf("resp.Data = %v, want nil when the result cannot be marshalled", resp.Data)
	}
	if _, err := json.Marshal(resp); err != nil {
		t.Errorf("Response envelope no longer marshals — writeJSON would fail: %v", err)
	}
}

// TestDispatchExtensionCommandNilResult pins the (nil, nil) empty-success
// contract: no error, and the wire response omits the data field entirely.
func TestDispatchExtensionCommandNilResult(t *testing.T) {
	ext.RegisterAgentCommand("ext_test_nilok", func(context.Context, ext.AgentDeps, json.RawMessage) (any, error) {
		return nil, nil
	})

	resp := dispatch(context.Background(), &DefaultHandler{},
		Command{ID: "x7", Type: "ext_test_nilok"}, ext.AgentDeps{})

	if resp.Error != "" || resp.Data != nil {
		t.Fatalf("resp = %+v, want empty success with nil Data", resp)
	}
	wire, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if strings.Contains(string(wire), `"data"`) {
		t.Errorf("wire response = %s, want the data field omitted on empty success", wire)
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

// TestChannel_extCommandRoundTripDeps drives a registered extension command
// through the REAL listenLoop over an in-process WebSocket (the same harness
// as TestChannel_commandRoundTrip) and asserts the handler received the
// Channel's populated ExtDeps — pinning the deps plumbing so a regression to
// `dispatch(..., ext.AgentDeps{})` fails loudly.
func TestChannel_extCommandRoundTripDeps(t *testing.T) {
	ext.RegisterAgentCommand("ext_test_ws_deps", func(_ context.Context, deps ext.AgentDeps, _ json.RawMessage) (any, error) {
		return deps.SourceDSN + "|" + deps.SourceHost, nil
	})

	var received Response
	var mu sync.Mutex
	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("accept error: %v", err)
			return
		}
		defer conn.CloseNow()

		ctx := r.Context()

		// Read and discard the initial heartbeat.
		if _, _, err := conn.Read(ctx); err != nil {
			t.Logf("read heartbeat error: %v", err)
			return
		}

		// Send the extension command.
		cmdBytes, _ := json.Marshal(Command{ID: "ext-ws-1", Type: "ext_test_ws_deps", Data: json.RawMessage(`{}`)})
		if err := conn.Write(ctx, websocket.MessageText, cmdBytes); err != nil {
			t.Logf("write error: %v", err)
			return
		}

		// Read the response.
		_, respBytes, err := conn.Read(ctx)
		if err != nil {
			t.Logf("read response error: %v", err)
			return
		}

		mu.Lock()
		json.Unmarshal(respBytes, &received)
		mu.Unlock()

		conn.Close(websocket.StatusNormalClosure, "done")
		close(done)
	}))
	defer srv.Close()

	cfg := ChannelConfig{
		Endpoint:          "ws" + strings.TrimPrefix(srv.URL, "http"),
		APIKey:            "test-key",
		Version:           "test",
		HeartbeatInterval: 10 * time.Second,
		MaxReconnectDelay: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		ch := NewChannel(cfg, &stubHandler{}, nil, nil)
		ch.ExtDeps = ext.AgentDeps{SourceDSN: "user:pass@tcp(src:3306)/db", SourceHost: "src:3306"}
		ch.Run(ctx)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for ext command round-trip")
	}

	cancel()

	mu.Lock()
	defer mu.Unlock()
	if received.ID != "ext-ws-1" {
		t.Errorf("response ID = %q, want %q", received.ID, "ext-ws-1")
	}
	if received.Error != "" {
		t.Errorf("unexpected error: %s", received.Error)
	}
	if received.Data != "user:pass@tcp(src:3306)/db|src:3306" {
		t.Errorf("response Data = %v, want the ExtDeps echo — the Channel's deps did not reach the handler", received.Data)
	}
}
