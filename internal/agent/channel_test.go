package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ─── dispatch tests ──────────────────────────────────────────────────────────

type stubHandler struct {
	resolvePK  func(context.Context, ResolvePKRequest) ([]PKResult, error)
	recover    func(context.Context, RecoverRequest) (string, error)
	forensics  func(context.Context, ForensicsQueryRequest) (*ForensicsResult, error)
}

func (s *stubHandler) HandleResolvePK(ctx context.Context, req ResolvePKRequest) ([]PKResult, error) {
	return s.resolvePK(ctx, req)
}
func (s *stubHandler) HandleRecover(ctx context.Context, req RecoverRequest) (string, error) {
	return s.recover(ctx, req)
}
func (s *stubHandler) HandleForensicsQuery(ctx context.Context, req ForensicsQueryRequest) (*ForensicsResult, error) {
	return s.forensics(ctx, req)
}

func TestDispatch_resolvePK(t *testing.T) {
	h := &stubHandler{
		resolvePK: func(_ context.Context, req ResolvePKRequest) ([]PKResult, error) {
			return []PKResult{{PKHash: req.Items[0].PKHash, PKValues: "42", Found: true}}, nil
		},
	}
	data, _ := json.Marshal(ResolvePKRequest{Items: []PKItem{{PKHash: "abc", Schema: "db", Table: "t"}}})
	cmd := Command{ID: "1", Type: "resolve_pk", Data: data}

	resp := dispatch(context.Background(), h, cmd)

	if resp.ID != "1" {
		t.Errorf("ID = %q, want %q", resp.ID, "1")
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	results, ok := resp.Data.([]PKResult)
	if !ok {
		t.Fatalf("Data type = %T, want []PKResult", resp.Data)
	}
	if len(results) != 1 || !results[0].Found {
		t.Errorf("unexpected results: %+v", results)
	}
}

func TestDispatch_recover(t *testing.T) {
	h := &stubHandler{
		recover: func(_ context.Context, req RecoverRequest) (string, error) {
			return "-- recovery SQL", nil
		},
	}
	data, _ := json.Marshal(RecoverRequest{Schema: "db", Table: "t", TimeStart: time.Now(), TimeEnd: time.Now()})
	cmd := Command{ID: "2", Type: "recover", Data: data}

	resp := dispatch(context.Background(), h, cmd)

	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.Data != "-- recovery SQL" {
		t.Errorf("Data = %v, want recovery SQL", resp.Data)
	}
}

func TestDispatch_forensicsQuery(t *testing.T) {
	h := &stubHandler{
		forensics: func(_ context.Context, req ForensicsQueryRequest) (*ForensicsResult, error) {
			return &ForensicsResult{Columns: []string{"col1"}, Rows: []map[string]any{{"col1": "val1"}}}, nil
		},
	}
	data, _ := json.Marshal(ForensicsQueryRequest{Query: "recent_queries"})
	cmd := Command{ID: "3", Type: "forensics_query", Data: data}

	resp := dispatch(context.Background(), h, cmd)

	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

func TestDispatch_unknownType(t *testing.T) {
	h := &stubHandler{}
	cmd := Command{ID: "4", Type: "nope", Data: json.RawMessage(`{}`)}

	resp := dispatch(context.Background(), h, cmd)

	if !strings.Contains(resp.Error, "unknown command type") {
		t.Errorf("error = %q, want 'unknown command type'", resp.Error)
	}
}

func TestDispatch_invalidPayload(t *testing.T) {
	h := &stubHandler{}
	cmd := Command{ID: "5", Type: "resolve_pk", Data: json.RawMessage(`{invalid`)}

	resp := dispatch(context.Background(), h, cmd)

	if !strings.Contains(resp.Error, "invalid resolve_pk payload") {
		t.Errorf("error = %q, want 'invalid resolve_pk payload'", resp.Error)
	}
}

// ─── Wire format tests ──────────────────────────────────────────────────────

func TestCommandJSON(t *testing.T) {
	raw := `{"id":"cmd-1","type":"resolve_pk","data":{"items":[{"pk_hash":"abc","schema":"db","table":"t"}]}}`
	var cmd Command
	if err := json.Unmarshal([]byte(raw), &cmd); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cmd.ID != "cmd-1" || cmd.Type != "resolve_pk" {
		t.Errorf("unexpected command: %+v", cmd)
	}
	var req ResolvePKRequest
	if err := json.Unmarshal(cmd.Data, &req); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if len(req.Items) != 1 || req.Items[0].PKHash != "abc" {
		t.Errorf("unexpected items: %+v", req.Items)
	}
}

func TestResponseJSON(t *testing.T) {
	resp := Response{ID: "1", Type: "resolve_pk", Data: []PKResult{{PKHash: "abc", PKValues: "42", Found: true}}}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"found":true`) {
		t.Errorf("missing found:true in %s", data)
	}
}

func TestHeartbeatJSON(t *testing.T) {
	hb := Heartbeat{
		Type:       "heartbeat",
		Version:    "1.0.0",
		Uptime:     "1h30m0s",
		BintrailID: "test-id",
		Timestamp:  time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(hb)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, `"type":"heartbeat"`) {
		t.Errorf("missing type in %s", s)
	}
	if !strings.Contains(s, `"version":"1.0.0"`) {
		t.Errorf("missing version in %s", s)
	}
}

// ─── Channel integration tests (in-process WebSocket) ────────────────────────

func TestChannel_commandRoundTrip(t *testing.T) {
	h := &stubHandler{
		resolvePK: func(_ context.Context, req ResolvePKRequest) ([]PKResult, error) {
			return []PKResult{{PKHash: req.Items[0].PKHash, PKValues: "99", Found: true}}, nil
		},
	}

	// Start a test WebSocket server.
	var received Response
	var mu sync.Mutex
	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify auth header.
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("accept error: %v", err)
			return
		}
		defer conn.CloseNow()

		ctx := r.Context()

		// Read and discard the initial heartbeat.
		_, _, err = conn.Read(ctx)
		if err != nil {
			t.Logf("read heartbeat error: %v", err)
			return
		}

		// Send a resolve_pk command.
		cmdData, _ := json.Marshal(ResolvePKRequest{Items: []PKItem{{PKHash: "h1", Schema: "db", Table: "t"}}})
		cmd := Command{ID: "test-1", Type: "resolve_pk", Data: cmdData}
		cmdBytes, _ := json.Marshal(cmd)
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

	// Run channel in background.
	go func() {
		ch := NewChannel(cfg, h, nil)
		ch.Run(ctx)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for command round-trip")
	}

	cancel()

	mu.Lock()
	defer mu.Unlock()
	if received.ID != "test-1" {
		t.Errorf("response ID = %q, want %q", received.ID, "test-1")
	}
	if received.Error != "" {
		t.Errorf("unexpected error: %s", received.Error)
	}
}

func TestChannel_heartbeat(t *testing.T) {
	h := &stubHandler{}
	heartbeats := make(chan Heartbeat, 5)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()

		ctx := r.Context()
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var hb Heartbeat
			if json.Unmarshal(data, &hb) == nil && hb.Type == "heartbeat" {
				heartbeats <- hb
			}
		}
	}))
	defer srv.Close()

	cfg := ChannelConfig{
		Endpoint:          "ws" + strings.TrimPrefix(srv.URL, "http"),
		APIKey:            "test",
		Version:           "v1.2.3",
		HeartbeatInterval: 50 * time.Millisecond,
		MaxReconnectDelay: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		ch := NewChannel(cfg, h, nil)
		ch.Run(ctx)
	}()

	// Expect at least 2 heartbeats (immediate + one tick).
	var count int
	for count < 2 {
		select {
		case hb := <-heartbeats:
			count++
			if hb.Version != "v1.2.3" {
				t.Errorf("heartbeat version = %q, want %q", hb.Version, "v1.2.3")
			}
		case <-ctx.Done():
			t.Fatalf("timed out after %d heartbeats", count)
		}
	}

	cancel()
}

func TestChannel_authHeader(t *testing.T) {
	authReceived := make(chan string, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authReceived <- r.Header.Get("Authorization")
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer srv.Close()

	cfg := ChannelConfig{
		Endpoint:          "ws" + strings.TrimPrefix(srv.URL, "http"),
		APIKey:            "my-secret-key",
		HeartbeatInterval: time.Hour,
		MaxReconnectDelay: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		ch := NewChannel(cfg, &stubHandler{}, nil)
		ch.Run(ctx)
	}()

	select {
	case auth := <-authReceived:
		if auth != "Bearer my-secret-key" {
			t.Errorf("auth = %q, want %q", auth, "Bearer my-secret-key")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for auth header")
	}
	cancel()
}

// ─── Handler tests ───────────────────────────────────────────────────────────

func TestByosPKHash(t *testing.T) {
	// SHA-256 of "42" is a well-known value.
	got := byosPKHash("42")
	want := "73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049"
	if got != want {
		t.Errorf("byosPKHash(%q) = %q, want %q", "42", got, want)
	}
}

func TestAllowedForensicsQueries(t *testing.T) {
	expected := []string{"recent_queries", "lock_waits", "table_io"}
	for _, name := range expected {
		if _, ok := allowedForensicsQueries[name]; !ok {
			t.Errorf("missing forensics query %q", name)
		}
	}
}
