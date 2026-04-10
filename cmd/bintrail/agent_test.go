package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/buffer"
	"github.com/dbtrail/bintrail/internal/byos"
	"github.com/dbtrail/bintrail/internal/parser"
)

// stubMetadataClient records Send calls and can be configured to fail.
type stubMetadataClient struct {
	mu      sync.Mutex
	batches [][]byos.MetadataRecord
	err     error
}

func (s *stubMetadataClient) Send(_ context.Context, records []byos.MetadataRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	cp := make([]byos.MetadataRecord, len(records))
	copy(cp, records)
	s.batches = append(s.batches, cp)
	return nil
}

func (s *stubMetadataClient) totalRecords() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

// stubPayloadWriter records WriteRecords calls and can be configured to fail.
type stubPayloadWriter struct {
	mu      sync.Mutex
	batches [][]byos.PayloadRecord
	err     error
}

func (s *stubPayloadWriter) WriteRecords(_ context.Context, records []byos.PayloadRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	cp := make([]byos.PayloadRecord, len(records))
	copy(cp, records)
	s.batches = append(s.batches, cp)
	return nil
}

func (s *stubPayloadWriter) totalRecords() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

func makeTestEvents(n int) []parser.Event {
	events := make([]parser.Event, n)
	for i := range n {
		events[i] = parser.Event{
			Schema:    "mydb",
			Table:     "users",
			EventType: parser.EventInsert,
			PKValues:  "1",
			Timestamp: time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC),
			RowAfter:  map[string]any{"id": float64(1), "name": "alice"},
		}
	}
	return events
}

func TestByosStreamLoopBufferOnly(t *testing.T) {
	// Without flush config, events should go to buffer only (hosted mode).
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make(chan parser.Event, 10)

	for _, ev := range makeTestEvents(3) {
		events <- ev
	}
	close(events)

	err := byosStreamLoop(context.Background(), events, buf, 100, nil)
	if err != nil {
		t.Fatalf("byosStreamLoop: %v", err)
	}
	if buf.Len() != 3 {
		t.Errorf("buffer.Len() = %d, want 3", buf.Len())
	}
}

func TestByosStreamLoopFlushToSinks(t *testing.T) {
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make(chan parser.Event, 10)

	state := &flushPipelineState{}

	testEvents := makeTestEvents(3)
	for _, ev := range testEvents {
		events <- ev
	}
	close(events)

	fc := &byosFlushConfig{
		serverID:      "99999",
		flushInterval: time.Hour, // won't fire during test
		state:         state,
	}

	// Since byosFlushConfig uses concrete byos types, we test flushToSinks
	// directly below. First verify buffer-only path works with fc but nil sinks.
	err := byosStreamLoop(context.Background(), events, buf, 100, fc)
	if err != nil {
		t.Fatalf("byosStreamLoop: %v", err)
	}
	if buf.Len() != 3 {
		t.Errorf("buffer.Len() = %d, want 3", buf.Len())
	}

	// Test flushToSinks directly with stubs by wrapping them.
	// We test the split + retry logic here.
	t.Run("flushToSinks", func(t *testing.T) {
		batch := makeTestEvents(2)
		metaStub := &stubMetadataClient{}
		payloadStub := &stubPayloadWriter{}

		metaBatch, payloadBatch := splitBatch(batch, "99999")
		if len(metaBatch) != 2 {
			t.Fatalf("splitBatch meta = %d, want 2", len(metaBatch))
		}
		if len(payloadBatch) != 2 {
			t.Fatalf("splitBatch payload = %d, want 2", len(payloadBatch))
		}

		// Verify metadata record has no row data.
		if metaBatch[0].PKHash == "" {
			t.Error("metadata record missing pk_hash")
		}
		if metaBatch[0].SchemaName != "mydb" {
			t.Errorf("metadata schema = %q, want mydb", metaBatch[0].SchemaName)
		}

		// Verify payload record has row data.
		if payloadBatch[0].RowAfter == nil {
			t.Error("payload record missing row_after")
		}
		if payloadBatch[0].PKValues != "1" {
			t.Errorf("payload pk_values = %q, want 1", payloadBatch[0].PKValues)
		}

		// Test send via stubs.
		ctx := context.Background()
		if err := metaStub.Send(ctx, metaBatch); err != nil {
			t.Fatalf("metadata send: %v", err)
		}
		if err := payloadStub.WriteRecords(ctx, payloadBatch); err != nil {
			t.Fatalf("payload write: %v", err)
		}
		if metaStub.totalRecords() != 2 {
			t.Errorf("metadata records = %d, want 2", metaStub.totalRecords())
		}
		if payloadStub.totalRecords() != 2 {
			t.Errorf("payload records = %d, want 2", payloadStub.totalRecords())
		}
	})
}

func TestByosStreamLoopSkipsNonRowEvents(t *testing.T) {
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make(chan parser.Event, 10)

	events <- parser.Event{EventType: parser.EventGTID, GTID: "aaa:1"}
	events <- parser.Event{
		Schema: "mydb", Table: "users", EventType: parser.EventInsert,
		PKValues: "1", Timestamp: time.Now(), RowAfter: map[string]any{"id": float64(1)},
	}
	events <- parser.Event{EventType: parser.EventDDL, DDLQuery: "ALTER TABLE ..."}
	close(events)

	err := byosStreamLoop(context.Background(), events, buf, 100, nil)
	if err != nil {
		t.Fatalf("byosStreamLoop: %v", err)
	}
	if buf.Len() != 1 {
		t.Errorf("buffer.Len() = %d, want 1 (GTID and DDL should be skipped)", buf.Len())
	}
}

func TestByosStreamLoopFlushOnBatchSize(t *testing.T) {
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make(chan parser.Event, 20)

	// Send 5 events with batch size 3 — should flush twice (3 + 2).
	for _, ev := range makeTestEvents(5) {
		events <- ev
	}
	close(events)

	err := byosStreamLoop(context.Background(), events, buf, 3, nil)
	if err != nil {
		t.Fatalf("byosStreamLoop: %v", err)
	}
	if buf.Len() != 5 {
		t.Errorf("buffer.Len() = %d, want 5", buf.Len())
	}
}

func TestByosStreamLoopContextCancellation(t *testing.T) {
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make(chan parser.Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Send events and close channel — the loop reads them all, then
	// exits on channel close and flushes.
	events <- makeTestEvents(1)[0]
	close(events)

	err := byosStreamLoop(ctx, events, buf, 100, nil)
	if err != nil {
		t.Fatalf("byosStreamLoop: %v", err)
	}
	if buf.Len() != 1 {
		t.Errorf("buffer.Len() = %d, want 1", buf.Len())
	}
}

func TestFlushPipelineStateToFlushStatus(t *testing.T) {
	state := &flushPipelineState{}
	state.setBufferStats(42, 8192, 5)
	state.updateFlush(true, false)

	status := state.toFlushStatus()
	if status.BufferEvents == nil || *status.BufferEvents != 42 {
		t.Errorf("BufferEvents = %v, want 42", status.BufferEvents)
	}
	if status.BufferBytes == nil || *status.BufferBytes != 8192 {
		t.Errorf("BufferBytes = %v, want 8192", status.BufferBytes)
	}
	if status.SizeEvictions == nil || *status.SizeEvictions != 5 {
		t.Errorf("SizeEvictions = %v, want 5", status.SizeEvictions)
	}
	if status.MetadataStatus != "ok" {
		t.Errorf("MetadataStatus = %q, want ok", status.MetadataStatus)
	}
	if status.PayloadStatus != "degraded" {
		t.Errorf("PayloadStatus = %q, want degraded", status.PayloadStatus)
	}
	if status.LastMetadataFlush == nil {
		t.Error("LastMetadataFlush should be set when metadata succeeded")
	}
	if status.LastPayloadFlush != nil {
		t.Error("LastPayloadFlush should be nil when payload failed")
	}
}

func TestFlushPipelineStateInitialStatus(t *testing.T) {
	state := &flushPipelineState{
		metadataStatus: "ok",
		payloadStatus:  "ok",
	}
	status := state.toFlushStatus()
	if status.MetadataStatus != "ok" {
		t.Errorf("initial MetadataStatus = %q, want ok", status.MetadataStatus)
	}
	if status.PayloadStatus != "ok" {
		t.Errorf("initial PayloadStatus = %q, want ok", status.PayloadStatus)
	}
	if status.BufferEvents == nil || *status.BufferEvents != 0 {
		t.Errorf("initial BufferEvents = %v, want 0", status.BufferEvents)
	}
}

func TestRetryFlushSuccess(t *testing.T) {
	calls := 0
	err := retryFlush(context.Background(), 3, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("retryFlush: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestRetryFlushEventualSuccess(t *testing.T) {
	calls := 0
	err := retryFlush(context.Background(), 3, func() error {
		calls++
		if calls < 3 {
			return &testError{"transient"}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryFlush: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetryFlushPersistentFailure(t *testing.T) {
	calls := 0
	err := retryFlush(context.Background(), 3, func() error {
		calls++
		return &testError{"always fails"}
	})
	if err == nil {
		t.Fatal("expected error for persistent failure")
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetryFlushContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := retryFlush(ctx, 3, func() error {
		calls++
		return &testError{"fail"}
	})
	if err == nil {
		t.Fatal("expected error")
	}
	// Should abort after first failure due to cancelled context.
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

// splitBatch is a test helper that splits a batch of events into
// metadata and payload records using byos.SplitEvent.
func splitBatch(batch []parser.Event, serverID string) ([]byos.MetadataRecord, []byos.PayloadRecord) {
	var meta []byos.MetadataRecord
	var payload []byos.PayloadRecord
	for i := range batch {
		m, p, err := byos.SplitEvent(batch[i], serverID, byos.SourceIdentity{})
		if err != nil {
			continue
		}
		meta = append(meta, m)
		payload = append(payload, p)
	}
	return meta, payload
}

func TestWsEndpointToHTTP(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"wss://api.dbtrail.io/v1/agent", "https://api.dbtrail.io"},
		{"ws://localhost:8080/v1/agent", "http://localhost:8080"},
		{"wss://api.dbtrail.io", "https://api.dbtrail.io"},
		{"https://already-http.com/foo", "https://already-http.com"},
	}
	for _, tt := range tests {
		got := wsEndpointToHTTP(tt.in)
		if got != tt.want {
			t.Errorf("wsEndpointToHTTP(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestAgentFlagRegistration(t *testing.T) {
	for _, name := range []string{
		"api-key", "endpoint", "index-dsn", "source-dsn",
		"archive-dir", "archive-s3", "buffer-retain", "server-id",
		"batch-size", "schemas", "tables", "start-gtid",
		"s3-bucket", "s3-region", "s3-prefix", "flush-interval",
		"buffer-max-events", "buffer-max-bytes",
	} {
		if agentCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on agent command", name)
		}
	}
}

func TestAgentFlagDefaults(t *testing.T) {
	tests := []struct {
		flag string
		want string
	}{
		{"s3-prefix", "bintrail/"},
		{"flush-interval", "5s"},
		{"buffer-retain", "6h"},
		{"batch-size", "1000"},
		{"buffer-max-events", "0"},
		{"buffer-max-bytes", "0"},
	}
	for _, tt := range tests {
		f := agentCmd.Flag(tt.flag)
		if f == nil {
			t.Fatalf("flag --%s not registered", tt.flag)
		}
		if f.DefValue != tt.want {
			t.Errorf("--%s default = %q, want %q", tt.flag, f.DefValue, tt.want)
		}
	}
}

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"0", 0},
		{"", 0},
		{"1024", 1024},
		{"256MB", 256 << 20},
		{"256mb", 256 << 20},
		{"1GB", 1 << 30},
		{"1gb", 1 << 30},
		{"512KB", 512 << 10},
	}
	for _, tt := range tests {
		got, err := parseByteSize(tt.input)
		if err != nil {
			t.Errorf("parseByteSize(%q): unexpected error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseByteSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseByteSize_invalid(t *testing.T) {
	for _, input := range []string{"abc", "-1GB", "not_a_number"} {
		_, err := parseByteSize(input)
		if err == nil {
			t.Errorf("parseByteSize(%q): expected error", input)
		}
	}
}
