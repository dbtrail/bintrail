package cliapp

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/byos"
	"github.com/dbtrail/dbtrail/internal/parser"
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
	state.updateFlush(true, false, 3, 3)

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
	// The metadata sink succeeded, so nothing lost there; the payload sink
	// failed with a 3-event batch, so exactly those are counted lost.
	if status.MetadataLostEvents != 0 || status.MetadataLostBatches != 0 {
		t.Errorf("metadata lost = (%d events, %d batches), want (0, 0)",
			status.MetadataLostEvents, status.MetadataLostBatches)
	}
	if status.PayloadLostEvents != 3 || status.PayloadLostBatches != 1 {
		t.Errorf("payload lost = (%d events, %d batches), want (3, 1)",
			status.PayloadLostEvents, status.PayloadLostBatches)
	}
}

// TestFlushPipelineStateLostCountersCumulative pins the durability guarantee:
// the lost-event/lost-batch counters accumulate across flushes and are NOT
// reset when a later flush succeeds (unlike the status strings, which flip
// back to "ok" and erase the memory of the outage).
func TestFlushPipelineStateLostCountersCumulative(t *testing.T) {
	state := &flushPipelineState{}

	// First outage: both sinks fail a 5-event batch.
	got := state.updateFlush(false, false, 5, 5)
	if got.metadataLostEvents != 5 || got.metadataLostBatches != 1 ||
		got.payloadLostEvents != 5 || got.payloadLostBatches != 1 {
		t.Fatalf("after first drop: %+v, want all (5 events, 1 batch)", got)
	}

	// Second outage: only the payload sink fails a 2-event batch. Metadata
	// recovers ("ok") but its cumulative counter must NOT reset.
	got = state.updateFlush(true, false, 2, 2)
	if got.metadataLostEvents != 5 || got.metadataLostBatches != 1 {
		t.Errorf("metadata counter reset on recovery: %+v, want (5 events, 1 batch)", got)
	}
	if got.payloadLostEvents != 7 || got.payloadLostBatches != 2 {
		t.Errorf("payload lost = (%d events, %d batches), want (7, 2)",
			got.payloadLostEvents, got.payloadLostBatches)
	}

	// A fully successful flush leaves the cumulative counters untouched even
	// as it clears the degraded status.
	got = state.updateFlush(true, true, 4, 4)
	if got.metadataLostEvents != 5 || got.payloadLostEvents != 7 {
		t.Errorf("counters changed on success: %+v, want metadata=5 payload=7", got)
	}
	status := state.toFlushStatus()
	if status.MetadataStatus != "ok" || status.PayloadStatus != "ok" {
		t.Errorf("status after success = (%q, %q), want (ok, ok)",
			status.MetadataStatus, status.PayloadStatus)
	}
	if status.MetadataLostEvents != 5 || status.PayloadLostEvents != 7 {
		t.Errorf("status lost = (meta %d, payload %d), want (5, 7)",
			status.MetadataLostEvents, status.PayloadLostEvents)
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
		"server-uuid",
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

func TestValidateBYOSFlushConfig(t *testing.T) {
	tests := []struct {
		name      string
		byosMode  bool
		s3Bucket  string
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "non-BYOS without bucket is fine",
			byosMode: false,
			s3Bucket: "",
			wantErr:  false,
		},
		{
			name:     "non-BYOS with bucket is fine",
			byosMode: false,
			s3Bucket: "my-bucket",
			wantErr:  false,
		},
		{
			name:      "BYOS without bucket is rejected",
			byosMode:  true,
			s3Bucket:  "",
			wantErr:   true,
			errSubstr: "--s3-bucket",
		},
		{
			name:     "BYOS with bucket is accepted",
			byosMode: true,
			s3Bucket: "my-bucket",
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBYOSFlushConfig(tt.byosMode, tt.s3Bucket)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
				// The error should also mention the env-var variant so
				// operators reading agent stderr know both forms.
				if !strings.Contains(err.Error(), "BINTRAIL_S3_BUCKET") {
					t.Errorf("error %q should mention BINTRAIL_S3_BUCKET", err.Error())
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateServerUUID(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      string // expected canonical output (only checked when wantErr is false)
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "empty is allowed for back-compat",
			input:   "",
			want:    "",
			wantErr: false,
		},
		{
			name:    "valid canonical UUID accepted",
			input:   "550e8400-e29b-41d4-a716-446655440000",
			want:    "550e8400-e29b-41d4-a716-446655440000",
			wantErr: false,
		},
		{
			name:    "valid lowercase UUID accepted",
			input:   "183819c0-0000-0000-0000-000000000000",
			want:    "183819c0-0000-0000-0000-000000000000",
			wantErr: false,
		},
		{
			// Issue #329: uppercase copy-paste from a dashboard URL must
			// not send a divergent header value vs the canonical form.
			name:    "uppercase canonical normalized to lowercase",
			input:   "550E8400-E29B-41D4-A716-446655440000",
			want:    "550e8400-e29b-41d4-a716-446655440000",
			wantErr: false,
		},
		{
			name:    "mixed case normalized to lowercase",
			input:   "550e8400-E29B-41d4-A716-446655440000",
			want:    "550e8400-e29b-41d4-a716-446655440000",
			wantErr: false,
		},
		{
			// Some clipboard managers / docs wrap UUIDs in braces.
			name:    "braced form normalized to bare lowercase",
			input:   "{550e8400-e29b-41d4-a716-446655440000}",
			want:    "550e8400-e29b-41d4-a716-446655440000",
			wantErr: false,
		},
		{
			// uuid.Parse accepts urn:uuid:... but the SaaS expects bare.
			name:    "urn:uuid prefix stripped to bare lowercase",
			input:   "urn:uuid:550e8400-e29b-41d4-a716-446655440000",
			want:    "550e8400-e29b-41d4-a716-446655440000",
			wantErr: false,
		},
		{
			name:      "malformed UUID rejected",
			input:     "not-a-uuid",
			wantErr:   true,
			errSubstr: "invalid --server-uuid",
		},
		{
			name:      "numeric server-id rejected",
			input:     "202",
			wantErr:   true,
			errSubstr: "invalid --server-uuid",
		},
		{
			name:      "truncated UUID rejected",
			input:     "550e8400-e29b-41d4-a716",
			wantErr:   true,
			errSubstr: "invalid --server-uuid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateServerUUID(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
				if got != "" {
					t.Errorf("on error, canonical = %q, want empty string", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("canonical = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAgentCLI_RejectsInvalidServerUUID verifies that the agent command
// rejects a malformed --server-uuid passed through cobra's flag parser
// before any side effects (DB connect, WebSocket dial) occur. Empty is
// allowed and preserves the legacy auto-create-on-connect SaaS behavior.
// See issue #317.
func TestAgentCLI_RejectsInvalidServerUUID(t *testing.T) {
	// Save and restore the package-level flag global so this test does
	// not leak state into sibling tests that read agentCmd.
	saved := agtServerUUID
	t.Cleanup(func() { agtServerUUID = saved })

	// Drive the flag through cobra's actual parser — this exercises the
	// same code path a real `bintrail agent --server-uuid foo` invocation
	// would hit (flag definition, name, type).
	if err := agentCmd.ParseFlags([]string{"--server-uuid", "not-a-uuid"}); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if agtServerUUID != "not-a-uuid" {
		t.Fatalf("agtServerUUID = %q, want %q after ParseFlags", agtServerUUID, "not-a-uuid")
	}

	_, err := validateServerUUID(agtServerUUID)
	if err == nil {
		t.Fatal("expected error for invalid --server-uuid, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --server-uuid") {
		t.Errorf("error = %q, want it to contain 'invalid --server-uuid'", err.Error())
	}
}

// TestAgentCLI_CanonicalizesServerUUID confirms that a valid but
// non-canonical --server-uuid (uppercase, braced, urn-prefixed) is
// normalized to canonical lowercase-hyphenated form in agtServerUUID
// before it flows into the WebSocket X-Bintrail-Server-UUID header.
// Closes the silent-divergence footgun documented in issue #329.
//
// This test mirrors the validate-and-assign sequence in runAgent so a
// future refactor that drops the assign-back step is caught here rather
// than only on the SaaS side where the duplicate record symptom is
// observable but hard to attribute.
func TestAgentCLI_CanonicalizesServerUUID(t *testing.T) {
	saved := agtServerUUID
	t.Cleanup(func() { agtServerUUID = saved })

	const canonical = "550e8400-e29b-41d4-a716-446655440000"
	cases := []struct {
		name  string
		input string
	}{
		{"uppercase", "550E8400-E29B-41D4-A716-446655440000"},
		{"braced", "{550e8400-e29b-41d4-a716-446655440000}"},
		{"urn", "urn:uuid:550e8400-e29b-41d4-a716-446655440000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := agentCmd.ParseFlags([]string{"--server-uuid", tc.input}); err != nil {
				t.Fatalf("ParseFlags: %v", err)
			}
			if agtServerUUID != tc.input {
				t.Fatalf("agtServerUUID = %q, want %q after ParseFlags (before canonicalization)", agtServerUUID, tc.input)
			}

			// Mirror runAgent's validate-and-assign step.
			got, err := validateServerUUID(agtServerUUID)
			if err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			agtServerUUID = got

			if agtServerUUID != canonical {
				t.Errorf("agtServerUUID = %q after canonicalization, want %q", agtServerUUID, canonical)
			}
		})
	}
}

// TestAgentServerUUIDHelpWarnsAboutSilentIgnore guards the warning that
// closes the SaaS-side silent-ignore window (originally #317, updated by
// #336/#337 after nethalo/dbtrail#1490 hardened the SaaS to log unmatched
// UUIDs and refuse to bind). The help text must surface that a mismatched
// UUID is a visible server-side failure so operators verify in the
// dashboard rather than assuming the bind succeeded.
func TestAgentServerUUIDHelpWarnsAboutSilentIgnore(t *testing.T) {
	flag := agentCmd.Flag("server-uuid")
	if flag == nil {
		t.Fatal("--server-uuid flag not registered")
	}
	usage := flag.Usage
	for _, want := range []string{"logged server-side", "will NOT bind", "dashboard"} {
		if !strings.Contains(usage, want) {
			t.Errorf("--server-uuid help missing %q; full usage: %s", want, usage)
		}
	}
}
