package byos

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMetadataClientSend(t *testing.T) {
	var receivedBody []byte
	var receivedContentType string
	var receivedAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("unexpected method %q", r.Method)
		}
		receivedContentType = r.Header.Get("Content-Type")
		receivedAuth = r.Header.Get("Authorization")
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewMetadataClient(srv.URL, "test-key")
	records := []MetadataRecord{
		{
			PKHash:         "abc123",
			SchemaName:     "mydb",
			TableName:      "users",
			EventType:      "INSERT",
			EventTimestamp: time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC),
			ServerID:       "srv-1",
			GTID:           "aaa:1",
			RowCount:       1,
			ServerUUID:     "11111111-1111-1111-1111-111111111111",
			SourceHost:     "10.0.0.5",
			SourcePort:     3306,
			SourceUser:     "repl",
		},
		{
			PKHash:         "def456",
			SchemaName:     "mydb",
			TableName:      "orders",
			EventType:      "UPDATE",
			EventTimestamp: time.Date(2026, 3, 31, 12, 1, 0, 0, time.UTC),
			ServerID:       "srv-1",
			RowCount:       1,
			ChangedColumns: []string{"status", "updated_at"},
		},
	}

	if err := client.Send(context.Background(), records); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if receivedContentType != "application/json" {
		t.Errorf("content-type = %q, want application/json", receivedContentType)
	}
	if receivedAuth != "Bearer test-key" {
		t.Errorf("authorization = %q, want %q", receivedAuth, "Bearer test-key")
	}

	var decoded []MetadataRecord
	if err := json.Unmarshal(receivedBody, &decoded); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("received %d records, want 2", len(decoded))
	}
	if decoded[0].PKHash != "abc123" {
		t.Errorf("first record pk_hash = %q, want abc123", decoded[0].PKHash)
	}
	if decoded[1].EventType != "UPDATE" {
		t.Errorf("second record event_type = %q, want UPDATE", decoded[1].EventType)
	}
	if len(decoded[1].ChangedColumns) != 2 {
		t.Errorf("second record changed_columns = %v, want [status updated_at]", decoded[1].ChangedColumns)
	}

	// Source identity fields (architecture §22.11) must round-trip on the
	// wire. Older clients that omit them serialize as empty thanks to
	// `omitempty`.
	if decoded[0].ServerUUID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("first record server_uuid = %q, want set", decoded[0].ServerUUID)
	}
	if decoded[0].SourceHost != "10.0.0.5" || decoded[0].SourcePort != 3306 || decoded[0].SourceUser != "repl" {
		t.Errorf("first record source identity = %q:%d/%s, want 10.0.0.5:3306/repl",
			decoded[0].SourceHost, decoded[0].SourcePort, decoded[0].SourceUser)
	}
	if decoded[1].ServerUUID != "" || decoded[1].SourceHost != "" {
		t.Errorf("second record should have empty source identity (omitempty), got uuid=%q host=%q",
			decoded[1].ServerUUID, decoded[1].SourceHost)
	}
}

func TestMetadataClientSendError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := NewMetadataClient(srv.URL, "test-key")
	err := client.Send(context.Background(), []MetadataRecord{{PKHash: "x"}})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestMetadataClientSendContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewMetadataClient(srv.URL, "test-key")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := client.Send(ctx, []MetadataRecord{{PKHash: "x"}})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}
