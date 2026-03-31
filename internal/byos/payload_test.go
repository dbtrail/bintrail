package byos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// memBackend is a simple in-memory storage.Backend for testing.
type memBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemBackend() *memBackend {
	return &memBackend{objects: make(map[string][]byte)}
}

func (m *memBackend) Put(_ context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = data
	m.mu.Unlock()
	return nil
}

func (m *memBackend) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	data, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memBackend) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *memBackend) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

func (m *memBackend) Exists(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	_, ok := m.objects[key]
	m.mu.Unlock()
	return ok, nil
}

func TestPayloadKey(t *testing.T) {
	ts := time.Date(2026, 3, 31, 14, 30, 0, 0, time.UTC)
	got := PayloadKey("srv-1", "mydb", "users", ts)

	if !strings.HasPrefix(got, "srv-1/mydb.users/2026-03-31/events_") {
		t.Errorf("PayloadKey = %q, want prefix srv-1/mydb.users/2026-03-31/events_", got)
	}
	if !strings.HasSuffix(got, ".parquet") {
		t.Errorf("PayloadKey = %q, want .parquet suffix", got)
	}
}

func TestPayloadKeyUTC(t *testing.T) {
	// Timestamps in non-UTC zones should still produce UTC date in the key.
	loc := time.FixedZone("UTC-5", -5*3600)
	ts := time.Date(2026, 4, 1, 1, 0, 0, 0, loc) // 2026-04-01 06:00 UTC
	got := PayloadKey("srv", "db", "t", ts)

	if !strings.Contains(got, "2026-04-01") {
		t.Errorf("PayloadKey = %q, expected UTC date 2026-04-01", got)
	}
}

func TestPayloadWriterWriteRecords(t *testing.T) {
	backend := newMemBackend()
	w := NewPayloadWriter(backend, "srv-1")

	ts := time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC)
	records := []PayloadRecord{
		{
			PKHash:         PKHash("1"),
			PKValues:       "1",
			SchemaName:     "mydb",
			TableName:      "users",
			EventType:      "INSERT",
			EventTimestamp: ts,
			RowAfter:       map[string]any{"id": 1, "name": "Alice"},
			SchemaVersion:  1,
		},
		{
			PKHash:         PKHash("2"),
			PKValues:       "2",
			SchemaName:     "mydb",
			TableName:      "users",
			EventType:      "INSERT",
			EventTimestamp: ts,
			RowAfter:       map[string]any{"id": 2, "name": "Bob"},
			SchemaVersion:  1,
		},
	}

	if err := w.WriteRecords(context.Background(), records); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}

	// Verify a Parquet file was uploaded.
	keys, _ := backend.List(context.Background(), "srv-1/mydb.users/")
	if len(keys) != 1 {
		t.Fatalf("expected 1 uploaded file, got %d", len(keys))
	}

	// Verify the file starts with the Parquet magic bytes (PAR1).
	data := backend.objects[keys[0]]
	if len(data) < 4 || string(data[:4]) != "PAR1" {
		t.Error("uploaded file does not start with Parquet magic bytes")
	}
}

func TestPayloadWriterGroupsByTable(t *testing.T) {
	backend := newMemBackend()
	w := NewPayloadWriter(backend, "srv-1")

	ts := time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC)
	records := []PayloadRecord{
		{
			PKHash: PKHash("1"), PKValues: "1",
			SchemaName: "mydb", TableName: "users",
			EventType: "INSERT", EventTimestamp: ts,
			RowAfter: map[string]any{"id": 1}, SchemaVersion: 1,
		},
		{
			PKHash: PKHash("100"), PKValues: "100",
			SchemaName: "mydb", TableName: "orders",
			EventType: "INSERT", EventTimestamp: ts,
			RowAfter: map[string]any{"id": 100}, SchemaVersion: 1,
		},
	}

	if err := w.WriteRecords(context.Background(), records); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}

	// Should produce two separate files (one per table).
	usersKeys, _ := backend.List(context.Background(), "srv-1/mydb.users/")
	ordersKeys, _ := backend.List(context.Background(), "srv-1/mydb.orders/")

	if len(usersKeys) != 1 {
		t.Errorf("expected 1 users file, got %d", len(usersKeys))
	}
	if len(ordersKeys) != 1 {
		t.Errorf("expected 1 orders file, got %d", len(ordersKeys))
	}
}

func TestPayloadWriterUpdateWithNulls(t *testing.T) {
	backend := newMemBackend()
	w := NewPayloadWriter(backend, "srv-1")

	ts := time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC)
	records := []PayloadRecord{
		{
			PKHash: PKHash("1"), PKValues: "1",
			SchemaName: "mydb", TableName: "users",
			EventType: "UPDATE", EventTimestamp: ts,
			RowBefore:      map[string]any{"id": 1, "name": "Alice"},
			RowAfter:       map[string]any{"id": 1, "name": "Bob"},
			ChangedColumns: []string{"name"},
			SchemaVersion:  1,
		},
	}

	if err := w.WriteRecords(context.Background(), records); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}

	keys, _ := backend.List(context.Background(), "srv-1/mydb.users/")
	if len(keys) != 1 {
		t.Fatalf("expected 1 file, got %d", len(keys))
	}
}

func TestPayloadWriterDeleteEvent(t *testing.T) {
	backend := newMemBackend()
	w := NewPayloadWriter(backend, "srv-1")

	ts := time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC)
	records := []PayloadRecord{
		{
			PKHash: PKHash("1"), PKValues: "1",
			SchemaName: "mydb", TableName: "users",
			EventType: "DELETE", EventTimestamp: ts,
			RowBefore:     map[string]any{"id": 1, "name": "Alice"},
			SchemaVersion: 1,
		},
	}

	if err := w.WriteRecords(context.Background(), records); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}

	keys, _ := backend.List(context.Background(), "srv-1/mydb.users/")
	if len(keys) != 1 {
		t.Fatalf("expected 1 file, got %d", len(keys))
	}
}
