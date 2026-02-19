package indexer

import (
	"encoding/json"
	"testing"
)

// ─── New ─────────────────────────────────────────────────────────────────────

func TestNew_defaultBatchSize(t *testing.T) {
	idx := New(nil, 0)
	if idx.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000, got %d", idx.batchSize)
	}
}

func TestNew_negativeBatchSize(t *testing.T) {
	idx := New(nil, -5)
	if idx.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000 for negative input, got %d", idx.batchSize)
	}
}

func TestNew_customBatchSize(t *testing.T) {
	idx := New(nil, 500)
	if idx.batchSize != 500 {
		t.Errorf("expected batchSize 500, got %d", idx.batchSize)
	}
}

// ─── marshalRow ──────────────────────────────────────────────────────────────

func TestMarshalRow_nil(t *testing.T) {
	got, err := marshalRow(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for nil input, got %v", got)
	}
}

func TestMarshalRow_simpleMap(t *testing.T) {
	row := map[string]any{"id": float64(42), "name": "Alice"}
	got, err := marshalRow(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if decoded["id"] != float64(42) {
		t.Errorf("expected id=42, got %v", decoded["id"])
	}
	if decoded["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", decoded["name"])
	}
}

func TestMarshalRow_validJSONBytes_promoted(t *testing.T) {
	// Valid JSON []byte should be promoted to json.RawMessage (not base64).
	jsonVal := []byte(`{"nested":"value"}`)
	row := map[string]any{"data": jsonVal}
	got, err := marshalRow(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The output should contain the raw JSON, not a base64 string.
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	var nested map[string]string
	if err := json.Unmarshal(decoded["data"], &nested); err != nil {
		t.Errorf("expected data to be embedded JSON object, got error: %v (raw: %s)", err, decoded["data"])
	}
	if nested["nested"] != "value" {
		t.Errorf("expected nested.nested=value, got %v", nested["nested"])
	}
}

func TestMarshalRow_invalidJSONBytes_base64(t *testing.T) {
	// Invalid JSON []byte falls through without promotion — json.Marshal
	// base64-encodes []byte by default.
	row := map[string]any{"blob": []byte{0xFF, 0xFE, 0xFD}}
	got, err := marshalRow(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should produce valid JSON (the []byte will be base64).
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	// Value should be a base64 string, not an object.
	if _, ok := decoded["blob"].(string); !ok {
		t.Errorf("expected base64 string for invalid JSON bytes, got %T: %v", decoded["blob"], decoded["blob"])
	}
}

func TestMarshalRow_nilValueInMap(t *testing.T) {
	row := map[string]any{"note": nil, "id": float64(1)}
	got, err := marshalRow(row)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if decoded["note"] != nil {
		t.Errorf("expected null for nil value, got %v", decoded["note"])
	}
}

// ─── marshalJSON ─────────────────────────────────────────────────────────────

func TestMarshalJSON_nil(t *testing.T) {
	got, err := marshalJSON(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for nil input, got %v", got)
	}
}

func TestMarshalJSON_nonNil(t *testing.T) {
	input := []string{"status", "amount"}
	got, err := marshalJSON(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !json.Valid(got) {
		t.Errorf("expected valid JSON, got %s", got)
	}

	var decoded []string
	json.Unmarshal(got, &decoded)
	if len(decoded) != 2 || decoded[0] != "status" || decoded[1] != "amount" {
		t.Errorf("expected [status amount], got %v", decoded)
	}
}

// ─── nullOrString ────────────────────────────────────────────────────────────

func TestNullOrString_empty(t *testing.T) {
	got := nullOrString("")
	if got != nil {
		t.Errorf("expected nil for empty string, got %v", got)
	}
}

func TestNullOrString_nonEmpty(t *testing.T) {
	got := nullOrString("abc:42")
	if got != "abc:42" {
		t.Errorf("expected 'abc:42', got %v", got)
	}
}
