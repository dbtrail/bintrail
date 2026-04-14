package byos

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestEnsurePartitionKey_FirstRun(t *testing.T) {
	b := newMemBackend()
	ctx := context.Background()

	if err := EnsurePartitionKey(ctx, b, "srv-A"); err != nil {
		t.Fatalf("EnsurePartitionKey first run: %v", err)
	}

	rc, err := b.Get(ctx, PartitionKeyMarker)
	if err != nil {
		t.Fatalf("Get marker: %v", err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	var rec partitionKeyRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		t.Fatalf("unmarshal marker: %v", err)
	}
	if rec.ServerID != "srv-A" {
		t.Errorf("ServerID = %q, want srv-A", rec.ServerID)
	}
	if rec.FirstSeen.IsZero() {
		t.Error("FirstSeen is zero, want non-zero timestamp")
	}
}

func TestEnsurePartitionKey_Match(t *testing.T) {
	b := newMemBackend()
	ctx := context.Background()

	if err := EnsurePartitionKey(ctx, b, "srv-A"); err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Capture the written bytes to verify the marker isn't overwritten.
	rc, _ := b.Get(ctx, PartitionKeyMarker)
	original, _ := io.ReadAll(rc)
	rc.Close()

	if err := EnsurePartitionKey(ctx, b, "srv-A"); err != nil {
		t.Fatalf("second call with matching serverID: %v", err)
	}

	rc, _ = b.Get(ctx, PartitionKeyMarker)
	after, _ := io.ReadAll(rc)
	rc.Close()

	if string(original) != string(after) {
		t.Errorf("marker was rewritten on matching call:\n  before = %s\n  after  = %s", original, after)
	}
}

func TestEnsurePartitionKey_Mismatch(t *testing.T) {
	b := newMemBackend()
	ctx := context.Background()

	if err := EnsurePartitionKey(ctx, b, "srv-A"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	err := EnsurePartitionKey(ctx, b, "srv-B")
	if err == nil {
		t.Fatal("EnsurePartitionKey with mismatched serverID returned nil, want error")
	}
	if !strings.Contains(err.Error(), "srv-A") || !strings.Contains(err.Error(), "srv-B") {
		t.Errorf("error should mention both server IDs; got: %v", err)
	}
	if !strings.Contains(err.Error(), "partition key mismatch") {
		t.Errorf("error should contain 'partition key mismatch'; got: %v", err)
	}
}
