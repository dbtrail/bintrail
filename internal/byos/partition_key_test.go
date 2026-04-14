package byos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestEnsurePartitionKey_FirstRun(t *testing.T) {
	b := newMemBackend()
	ctx := context.Background()

	if err := EnsurePartitionKey(ctx, b, "srv-A"); err != nil {
		t.Fatalf("EnsurePartitionKey first run: %v", err)
	}

	// The marker key is a wire contract — if this assertion fails, existing
	// customer installations would silently re-initialize on upgrade.
	rc, err := b.Get(ctx, ".bintrail-partition-key")
	if err != nil {
		t.Fatalf("marker not written at expected key: %v", err)
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
	if rec.Version != partitionKeyMarkerVersion {
		t.Errorf("Version = %d, want %d", rec.Version, partitionKeyMarkerVersion)
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

	rc, _ := b.Get(ctx, partitionKeyMarker)
	original, _ := io.ReadAll(rc)
	rc.Close()

	if err := EnsurePartitionKey(ctx, b, "srv-A"); err != nil {
		t.Fatalf("second call with matching serverID: %v", err)
	}

	rc, _ = b.Get(ctx, partitionKeyMarker)
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

func TestEnsurePartitionKey_MalformedMarker(t *testing.T) {
	b := newMemBackend()
	ctx := context.Background()

	// Pre-populate a corrupted marker body — e.g. a partial upload that
	// left non-JSON bytes behind. The code must hard-fail, NOT silently
	// overwrite with a new marker (which would re-arm the #198 cutover).
	if err := b.Put(ctx, partitionKeyMarker, bytes.NewReader([]byte("not json{"))); err != nil {
		t.Fatalf("seed marker: %v", err)
	}
	before, _ := io.ReadAll(mustGet(ctx, b))

	err := EnsurePartitionKey(ctx, b, "srv-A")
	if err == nil {
		t.Fatal("EnsurePartitionKey with malformed marker returned nil, want error")
	}
	if !strings.Contains(err.Error(), "unparseable") {
		t.Errorf("error should call out 'unparseable'; got: %v", err)
	}
	if !strings.Contains(err.Error(), "#198") {
		t.Errorf("error should reference issue #198 so operators find the context; got: %v", err)
	}

	after, _ := io.ReadAll(mustGet(ctx, b))
	if !bytes.Equal(before, after) {
		t.Error("malformed marker was silently overwritten; want it preserved so operator can inspect")
	}
}

func TestEnsurePartitionKey_EmptyMarker(t *testing.T) {
	b := newMemBackend()
	ctx := context.Background()

	if err := b.Put(ctx, partitionKeyMarker, bytes.NewReader(nil)); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	err := EnsurePartitionKey(ctx, b, "srv-A")
	if err == nil {
		t.Fatal("EnsurePartitionKey with empty marker returned nil, want error")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error should call out 'empty'; got: %v", err)
	}
}

func TestEnsurePartitionKey_NewerVersionRejected(t *testing.T) {
	b := newMemBackend()
	ctx := context.Background()

	// A marker written by a future agent with a higher Version must be
	// refused — otherwise this agent might silently ignore fields the
	// newer format relies on and re-arm the #198 cutover.
	body, _ := json.Marshal(partitionKeyRecord{
		Version:   partitionKeyMarkerVersion + 1,
		ServerID:  "srv-A",
		FirstSeen: time.Now().UTC(),
	})
	if err := b.Put(ctx, partitionKeyMarker, bytes.NewReader(body)); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	err := EnsurePartitionKey(ctx, b, "srv-A")
	if err == nil {
		t.Fatal("EnsurePartitionKey with future-version marker returned nil, want error")
	}
	if !strings.Contains(err.Error(), "newer agent") {
		t.Errorf("error should call out 'newer agent'; got: %v", err)
	}
	if !strings.Contains(err.Error(), "Upgrade this agent") {
		t.Errorf("error should direct the operator to upgrade; got: %v", err)
	}
}

func TestEnsurePartitionKey_EmptyServerIDInMarker(t *testing.T) {
	b := newMemBackend()
	ctx := context.Background()

	// A marker that round-trips through json.Unmarshal but carries
	// ServerID="" should be rejected as corrupt rather than trigger
	// a misleading "mismatch" message against the current serverID.
	body, _ := json.Marshal(partitionKeyRecord{Version: 1, ServerID: ""})
	if err := b.Put(ctx, partitionKeyMarker, bytes.NewReader(body)); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	err := EnsurePartitionKey(ctx, b, "srv-A")
	if err == nil {
		t.Fatal("EnsurePartitionKey with empty server_id returned nil, want error")
	}
	if !strings.Contains(err.Error(), "empty server_id") {
		t.Errorf("error should call out 'empty server_id'; got: %v", err)
	}
}

// failingBackend wraps a memBackend and injects errors on a named operation.
type failingBackend struct {
	*memBackend
	failOp  string // "Exists", "Get", "Put"
	failErr error
}

func (f *failingBackend) Exists(ctx context.Context, key string) (bool, error) {
	if f.failOp == "Exists" {
		return false, f.failErr
	}
	return f.memBackend.Exists(ctx, key)
}

func (f *failingBackend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if f.failOp == "Get" {
		return nil, f.failErr
	}
	return f.memBackend.Get(ctx, key)
}

func (f *failingBackend) Put(ctx context.Context, key string, r io.Reader) error {
	if f.failOp == "Put" {
		return f.failErr
	}
	return f.memBackend.Put(ctx, key, r)
}

func TestEnsurePartitionKey_BackendErrors(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("s3 throttled")

	t.Run("Exists fails", func(t *testing.T) {
		b := &failingBackend{memBackend: newMemBackend(), failOp: "Exists", failErr: sentinel}
		err := EnsurePartitionKey(ctx, b, "srv-A")
		if err == nil || !errors.Is(err, sentinel) {
			t.Fatalf("want wrapped sentinel, got: %v", err)
		}
		if !strings.Contains(err.Error(), "check partition-key marker") {
			t.Errorf("error should name the operation; got: %v", err)
		}
	})

	t.Run("Put fails on first run", func(t *testing.T) {
		b := &failingBackend{memBackend: newMemBackend(), failOp: "Put", failErr: sentinel}
		err := EnsurePartitionKey(ctx, b, "srv-A")
		if err == nil || !errors.Is(err, sentinel) {
			t.Fatalf("want wrapped sentinel, got: %v", err)
		}
		if !strings.Contains(err.Error(), "write partition-key marker") {
			t.Errorf("error should name the operation; got: %v", err)
		}
	})

	t.Run("Get fails on subsequent run", func(t *testing.T) {
		mem := newMemBackend()
		if err := EnsurePartitionKey(ctx, mem, "srv-A"); err != nil {
			t.Fatalf("seed first run: %v", err)
		}
		b := &failingBackend{memBackend: mem, failOp: "Get", failErr: sentinel}
		err := EnsurePartitionKey(ctx, b, "srv-A")
		if err == nil || !errors.Is(err, sentinel) {
			t.Fatalf("want wrapped sentinel, got: %v", err)
		}
		if !strings.Contains(err.Error(), "read partition-key marker") {
			t.Errorf("error should name the operation; got: %v", err)
		}
	})
}

func mustGet(ctx context.Context, b storageBackendGetter) io.ReadCloser {
	rc, err := b.Get(ctx, partitionKeyMarker)
	if err != nil {
		panic(err)
	}
	return rc
}

type storageBackendGetter interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}
