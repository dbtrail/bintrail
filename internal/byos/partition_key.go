package byos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/dbtrail/bintrail/internal/storage"
)

// PartitionKeyMarker is the key (relative to the storage backend's prefix)
// where the partition-key marker file lives.
const PartitionKeyMarker = ".bintrail-partition-key"

type partitionKeyRecord struct {
	ServerID  string    `json:"server_id"`
	FirstSeen time.Time `json:"first_seen"`
}

// EnsurePartitionKey reads or writes a marker file that freezes the S3
// partition key used for BYOS payloads. On first call the marker is created
// with serverID. On subsequent calls the marker is read and compared: if it
// matches, no-op; if it differs, returns an error explaining the divergence.
//
// This catches the silent partition-key cutover that happens when a BYOS+S3
// agent is upgraded from a configuration that had --index-dsn (UUID partition
// key, from the resolved bintrail_id) to one without it (numeric partition
// key, from --server-id). Pre-upgrade objects live under a different S3
// prefix and would not be queried under the new layout — see issue #198.
func EnsurePartitionKey(ctx context.Context, b storage.Backend, serverID string) error {
	ok, err := b.Exists(ctx, PartitionKeyMarker)
	if err != nil {
		return fmt.Errorf("check partition-key marker: %w", err)
	}
	if !ok {
		rec := partitionKeyRecord{
			ServerID:  serverID,
			FirstSeen: time.Now().UTC(),
		}
		body, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal partition-key marker: %w", err)
		}
		if err := b.Put(ctx, PartitionKeyMarker, bytes.NewReader(body)); err != nil {
			return fmt.Errorf("write partition-key marker: %w", err)
		}
		return nil
	}

	rc, err := b.Get(ctx, PartitionKeyMarker)
	if err != nil {
		return fmt.Errorf("read partition-key marker: %w", err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read partition-key marker body: %w", err)
	}
	var rec partitionKeyRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return fmt.Errorf("parse partition-key marker: %w", err)
	}
	if rec.ServerID != serverID {
		return fmt.Errorf(
			"partition key mismatch: prior agent runs used server_id=%q (since %s), "+
				"current run would use server_id=%q. This happens when upgrading a BYOS+S3 "+
				"agent from a configuration with --index-dsn (UUID partition key from resolved "+
				"bintrail_id) to one without it (numeric partition key from --server-id). "+
				"Pre-upgrade objects live under a different S3 prefix and would not be "+
				"queried under the new layout. Either migrate existing objects to the new "+
				"prefix, or restore the previous configuration so the partition key matches",
			rec.ServerID, rec.FirstSeen.Format(time.RFC3339), serverID,
		)
	}
	return nil
}
