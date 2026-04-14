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

// partitionKeyMarker is the key (relative to the storage backend's prefix)
// where the partition-key marker file lives. The leading dot keeps it out
// of `ls`-style listings and typical `*.parquet` lifecycle filters.
const partitionKeyMarker = ".bintrail-partition-key"

// partitionKeyMarkerVersion is the current wire format version for the marker
// record. Bump when adding fields that older readers cannot ignore.
const partitionKeyMarkerVersion = 1

// partitionKeyRecord is the JSON wire format for the marker file. Only
// ServerID is load-bearing for the mismatch check; Version and FirstSeen
// are informational (Version is reserved for future forward-compatibility;
// FirstSeen is surfaced in the mismatch error to help operators correlate
// with install history).
type partitionKeyRecord struct {
	Version   int       `json:"version"`
	ServerID  string    `json:"server_id"`
	FirstSeen time.Time `json:"first_seen"` // always UTC on write
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
	ok, err := b.Exists(ctx, partitionKeyMarker)
	if err != nil {
		return fmt.Errorf("check partition-key marker: %w", err)
	}
	if !ok {
		rec := partitionKeyRecord{
			Version:   partitionKeyMarkerVersion,
			ServerID:  serverID,
			FirstSeen: time.Now().UTC(),
		}
		body, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal partition-key marker: %w", err)
		}
		if err := b.Put(ctx, partitionKeyMarker, bytes.NewReader(body)); err != nil {
			return fmt.Errorf("write partition-key marker: %w", err)
		}
		return nil
	}

	rc, err := b.Get(ctx, partitionKeyMarker)
	if err != nil {
		return fmt.Errorf("read partition-key marker: %w", err)
	}
	defer rc.Close()
	// Cap the read so a replaced or corrupt marker can't exhaust memory.
	// The real marker is well under 1 KiB; 64 KiB is a generous ceiling.
	body, err := io.ReadAll(io.LimitReader(rc, 64*1024))
	if err != nil {
		return fmt.Errorf("read partition-key marker body: %w", err)
	}
	if len(body) == 0 {
		return fmt.Errorf(
			"partition-key marker at %q is empty (likely a partial upload). "+
				"Verify which server_id the existing objects under this S3 prefix belong "+
				"to before deleting the marker — removing a valid marker silently re-arms "+
				"the cutover described in #198",
			partitionKeyMarker)
	}
	var rec partitionKeyRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return fmt.Errorf(
			"partition-key marker at %q is unparseable (%d bytes: %w). "+
				"This usually means a partial upload or manual edit. Verify which "+
				"server_id the existing objects under this S3 prefix belong to before "+
				"deleting the marker — removing a valid marker silently re-arms the "+
				"cutover described in #198",
			partitionKeyMarker, len(body), err)
	}
	if rec.ServerID == "" {
		return fmt.Errorf(
			"partition-key marker at %q has empty server_id (corrupt). "+
				"Verify which server_id the existing objects under this S3 prefix belong "+
				"to before deleting the marker — removing a valid marker silently re-arms "+
				"the cutover described in #198",
			partitionKeyMarker)
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
