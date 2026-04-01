// Package buffer provides an in-memory event buffer for BYOS mode.
// It stores recent binlog events so the agent can answer resolve_pk and
// recover commands without hitting S3 for recent data. The buffer is
// safe for concurrent use: one goroutine (stream) writes via Insert,
// while another (agent handler) reads via Fetch and ResolvePK.
package buffer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// idOffset is added to buffer-local auto-increment IDs to avoid collisions
// with MySQL event_ids when MergeResults deduplicates by event_id.
const idOffset = uint64(1) << 32

// entry is a single event stored in the buffer.
type entry struct {
	row    query.ResultRow
	pkHash string // SHA-256 hex of pk_values
}

// Buffer is a thread-safe, in-memory event store for recent binlog events.
type Buffer struct {
	mu     sync.RWMutex
	events []entry
	nextID uint64
	maxAge time.Duration
	logger *slog.Logger
}

// New creates a Buffer that retains events for up to maxAge.
func New(maxAge time.Duration, logger *slog.Logger) *Buffer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Buffer{
		maxAge: maxAge,
		nextID: 1,
		logger: logger,
	}
}

// Insert converts parser events to result rows and appends them to the buffer.
func (b *Buffer) Insert(events []parser.Event) {
	if len(events) == 0 {
		return
	}

	entries := make([]entry, 0, len(events))
	for i := range events {
		ev := &events[i]

		var gtid *string
		if ev.GTID != "" {
			s := ev.GTID
			gtid = &s
		}

		row := query.ResultRow{
			BinlogFile:     ev.BinlogFile,
			StartPos:       ev.StartPos,
			EndPos:         ev.EndPos,
			EventTimestamp: ev.Timestamp.UTC(),
			GTID:           gtid,
			SchemaName:     ev.Schema,
			TableName:      ev.Table,
			EventType:      ev.EventType,
			PKValues:       ev.PKValues,
			ChangedColumns: parser.ChangedColumns(ev.RowBefore, ev.RowAfter),
			RowBefore:      ev.RowBefore,
			RowAfter:       ev.RowAfter,
			SchemaVersion:  ev.SchemaVersion,
		}

		entries = append(entries, entry{
			row:    row,
			pkHash: pkHash(ev.PKValues),
		})
	}

	b.mu.Lock()
	for i := range entries {
		entries[i].row.EventID = b.nextID + idOffset
		b.nextID++
	}
	b.events = append(b.events, entries...)
	b.mu.Unlock()
}

// Fetch returns events matching the query options. The returned rows are
// compatible with query.MergeResults for merging with MySQL/Parquet results.
func (b *Buffer) Fetch(_ context.Context, opts query.Options) []query.ResultRow {
	b.mu.RLock()
	defer b.mu.RUnlock()

	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	var results []query.ResultRow
	for i := range b.events {
		e := &b.events[i]
		if !matchesOpts(e, opts) {
			continue
		}
		results = append(results, e.row)
		if len(results) >= limit {
			break
		}
	}
	return results
}

// ResolvePK looks up pk_values for the given pk_hash, schema, and table.
// Returns the pk_values string and true if found.
func (b *Buffer) ResolvePK(hash, schema, table string) (string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for i := range b.events {
		e := &b.events[i]
		if e.pkHash == hash && e.row.SchemaName == schema && e.row.TableName == table {
			return e.row.PKValues, true
		}
	}
	return "", false
}

// Evict removes events older than maxAge and returns the count removed.
func (b *Buffer) Evict() int {
	cutoff := time.Now().UTC().Add(-b.maxAge)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Events are append-ordered by time, so find the first non-expired index.
	idx := 0
	for idx < len(b.events) && b.events[idx].row.EventTimestamp.Before(cutoff) {
		idx++
	}
	if idx == 0 {
		return 0
	}

	// Shift remaining events to the front to allow GC of evicted entries.
	b.events = append([]entry(nil), b.events[idx:]...)
	return idx
}

// Snapshot returns a copy of all current events for archival purposes
// (e.g. writing to Parquet). The caller gets its own slice and can process
// it without holding any lock.
func (b *Buffer) Snapshot() []query.ResultRow {
	b.mu.RLock()
	defer b.mu.RUnlock()

	rows := make([]query.ResultRow, len(b.events))
	for i := range b.events {
		rows[i] = b.events[i].row
	}
	return rows
}

// Len returns the number of events in the buffer.
func (b *Buffer) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.events)
}

// ─── Filter matching ────────────────────────────────────────────────────────

func matchesOpts(e *entry, opts query.Options) bool {
	r := &e.row

	if opts.Schema != "" && r.SchemaName != opts.Schema {
		return false
	}
	if opts.Table != "" && r.TableName != opts.Table {
		return false
	}
	if opts.PKValues != "" && r.PKValues != opts.PKValues {
		return false
	}
	if opts.EventType != nil && r.EventType != *opts.EventType {
		return false
	}
	if opts.GTID != "" {
		if r.GTID == nil || *r.GTID != opts.GTID {
			return false
		}
	}
	if opts.Since != nil && r.EventTimestamp.Before(*opts.Since) {
		return false
	}
	if opts.Until != nil && r.EventTimestamp.After(*opts.Until) {
		return false
	}
	if opts.ChangedColumn != "" && !slices.Contains(r.ChangedColumns, opts.ChangedColumn) {
		return false
	}
	for _, dt := range opts.DenyTables {
		if r.SchemaName == dt.Schema && r.TableName == dt.Table {
			return false
		}
	}
	return true
}

// pkHash computes the SHA-256 hex digest of pkValues, matching MySQL's
// SHA2(pk_values, 256) and the byosPKHash function in the agent handler.
func pkHash(pkValues string) string {
	h := sha256.Sum256([]byte(pkValues))
	return hex.EncodeToString(h[:])
}
