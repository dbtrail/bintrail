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
	"encoding/json"
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
	row       query.ResultRow
	pkHash    string // SHA-256 hex of pk_values
	approxSize int64  // estimated heap bytes
}

// Config controls buffer capacity and eviction behavior.
type Config struct {
	MaxAge    time.Duration // age-based eviction threshold
	MaxEvents int           // max event count; 0 = unlimited
	MaxBytes  int64         // max approximate byte usage; 0 = unlimited
	Logger    *slog.Logger
}

// Buffer is a thread-safe, in-memory event store for recent binlog events.
type Buffer struct {
	mu            sync.RWMutex
	events        []entry
	nextID        uint64
	maxAge        time.Duration
	maxEvents     int
	maxBytes      int64
	curBytes      int64 // running total of approxSize across all entries
	sizeEvictions int64 // cumulative events evicted by size cap
	logger        *slog.Logger
}

// New creates a Buffer with the given configuration. Zero values for
// MaxEvents and MaxBytes mean unlimited (no cap).
func New(cfg Config) *Buffer {
	if cfg.MaxEvents < 0 {
		panic("buffer.Config: MaxEvents must be non-negative")
	}
	if cfg.MaxBytes < 0 {
		panic("buffer.Config: MaxBytes must be non-negative")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Buffer{
		maxAge:    cfg.MaxAge,
		maxEvents: cfg.MaxEvents,
		maxBytes:  cfg.MaxBytes,
		nextID:    1,
		logger:    cfg.Logger,
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

		var connID *uint32
		if ev.ConnectionID != 0 {
			v := ev.ConnectionID
			connID = &v
		}

		row := query.ResultRow{
			BinlogFile:     ev.BinlogFile,
			StartPos:       ev.StartPos,
			EndPos:         ev.EndPos,
			EventTimestamp: ev.Timestamp.UTC(),
			GTID:           gtid,
			ConnectionID:   connID,
			SchemaName:     ev.Schema,
			TableName:      ev.Table,
			EventType:      ev.EventType,
			PKValues:       ev.PKValues,
			ChangedColumns: parser.ChangedColumns(ev.RowBefore, ev.RowAfter),
			RowBefore:      ev.RowBefore,
			RowAfter:       ev.RowAfter,
			SchemaVersion:  ev.SchemaVersion,
		}

		sz := estimateRowSize(&row)
		entries = append(entries, entry{
			row:        row,
			pkHash:     pkHash(ev.PKValues),
			approxSize: sz,
		})
	}

	b.mu.Lock()
	for i := range entries {
		entries[i].row.EventID = b.nextID + idOffset
		b.nextID++
		b.curBytes += entries[i].approxSize
	}
	b.events = append(b.events, entries...)
	b.enforceSizeLimits()
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

	b.dropFront(idx)
	return idx
}

// dropFront removes the first n entries from the buffer, adjusting curBytes.
// Must be called with b.mu held for writing.
func (b *Buffer) dropFront(n int) {
	for _, e := range b.events[:n] {
		b.curBytes -= e.approxSize
	}
	b.events = append([]entry(nil), b.events[n:]...)
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

// ApproxBytes returns the approximate in-memory byte usage of all buffered events.
func (b *Buffer) ApproxBytes() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.curBytes
}

// SizeEvictions returns the cumulative count of events evicted due to size
// caps (maxEvents or maxBytes) since the buffer was created.
func (b *Buffer) SizeEvictions() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.sizeEvictions
}

// enforceSizeLimits evicts the oldest entries when maxEvents or maxBytes are
// exceeded. Must be called with b.mu held for writing.
func (b *Buffer) enforceSizeLimits() {
	drop := 0

	// Enforce event count cap.
	if b.maxEvents > 0 && len(b.events) > b.maxEvents {
		drop = len(b.events) - b.maxEvents
	}

	// Enforce byte cap — compute how many additional entries must go by
	// simulating the curBytes decrease from the entries we already plan to drop.
	if b.maxBytes > 0 {
		simBytes := b.curBytes
		for i := range drop {
			simBytes -= b.events[i].approxSize
		}
		for drop < len(b.events) && simBytes > b.maxBytes {
			simBytes -= b.events[drop].approxSize
			drop++
		}
	}

	if drop > 0 {
		b.dropFront(drop)
		b.sizeEvictions += int64(drop)
		b.logger.Warn("BYOS buffer size eviction",
			"evicted", drop,
			"remaining", len(b.events),
			"approx_bytes", b.curBytes,
		)
	}
}

// ─── Size estimation ────────────────────────────────────────────────────────

// estimateRowSize returns an approximate byte cost for a ResultRow.
// This is not exact — it sums string lengths and uses fixed estimates for
// maps, pointers, and struct overhead. Good enough for backpressure decisions.
func estimateRowSize(r *query.ResultRow) int64 {
	const overhead = 200 // struct fields, pointers, slice headers
	var n int64 = overhead

	n += int64(len(r.BinlogFile))
	n += int64(len(r.SchemaName))
	n += int64(len(r.TableName))
	n += int64(len(r.PKValues))
	n += 64 // pkHash hex string

	if r.GTID != nil {
		n += int64(len(*r.GTID))
	}
	for _, c := range r.ChangedColumns {
		n += int64(len(c))
	}
	n += estimateMapSize(r.RowBefore)
	n += estimateMapSize(r.RowAfter)
	return n
}

// estimateMapSize returns an approximate byte size for a map[string]any,
// as typically found in RowBefore/RowAfter JSON column data.
func estimateMapSize(m map[string]any) int64 {
	if m == nil {
		return 0
	}
	const perEntry = 64 // map bucket overhead per key
	var n int64
	for k, v := range m {
		n += perEntry + int64(len(k))
		n += estimateValueSize(v)
	}
	return n
}

func estimateValueSize(v any) int64 {
	switch val := v.(type) {
	case string:
		return int64(len(val))
	case []byte:
		return int64(len(val))
	case json.RawMessage:
		return int64(len(val))
	case map[string]any:
		return estimateMapSize(val)
	case []any:
		var sz int64 = 24 // slice header
		for _, elem := range val {
			sz += estimateValueSize(elem)
		}
		return sz
	case nil:
		return 0
	default:
		return 8 // numbers, bools
	}
}

// pkHash computes the SHA-256 hex digest of pkValues, matching MySQL's
// SHA2(pk_values, 256) and the byosPKHash function in the agent handler.
func pkHash(pkValues string) string {
	h := sha256.Sum256([]byte(pkValues))
	return hex.EncodeToString(h[:])
}
