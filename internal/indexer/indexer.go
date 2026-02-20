// Package indexer consumes parsed binlog events and batch-inserts them into
// the binlog_events table in the index MySQL database.
package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bintrail/bintrail/internal/parser"
)

// Indexer consumes parser.Events from a channel and batch-inserts them into
// the binlog_events table.
type Indexer struct {
	db        *sql.DB
	batchSize int
}

// New creates an Indexer writing to db with the given batch size.
func New(db *sql.DB, batchSize int) *Indexer {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &Indexer{db: db, batchSize: batchSize}
}

// Run reads events from the channel until it is closed or ctx is cancelled,
// flushing to MySQL in batches. Returns the total number of rows inserted.
func (idx *Indexer) Run(ctx context.Context, events <-chan parser.Event) (int64, error) {
	batch := make([]parser.Event, 0, idx.batchSize)
	var total int64

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := idx.insertBatch(batch)
		if err != nil {
			return err
		}
		total += n
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case ev, ok := <-events:
			if !ok {
				// Channel closed — flush the final partial batch.
				return total, flush()
			}
			batch = append(batch, ev)
			if len(batch) >= idx.batchSize {
				if err := flush(); err != nil {
					return total, err
				}
			}
		}
	}
}

// InsertBatch writes a batch of events and returns the count of rows inserted.
// This exported method allows callers (e.g. the stream command) that need
// manual checkpoint control between batches.
func (idx *Indexer) InsertBatch(batch []parser.Event) (int64, error) {
	return idx.insertBatch(batch)
}

// BatchSize returns the configured batch size.
func (idx *Indexer) BatchSize() int {
	return idx.batchSize
}

// insertBatch writes a batch of events in a single multi-row INSERT.
// event_id and pk_hash are omitted — they are AUTO_INCREMENT and STORED
// generated respectively, so MySQL computes them on write.
func (idx *Indexer) insertBatch(batch []parser.Event) (int64, error) {
	// 12 placeholders per row
	valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
	insertSQL := `INSERT INTO binlog_events ` +
		`(binlog_file, start_pos, end_pos, event_timestamp, gtid, ` +
		`schema_name, table_name, event_type, pk_values, ` +
		`changed_columns, row_before, row_after) VALUES ` + valClause

	args := make([]any, 0, len(batch)*12)
	for i := range batch {
		ev := &batch[i]

		changed, err := marshalJSON(parser.ChangedColumns(ev.RowBefore, ev.RowAfter))
		if err != nil {
			return 0, fmt.Errorf("marshal changed_columns for %s.%s: %w", ev.Schema, ev.Table, err)
		}
		rowBefore, err := marshalRow(ev.RowBefore)
		if err != nil {
			return 0, fmt.Errorf("marshal row_before for %s.%s: %w", ev.Schema, ev.Table, err)
		}
		rowAfter, err := marshalRow(ev.RowAfter)
		if err != nil {
			return 0, fmt.Errorf("marshal row_after for %s.%s: %w", ev.Schema, ev.Table, err)
		}

		args = append(args,
			ev.BinlogFile,
			ev.StartPos,
			ev.EndPos,
			ev.Timestamp,
			nullOrString(ev.GTID),
			ev.Schema,
			ev.Table,
			uint8(ev.EventType),
			ev.PKValues,
			changed,
			rowBefore,
			rowAfter,
		)
	}

	result, err := idx.db.Exec(insertSQL, args...)
	if err != nil {
		return 0, fmt.Errorf("batch INSERT of %d events failed: %w", len(batch), err)
	}
	n, _ := result.RowsAffected()
	return n, nil
}

// ─── Serialisation helpers ────────────────────────────────────────────────────

// marshalRow encodes a named row map to JSON, returning nil for a nil map.
// []byte values that contain valid JSON (e.g. from MySQL JSON columns) are
// embedded as raw JSON rather than base64-encoded.
func marshalRow(row map[string]any) ([]byte, error) {
	if row == nil {
		return nil, nil
	}
	// Promote valid-JSON []byte values to json.RawMessage so they are embedded
	// rather than base64-encoded in the output JSON.
	normalized := make(map[string]any, len(row))
	for k, v := range row {
		if b, ok := v.([]byte); ok && json.Valid(b) {
			normalized[k] = json.RawMessage(b)
		} else {
			normalized[k] = v
		}
	}
	return json.Marshal(normalized)
}

// marshalJSON encodes v to JSON, returning nil if v is nil.
func marshalJSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

// nullOrString returns nil when s is empty (stored as SQL NULL), else s.
func nullOrString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
