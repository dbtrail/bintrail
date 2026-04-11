// Package indexer consumes parsed binlog events and batch-inserts them into
// the binlog_events table in the index MySQL database.
package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/bintrail/internal/parser"
)

// Indexer consumes parser.Events from a channel and batch-inserts them into
// the binlog_events table.
type Indexer struct {
	db        *sql.DB
	batchSize int
	onDDL     func(ev parser.Event) error
}

// New creates an Indexer writing to db with the given batch size.
func New(db *sql.DB, batchSize int) *Indexer {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &Indexer{db: db, batchSize: batchSize}
}

// SetOnDDL registers a callback invoked when a DDL event is received.
// The current batch is flushed before the callback is called.
// DDL events are NOT inserted into binlog_events.
func (idx *Indexer) SetOnDDL(fn func(parser.Event) error) {
	idx.onDDL = fn
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
			// DDL events: flush current batch, invoke callback, skip insertion.
			if ev.EventType == parser.EventDDL {
				if err := flush(); err != nil {
					return total, err
				}
				if idx.onDDL != nil {
					if err := idx.onDDL(ev); err != nil {
						return total, fmt.Errorf("onDDL callback: %w", err)
					}
				}
				continue
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
	// 14 placeholders per row
	valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
	insertSQL := `INSERT INTO binlog_events ` +
		`(binlog_file, start_pos, end_pos, event_timestamp, gtid, connection_id, ` +
		`schema_name, table_name, event_type, pk_values, ` +
		`changed_columns, row_before, row_after, schema_version) VALUES ` + valClause

	args := make([]any, 0, len(batch)*14)
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
			nullOrUint32(ev.ConnectionID),
			ev.Schema,
			ev.Table,
			uint8(ev.EventType),
			ev.PKValues,
			changed,
			rowBefore,
			rowAfter,
			ev.SchemaVersion,
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

// nullOrUint32 returns nil when v is 0 (stored as SQL NULL), else v.
func nullOrUint32(v uint32) any {
	if v == 0 {
		return nil
	}
	return v
}

// EnsureSchema adds any columns introduced after the initial schema to
// binlog_events and schema_snapshots. It is idempotent — safe to call on
// every startup.
func EnsureSchema(db *sql.DB) error {
	if err := ensureColumn(db, "binlog_events", "connection_id",
		`ALTER TABLE binlog_events ADD COLUMN connection_id INT UNSIGNED DEFAULT NULL COMMENT 'MySQL connection ID (pseudo_thread_id) that produced this event' AFTER gtid`,
	); err != nil {
		return err
	}
	// column_type carries the full type declaration (e.g. "datetime(6)") so
	// full-table reconstruct (#187, #212) can tell the declared fractional
	// precision of DATETIME/TIMESTAMP PK columns. Without this the PK
	// canonicalizer cannot distinguish DATETIME(0) from DATETIME(6) with
	// whole-second values.
	if err := ensureColumn(db, "schema_snapshots", "column_type",
		`ALTER TABLE schema_snapshots ADD COLUMN column_type VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'full type from information_schema.COLUMNS.COLUMN_TYPE' AFTER data_type`,
	); err != nil {
		return err
	}
	return nil
}

// ensureColumn runs an idempotent ALTER TABLE ADD COLUMN: checks
// information_schema, bails out if the column already exists, and swallows
// the "duplicate column" error if a concurrent process added it between our
// check and the ALTER.
func ensureColumn(db *sql.DB, table, column, alterSQL string) error {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME   = ?
		  AND COLUMN_NAME  = ?`, table, column).Scan(&count)
	if err != nil {
		return fmt.Errorf("check %s.%s column: %w", table, column, err)
	}
	if count > 0 {
		return nil
	}
	if _, err := db.Exec(alterSQL); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1060 {
			return nil
		}
		return fmt.Errorf("add %s.%s column: %w", table, column, err)
	}
	return nil
}
