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

	"github.com/dbtrail/dbtrail/internal/event"
)

// Indexer consumes event.Events from a channel and batch-inserts them into
// the binlog_events table.
type Indexer struct {
	db        *sql.DB
	batchSize int
	onDDL     func(ev event.Event) error
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
func (idx *Indexer) SetOnDDL(fn func(event.Event) error) {
	idx.onDDL = fn
}

// Run reads events from the channel until it is closed or ctx is cancelled,
// flushing to MySQL in batches. Returns the total number of rows inserted.
func (idx *Indexer) Run(ctx context.Context, events <-chan event.Event) (int64, error) {
	batch := make([]event.Event, 0, idx.batchSize)
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
			if ev.EventType == event.EventDDL {
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
func (idx *Indexer) InsertBatch(batch []event.Event) (int64, error) {
	return idx.insertBatch(batch)
}

// BatchSize returns the configured batch size.
func (idx *Indexer) BatchSize() int {
	return idx.batchSize
}

// insertBatch writes a batch of events in a single multi-row INSERT.
// event_id and pk_hash are omitted — they are AUTO_INCREMENT and STORED
// generated respectively, so MySQL computes them on write.
func (idx *Indexer) insertBatch(batch []event.Event) (int64, error) {
	// 14 placeholders per row
	valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
	insertSQL := `INSERT INTO binlog_events ` +
		`(binlog_file, start_pos, end_pos, event_timestamp, gtid, connection_id, ` +
		`schema_name, table_name, event_type, pk_values, ` +
		`changed_columns, row_before, row_after, schema_version) VALUES ` + valClause

	args := make([]any, 0, len(batch)*14)
	for i := range batch {
		ev := &batch[i]

		changed, err := marshalJSON(event.ChangedColumns(ev.RowBefore, ev.RowAfter))
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
// binlog_events, schema_snapshots, and stream_state. It is idempotent — safe
// to call on every startup.
func EnsureSchema(db *sql.DB) error {
	if err := ensureColumn(db, "binlog_events", "connection_id",
		`ALTER TABLE binlog_events ADD COLUMN connection_id INT UNSIGNED DEFAULT NULL COMMENT 'MySQL connection ID (pseudo_thread_id) that produced this event' AFTER gtid`,
	); err != nil {
		return err
	}
	// column_type carries the full type declaration (e.g. "datetime(6)") so
	// full-table reconstruct (#187, #212) can tell the declared fractional
	// precision of DATETIME/TIMESTAMP PK columns, and so the shim can map
	// ENUM/SET ordinals back to labels (#472). Without this the PK
	// canonicalizer cannot distinguish DATETIME(0) from DATETIME(6) with
	// whole-second values.
	if err := ensureColumn(db, "schema_snapshots", "column_type",
		`ALTER TABLE schema_snapshots ADD COLUMN column_type TEXT DEFAULT NULL COMMENT 'full type from information_schema.COLUMNS.COLUMN_TYPE' AFTER data_type`,
	); err != nil {
		return err
	}
	// #212 created column_type as VARCHAR(128), which a realistic ENUM
	// declaration exceeds — and under strict mode the resulting 1406
	// ("Data too long") aborts the ENTIRE snapshot transaction, not just
	// one column. Widen pre-existing installs to TEXT (#472). Existing
	// values are preserved; the resolver already COALESCEs NULL to ''.
	if err := ensureColumnWidened(db, "schema_snapshots", "column_type", "text",
		`ALTER TABLE schema_snapshots MODIFY COLUMN column_type TEXT DEFAULT NULL COMMENT 'full type from information_schema.COLUMNS.COLUMN_TYPE'`,
	); err != nil {
		return err
	}
	// pg_type_oid/pg_type_mod carry the PostgreSQL per-column type identity (pg_type
	// OID + atttypmod) from a pgoutput RelationMessage (#533). They are captured at
	// stream time because the offline recover path has no live PostgreSQL catalog to
	// rebuild them from. Nullable: MySQL snapshots leave them NULL (MySQL uses
	// data_type/column_type). WritePGSnapshot writes them; the type-faithful renderer
	// that reads them is a later #533 slice.
	if err := ensureColumn(db, "schema_snapshots", "pg_type_oid",
		`ALTER TABLE schema_snapshots ADD COLUMN pg_type_oid INT UNSIGNED DEFAULT NULL COMMENT 'PostgreSQL pg_type OID (pgoutput RelationMessage); NULL for MySQL snapshots (#533)' AFTER is_generated`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "schema_snapshots", "pg_type_mod",
		`ALTER TABLE schema_snapshots ADD COLUMN pg_type_mod INT DEFAULT NULL COMMENT 'PostgreSQL atttypmod (pgoutput RelationMessage); NULL for MySQL snapshots (#533)' AFTER pg_type_oid`,
	); err != nil {
		return err
	}
	// gap_lost_at/_detail record an unfillable-gap auto-advance durably
	// (#402): the advanced checkpoint is persisted, so without these columns
	// the only trace of the permanently lost events would be an in-memory
	// flag that a daemon restart silently discards.
	if err := ensureColumn(db, "stream_state", "gap_lost_at",
		`ALTER TABLE stream_state ADD COLUMN gap_lost_at DATETIME DEFAULT NULL COMMENT 'when an unfillable binlog gap forced an auto-advance (events permanently lost); cleared by an explicit monitor Stop or --reset' AFTER bintrail_id`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "stream_state", "gap_lost_detail",
		`ALTER TABLE stream_state ADD COLUMN gap_lost_detail TEXT DEFAULT NULL COMMENT 'human-readable description of the lost gap' AFTER gap_lost_at`,
	); err != nil {
		return err
	}
	// flavor records the source database flavor (mysql/mariadb) so a resume
	// parses the saved gtid_set with the correct GTID parser. NOT NULL DEFAULT
	// 'mysql' means existing rows read back as mysql with no data migration,
	// keeping every pre-MariaDB install unchanged.
	if err := ensureColumn(db, "stream_state", "flavor",
		`ALTER TABLE stream_state ADD COLUMN flavor VARCHAR(16) NOT NULL DEFAULT 'mysql' COMMENT 'source flavor: mysql or mariadb; selects the GTID parser on resume' AFTER gtid_set`,
	); err != nil {
		return err
	}
	// delete_rule/update_rule carry each FK's referential action so cascade
	// recovery can tell which edges are ON DELETE CASCADE at recovery time.
	// `recover` is source-less, so the rule must live in the index rather than
	// be re-queried from the source. NOT NULL DEFAULT '' means pre-existing
	// rows read back as "unknown" (treated as non-cascade) with no data
	// migration; a fresh snapshot of the schema populates the real rule.
	//
	// fk_constraints post-dates the original schema and may be absent on very
	// old indexes (TakeSnapshot tolerates its absence). Only migrate it when
	// present; `bintrail init` (the DDL) creates the table with these columns,
	// and a snapshot then populates the rule once the table exists. The block is
	// gated by `if hasFK` (rather than an early return) so any migration added
	// after it still runs on an index that predates fk_constraints.
	hasFK, err := tableExists(db, "fk_constraints")
	if err != nil {
		return err
	}
	if hasFK {
		if err := ensureColumn(db, "fk_constraints", "delete_rule",
			`ALTER TABLE fk_constraints ADD COLUMN delete_rule VARCHAR(16) NOT NULL DEFAULT '' COMMENT 'ON DELETE rule (CASCADE/RESTRICT/SET NULL/NO ACTION); empty for pre-cascade-recovery snapshots' AFTER referenced_column_name`,
		); err != nil {
			return err
		}
		if err := ensureColumn(db, "fk_constraints", "update_rule",
			`ALTER TABLE fk_constraints ADD COLUMN update_rule VARCHAR(16) NOT NULL DEFAULT '' COMMENT 'ON UPDATE rule; empty for pre-cascade-recovery snapshots' AFTER delete_rule`,
		); err != nil {
			return err
		}
	}
	return nil
}

// tableExists reports whether a base table named `table` exists in the current
// database.
func tableExists(db *sql.DB, table string) (bool, error) {
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table).Scan(&n); err != nil {
		return false, fmt.Errorf("check table %s: %w", table, err)
	}
	return n > 0, nil
}

// ensureColumnWidened runs an idempotent ALTER TABLE MODIFY COLUMN: it
// checks the column's current information_schema DATA_TYPE and bails out
// when it already matches wantDataType (lowercase, e.g. "text"). A column
// that does not exist at all is also a no-op — ensureColumn owns creation;
// this helper only ever widens an existing one.
func ensureColumnWidened(db *sql.DB, table, column, wantDataType, alterSQL string) error {
	var dataType string
	err := db.QueryRow(`SELECT DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME   = ?
		  AND COLUMN_NAME  = ?`, table, column).Scan(&dataType)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check %s.%s type: %w", table, column, err)
	}
	if strings.EqualFold(dataType, wantDataType) {
		return nil
	}
	if _, err := db.Exec(alterSQL); err != nil {
		return fmt.Errorf("widen %s.%s to %s: %w", table, column, wantDataType, err)
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

// InsertSchemaChange records a DDL detection in the schema_changes table.
// snapshotID may be nil when no auto-snapshot was taken (file mode).
func InsertSchemaChange(db *sql.DB, ev event.Event, snapshotID *int) error {
	var snapArg any
	if snapshotID != nil {
		snapArg = *snapshotID
	}
	_, err := db.Exec(`
		INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, gtid, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.Timestamp, ev.BinlogFile, ev.EndPos,
		nullOrString(ev.GTID), ev.Schema, ev.Table, ev.DDLType, ev.DDLQuery, snapArg)
	return err
}
