// Package event holds the source-agnostic row-event type that flows from a
// capture backend into the indexer and the whole downstream value stack (query,
// recover, reconstruct, shim, console). Today the MySQL/MariaDB binlog parser
// (internal/parser) produces it; a planned PostgreSQL WAL decoder is intended to
// produce the same Event without the read side linking any new capture library.
//
// It deliberately imports NO source driver (no go-mysql): everything downstream
// of an Event is source-neutral, so the read-side packages link no capture
// library — enforced by TestReadLayerDoesNotLinkGoMySQL. (Extracted from
// internal/parser — #528.)
package event

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// EventType represents the type of operation captured by a change event (DML or DDL).
type EventType uint8

const (
	EventInsert EventType = 1
	EventUpdate EventType = 2
	EventDelete EventType = 3
	EventDDL    EventType = 4
	EventGTID   EventType = 5 // GTID-only tracking event (no row data)
	// EventSnapshot is a synthetic event type emitted by query --include-snapshot
	// for rows read from a mydumper baseline Parquet file. No capture backend
	// produces this type — it exists so baseline rows can flow through the
	// same ResultRow pipeline as real change events.
	EventSnapshot EventType = 6
	// EventCommit marks a transaction commit boundary, emitted by the StreamParser
	// at an XID_EVENT (InnoDB DML) and — as a catch-all when the next transaction's
	// GTID_EVENT arrives — for transactions that carry a GTID but emit no XID and
	// aren't table DDL: implicitly-committed DDL/DCL (GRANT, CREATE DATABASE,
	// CREATE INDEX, ANALYZE TABLE, ...) and no-XID explicit terminators (XA COMMIT;
	// a COMMIT of a non-transactional transaction — a normal InnoDB COMMIT ends in
	// an XID_EVENT instead). It carries no row data, only the committed
	// transaction's GTID. The consumer advances the durable GTID checkpoint ONLY on
	// this event (and on EventDDL), never on the leading EventGTID, so a checkpoint
	// can never claim a half-streamed transaction (#491). The file parser does not
	// produce it.
	EventCommit EventType = 7
	// EventRelation carries a PostgreSQL relation's shape (column names, ordinals,
	// PK flags, type OIDs) in Relation, with no row data. The pgcapture decoder
	// emits one when it sees a pgoutput RelationMessage for an in-scope table; the
	// consumer persists it as a schema snapshot (metadata.WritePGSnapshot) and
	// stamps subsequent rows' SchemaVersion (#533). It is NEVER written to
	// binlog_events — the consumer handles it out-of-band — so it does not break the
	// "EventType numeric values are a persistence contract" rule for stored rows.
	EventRelation EventType = 8
)

// Event is a fully resolved change event with column names attached. It carries
// everything the indexer needs to write one row to binlog_events. DDL events
// (EventType=EventDDL) carry DDLQuery and DDLType instead of row data.
//
// Position fields are source-agnostic — the downstream stack treats them as
// opaque metadata (it never orders, compares, or computes on them). For a
// MySQL/MariaDB binlog source they hold the binlog coordinates: BinlogFile plus
// StartPos/EndPos byte offsets, and the GTID when enabled. A future PostgreSQL
// WAL decoder is expected to reuse these same fields for the LSN (so no field
// rename is needed to carry one); the exact LSN-encoding contract is that
// backend's to define (#530), not something this type fixes today.
type Event struct {
	BinlogFile   string // MySQL: binlog filename. (Wide enough to also hold a future Postgres LSN string.)
	StartPos     uint64 // MySQL: binlog byte offset. (uint64 also fits a Postgres LSN.)
	EndPos       uint64 // MySQL: binlog byte offset. (uint64 also fits a Postgres LSN.)
	Timestamp    time.Time
	GTID         string // empty when GTID is not enabled on the source
	ConnectionID uint32 // MySQL pseudo_thread_id from the transaction's QUERY(BEGIN) event; 0 = unknown
	Schema       string
	Table        string
	// EventType numeric values are a PERSISTENCE CONTRACT: stored as a number in
	// binlog_events.event_type and filtered by it. Never renumber existing values.
	EventType     EventType
	PKValues      string         // pipe-delimited PK values in ordinal order
	RowBefore     map[string]any // nil for INSERT
	RowAfter      map[string]any // nil for DELETE
	SchemaVersion uint32         // actual snapshot_id from schema_snapshots; updated by SwapResolver on DDL
	DDLQuery      string         // original DDL statement (EventDDL only)
	DDLType       DDLKind        // ALTER TABLE, CREATE TABLE, DROP TABLE, RENAME TABLE, TRUNCATE TABLE (EventDDL only)
	// Relation carries a PostgreSQL relation's shape (EventRelation only); the
	// consumer persists it as a schema snapshot and stamps subsequent rows'
	// SchemaVersion. nil for every other event type; never written to binlog_events.
	Relation *metadata.PGRelationSchema
}

// Filters controls which schemas and tables produce events.
// A nil map means "accept all" for that dimension.
type Filters struct {
	Schemas map[string]bool // keyed by schema name
	Tables  map[string]bool // keyed by "schema.table"
}

// Matches returns true when the schema+table passes both filter dimensions.
func (f *Filters) Matches(schema, table string) bool {
	if f.Schemas != nil && !f.Schemas[schema] {
		return false
	}
	if f.Tables != nil && !f.Tables[schema+"."+table] {
		return false
	}
	return true
}

// BuildPKValues produces a pipe-delimited string of PK values in ordinal order.
// Pipe (|) and backslash (\) inside values are escaped to prevent ambiguity.
// pkColumns must be in ordinal_position order (as returned by TableMeta.PKColumnMetas).
func BuildPKValues(pkColumns []metadata.ColumnMeta, row map[string]any) string {
	parts := make([]string, 0, len(pkColumns))
	for _, col := range pkColumns {
		val := fmt.Sprintf("%v", row[col.Name])
		val = strings.ReplaceAll(val, `\`, `\\`)
		val = strings.ReplaceAll(val, `|`, `\|`)
		parts = append(parts, val)
	}
	return strings.Join(parts, "|")
}

// ChangedColumns returns the sorted list of column names whose values differ
// between before and after images. Returns nil for INSERT/DELETE events where
// one image is nil.
func ChangedColumns(before, after map[string]any) []string {
	if before == nil || after == nil {
		return nil
	}
	var changed []string
	for key := range before {
		if !reflect.DeepEqual(before[key], after[key]) {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

// DDLKind identifies the type of DDL statement detected in a binlog QUERY_EVENT.
// The string values are persisted in schema_changes.ddl_type; keep them stable.
type DDLKind string

const (
	DDLAlterTable    DDLKind = "ALTER TABLE"
	DDLCreateTable   DDLKind = "CREATE TABLE"
	DDLDropTable     DDLKind = "DROP TABLE"
	DDLRenameTable   DDLKind = "RENAME TABLE"
	DDLTruncateTable DDLKind = "TRUNCATE TABLE"
)
