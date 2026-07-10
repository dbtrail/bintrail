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
	"unicode/utf8"

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
	// QueryText is the original SQL statement that produced this row event,
	// captured from the statement's ROWS_QUERY_EVENT (MySQL,
	// binlog_rows_query_log_events=ON) or ANNOTATE_ROWS event (MariaDB,
	// binlog_annotate_row_events=ON). Empty when the source does not log it —
	// capture is opt-in on the source (#699). Statement-scoped: a multi-statement
	// transaction stamps each statement's own text onto its rows.
	QueryText string
	Schema    string
	Table     string
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
	// StmtEnd is true on the row events of the ROWS_EVENT that carries STMT_END_F —
	// the last chunk of a statement. A statement larger than
	// binlog_row_event_max_size (8 KiB default) is split into several ROWS_EVENTs
	// under ONE TABLE_MAP, and only the last one sets STMT_END_F. The stream
	// consumer keys its POSITION-mode safe checkpoint off this flag so a resume
	// never lands on a byte offset BETWEEN two chunks — a mid-statement position
	// has no preceding TABLE_MAP for the live syncer and closes the stream with
	// "invalid table id, no corresponding table map event" (#775). Transient:
	// never written to binlog_events.
	StmtEnd bool
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
		parts = append(parts, EscapePKValue(formatPKValue(row[col.Name])))
	}
	return strings.Join(parts, "|")
}

// EscapePKValue applies the pipe/backslash escaping BuildPKValues uses for
// each individual PK column value. Exported so callers holding a raw,
// already-decoded single-column PK value (e.g. internal/shim, after
// MySQL-unescaping a SQL string literal) can re-encode it into the same
// at-rest form stored in binlog_events.pk_values before using it as a match
// filter — the string-literal unescaping and this pipe-delimiter escaping
// are two unrelated encodings, and neither implies the other.
func EscapePKValue(val string) string {
	val = strings.ReplaceAll(val, `\`, `\\`)
	val = strings.ReplaceAll(val, `|`, `\|`)
	return val
}

// formatPKValue renders a single PK value for BuildPKValues. []byte needs its
// own case: since #756, metadata.MapRow hands back a BINARY/VARBINARY value as
// []byte (routing it through marshalRow's base64-safe path instead of a raw Go
// string that json.Marshal could corrupt) — a real-world PK shape, e.g. a
// BINARY(16) UUID primary key. Without this case, "%v" on a []byte prints Go's
// bracketed decimal-byte representation (e.g. "[233 12 ...]") instead of the
// raw bytes, which would silently change pk_hash/pk_values for every row with
// such a PK. string(b) reproduces exactly what "%v" printed for that same
// value before #756 (when it was still a raw Go string of the same bytes).
func formatPKValue(v any) string {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
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

// ─── Query-text sanitization (#699) ──────────────────────────────────────────

// MaxQueryTextBytes caps a captured statement's stored size. ROWS_QUERY/
// ANNOTATE events carry the FULL original statement — a bulk INSERT can run to
// megabytes — and every row event of that statement carries the text, so an
// uncapped statement would bloat the batch INSERT on the index path and the
// buffer/payload Parquet on the BYOS path. 16 KiB keeps any realistic
// hand-written or ORM statement intact; only bulk-statement tails are cut.
const MaxQueryTextBytes = 16 * 1024

// QueryTextTruncationMarker is appended to a capped statement so a forensics
// reader can tell a truncated statement from a complete one. (A truncated
// statement is deliberately NOT fed to STATEMENT_DIGEST — it usually ends
// mid-token and would not parse.)
const QueryTextTruncationMarker = " /* bintrail:truncated */"

// SanitizeQueryText prepares a captured statement for storage: it replaces
// invalid UTF-8 (a _binary'...' literal embeds raw bytes, which MySQL strict
// mode would reject with error 1366, aborting the whole batch INSERT) and
// caps the byte length at a rune boundary, appending
// QueryTextTruncationMarker when cut. Applied ONCE at the capture boundary
// (the parsers' ROWS_QUERY/ANNOTATE cases) so every downstream path — index
// batch INSERT, BYOS buffer accounting, payload Parquet — sees bounded, valid
// text; the indexer re-applies it as defense in depth.
func SanitizeQueryText(s string) string {
	if s == "" {
		return ""
	}
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= MaxQueryTextBytes {
		return s
	}
	cut := MaxQueryTextBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + QueryTextTruncationMarker
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
