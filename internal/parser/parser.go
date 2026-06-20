// Package parser reads MySQL ROW-format binlog files and emits typed events.
// It uses go-mysql-org/go-mysql for low-level binlog decoding and the
// metadata.Resolver to map column ordinals to column names.
package parser

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── Event types ─────────────────────────────────────────────────────────────

// EventType represents the type of operation captured by a binlog event (DML or DDL).
type EventType uint8

const (
	EventInsert EventType = 1
	EventUpdate EventType = 2
	EventDelete EventType = 3
	EventDDL    EventType = 4
	EventGTID   EventType = 5 // GTID-only tracking event (no row data)
	// EventSnapshot is a synthetic event type emitted by query --include-snapshot
	// for rows read from a mydumper baseline Parquet file. The parser never
	// produces this type — it exists so baseline rows can flow through the
	// same ResultRow pipeline as real binlog events.
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
)

// Event is a fully resolved binlog row event with column names attached.
// It carries everything the indexer needs to write one row to binlog_events.
// DDL events (EventType=4) carry DDLQuery and DDLType instead of row data.
type Event struct {
	BinlogFile    string
	StartPos      uint64
	EndPos        uint64
	Timestamp     time.Time
	GTID          string // empty when GTID is not enabled on the source
	ConnectionID  uint32 // MySQL pseudo_thread_id from the transaction's QUERY(BEGIN) event; 0 = unknown
	Schema        string
	Table         string
	EventType     EventType
	PKValues      string         // pipe-delimited PK values in ordinal order
	RowBefore     map[string]any // nil for INSERT
	RowAfter      map[string]any // nil for DELETE
	SchemaVersion uint32         // actual snapshot_id from schema_snapshots; updated by SwapResolver on DDL
	DDLQuery      string         // original DDL statement (EventDDL only)
	DDLType       DDLKind        // ALTER TABLE, CREATE TABLE, DROP TABLE, RENAME TABLE, TRUNCATE TABLE (EventDDL only)
}

// ─── Filters ─────────────────────────────────────────────────────────────────

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

// ─── Parser ──────────────────────────────────────────────────────────────────

// Parser reads binlog files and emits Events onto a channel.
type Parser struct {
	binlogDir     string
	resolver      atomic.Pointer[metadata.Resolver]
	filters       Filters
	logger        *slog.Logger
	schemaVersion atomic.Uint32 // actual snapshot_id from schema_snapshots; updated by SwapResolver
}

// New creates a Parser that reads from binlogDir, resolves column names via
// resolver, and applies the given filters.
// logger may be nil, in which case slog.Default() is used.
func New(binlogDir string, resolver *metadata.Resolver, filters Filters, logger *slog.Logger) *Parser {
	if logger == nil {
		logger = slog.Default()
	}
	p := &Parser{
		binlogDir: binlogDir,
		filters:   filters,
		logger:    logger,
	}
	if resolver != nil {
		p.schemaVersion.Store(uint32(resolver.SnapshotID()))
		p.resolver.Store(resolver)
	}
	return p
}

// SwapResolver atomically replaces the resolver used for column resolution
// and updates schemaVersion to the new resolver's SnapshotID.
// Safe to call concurrently while ParseFile is running.
func (p *Parser) SwapResolver(r *metadata.Resolver) {
	p.schemaVersion.Store(uint32(r.SnapshotID()))
	p.resolver.Store(r)
}

// ParseFiles parses multiple binlog files in order, sending events to the channel.
func (p *Parser) ParseFiles(ctx context.Context, filenames []string, events chan<- Event) error {
	for _, name := range filenames {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := p.ParseFile(ctx, name, events); err != nil {
			return fmt.Errorf("error in %s: %w", name, err)
		}
	}
	return nil
}

// ParseFile parses a single binlog file and sends matching events to the channel.
// It stops early if ctx is cancelled.
func (p *Parser) ParseFile(ctx context.Context, filename string, events chan<- Event) error {
	fullPath := filepath.Join(p.binlogDir, filename)

	// currentGTID holds the GTID of the transaction currently being processed.
	// It is updated on every GTID_LOG_EVENT and carried into every subsequent
	// rows event until the next GTID_LOG_EVENT resets it.
	var currentGTID string
	// currentConnectionID holds the MySQL pseudo_thread_id from the most
	// recent QueryEvent. For DML transactions, this is the QUERY(BEGIN)
	// event that precedes the row events.
	var currentConnectionID uint32

	bp := replication.NewBinlogParser()

	// handleEvent processes one binlog event. It is recursive: with
	// binlog_transaction_compression=ON the source wraps each transaction's
	// events (BEGIN + TABLE_MAP + rows + XID) in a single zstd-compressed
	// Transaction_payload event, which go-mysql delivers pre-decoded in
	// ev.Events — recursing dispatches them through the same cases.
	var handleEvent func(binlogEv *replication.BinlogEvent) error
	handleEvent = func(binlogEv *replication.BinlogEvent) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		switch ev := binlogEv.Event.(type) {
		case *replication.GTIDEvent:
			currentGTID = formatGTID(ev.SID, ev.GNO)

		case *replication.MariadbGTIDEvent:
			// MariaDB source: the GTID arrives as domain-server-seq (e.g.
			// "0-1-100"). ev.GTID.String() returns "" for the zero GTID,
			// mirroring formatGTID's not-enabled behavior.
			currentGTID = ev.GTID.String()

		case *replication.QueryEvent:
			currentConnectionID = ev.SlaveProxyID
			ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if ddlEv, ok := parseDDL(p.logger, filename, binlogEv.Header.LogPos, ts, currentGTID, string(ev.Query), p.schemaVersion.Load()); ok {
				select {
				case events <- ddlEv:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

		case *replication.RowsEvent:
			return handleRows(ctx, p.logger, p.resolver.Load(), &p.filters, binlogEv, ev, filename, currentGTID, currentConnectionID, p.schemaVersion.Load(), events)

		case *replication.TransactionPayloadEvent:
			for _, inner := range ev.Events {
				rewriteInnerHeader(inner.Header, binlogEv.Header)
				if err := handleEvent(inner); err != nil {
					return err
				}
			}
		}
		return nil
	}

	return bp.ParseFile(fullPath, 0, handleEvent)
}

// rewriteInnerHeader stamps a Transaction_payload inner event's header with
// the outer payload event's file coordinates. Inner events were never written
// to the binlog individually: MySQL zeroes their end_log_pos (LogPos=0 —
// verified on 8.0.46 and in go-mysql's 8.0.27 test fixture) while EventSize
// is the genuine event length, so deriving start_pos as LogPos-EventSize
// would underflow uint64. The physical location of every inner event IS the
// payload event that carries it.
func rewriteInnerHeader(inner, outer *replication.EventHeader) {
	inner.LogPos = outer.LogPos
	inner.EventSize = outer.EventSize
}

// ─── Row event handler ────────────────────────────────────────────────────────

// firstPartialImage reports whether any row image in a RowsEvent omitted
// columns — the signature of a non-FULL binlog_row_image (MINIMAL/NOBLOB). It
// returns the absent column indices of the first partial image found, or nil
// when every image is complete. The returned indices are go-mysql's 0-based
// column ordinals (NOT bintrail's 1-based metadata.ColumnMeta.OrdinalPosition);
// the guard surfaces them verbatim in its error for diagnosis.
//
// go-mysql populates RowsEvent.SkippedColumns in lock-step with RowsEvent.Rows:
// one entry per decoded image (for UPDATE there are two images per logical row —
// before and after), each listing the column ordinals whose present-bit was
// clear in the event's column bitmap. Under binlog_row_image=FULL every bit is
// set, so each entry is an empty (non-nil) slice; under MINIMAL/NOBLOB the
// omitted columns appear here while go-mysql pads them to nil in Rows.
// Confirmed empirically against go-mysql v1.13.0, including that a FULL image of
// a table with a VIRTUAL generated column yields no skips (no false positive).
func firstPartialImage(skipped [][]int) []int {
	for _, image := range skipped {
		if len(image) > 0 {
			return image
		}
	}
	return nil
}

// handleRows processes a RowsEvent, resolving column names and dispatching to
// the appropriate emit function. It is shared by Parser.ParseFile and StreamParser.Run.
func handleRows(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	filters *Filters,
	binlogEv *replication.BinlogEvent,
	rowsEv *replication.RowsEvent,
	filename, currentGTID string,
	connectionID uint32,
	schemaVersion uint32,
	out chan<- Event,
) error {
	schema := string(rowsEv.Table.Schema)
	table := string(rowsEv.Table.Table)

	if !filters.Matches(schema, table) {
		return nil
	}

	if resolver == nil {
		logger.Warn("no resolver available — skipping event",
			"file", filename, "pos", binlogEv.Header.LogPos,
			"schema", schema, "table", table)
		return nil
	}

	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		// Table not in snapshot — warn and skip all rows for this event.
		logger.Warn("table not in snapshot — skipping",
			"file", filename,
			"pos", binlogEv.Header.LogPos,
			"error", err)
		return nil
	}

	// Column count validation: the binlog TABLE_MAP_EVENT reports how many
	// columns exist in the table at write time. If it differs from the snapshot,
	// the column-to-name mapping would be wrong — skip and warn.
	if int(rowsEv.Table.ColumnCount) != len(tm.Columns) {
		logger.Warn("column count mismatch — skipping (consider re-running `bintrail snapshot`)",
			"file", filename,
			"pos", binlogEv.Header.LogPos,
			"schema", schema,
			"table", table,
			"binlog_columns", rowsEv.Table.ColumnCount,
			"snapshot_columns", len(tm.Columns))
		return nil
	}

	// Partial row image guard (#493): bintrail requires binlog_row_image=FULL.
	// Under MINIMAL/NOBLOB, MySQL omits columns from the before/after image and
	// go-mysql pads the absent positions to nil — which we would otherwise store
	// as a genuine NULL, silently corrupting the before/after images that
	// `recover` later trusts. The server-global SHOW VARIABLES check in
	// `metadata.ValidateBinlogRowImage` is one-shot and session-settable, so it
	// can be bypassed; this per-row check is the authoritative chokepoint that
	// covers BOTH the file-index and stream paths. Fail loud rather than index
	// NULL-filled rows.
	if firstSkipped := firstPartialImage(rowsEv.SkippedColumns); firstSkipped != nil {
		return fmt.Errorf(
			"partial binlog row image detected at %s:%d for %s.%s (%d column(s) absent: %v); "+
				"bintrail requires binlog_row_image=FULL — set it server-wide (a session-level "+
				"override produced this event) and re-generate the binlog, or these events would "+
				"be indexed with absent columns stored as NULL",
			filename, binlogEv.Header.LogPos, schema, table,
			len(firstSkipped), firstSkipped)
	}

	// LogPos points to the byte AFTER the event. Subtract EventSize to get start.
	startPos := uint64(binlogEv.Header.LogPos) - uint64(binlogEv.Header.EventSize)
	endPos := uint64(binlogEv.Header.LogPos)
	ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
	pkCols := tm.PKColumnMetas()

	switch binlogEv.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2:
		return emitInserts(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, startPos, endPos, ts, pkCols, schemaVersion, out)

	case replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:
		return emitDeletes(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, startPos, endPos, ts, pkCols, schemaVersion, out)

	case replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		return emitUpdates(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, startPos, endPos, ts, pkCols, schemaVersion, out)

	default:
		// A RowsEvent whose type matches none of the above — e.g. MariaDB's
		// MARIADB_WRITE/UPDATE/DELETE_ROWS_COMPRESSED_EVENT_V1 (log_bin_compress=ON),
		// or PARTIAL_UPDATE_ROWS_EVENT which a MySQL source emits under
		// binlog_row_value_options=PARTIAL_JSON (out of support; binlog_row_image=FULL
		// is required). Decoding these is deferred; warn loudly — including how many
		// rows were skipped — rather than dropping them silently (a data-loss class).
		// Standard MySQL ROW DML always matches a specific case above.
		logger.Warn("unhandled row event type — rows skipped (e.g. MariaDB compressed-row events are not yet decoded)",
			"file", filename,
			"pos", binlogEv.Header.LogPos,
			"schema", schema,
			"table", table,
			"event_type", binlogEv.Header.EventType,
			"rows_skipped", len(rowsEv.Rows))
	}

	return nil
}

func emitInserts(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	rows [][]any,
	schema, table, filename, gtid string,
	connectionID uint32,
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion uint32,
	out chan<- Event,
) error {
	for _, row := range rows {
		named, err := resolver.MapRow(schema, table, row)
		if err != nil {
			logger.Warn("failed to map INSERT row — skipping",
				"schema", schema, "table", table, "error", err)
			continue
		}
		ev := Event{
			BinlogFile: filename, StartPos: startPos, EndPos: endPos,
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID,
			Schema: schema, Table: table, EventType: EventInsert,
			PKValues:      BuildPKValues(pkCols, named),
			RowAfter:      named,
			SchemaVersion: schemaVersion,
		}
		select {
		case out <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func emitDeletes(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	rows [][]any,
	schema, table, filename, gtid string,
	connectionID uint32,
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion uint32,
	out chan<- Event,
) error {
	for _, row := range rows {
		named, err := resolver.MapRow(schema, table, row)
		if err != nil {
			logger.Warn("failed to map DELETE row — skipping",
				"schema", schema, "table", table, "error", err)
			continue
		}
		ev := Event{
			BinlogFile: filename, StartPos: startPos, EndPos: endPos,
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID,
			Schema: schema, Table: table, EventType: EventDelete,
			PKValues:      BuildPKValues(pkCols, named),
			RowBefore:     named,
			SchemaVersion: schemaVersion,
		}
		select {
		case out <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func emitUpdates(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	rows [][]any,
	schema, table, filename, gtid string,
	connectionID uint32,
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion uint32,
	out chan<- Event,
) error {
	// go-mysql delivers UPDATE rows as interleaved before/after pairs:
	//   rows[0]=before0, rows[1]=after0, rows[2]=before1, rows[3]=after1, ...
	for i := 0; i+1 < len(rows); i += 2 {
		before, err := resolver.MapRow(schema, table, rows[i])
		if err != nil {
			logger.Warn("failed to map UPDATE before-row — skipping",
				"schema", schema, "table", table, "error", err)
			continue
		}
		after, err := resolver.MapRow(schema, table, rows[i+1])
		if err != nil {
			logger.Warn("failed to map UPDATE after-row — skipping",
				"schema", schema, "table", table, "error", err)
			continue
		}
		ev := Event{
			BinlogFile: filename, StartPos: startPos, EndPos: endPos,
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID,
			Schema: schema, Table: table, EventType: EventUpdate,
			PKValues:      BuildPKValues(pkCols, before), // PK from before-image
			RowBefore:     before,
			RowAfter:      after,
			SchemaVersion: schemaVersion,
		}
		select {
		case out <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

// formatGTID formats a MySQL GTID from the raw 16-byte server UUID (SID) and
// the group number (GNO). Returns an empty string if SID is not 16 bytes
// (GTID not enabled on the source server).
func formatGTID(sid []byte, gno int64) string {
	if len(sid) != 16 {
		return ""
	}
	// SID bytes map directly to the UUID string groups without byte-swapping.
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x:%d",
		sid[0:4], sid[4:6], sid[6:8], sid[8:10], sid[10:16], gno)
}

// DDLKind identifies the type of DDL statement detected in a binlog QUERY_EVENT.
type DDLKind string

const (
	DDLAlterTable    DDLKind = "ALTER TABLE"
	DDLCreateTable   DDLKind = "CREATE TABLE"
	DDLDropTable     DDLKind = "DROP TABLE"
	DDLRenameTable   DDLKind = "RENAME TABLE"
	DDLTruncateTable DDLKind = "TRUNCATE TABLE"
)

// ddlTableRe extracts the schema and table name from DDL statements.
// Handles: ALTER TABLE [schema.]table, CREATE TABLE [IF NOT EXISTS] [schema.]table,
// DROP TABLE [IF EXISTS] [schema.]table, RENAME TABLE [schema.]table,
// TRUNCATE [TABLE] [schema.]table.
// Backtick-quoted identifiers are supported via `([^`]+)`.
var ddlTableRe = regexp.MustCompile(
	`(?i)(?:ALTER\s+TABLE|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|DROP\s+TABLE(?:\s+IF\s+EXISTS)?|RENAME\s+TABLE|TRUNCATE(?:\s+TABLE)?)\s+` +
		"(?:`([^`]+)`\\.`([^`]+)`|`([^`]+)`|(\\w+)\\.(\\w+)|(\\w+))")

// parseDDL parses a QUERY_EVENT for DDL statements and returns a DDL Event.
// Returns zero Event and false if the query is not a DDL statement.
// TRUNCATE is included for audit purposes but does not invalidate the snapshot
// (callers should skip auto-snapshot for DDLTruncateTable).
func parseDDL(logger *slog.Logger, filename string, logPos uint32, timestamp time.Time, gtid, queryStr string, schemaVersion uint32) (Event, bool) {
	upper := strings.ToUpper(strings.TrimSpace(queryStr))

	var ddlType DDLKind
	switch {
	case strings.HasPrefix(upper, "ALTER TABLE"):
		ddlType = DDLAlterTable
	case strings.HasPrefix(upper, "CREATE TABLE"):
		ddlType = DDLCreateTable
	case strings.HasPrefix(upper, "DROP TABLE"):
		ddlType = DDLDropTable
	case strings.HasPrefix(upper, "RENAME TABLE"):
		ddlType = DDLRenameTable
	case strings.HasPrefix(upper, "TRUNCATE"):
		ddlType = DDLTruncateTable
	default:
		return Event{}, false
	}

	// Extract schema and table from the DDL query.
	var schema, table string
	m := ddlTableRe.FindStringSubmatch(queryStr)
	if m != nil {
		switch {
		case m[1] != "" && m[2] != "": // `schema`.`table`
			schema, table = m[1], m[2]
		case m[3] != "": // `table` (no schema)
			table = m[3]
		case m[4] != "" && m[5] != "": // schema.table (unquoted)
			schema, table = m[4], m[5]
		case m[6] != "": // table (unquoted, no schema)
			table = m[6]
		}
	}

	startPos := uint64(0) // DDL events have no row-level start position
	endPos := uint64(logPos)

	logger.Warn("DDL detected",
		"file", filename,
		"pos", logPos,
		"ddl_type", ddlType,
		"schema", schema,
		"table", table,
		"query", queryStr)

	return Event{
		BinlogFile:    filename,
		StartPos:      startPos,
		EndPos:        endPos,
		Timestamp:     timestamp,
		GTID:          gtid,
		Schema:        schema,
		Table:         table,
		EventType:     EventDDL,
		DDLQuery:      queryStr,
		DDLType:       ddlType,
		SchemaVersion: schemaVersion,
	}, true
}
