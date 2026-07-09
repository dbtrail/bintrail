// Package parser reads MySQL ROW-format binlog files and emits typed events.
// It uses go-mysql-org/go-mysql for low-level binlog decoding and the
// metadata.Resolver to map column ordinals to column names.
package parser

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
)

// ─── Event types ─────────────────────────────────────────────────────────────

// EventType is the source-agnostic event-type, moved to internal/event (#528).
// Aliased here so the capture layer and existing callers keep using parser.EventType.
type EventType = event.EventType

const (
	EventInsert   = event.EventInsert
	EventUpdate   = event.EventUpdate
	EventDelete   = event.EventDelete
	EventDDL      = event.EventDDL
	EventGTID     = event.EventGTID
	EventSnapshot = event.EventSnapshot
	EventCommit   = event.EventCommit
)

// Event is the source-agnostic change event, moved to internal/event (#528).
// Aliased here so the binlog parser and existing callers keep using parser.Event.
type Event = event.Event

// ─── Filters ─────────────────────────────────────────────────────────────────

// Filters is the source-agnostic schema/table filter, moved to internal/event
// (#528). Aliased here so the parser and existing callers keep using parser.Filters.
type Filters = event.Filters

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
	// currentQueryText holds the original SQL statement from the most recent
	// ROWS_QUERY_EVENT (MySQL, binlog_rows_query_log_events=ON) or
	// ANNOTATE_ROWS event (MariaDB, binlog_annotate_row_events=ON). It is
	// statement-scoped: each statement's event overwrites the previous one,
	// and one statement's text covers ALL of its (possibly chained) rows
	// events. Cleared in three places, each load-bearing: QUERY and GTID
	// boundaries (across transactions), and the STMT_END_F rows event (the
	// statement boundary WITHIN a transaction — the only clear that stops a
	// later ROWS_QUERY-less statement in the same transaction from inheriting
	// stale text when the variable is toggled off mid-transaction).
	var currentQueryText string

	bp := replication.NewBinlogParser()
	// Pin TIMESTAMP-column string rendering to UTC. go-mysql's default (nil
	// location) formats fracTime.String() using the raw time.Unix(...) value,
	// whose Location is the process's time.Local — leaking host-local wall
	// clock into stored data even though DATETIME (decoded separately, always
	// time.UTC) and the rest of the system (verify, shim, reconstruct) assume
	// UTC at rest (#757).
	bp.SetTimestampStringLocation(time.UTC)

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
			currentGTID = formatGTID(binlogEv.Header.EventType, ev.SID, ev.GNO)
			currentQueryText = "" // transaction boundary — statement text never crosses it

		case *replication.MariadbGTIDEvent:
			// MariaDB source: the GTID arrives as domain-server-seq (e.g.
			// "0-1-100"). ev.GTID.String() returns "" for the zero GTID,
			// mirroring formatGTID's not-enabled behavior.
			currentGTID = ev.GTID.String()
			currentQueryText = ""

		case *replication.QueryEvent:
			currentConnectionID = ev.SlaveProxyID
			// A new QUERY (BEGIN of the next transaction, or a DDL) opens a new
			// statement scope; any ROWS_QUERY text from the previous statement
			// is stale. The statement's own ROWS_QUERY_EVENT arrives AFTER this.
			currentQueryText = ""
			ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if ddlEv, ok := parseDDL(p.logger, filename, binlogEv.Header.LogPos, ts, currentGTID, string(ev.Query), p.schemaVersion.Load()); ok {
				select {
				case events <- ddlEv:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else if kw, isDML := statementDML(string(ev.Query)); isDML {
				// STATEMENT/MIXED-format DML (or a session flip off ROW): the row
				// image is not in the binlog, so this change cannot be captured.
				// Fail LOUD + metric, symmetric to the partial-image guard (#493).
				// NOTE: never log the statement text — a DML statement embeds row
				// VALUES; keyword + file/pos + connection_id locate it without
				// leaking data into operator logs.
				p.logger.Warn("statement-format DML in binlog — event NOT captured (bintrail requires binlog_format=ROW; a STATEMENT/MIXED format or a session-level override produced this)",
					"file", filename,
					"pos", binlogEv.Header.LogPos,
					"statement_type", kw,
					"connection_id", ev.SlaveProxyID)
				observe.StatementDMLDropped()
			}

		case *replication.RowsQueryEvent:
			// The original SQL statement (binlog_rows_query_log_events=ON),
			// emitted right before the statement's TABLE_MAP + rows events.
			// Sanitized ONCE here at the capture boundary — every downstream
			// path (index INSERT, BYOS buffer/payload) sees bounded, valid
			// UTF-8 text.
			currentQueryText = event.SanitizeQueryText(string(ev.Query))

		case *replication.MariadbAnnotateRowsEvent:
			// MariaDB's positional sibling of ROWS_QUERY_EVENT
			// (binlog_annotate_row_events=ON).
			currentQueryText = event.SanitizeQueryText(string(ev.Query))

		case *replication.RowsEvent:
			err := handleRows(ctx, p.logger, p.resolver.Load(), &p.filters, binlogEv, ev, filename, currentGTID, currentConnectionID, currentQueryText, p.schemaVersion.Load(), events)
			// The LAST rows event of a statement carries STMT_END_F — the
			// actual statement boundary. Clearing here keeps one statement's
			// text alive across its chained/split rows events, while a later
			// statement in the SAME transaction that logged no ROWS_QUERY
			// (variable toggled off mid-transaction — MySQL allows it; there
			// is no GTID/QUERY boundary in between) can never inherit it.
			if ev.Flags&replication.RowsEventStmtEndFlag != 0 {
				currentQueryText = ""
			}
			return err

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
	queryText string,
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

	// Column NAME cross-check (#700): with binlog_row_metadata=FULL the
	// TABLE_MAP event embeds the table's real column names at write time,
	// giving us per-event ground truth to hold the snapshot against. This
	// catches the drift class the count guard above CANNOT see: a same-count
	// schema change (a column rename, or a DROP+ADD in one ALTER) leaves the
	// count equal while every value after the changed ordinal would be
	// attributed to the WRONG column name — silent corruption, worse than the
	// count-mismatch skip. The compare is case-insensitive per MySQL's
	// identifier rules (a case-only rename does not change the mapping) and
	// includes generated columns (present in both the snapshot and the FULL
	// TABLE_MAP metadata — a different knob from the #493 guard's
	// binlog_row_image below). Under the default binlog_row_metadata=MINIMAL the event
	// carries no names and the check degrades to a no-op.
	//
	// A divergence splits on the event's age relative to the snapshot:
	//
	//   * Event AT-OR-AFTER the snapshot → the snapshot is STALE (the schema
	//     changed after it was taken). Fail loud like the partial-image guard
	//     below — proceeding would write wrong data that `recover` later
	//     trusts, and the remediation (re-run `bintrail snapshot`) genuinely
	//     converges: the fresh snapshot matches these events.
	//
	//   * Event BEFORE the snapshot → a routine historical state, not a
	//     stale snapshot: re-indexing old files after a rename, or a stream
	//     catching up through a backlog written before the operator
	//     re-snapshotted. Re-snapshotting is a NO-OP here (the old names are
	//     baked into the binlog), so a hard error would be a permanent dead
	//     end with no converging remediation. Proceed exactly as before
	//     #700 — values index under the snapshot's CURRENT names, which is
	//     positionally correct for a pure rename (and is what makes the
	//     generated recovery SQL executable against the live table) — and
	//     warn loudly per rows event, the count guard's verbosity class.
	//
	// The snapshot time comes from the bintrail host clock (TakeSnapshot)
	// while event timestamps come from the source server; a large clock skew
	// can misclassify a rename taken moments around the snapshot — the
	// failure mode is a loud warning instead of a hard stop, never silence.
	// A zero snapshot time (unknown) stays strict,
	// and so does a zero EVENT timestamp (tool-generated/rewritten binlogs
	// occasionally carry zeroed headers — an unknown age must not take the
	// lenient path).
	if names := rowsEv.Table.ColumnNameString(); len(names) > 0 && len(names) == len(tm.Columns) {
		for i := range names {
			if mysqlIdentEqualFold(names[i], tm.Columns[i].Name) {
				continue
			}
			eventTime := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if snapTime := resolver.SnapshotTime(); binlogEv.Header.Timestamp != 0 && !snapTime.IsZero() && eventTime.Before(snapTime) {
				logger.Warn("column names differ from snapshot for a pre-snapshot event — indexing under the snapshot's names",
					"file", filename,
					"pos", binlogEv.Header.LogPos,
					"schema", schema,
					"table", table,
					"ordinal", i+1,
					"binlog_column", names[i],
					"snapshot_column", tm.Columns[i].Name,
					"event_time", eventTime,
					"snapshot_time", snapTime)
				break
			}
			return fmt.Errorf(
				"schema drift detected at %s:%d for %s.%s: binlog TABLE_MAP column %d is %q but schema snapshot %d has %q; "+
					"the snapshot is stale (a column was renamed, or dropped and re-added, since it was taken) and indexing "+
					"these events would attribute row values to the wrong columns — run `bintrail snapshot` against the "+
					"current schema, then re-run indexing (a failed file re-indexes from the start; a stream resumes from "+
					"its checkpoint); if this event actually PREDATES the snapshot, check for clock skew between the "+
					"bintrail host and the source server",
				filename, binlogEv.Header.LogPos, schema, table,
				i+1, names[i], schemaVersion, tm.Columns[i].Name)
		}
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
		return emitInserts(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, queryText, startPos, endPos, ts, pkCols, schemaVersion, out)

	case replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:
		return emitDeletes(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, queryText, startPos, endPos, ts, pkCols, schemaVersion, out)

	case replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		return emitUpdates(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, connectionID, queryText, startPos, endPos, ts, pkCols, schemaVersion, out)

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
	queryText string,
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
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID, QueryText: queryText,
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
	queryText string,
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
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID, QueryText: queryText,
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
	queryText string,
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
			Timestamp: ts, GTID: gtid, ConnectionID: connectionID, QueryText: queryText,
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

// mysqlIdentEqualFold reports whether two column identifiers are equal under
// MySQL's case-insensitive identifier comparison. strings.EqualFold covers
// every case MySQL folds except the Turkish dotted/dotless I pair: MySQL's
// identifier collation treats İ (U+0130) and ı (U+0131) as equal to I/i
// (verified on 8.4: CREATE TABLE t (i INT, İ INT) fails with a duplicate-
// column error, and RENAME COLUMN İstanbul TO istanbul succeeds as a
// case-style rename), while Unicode simple folding maps neither to 'i' — so
// a plain EqualFold would flag that legal case-style rename as drift (#700).
// Accents are NOT folded by MySQL identifiers (verified on 8.4: CREATE TABLE
// t (e INT, é INT) succeeds with two distinct columns), so no wider
// normalization is needed.
func mysqlIdentEqualFold(a, b string) bool {
	if strings.EqualFold(a, b) {
		return true
	}
	dotless := func(r rune) rune {
		if r == 'İ' || r == 'ı' {
			return 'i'
		}
		return r
	}
	return strings.EqualFold(strings.Map(dotless, a), strings.Map(dotless, b))
}

// BuildPKValues forwards to event.BuildPKValues (kept for back-compat; the
// canonical doc lives on event.BuildPKValues).
func BuildPKValues(pkColumns []metadata.ColumnMeta, row map[string]any) string {
	return event.BuildPKValues(pkColumns, row)
}

// ChangedColumns forwards to event.ChangedColumns (kept for back-compat; the
// canonical doc lives on event.ChangedColumns).
func ChangedColumns(before, after map[string]any) []string {
	return event.ChangedColumns(before, after)
}

// formatGTID formats a MySQL GTID from the raw 16-byte server UUID (SID) and
// the group number (GNO). Returns an empty string when there is no real GTID
// to report: eventType is ANONYMOUS_GTID_EVENT (gtid_mode=OFF still wraps
// every transaction in this event; go-mysql decodes it into the same
// GTIDEvent struct as a real GTID_EVENT, with a 16-zero-byte SID that would
// otherwise pass the length check below and format into a fake-but-valid-
// looking GTID, e.g. "00000000-0000-0000-0000-000000000000:0" — #678), or SID
// is not 16 bytes.
func formatGTID(eventType replication.EventType, sid []byte, gno int64) string {
	if eventType == replication.ANONYMOUS_GTID_EVENT {
		return ""
	}
	if len(sid) != 16 {
		return ""
	}
	// SID bytes map directly to the UUID string groups without byte-swapping.
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x:%d",
		sid[0:4], sid[4:6], sid[6:8], sid[8:10], sid[10:16], gno)
}

// DDLKind is the source-agnostic DDL-kind, moved to internal/event (#528).
// Aliased here so the parser and existing callers keep using parser.DDLKind.
type DDLKind = event.DDLKind

const (
	DDLAlterTable    = event.DDLAlterTable
	DDLCreateTable   = event.DDLCreateTable
	DDLDropTable     = event.DDLDropTable
	DDLRenameTable   = event.DDLRenameTable
	DDLTruncateTable = event.DDLTruncateTable
)

// ddlTableRe extracts the schema and table name from DDL statements.
// Handles: ALTER TABLE [schema.]table, CREATE TABLE [IF NOT EXISTS] [schema.]table,
// DROP TABLE [IF EXISTS] [schema.]table, RENAME TABLE [schema.]table,
// TRUNCATE [TABLE] [schema.]table.
// Backtick-quoted identifiers are supported via `([^`]+)`.
var ddlTableRe = regexp.MustCompile(
	`(?i)(?:ALTER\s+TABLE|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|DROP\s+TABLE(?:\s+IF\s+EXISTS)?|RENAME\s+TABLE|TRUNCATE(?:\s+TABLE)?)\s+` +
		"(?:`([^`]+)`\\.`([^`]+)`|`([^`]+)`|(\\w+)\\.(\\w+)|(\\w+))")

// dmlKeywords are the statement prefixes that, under binlog_format=ROW, would
// have been logged as ROW events (and captured). Seeing one as a QUERY_EVENT
// means the source logged this DML in STATEMENT form (binlog_format=STATEMENT
// or MIXED, or a session-level override) — the row image is NOT in the binlog
// and the change cannot be captured. TRUNCATE is deliberately NOT here: it is
// always statement-logged and never produces row events under any binlog_format,
// so it is not a "row-DML that ROW format would have captured" and must never
// trip this loss detector (a false increment of statement_dml_dropped reads as
// data loss when nothing was lost). parseDDL claims TRUNCATE as DDL in the
// no-comment case; a comment-prefixed TRUNCATE that slips past parseDDL must
// still fall through here silently — again, nothing was lost.
var dmlKeywords = []string{"INSERT", "UPDATE", "DELETE", "REPLACE", "LOAD DATA"}

// statementDML reports whether queryStr is a data-modifying statement logged in
// STATEMENT form — the DML that binlog_format=ROW would have captured as row
// events. It trims leading whitespace and SQL comments, then matches a DML
// keyword prefix on a word boundary. Transaction-control (BEGIN/COMMIT/ROLLBACK/
// SAVEPOINT/XA/SET) and every DDL/DCL verb are excluded by construction: only
// the dmlKeywords allowlist matches, so a keyword prefix on a non-DML statement
// cannot false-positive. Returns the matched keyword (for the warning) and true,
// or "" and false. Callers MUST give DDL (parseDDL) the chance to claim the
// statement first. TRUNCATE is excluded (see dmlKeywords) — it never produces
// row events, so it must return false here even when parseDDL misses it.
func statementDML(queryStr string) (string, bool) {
	upper := strings.ToUpper(stripLeadingSQLComments(queryStr))
	for _, kw := range dmlKeywords {
		if !strings.HasPrefix(upper, kw) {
			continue
		}
		// Word boundary: the keyword must be the whole statement or be followed
		// by whitespace, so INSERT matches "INSERT INTO ..." but not an
		// identifier that merely starts with those letters.
		if rest := upper[len(kw):]; rest == "" || isSQLSpace(rest[0]) {
			return kw, true
		}
	}
	return "", false
}

// stripLeadingSQLComments removes leading whitespace and SQL comments so the
// DML-prefix check sees the first real keyword. Handles the three MySQL comment
// forms (/* */ block, -- to EOL, # to EOL); loops because a statement can carry
// several. The common case (no leading comment) returns after one TrimLeft.
func stripLeadingSQLComments(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n\f\v")
		switch {
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s[2:], "*/")
			if end < 0 {
				return "" // unterminated — no real statement follows
			}
			s = s[2+end+2:]
		case strings.HasPrefix(s, "--"):
			// MySQL treats -- as a comment only when followed by whitespace/EOL.
			if len(s) > 2 && !isSQLSpace(s[2]) {
				return s
			}
			nl := strings.IndexByte(s, '\n')
			if nl < 0 {
				return ""
			}
			s = s[nl+1:]
		case strings.HasPrefix(s, "#"):
			nl := strings.IndexByte(s, '\n')
			if nl < 0 {
				return ""
			}
			s = s[nl+1:]
		default:
			return s
		}
	}
}

func isSQLSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n' || b == '\f' || b == '\v'
}

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
