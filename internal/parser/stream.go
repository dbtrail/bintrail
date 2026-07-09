// Package parser — this file provides StreamParser, which reads events from a
// BinlogStreamer (network replication) and emits them on the same Event channel
// as Parser (file-based). Both use the shared handleRows function internally.
package parser

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
)

// StreamParser reads events from a live BinlogStreamer and sends parsed row
// events to an output channel, mirroring the interface of Parser but without
// requiring binlog files on disk.
type StreamParser struct {
	resolver      atomic.Pointer[metadata.Resolver]
	filters       Filters
	logger        *slog.Logger
	schemaVersion atomic.Uint32 // actual snapshot_id from schema_snapshots; updated by SwapResolver
	// onDDL, when set, runs SYNCHRONOUSLY inside Run for a DDL event, BEFORE
	// that event is emitted and before ANY subsequent binlog event is
	// decoded. See SetSyncDDLHook for why this must not move off the parse
	// path, and why the ordering (hook, then emit) is load-bearing for #760.
	onDDL atomic.Pointer[func(Event) error]
}

// NewStreamParser creates a StreamParser that resolves column names via
// resolver and applies the given filters.
// logger may be nil, in which case slog.Default() is used.
func NewStreamParser(resolver *metadata.Resolver, filters Filters, logger *slog.Logger) *StreamParser {
	if logger == nil {
		logger = slog.Default()
	}
	sp := &StreamParser{filters: filters, logger: logger}
	if resolver != nil {
		sp.schemaVersion.Store(uint32(resolver.SnapshotID()))
		sp.resolver.Store(resolver)
	}
	return sp
}

// SwapResolver atomically replaces the resolver used for column resolution
// and updates schemaVersion to the new resolver's SnapshotID.
// Safe to call concurrently while Run is executing in another goroutine.
func (sp *StreamParser) SwapResolver(r *metadata.Resolver) {
	sp.schemaVersion.Store(uint32(r.SnapshotID()))
	sp.resolver.Store(r)
}

// SetSyncDDLHook registers fn to run synchronously inside Run, for each DDL
// event, BEFORE that event is emitted on out and BEFORE any subsequent
// binlog event is decoded.
//
// This is the ONLY correct place for the auto-snapshot-on-DDL work (#396):
// the binlog is sequential — `CREATE TABLE t; INSERT INTO t;` puts the row
// events immediately after the DDL — but the parser goroutine runs ahead of
// the consumer through the events channel. A consumer-side handler swaps the
// resolver only after the parser has already decoded (and, for an unknown
// table, silently skipped) the rows that followed the DDL. Blocking the parse
// loop until fn returns closes that window: fn typically takes a fresh schema
// snapshot and calls SwapResolver, so the very next row event decodes with
// the post-DDL schema.
//
// fn returning a non-nil error aborts Run immediately, WITHOUT ever emitting
// the DDL event (#760): if the post-DDL snapshot/resolver refresh failed, the
// resolver is stale and every row event that follows this DDL in the binlog
// would otherwise be silently skipped as "column count mismatch" / "table
// not in snapshot" while the stream checkpoint keeps advancing past them — a
// permanent, unmarked loss. The checkpoint (in this package's consumers)
// advances off events it actually receives, including the DDL event itself;
// withholding that event on hook failure means the checkpoint stays exactly
// where it was BEFORE this DDL. A supervisor restart then resumes from
// there, re-reads the same DDL off the binlog, re-runs this hook (retrying
// the snapshot), and only then decodes the rows that follow — with a fresh
// resolver. Emitting the DDL event first and running fn after would let the
// checkpoint advance past the DDL before a failure is even known, defeating
// the retry.
//
// While fn runs, no new events are produced (the replication connection
// buffers server-side); consumers keep draining already-emitted events.
// Safe to call concurrently with Run.
func (sp *StreamParser) SetSyncDDLHook(fn func(Event) error) {
	if fn == nil {
		sp.onDDL.Store(nil)
		return
	}
	sp.onDDL.Store(&fn)
}

// Run reads events from the streamer and sends matching row events to out.
// It tracks the current binlog filename (from RotateEvent) and GTID (from
// GTIDEvent), and uses them to populate each emitted Event.
//
// Returns nil when the context is cancelled (graceful shutdown) or when the
// streamer is closed. Returns a non-nil error on network or decode failure.
func (sp *StreamParser) Run(ctx context.Context, streamer *replication.BinlogStreamer, out chan<- Event) error {
	var currentFile string
	var currentGTID string
	var currentConnectionID uint32 // pseudo_thread_id from most recent QueryEvent
	// currentQueryText holds the original SQL statement from the most recent
	// ROWS_QUERY_EVENT (MySQL, binlog_rows_query_log_events=ON) or
	// ANNOTATE_ROWS event (MariaDB, binlog_annotate_row_events=ON; the syncer
	// must request it via BINLOG_SEND_ANNOTATE_ROWS_EVENT). Statement-scoped —
	// see the file parser's currentQueryText for the full contract.
	var currentQueryText string

	// emitCommit signals a transaction commit boundary so the stream consumer can
	// advance the durable GTID checkpoint only after the transaction's rows have
	// been received (#491). It commits the in-flight transaction (currentGTID) and
	// clears it, so the next-GTID fallback below won't re-commit the same GTID.
	// No-op for non-GTID sources (currentGTID empty); a GTID-enabled source running
	// in position mode still emits these, harmlessly — the consumer ignores commit
	// events when accGTID is nil, and binlogPos lands on the XID boundary.
	emitCommit := func(hdr *replication.EventHeader) error {
		if currentGTID == "" {
			return nil
		}
		commitEv := Event{
			BinlogFile: currentFile,
			EndPos:     uint64(hdr.LogPos),
			Timestamp:  time.Unix(int64(hdr.Timestamp), 0).UTC(),
			GTID:       currentGTID,
			EventType:  EventCommit,
		}
		select {
		case out <- commitEv:
			currentGTID = "" // committed; the next-GTID fallback must not re-commit it
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// emitGTIDTracking emits an EventGTID for the in-flight currentGTID so the
	// stream consumer accumulates it even when no rows follow (#124). No-op when
	// currentGTID is empty (non-GTID source). Shared by the MySQL GTIDEvent and
	// MariaDB MariadbGTIDEvent cases so both flavors track identically.
	emitGTIDTracking := func(hdr *replication.EventHeader) error {
		if currentGTID == "" {
			return nil
		}
		gtidEv := Event{
			BinlogFile: currentFile,
			EndPos:     uint64(hdr.LogPos),
			Timestamp:  time.Unix(int64(hdr.Timestamp), 0).UTC(),
			GTID:       currentGTID,
			EventType:  EventGTID,
		}
		select {
		case out <- gtidEv:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// handleEvent processes one binlog event. It is recursive: with
	// binlog_transaction_compression=ON the source wraps each transaction's
	// events (BEGIN + TABLE_MAP + rows + XID) in a single zstd-compressed
	// Transaction_payload event — delivered as-is over the replication
	// protocol — which go-mysql hands over pre-decoded in ev.Events.
	// Returns ctx.Err() on cancellation; the Run loop translates that into
	// a graceful nil.
	var handleEvent func(binlogEv *replication.BinlogEvent) error
	handleEvent = func(binlogEv *replication.BinlogEvent) error {
		switch ev := binlogEv.Event.(type) {
		case *replication.RotateEvent:
			currentFile = string(ev.NextLogName)

		case *replication.GTIDEvent:
			// A new transaction is starting, so the previous one has terminated. If
			// it wasn't already committed by its XID or a table DDL, commit it now.
			// This is the catch-all for transactions that carry a GTID but emit no
			// XID and aren't table DDL — two families:
			//   * implicitly-committed DDL/DCL: GRANT/REVOKE, CREATE/DROP DATABASE,
			//     CREATE/DROP VIEW/TRIGGER/PROCEDURE/FUNCTION, CREATE/DROP INDEX,
			//     ANALYZE/OPTIMIZE TABLE;
			//   * explicit terminators logged as a QUERY with no XID: XA COMMIT, or
			//     a COMMIT of a non-transactional/mixed-engine transaction.
			// (A normal InnoDB COMMIT does NOT reach here — it ends in an XID_EVENT.)
			// Without this their GTID would never advance the checkpoint, causing
			// endless re-streaming and eventually a false data-loss gap alarm (#491).
			//
			// Limitation: the fallback fires on the NEXT GTID, so the LAST such
			// statement before an idle period or shutdown stays uncommitted until
			// traffic resumes — it is re-streamed on restart (harmless: these carry
			// no rows). DML is unaffected (it commits immediately at its XID).
			if err := emitCommit(binlogEv.Header); err != nil {
				return err
			}
			currentGTID = formatGTID(binlogEv.Header.EventType, ev.SID, ev.GNO)
			currentQueryText = "" // transaction boundary — statement text never crosses it
			if err := emitGTIDTracking(binlogEv.Header); err != nil {
				return err
			}

		case *replication.MariadbGTIDEvent:
			// MariaDB analogue of GTIDEvent: a MariaDB source emits a
			// MariadbGTIDEvent (domain-server-seq) instead of a GTIDEvent. The
			// commit-boundary and tracking-emit logic is identical — see the
			// GTIDEvent case above for the #491 next-GTID-fallback rationale.
			// ev.GTID.String() yields "0-1-100" and "" for the zero GTID, so the
			// emit guard inside emitGTIDTracking is built in.
			if err := emitCommit(binlogEv.Header); err != nil {
				return err
			}
			currentGTID = ev.GTID.String()
			currentQueryText = ""
			if err := emitGTIDTracking(binlogEv.Header); err != nil {
				return err
			}

		case *replication.QueryEvent:
			currentConnectionID = ev.SlaveProxyID
			// New statement scope (BEGIN of the next transaction, or a DDL) —
			// the previous statement's ROWS_QUERY text is stale.
			currentQueryText = ""
			ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if ddlEv, ok := parseDDL(sp.logger, currentFile, binlogEv.Header.LogPos, ts, currentGTID, string(ev.Query), sp.schemaVersion.Load()); ok {
				// Synchronous DDL hook runs BEFORE ddlEv is emitted (#760,
				// reordered from emit-then-hook). The consumer (streamLoop)
				// advances the durable checkpoint's binlogPos/GTID off events
				// it receives, including EventDDL itself — so if ddlEv were
				// emitted first and the hook then failed, the checkpoint
				// would already have moved past this DDL by the time Run
				// aborts. A restart would resume AFTER the DDL, never
				// re-read the QUERY_EVENT that carries it, and never re-fire
				// this hook — leaving the resolver stale forever while rows
				// keep getting silently skipped as "column count mismatch".
				// Running the hook first and returning its error WITHOUT
				// ever sending ddlEv means a failure leaves the checkpoint
				// exactly where it was before this DDL; a restart re-reads
				// the DDL from the binlog and retries the snapshot.
				if hook := sp.onDDL.Load(); hook != nil {
					if err := (*hook)(ddlEv); err != nil {
						return fmt.Errorf("sync DDL hook: %w", err)
					}
				}
				select {
				case out <- ddlEv:
				case <-ctx.Done():
					return ctx.Err()
				}
				// Table DDL auto-commits its own GTID; EventDDL is the commit
				// boundary the consumer acts on, so clear the in-flight GTID to keep
				// the next-GTID fallback from re-committing it. Other QueryEvents
				// (BEGIN, SAVEPOINT, ...) deliberately do NOT commit here — DML
				// commits at its XID below, and other implicitly-committed statements
				// commit via the next-GTID fallback (#491).
				currentGTID = ""
			} else if kw, isDML := statementDML(string(ev.Query)); isDML {
				// STATEMENT/MIXED-format DML (or a session flip off ROW): the row
				// image is not in the binlog, so this change cannot be captured.
				// Fail LOUD + metric, symmetric to the partial-image guard (#493).
				// NOTE: never log the statement text — a DML statement embeds row
				// VALUES; keyword + file/pos + connection_id locate it without
				// leaking data into operator logs.
				sp.logger.Warn("statement-format DML in binlog — event NOT captured (bintrail requires binlog_format=ROW; a STATEMENT/MIXED format or a session-level override produced this)",
					"file", currentFile,
					"pos", binlogEv.Header.LogPos,
					"statement_type", kw,
					"connection_id", ev.SlaveProxyID)
				observe.StatementDMLDropped()
			}

		case *replication.XIDEvent:
			// InnoDB transaction commit — the boundary at which it's safe to
			// advance the durable GTID checkpoint (#491).
			currentQueryText = ""
			if err := emitCommit(binlogEv.Header); err != nil {
				return err
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
			// (binlog_annotate_row_events=ON on the source; only sent when the
			// syncer requested BINLOG_SEND_ANNOTATE_ROWS_EVENT).
			currentQueryText = event.SanitizeQueryText(string(ev.Query))

		case *replication.RowsEvent:
			err := handleRows(ctx, sp.logger, sp.resolver.Load(), &sp.filters, binlogEv, ev, currentFile, currentGTID, currentConnectionID, currentQueryText, sp.schemaVersion.Load(), out)
			// Statement boundary — see the file parser's RowsEvent case: the
			// STMT_END_F clear prevents a ROWS_QUERY-less later statement in
			// the same transaction from inheriting this statement's text.
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

	for {
		binlogEv, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled — graceful shutdown
			}
			return err
		}

		if err := handleEvent(binlogEv); err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled during event processing
			}
			return err
		}
	}
}
