// Package parser — this file provides StreamParser, which reads events from a
// BinlogStreamer (network replication) and emits them on the same Event channel
// as Parser (file-based). Both use the shared handleRows function internally.
package parser

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// StreamParser reads events from a live BinlogStreamer and sends parsed row
// events to an output channel, mirroring the interface of Parser but without
// requiring binlog files on disk.
type StreamParser struct {
	resolver      atomic.Pointer[metadata.Resolver]
	filters       Filters
	logger        *slog.Logger
	schemaVersion atomic.Uint32 // actual snapshot_id from schema_snapshots; updated by SwapResolver
	// onDDL, when set, runs SYNCHRONOUSLY inside Run after a DDL event is
	// emitted and before ANY subsequent binlog event is decoded. See
	// SetSyncDDLHook for why this must not move off the parse path.
	onDDL atomic.Pointer[func(Event)]
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

// SetSyncDDLHook registers fn to run synchronously inside Run, after each DDL
// event is emitted and BEFORE any subsequent binlog event is decoded.
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
// While fn runs, no new events are produced (the replication connection
// buffers server-side); consumers keep draining already-emitted events.
// Safe to call concurrently with Run.
func (sp *StreamParser) SetSyncDDLHook(fn func(Event)) {
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
			currentGTID = formatGTID(ev.SID, ev.GNO)
			if currentGTID != "" {
				ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
				gtidEv := Event{
					BinlogFile: currentFile,
					EndPos:     uint64(binlogEv.Header.LogPos),
					Timestamp:  ts,
					GTID:       currentGTID,
					EventType:  EventGTID,
				}
				select {
				case out <- gtidEv:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

		case *replication.QueryEvent:
			currentConnectionID = ev.SlaveProxyID
			ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if ddlEv, ok := parseDDL(sp.logger, currentFile, binlogEv.Header.LogPos, ts, currentGTID, string(ev.Query), sp.schemaVersion.Load()); ok {
				select {
				case out <- ddlEv:
				case <-ctx.Done():
					return ctx.Err()
				}
				// Synchronous DDL hook: the resolver refresh must complete
				// before the next event is processed, or the rows that follow a
				// CREATE/ALTER in the binlog are skipped as unknown (#396).
				if hook := sp.onDDL.Load(); hook != nil {
					(*hook)(ddlEv)
				}
				// Table DDL auto-commits its own GTID; EventDDL is the commit
				// boundary the consumer acts on, so clear the in-flight GTID to keep
				// the next-GTID fallback from re-committing it. Other QueryEvents
				// (BEGIN, SAVEPOINT, ...) deliberately do NOT commit here — DML
				// commits at its XID below, and other implicitly-committed statements
				// commit via the next-GTID fallback (#491).
				currentGTID = ""
			}

		case *replication.XIDEvent:
			// InnoDB transaction commit — the boundary at which it's safe to
			// advance the durable GTID checkpoint (#491).
			if err := emitCommit(binlogEv.Header); err != nil {
				return err
			}

		case *replication.RowsEvent:
			return handleRows(ctx, sp.logger, sp.resolver.Load(), &sp.filters, binlogEv, ev, currentFile, currentGTID, currentConnectionID, sp.schemaVersion.Load(), out)

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
