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

	"github.com/dbtrail/bintrail/internal/metadata"
)

// StreamParser reads events from a live BinlogStreamer and sends parsed row
// events to an output channel, mirroring the interface of Parser but without
// requiring binlog files on disk.
type StreamParser struct {
	resolver      atomic.Pointer[metadata.Resolver]
	filters       Filters
	logger        *slog.Logger
	schemaVersion atomic.Uint32 // actual snapshot_id from schema_snapshots; updated by SwapResolver
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

	for {
		binlogEv, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled — graceful shutdown
			}
			return err
		}

		switch ev := binlogEv.Event.(type) {
		case *replication.RotateEvent:
			currentFile = string(ev.NextLogName)

		case *replication.GTIDEvent:
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
					return nil
				}
			}

		case *replication.QueryEvent:
			currentConnectionID = ev.SlaveProxyID
			ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
			if ddlEv, ok := parseDDL(sp.logger, currentFile, binlogEv.Header.LogPos, ts, currentGTID, string(ev.Query), sp.schemaVersion.Load()); ok {
				select {
				case out <- ddlEv:
				case <-ctx.Done():
					return nil
				}
			}

		case *replication.RowsEvent:
			if err := handleRows(ctx, sp.logger, sp.resolver.Load(), &sp.filters, binlogEv, ev, currentFile, currentGTID, currentConnectionID, sp.schemaVersion.Load(), out); err != nil {
				if ctx.Err() != nil {
					return nil // context cancelled during row processing
				}
				return err
			}
		}
	}
}
