// Package parser — this file provides StreamParser, which reads events from a
// BinlogStreamer (network replication) and emits them on the same Event channel
// as Parser (file-based). Both use the shared handleRows function internally.
package parser

import (
	"context"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/bintrail/bintrail/internal/metadata"
)

// StreamParser reads events from a live BinlogStreamer and sends parsed row
// events to an output channel, mirroring the interface of Parser but without
// requiring binlog files on disk.
type StreamParser struct {
	resolver *metadata.Resolver
	filters  Filters
	logger   *slog.Logger
}

// NewStreamParser creates a StreamParser that resolves column names via
// resolver and applies the given filters.
// logger may be nil, in which case slog.Default() is used.
func NewStreamParser(resolver *metadata.Resolver, filters Filters, logger *slog.Logger) *StreamParser {
	if logger == nil {
		logger = slog.Default()
	}
	return &StreamParser{resolver: resolver, filters: filters, logger: logger}
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
	schemaVersion := 0
	if sp.resolver != nil {
		schemaVersion = sp.resolver.SnapshotID()
	}

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

		case *replication.QueryEvent:
			if warnOnDDL(sp.logger, currentFile, binlogEv.Header.LogPos, string(ev.Query)) {
				schemaVersion++
			}

		case *replication.RowsEvent:
			if err := handleRows(ctx, sp.logger, sp.resolver, &sp.filters, binlogEv, ev, currentFile, currentGTID, schemaVersion, out); err != nil {
				if ctx.Err() != nil {
					return nil // context cancelled during row processing
				}
				return err
			}
		}
	}
}
