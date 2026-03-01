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
	"sort"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/bintrail/bintrail/internal/metadata"
)

// ─── Event types ─────────────────────────────────────────────────────────────

// EventType represents the DML operation captured by a binlog row event.
type EventType uint8

const (
	EventInsert EventType = 1
	EventUpdate EventType = 2
	EventDelete EventType = 3
)

// Event is a fully resolved binlog row event with column names attached.
// It carries everything the indexer needs to write one row to binlog_events.
type Event struct {
	BinlogFile    string
	StartPos      uint64
	EndPos        uint64
	Timestamp     time.Time
	GTID          string // empty when GTID is not enabled on the source
	Schema        string
	Table         string
	EventType     EventType
	PKValues      string         // pipe-delimited PK values in ordinal order
	RowBefore     map[string]any // nil for INSERT
	RowAfter      map[string]any // nil for DELETE
	SchemaVersion int            // resolver.SnapshotID() at parse time; incremented on each DDL detection
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
	resolver      *metadata.Resolver
	filters       Filters
	logger        *slog.Logger
	schemaVersion int // starts at resolver.SnapshotID(); incremented on each DDL detection
}

// New creates a Parser that reads from binlogDir, resolves column names via
// resolver, and applies the given filters.
// logger may be nil, in which case slog.Default() is used.
func New(binlogDir string, resolver *metadata.Resolver, filters Filters, logger *slog.Logger) *Parser {
	if logger == nil {
		logger = slog.Default()
	}
	return &Parser{
		binlogDir:     binlogDir,
		resolver:      resolver,
		filters:       filters,
		logger:        logger,
		schemaVersion: resolver.SnapshotID(),
	}
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

	bp := replication.NewBinlogParser()

	err := bp.ParseFile(fullPath, 0, func(binlogEv *replication.BinlogEvent) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		switch ev := binlogEv.Event.(type) {
		case *replication.GTIDEvent:
			currentGTID = formatGTID(ev.SID, ev.GNO)

		case *replication.QueryEvent:
			if warnOnDDL(p.logger, filename, binlogEv.Header.LogPos, string(ev.Query)) {
				p.schemaVersion++
			}

		case *replication.RowsEvent:
			return handleRows(ctx, p.logger, p.resolver, &p.filters, binlogEv, ev, filename, currentGTID, p.schemaVersion, events)
		}
		return nil
	})

	return err
}

// ─── Row event handler ────────────────────────────────────────────────────────

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
	schemaVersion int,
	out chan<- Event,
) error {
	schema := string(rowsEv.Table.Schema)
	table := string(rowsEv.Table.Table)

	if !filters.Matches(schema, table) {
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

	// LogPos points to the byte AFTER the event. Subtract EventSize to get start.
	startPos := uint64(binlogEv.Header.LogPos) - uint64(binlogEv.Header.EventSize)
	endPos := uint64(binlogEv.Header.LogPos)
	ts := time.Unix(int64(binlogEv.Header.Timestamp), 0).UTC()
	pkCols := tm.PKColumnMetas()

	switch binlogEv.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0,
		replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2:
		return emitInserts(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, startPos, endPos, ts, pkCols, schemaVersion, out)

	case replication.DELETE_ROWS_EVENTv0,
		replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:
		return emitDeletes(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, startPos, endPos, ts, pkCols, schemaVersion, out)

	case replication.UPDATE_ROWS_EVENTv0,
		replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:
		return emitUpdates(ctx, logger, resolver, rowsEv.Rows, schema, table, filename, currentGTID, startPos, endPos, ts, pkCols, schemaVersion, out)
	}

	return nil
}

func emitInserts(
	ctx context.Context,
	logger *slog.Logger,
	resolver *metadata.Resolver,
	rows [][]any,
	schema, table, filename, gtid string,
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion int,
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
			Timestamp: ts, GTID: gtid,
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
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion int,
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
			Timestamp: ts, GTID: gtid,
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
	startPos, endPos uint64,
	ts time.Time,
	pkCols []metadata.ColumnMeta,
	schemaVersion int,
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
			Timestamp: ts, GTID: gtid,
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

// warnOnDDL logs a warning when a DDL statement is found in a QUERY_EVENT.
// DDL changes table structure and may invalidate the current schema snapshot.
// Returns true if the query is DDL, false otherwise.
func warnOnDDL(logger *slog.Logger, filename string, logPos uint32, query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	isDDL := strings.HasPrefix(upper, "ALTER TABLE") ||
		strings.HasPrefix(upper, "CREATE TABLE") ||
		strings.HasPrefix(upper, "DROP TABLE") ||
		strings.HasPrefix(upper, "TRUNCATE") ||
		strings.HasPrefix(upper, "RENAME TABLE")
	if isDDL {
		logger.Warn("DDL detected — consider re-running `bintrail snapshot`",
			"file", filename,
			"pos", logPos,
			"query", query)
	}
	return isDDL
}
