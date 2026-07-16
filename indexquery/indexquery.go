// Package indexquery exposes read-only programmatic access to the bintrail
// index — live MySQL partitions and Parquet archives merged — for tooling and
// embedding distributions that import the core as a module.
//
// It is a thin facade over the internal query layer: type aliases to the
// internal types (usable across module boundaries through the alias) plus
// one-line wrappers over the internal entry points, so external code can run
// the exact fetch/merge/gap-detection pipeline the CLI commands use. The
// index schema stays owned by the core: EnsureSchema is the only sanctioned
// migration entry point, and this package adds no write surface beyond it.
package indexquery

import (
	"context"
	"database/sql"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Aliases to the internal query types. FetchMergedOptions is fully
// constructible through these (including its ArchiveFetcher field); GapError
// and SourceEmptyError are aliased so callers can inspect FetchMerged
// failures with errors.As.
type (
	Options            = query.Options
	ResultRow          = query.ResultRow
	Engine             = query.Engine
	QueryPlan          = query.QueryPlan
	FetchMergedOptions = query.FetchMergedOptions
	ArchiveFetcher     = query.ArchiveFetcher
	GapError           = query.GapError
	SourceEmptyError   = query.SourceEmptyError
	Tuning             = duckdbutil.Tuning
)

// EventType aliases the event-type vocabulary so external code can interpret
// ResultRow.EventType without magic numbers. The numeric values are a
// persistence contract (binlog_events.event_type) — never renumber.
type EventType = event.EventType

// The persisted event-type values a ResultRow can carry. (Capture-side
// transient types — commit markers, PG relation messages — are never written
// to binlog_events, so they cannot appear in query results.)
const (
	EventInsert   = event.EventInsert
	EventUpdate   = event.EventUpdate
	EventDelete   = event.EventDelete
	EventDDL      = event.EventDDL
	EventGTID     = event.EventGTID
	EventSnapshot = event.EventSnapshot
)

// EventTypeName returns the canonical upper-case name for a persisted event
// type ("INSERT", "UPDATE", "DELETE", "DDL", "GTID", "SNAPSHOT"), or
// "UNKNOWN" for a value outside that vocabulary.
func EventTypeName(t EventType) string {
	switch t {
	case EventInsert:
		return "INSERT"
	case EventUpdate:
		return "UPDATE"
	case EventDelete:
		return "DELETE"
	case EventDDL:
		return "DDL"
	case EventGTID:
		return "GTID"
	case EventSnapshot:
		return "SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}

// New returns a query engine over the index database.
func New(db *sql.DB) *Engine { return query.New(db) }

// FetchMerged fetches events from live MySQL partitions and Parquet archives,
// deduplicates and sorts them, and enforces coverage-gap detection according
// to o.AllowGaps. See query.FetchMerged for the full failure-mode contract.
//
// Two caveats external callers cannot discover from the signature:
//
//   - Callers running with redaction options (an RBAC profile — DenyTables /
//     RedactColumns / ProfileActive on o.Opts) must set o.NoArchive = true:
//     Parquet archives store the raw rows, so the archive branch would serve
//     unredacted columns.
//   - The returned *QueryPlan may be nil when the planner didn't run (e.g. no
//     DBName or no time range) — nil-check it before use.
func FetchMerged(ctx context.Context, db *sql.DB, engine *Engine, o FetchMergedOptions) ([]ResultRow, *QueryPlan, error) {
	return query.FetchMerged(ctx, db, engine, o)
}

// FormatGapWarning renders the planner's gap hours as the standard
// human-readable coverage warning.
func FormatGapWarning(gaps []time.Time) string { return query.FormatGapWarning(gaps) }

// Connect opens and verifies a MySQL connection with the core's connection
// invariants applied (parseTime=true, UTC, default connect timeout). The
// caller closes the returned *sql.DB.
func Connect(dsn string) (*sql.DB, error) { return config.Connect(dsn) }

// ParseSourceDSN decomposes a go-sql-driver DSN into host, port, user, and
// password. It requires a TCP address and rejects unix-socket DSNs.
func ParseSourceDSN(dsn string) (host string, port uint16, user, password string, err error) {
	return config.ParseSourceDSN(dsn)
}

// EnsureSchema brings the index schema up to date (idempotent — safe to call
// on every startup). This is the only schema-mutating entry point exposed
// here; the schema definition itself stays owned by the core.
func EnsureSchema(db *sql.DB) error { return indexer.EnsureSchema(db) }

// WrapSchemaMigrationErr rewrites an EnsureSchema failure caused by a
// read-only DSN (missing ALTER/CREATE privilege) into an actionable error.
// Read-plane callers only — capture-plane callers must keep failing hard on
// the raw error.
func WrapSchemaMigrationErr(err error) error { return indexer.WrapSchemaMigrationErr(err) }

// AddDuckDBTuningFlags registers the shared DuckDB resource flags
// (--ultrafast, --duckdb-threads, --duckdb-memory-limit) on an offline
// read command.
func AddDuckDBTuningFlags(cmd *cobra.Command) { cli.AddDuckDBTuningFlags(cmd) }

// DuckDBTuningFromFlags resolves the effective DuckDB tuning for a command
// carrying the flags registered by AddDuckDBTuningFlags. An invalid
// --duckdb-memory-limit is a hard error.
func DuckDBTuningFromFlags(cmd *cobra.Command) (Tuning, error) {
	return cli.DuckDBTuningFromFlags(cmd)
}

// TunedArchiveFetcher adapts a DuckDB Tuning into an ArchiveFetcher suitable
// for FetchMergedOptions.ArchiveFetcher.
func TunedArchiveFetcher(t Tuning) ArchiveFetcher { return cli.TunedArchiveFetcher(t) }

// OutputJSON writes v to stdout as indented JSON.
func OutputJSON(v any) error { return cliutil.OutputJSON(v) }
