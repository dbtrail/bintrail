package shim

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// Handler implements server.Handler. It serves the small subset of
// MySQL protocol the BYOS time-travel SQL story needs: USE <db>,
// `SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>`,
// and a handful of bookkeeping queries the standard MySQL clients send
// during connection setup.
//
// Anything else returns a clear error to the client. The handler does
// not proxy non-flashback queries to the real MySQL — that's the job
// of ProxySQL sitting in front of the shim.
type Handler struct {
	server.EmptyHandler

	indexDB *sql.DB
	cfg     Config
	logger  *slog.Logger
	// archiveFetcher resolves S3 / local Parquet archive sources during
	// FetchMerged. Defaults to parquetquery.Fetch (the same fetcher
	// `bintrail query` and `bintrail recover` use) — exposed as a field
	// so tests can inject a fake without DuckDB or real S3.
	archiveFetcher query.ArchiveFetcher

	mu sync.Mutex
	db string // currently selected database (per COM_INIT_DB)
}

// Config tunes the shim's data-fetch behaviour.
//
// Construct via NewHandler (defaults AllowGaps=true) for the standard
// behaviour. The bare zero-value Config{AllowGaps:false, NoArchive:false}
// is valid but strict: it queries archives AND aborts the customer's
// query if the planner detects a coverage gap. Most operators want
// NewHandler's defaults — only build a custom Config if you have a
// specific reason to flip these.
type Config struct {
	// AllowGaps mirrors query.FetchMergedOptions.AllowGaps. NewHandler
	// sets this to true so coverage gaps surface as slog.Warn rather
	// than aborting the customer's query — matches the
	// warn-and-continue behaviour of bintrail recover. Setting false
	// makes a query against a time range that includes a gap return
	// an error to the client.
	AllowGaps bool
	// NoArchive disables archive auto-discovery + the archive fetch
	// loop, even if archive_state has rows. Defaults to false (archives
	// are queried). Independent of AllowGaps.
	NoArchive bool
}

// NewHandler constructs a Handler bound to a bintrail index DSN with
// default config (archives auto-discovered, gaps warned).
func NewHandler(indexDB *sql.DB, logger *slog.Logger) *Handler {
	return NewHandlerWithConfig(indexDB, Config{AllowGaps: true}, logger)
}

// NewHandlerWithConfig is the configurable form of NewHandler.
func NewHandlerWithConfig(indexDB *sql.DB, cfg Config, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		indexDB:        indexDB,
		cfg:            cfg,
		logger:         logger,
		archiveFetcher: parquetquery.Fetch,
	}
}

// UseDB stores the schema the client selected. _flashback queries
// without an explicit schema use this value.
func (h *Handler) UseDB(dbName string) error {
	h.mu.Lock()
	h.db = dbName
	h.mu.Unlock()
	return nil
}

// HandleQuery dispatches the incoming statement. We first try to
// parse it as a time-travel query (any of _flashback, _snapshot,
// _diff); if it's recognised but malformed we return that error to
// the client. If it's something else entirely we fall through to a
// small allow-list of handshake noise so MySQL clients don't choke
// on connection setup.
func (h *Handler) HandleQuery(qstr string) (*mysql.Result, error) {
	h.mu.Lock()
	currentDB := h.db
	h.mu.Unlock()

	q, perr := Parse(qstr, currentDB)
	if perr == nil {
		switch q.Type {
		case TypeFlashback, TypeSnapshot:
			return h.runPointInTime(q)
		case TypeDiff:
			return h.runDiff(q)
		default:
			return nil, fmt.Errorf("unsupported query type: %s", q.Type)
		}
	}
	if !errors.Is(perr, ErrNotTimeTravel) {
		return nil, perr
	}

	if isHandshakeNoise(qstr) {
		return &mysql.Result{Status: 2}, nil
	}

	return nil, fmt.Errorf(
		"this server only handles _flashback / _snapshot / _diff virtual-schema queries; got: %s",
		strings.TrimSpace(qstr),
	)
}

// runPointInTime resolves a _flashback or _snapshot query against
// the bintrail index + archives and reconstructs the row's state at
// q.AsOf.
//
// Semantics: returns the row_after of the most recent event for that
// PK at-or-before q.AsOf. That's the right answer for INSERT/UPDATE
// (the post-image is the row's state). For a DELETE, we fall back to
// the DELETE's row_before — the row's state captured at the moment
// of deletion. A future revision could distinguish "row didn't exist
// at AsOf" from "row was just deleted" using event_type, but the
// MVP treats both as "here's the most recent known state".
//
// _flashback and _snapshot share this implementation today. _snapshot
// is intended to grow baseline-lookup support (querying the
// dump/baseline pipeline for rows that never appeared in binlog
// events) — that's a future iteration.
func (h *Handler) runPointInTime(q TimeTravelQuery) (*mysql.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// LimitPerPK=1 (not Limit=1) is the right knob: query.Engine emits
	// the global result-set in ASC order by (event_timestamp, event_id),
	// so a plain Limit=1 would return the *earliest* event in the
	// time window, not the latest. LimitPerPK uses an inner
	// ROW_NUMBER OVER (PARTITION BY pk_values ORDER BY event_timestamp
	// DESC, event_id DESC) so the kept event for each PK is the most
	// recent one — exactly what _flashback "row state at AsOf" needs.
	engine := query.New(h.indexDB)
	rows, _, err := query.FetchMerged(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:     q.Schema,
			Table:      q.Table,
			PKValues:   q.PKValue,
			Until:      &q.AsOf,
			LimitPerPK: 1,
		},
		DBName:         q.Schema,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", q.Type, err)
	}

	image := selectImage(rows)
	if image == nil {
		return emptyResult(), nil
	}
	return imageToResult(image)
}

// selectImage picks the row image that represents the row's state at
// the queried point in time, given the LimitPerPK=1 result of a
// _flashback / _snapshot fetch.
//
// The caller passes whatever FetchMerged returned. Because we issue
// the fetch with LimitPerPK=1 and a single PKValues filter, at most
// one row reaches this helper — but selectImage tolerates an empty
// or larger slice and only ever inspects rows[0], so a future caller
// that loosens the limit cannot accidentally pick the wrong event.
//
// Priority: row_after wins when present (the post-image of an
// INSERT/UPDATE is the row's state). Fall back to row_before for
// DELETE events, where row_after is empty but row_before captures the
// row's state at the moment of deletion. Returns nil to signal the
// caller should respond with an empty resultset — either because
// there were no rows, or because both images were empty (which would
// indicate corrupted index data, treated as "no answer" rather than
// fabricating one).
//
// Extracted as a pure helper specifically so the priority rule can
// be unit-tested without spinning up a real MySQL: a future refactor
// that swaps the row_after / row_before order would silently return
// stale data on every UPDATE, with the regression invisible to any
// test that doesn't exercise this exact branch.
func selectImage(rows []query.ResultRow) map[string]any {
	if len(rows) == 0 {
		return nil
	}
	latest := rows[0]
	if len(latest.RowAfter) > 0 {
		return latest.RowAfter
	}
	if len(latest.RowBefore) > 0 {
		return latest.RowBefore
	}
	return nil
}

// runDiff resolves a _diff query: every event for the given PK
// between q.Since and q.Until, one resultset row per event.
//
// Each resultset row exposes the event metadata (event_id,
// event_timestamp, event_type, gtid) plus the row_after and
// row_before images encoded as JSON strings. Customers run this when
// they need an audit-style view of "what changed to this row in this
// time window".
func (h *Handler) runDiff(q TimeTravelQuery) (*mysql.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No row cap: a _diff query is already PK-scoped + time-windowed, so the
	// upper bound on returned events is bounded by how often that one row
	// actually changes within the window. A silent truncation here would
	// hand the customer a partial audit history with no signal — worse than
	// the rare cost of a few thousand rows for an unusually hot row.
	// Customers needing pagination can narrow the BETWEEN range.
	engine := query.New(h.indexDB)
	rows, _, err := query.FetchMerged(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:   q.Schema,
			Table:    q.Table,
			PKValues: q.PKValue,
			Since:    &q.Since,
			Until:    &q.Until,
		},
		DBName:         q.Schema,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", q.Type, err)
	}

	cols := []string{"event_id", "event_timestamp", "event_type", "gtid", "row_before", "row_after"}
	values := make([][]any, 0, len(rows))
	for _, r := range rows {
		gtid := ""
		if r.GTID != nil {
			gtid = *r.GTID
		}
		values = append(values, []any{
			r.EventID,
			r.EventTimestamp.UTC().Format("2006-01-02 15:04:05"),
			eventTypeName(r.EventType),
			gtid,
			marshalImage(r.RowBefore),
			marshalImage(r.RowAfter),
		})
	}
	rs, err := mysql.BuildSimpleTextResultset(cols, values)
	if err != nil {
		return nil, fmt.Errorf("build _diff resultset: %w", err)
	}
	return &mysql.Result{Resultset: rs}, nil
}

// eventTypeName turns parser.EventType (a uint8) into a human-readable
// string for the _diff resultset. The parser package does not export a
// String() method so this lookup lives here.
func eventTypeName(t parser.EventType) string {
	switch t {
	case parser.EventInsert:
		return "INSERT"
	case parser.EventUpdate:
		return "UPDATE"
	case parser.EventDelete:
		return "DELETE"
	}
	return fmt.Sprintf("type_%d", t)
}

// marshalImage renders a row image as a JSON string for the _diff
// resultset. nil maps render as the empty string so customers can
// distinguish "no image" (INSERT lacks row_before, DELETE lacks
// row_after) from "empty image".
func marshalImage(image map[string]any) string {
	if image == nil {
		return ""
	}
	b, err := json.Marshal(image)
	if err != nil {
		return ""
	}
	return string(b)
}

// imageToResult turns a single-row JSON object into a mysql.Result
// shaped for the wire protocol. Column order is the JSON key order
// after sorting alphabetically — deterministic, and good enough for
// the MVP. A future revision can pick the order from the schema
// snapshot to match the source table's DDL.
func imageToResult(image map[string]any) (*mysql.Result, error) {
	if len(image) == 0 {
		return emptyResult(), nil
	}

	cols := make([]string, 0, len(image))
	for k := range image {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	row := make([]any, len(cols))
	for i, c := range cols {
		row[i] = image[c]
	}

	rs, err := mysql.BuildSimpleTextResultset(cols, [][]any{row})
	if err != nil {
		return nil, fmt.Errorf("build resultset: %w", err)
	}
	return &mysql.Result{Resultset: rs}, nil
}

// emptyResult is the wire-protocol "zero rows" reply. We still need a
// resultset (so the client gets a proper SELECT response, not an OK
// packet), so we use the original column list with no rows.
func emptyResult() *mysql.Result {
	rs, _ := mysql.BuildSimpleTextResultset([]string{"_flashback"}, nil)
	return &mysql.Result{Resultset: rs}
}

// handshakePrefixes are the SET / SELECT @@ prefixes that real MySQL
// clients (mysql CLI, go-sql-driver/mysql, ProxySQL backend probes)
// fire automatically during connection setup. Allow-listing them
// keeps the handshake happy without us implementing the statements.
//
// The list is deliberately narrow: an unrecognised SET (e.g. SET
// PASSWORD, SET ROLE, SET GLOBAL) falls through to the rejection
// path so a customer / attacker cannot pretend their privileged
// statement succeeded by exploiting an over-broad `SET ` prefix.
//
// Each prefix MUST end with a delimiter (' ' or '=') so a longer
// keyword cannot smuggle itself in: e.g. `set autocommitfoo` no
// longer matches `set autocommit` because the prefix `set autocommit`
// requires a following ` ` or `=`. We list both delimiter variants
// for the SET shapes that take an argument.
var handshakePrefixes = []string{
	"set names ",
	"set autocommit ",
	"set autocommit=",
	"set session ",
	"set @@session",
	"set sql_mode ",
	"set sql_mode=",
	"set sql_select_limit ",
	"set sql_select_limit=",
	"set time_zone ",
	"set time_zone=",
	"set character_set_results ",
	"set character_set_results=",
	"select @@version",
	"select @@session.",
	"select @@global.",
	"show warnings",
	"show variables",
}

// handshakeExact is the set of full statements (no prefix matching)
// that we treat as setup noise.
var handshakeExact = map[string]struct{}{
	"select version()":  {},
	"select database()": {},
	"select user()":     {},
	"select 1":          {},
}

// isHandshakeNoise matches the handful of statements MySQL clients
// issue automatically and that have no meaningful behaviour for a
// shim. Returning success keeps the connection alive without us
// having to implement them.
func isHandshakeNoise(q string) bool {
	q = strings.TrimSpace(strings.ToLower(q))
	q = strings.TrimSuffix(q, ";")
	q = strings.TrimSpace(q)
	if _, ok := handshakeExact[q]; ok {
		return true
	}
	for _, p := range handshakePrefixes {
		if strings.HasPrefix(q, p) {
			return true
		}
	}
	return false
}

