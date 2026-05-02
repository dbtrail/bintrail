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

	mu sync.Mutex
	db string // currently selected database (per COM_INIT_DB)
}

// Config tunes the shim's data-fetch behaviour. Zero values are valid:
// the handler then queries only the live MySQL index (the same shape
// the original MVP shipped with).
type Config struct {
	// AllowGaps mirrors query.FetchMergedOptions.AllowGaps. The shim
	// defaults to true so coverage gaps surface as slog.Warn rather
	// than aborting the customer's query — matches the warn-and-continue
	// behaviour of bintrail recover.
	AllowGaps bool
	// NoArchive disables archive auto-discovery + the archive fetch
	// loop, even if archive_state has rows. Defaults to false.
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
	return &Handler{indexDB: indexDB, cfg: cfg, logger: logger}
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

	engine := query.New(h.indexDB)
	rows, _, err := query.FetchMerged(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:   q.Schema,
			Table:    q.Table,
			PKValues: q.PKValue,
			Until:    &q.AsOf,
			Limit:    1,
		},
		DBName:    q.Schema,
		NoArchive: h.cfg.NoArchive,
		AllowGaps: h.cfg.AllowGaps,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", q.Type, err)
	}

	if len(rows) == 0 {
		return emptyResult(), nil
	}

	// Pick the most recent event by (timestamp, event_id).
	sort.SliceStable(rows, func(i, j int) bool {
		if !rows[i].EventTimestamp.Equal(rows[j].EventTimestamp) {
			return rows[i].EventTimestamp.After(rows[j].EventTimestamp)
		}
		return rows[i].EventID > rows[j].EventID
	})
	latest := rows[0]

	var image map[string]any
	switch {
	case len(latest.RowAfter) > 0:
		image = latest.RowAfter
	case len(latest.RowBefore) > 0:
		image = latest.RowBefore
	default:
		return emptyResult(), nil
	}

	return imageToResult(image)
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

	engine := query.New(h.indexDB)
	rows, _, err := query.FetchMerged(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:   q.Schema,
			Table:    q.Table,
			PKValues: q.PKValue,
			Since:    &q.Since,
			Until:    &q.Until,
			Limit:    diffMaxRows,
		},
		DBName:    q.Schema,
		NoArchive: h.cfg.NoArchive,
		AllowGaps: h.cfg.AllowGaps,
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

// diffMaxRows caps a single _diff response. 1000 events is enough
// for any realistic per-PK history within a customer-facing window;
// a hot row that exceeded this would still be queryable via repeated
// narrower-range calls.
const diffMaxRows = 1000

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

// isHandshakeNoise matches the handful of statements MySQL clients
// issue automatically and that have no meaningful behaviour for a
// shim. Returning success keeps the connection alive without us having
// to implement them.
func isHandshakeNoise(q string) bool {
	q = strings.TrimSpace(strings.ToLower(q))
	q = strings.TrimSuffix(q, ";")
	switch {
	case strings.HasPrefix(q, "set "):
		return true
	case strings.HasPrefix(q, "select @@"):
		return true
	case strings.HasPrefix(q, "show warnings"):
		return true
	case q == "select version()" || q == "select database()":
		return true
	}
	return false
}

