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
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// resolverCacheTTL bounds how long a stale schema_snapshots view
// can serve column-ordering lookups. 30s is short enough that a
// fresh `bintrail snapshot` is visible within ops-monitoring time
// (the typical "I just ran snapshot, why don't I see my new
// column?" reaction window) and long enough to absorb any
// reasonable shim QPS without re-loading the entire snapshot per
// query — the previous per-query reload measurably loaded
// information_schema-style data on every customer query.
const resolverCacheTTL = 30 * time.Second

// Handler implements server.Handler. It serves the small subset of
// MySQL protocol the time-travel SQL story needs: USE <db>,
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
	// resolverFn loads a metadata.Resolver from the latest
	// schema_snapshots row. Production wires this to
	// metadata.NewResolver(indexDB, 0), which issues a MAX(snapshot_id)
	// lookup plus a full per-snapshot row scan that materialises every
	// table's column metadata into memory — non-trivial under load.
	// Tests inject a fake to exercise the column-ordering paths without
	// an indexDB.
	//
	// Wrapped by resolverCache below so successive queries share one
	// load for up to resolverCacheTTL; a fresh `bintrail snapshot`
	// becomes visible at the next cache miss without explicit
	// invalidation. Use resolver() (not resolverFn directly) from
	// production code so the cache + sticky-fallback policy applies.
	resolverFn    func() (*metadata.Resolver, error)
	resolverCache resolverCache

	mu sync.Mutex
	db string // currently selected database (per COM_INIT_DB)
}

// resolverCache memoises the latest metadata.Resolver across shim
// queries. The zero value is ready to use.
//
// Caching policy:
//   - Hit-within-TTL → return cached resolver, no loader call.
//   - Miss-or-expired → run loader OUTSIDE the mutex (so a slow
//     index DB does not serialise concurrent shim queries),
//     then re-acquire to publish.
//   - Loader fails AND a prior resolver is cached → sticky
//     fallback: return the stale resolver rather than the error
//     so transient index-DB blips don't oscillate wire-protocol
//     column order between DDL and alphabetical across consecutive
//     customer queries. Logged at Warn (rate-limited to once per
//     TTL window) so a *persistent* outage is still operator-
//     visible — without rate limiting a hot shim would spam the
//     log; without warning at all the outage is invisible
//     because the wire response still looks healthy.
//   - Loader fails AND no prior resolver → surface the error so
//     columnOrderFor can apply its sentinel-vs-real-error split.
//
// We do NOT extend the timestamp on a sticky-fallback hit: the
// next query still tries to refresh, so a recovered DB picks up
// the new snapshot at the next attempt rather than waiting
// another full TTL.
//
// Thundering-herd note: N concurrent cache misses do N redundant
// loads (instead of singleflight collapsing to 1+N-1 waits). The
// trade-off is intentional — TTL bounds miss frequency to once
// per 30s and shim QPS is interactive (customer-driven), so the
// extra load cost is bounded; in exchange we avoid serialising
// every query behind one slow loader. Add singleflight if
// profiling shows the redundant loads matter.
type resolverCache struct {
	mu           sync.Mutex
	loaded       *metadata.Resolver
	loadedAt     time.Time
	lastWarnedAt time.Time // sticky-fallback Warn rate-limiter
}

// get returns the cached Resolver when fresh, otherwise invokes
// load (outside the mutex). On load error: returns the stale
// Resolver if cached + emits a rate-limited Warn; or surfaces the
// error when no prior resolver exists.
//
// `now` is injected for deterministic tests; production passes
// time.Now. logger receives the sticky-fallback Warn — pass
// slog.Default() if you don't have a per-handler logger.
func (c *resolverCache) get(
	now func() time.Time,
	ttl time.Duration,
	load func() (*metadata.Resolver, error),
	logger *slog.Logger,
) (*metadata.Resolver, error) {
	// Snapshot under lock so the publish below races with us only
	// to its own benefit (we'd see the fresher resolver on relock).
	c.mu.Lock()
	cached := c.loaded
	cachedAt := c.loadedAt
	c.mu.Unlock()

	if cached != nil && now().Sub(cachedAt) < ttl {
		return cached, nil
	}

	r, loadErr := load()

	c.mu.Lock()
	defer c.mu.Unlock()

	if loadErr != nil {
		// Distinguish three sub-cases on the relock:
		//   (a) another goroutine refreshed during our load → use
		//       the fresh resolver, no warn (it's not actually
		//       stale).
		//   (b) cache was empty when we started AND another
		//       goroutine populated it → same as (a).
		//   (c) nothing changed → genuine sticky fallback. Warn
		//       rate-limited so the operator sees a persistent
		//       outage but the log isn't spammed at shim QPS.
		if c.loaded != nil && !c.loadedAt.Equal(cachedAt) {
			return c.loaded, nil // (a) or (b)
		}
		if c.loaded != nil {
			if now().Sub(c.lastWarnedAt) >= ttl {
				logger.Warn(
					"shim: resolver refresh failed; serving stale snapshot",
					"err", loadErr,
					"stale_age", now().Sub(c.loadedAt).Round(time.Second),
				)
				c.lastWarnedAt = now()
			}
			return c.loaded, nil
		}
		return nil, loadErr
	}

	c.loaded = r
	c.loadedAt = now()
	c.lastWarnedAt = time.Time{} // recovered — reset rate-limit so next outage warns immediately
	return r, nil
}

// Config tunes the shim's data-fetch behaviour.
//
// The zero value is the production default: archives auto-discovered,
// AllowGaps=false (strict — coverage gaps and archive-fetch failures
// abort the customer's query with a wire-protocol error). Build a
// non-zero Config only to flip NoArchive or to opt back into the
// permissive AllowGaps=true behaviour.
type Config struct {
	// AllowGaps mirrors query.FetchMergedOptions.AllowGaps. The
	// production default is false: coverage gaps and archive-fetch
	// failures abort the query with an error visible to the connected
	// MySQL client. Setting true downgrades both to slog.Warn and
	// returns whatever rows were collected — useful for operators who
	// prefer partial results over query failures during transient S3
	// hiccups, but the warning is server-side only and invisible to
	// the wire-protocol client (see #257).
	AllowGaps bool
	// NoArchive disables archive auto-discovery + the archive fetch
	// loop, even if archive_state has rows. Defaults to false (archives
	// are queried). Independent of AllowGaps.
	NoArchive bool
	// IndexDBName is the schema where binlog_events lives. The planner
	// scopes information_schema.PARTITIONS to it; the user query's
	// schema is the wrong answer (every hour misclassified as a gap).
	IndexDBName string
}

// NewHandler constructs a Handler bound to a bintrail index DSN with
// the production default config (strict: archives auto-discovered,
// gaps and archive failures abort the query).
func NewHandler(indexDB *sql.DB, logger *slog.Logger) *Handler {
	return NewHandlerWithConfig(indexDB, Config{}, logger)
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
		resolverFn:     func() (*metadata.Resolver, error) { return metadata.NewResolver(indexDB, 0) },
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
		// Parser recognised a virtual-schema query but rejected its shape
		// (or its AS OF literal, or the missing USE <db>). Wire it as
		// ER_PARSE_ERROR (1064) — the same code MySQL uses for any SQL
		// syntax error — so ORMs and monitoring can tell user input from
		// a server crash. Failures from runPointInTime / runDiff (DB
		// timeouts, FetchMerged errors, resultset-build bugs) keep
		// returning plain fmt.Errorf so go-mysql/server emits 1105 —
		// that is the inverse half of the contract and #277 explicitly
		// asks to preserve it.
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, perr.Error())
	}

	if isHandshakeNoise(qstr) {
		return &mysql.Result{Status: 2}, nil
	}

	return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, fmt.Sprintf(
		"this server only handles _flashback / _snapshot / _diff virtual-schema queries; got: %s",
		strings.TrimSpace(qstr),
	))
}

// wrapFetchError translates an error from query.FetchMerged into the
// right wire shape for HandleQuery to return. A coverage gap is a
// client-input concern (the AS OF / time range is outside what this
// index retains) and must be distinguishable from a real internal
// failure (DB timeout, archive S3 outage, build-resultset bug). MySQL
// itself uses ER_NO_PARTITION_FOR_GIVEN_VALUE (1526) for "no partition
// matches the value you queried" — semantically identical to a bintrail
// coverage gap. Anything else stays a plain Go error so go-mysql/server
// emits the catch-all ER_UNKNOWN_ERROR (1105), preserving the
// user-vs-server-fault distinction PR #282 established for issue #277.
//
// Both branches prefix qType so an operator with multiple concurrent
// shim sessions can attribute the error to a _flashback / _diff /
// _snapshot query without correlating logs.
func wrapFetchError(qType QueryType, err error) error {
	var gapErr *query.GapError
	if errors.As(err, &gapErr) {
		return mysql.NewError(mysql.ER_NO_PARTITION_FOR_GIVEN_VALUE,
			fmt.Sprintf("resolve %s: %s", qType, gapErr.Error()))
	}
	return fmt.Errorf("resolve %s: %w", qType, err)
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
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	})
	if err != nil {
		return nil, wrapFetchError(q.Type, err)
	}

	image := selectImage(rows)
	if image == nil {
		return emptyResult(), nil
	}
	return imageToResult(image, h.columnOrderFor(q.Schema, q.Table))
}

// columnOrderFor returns the column names of schema.table in DDL
// (ordinal_position) order so the wire-protocol resultset matches
// what a regular MySQL `SELECT *` would emit. Returns nil when no
// snapshot is available or the table is missing from the latest
// snapshot — the caller falls back to alphabetical ordering of the
// JSON image keys.
//
// Logging policy is split deliberately so operators can tell
// "first-install with no snapshot yet" apart from real DB-side
// failure:
//
//   - metadata.ErrNoSnapshots → Debug. Benign first-install state;
//     the operator just hasn't run `bintrail snapshot` yet.
//   - any other resolver-load error → Warn. Index DB is unreachable
//     or schema_snapshots is unreadable — a real config/infra
//     problem the operator should see at default --log-level info.
//   - table not in snapshot → Debug. Common for tables created
//     after the latest snapshot was taken; benign and self-fixing
//     once a fresh snapshot runs.
//
// A janky-but-deterministic fallback is strictly better than a
// hard failure on what is otherwise a working query — but the
// fallback should be loud when it's hiding a real outage.
func (h *Handler) columnOrderFor(schema, table string) []string {
	if h.resolverFn == nil {
		return nil
	}
	r, err := h.resolverCache.get(time.Now, resolverCacheTTL, h.resolverFn, h.logger)
	if err != nil {
		if errors.Is(err, metadata.ErrNoSnapshots) {
			h.logger.Debug("shim: no snapshots yet; falling back to alphabetical column order",
				"schema", schema, "table", table)
		} else {
			h.logger.Warn("shim: schema_snapshots lookup failed; falling back to alphabetical column order",
				"err", err, "schema", schema, "table", table)
		}
		return nil
	}
	tm, err := r.Resolve(schema, table)
	if err != nil {
		h.logger.Debug("shim: table not in latest snapshot; falling back to alphabetical column order",
			"err", err, "schema", schema, "table", table)
		return nil
	}
	cols := make([]string, 0, len(tm.Columns))
	for _, c := range tm.Columns {
		cols = append(cols, c.Name)
	}
	return cols
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
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	})
	if err != nil {
		return nil, wrapFetchError(q.Type, err)
	}

	// Compute the source-table column order once per query so the
	// per-row JSON images encode keys in DDL order (matching what
	// _flashback / _snapshot return for SELECT *). Without this the
	// row_before / row_after JSON keys would alphabetise — surprising
	// when a customer compares _diff output side by side with the
	// reconstructed flashback row.
	ddlOrder := h.columnOrderFor(q.Schema, q.Table)
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
			marshalImageOrdered(r.RowBefore, ddlOrder),
			marshalImageOrdered(r.RowAfter, ddlOrder),
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

// marshalImageOrdered renders a row image as a JSON string for the
// _diff resultset, emitting keys in ddlOrder so the JSON column
// order matches what _flashback / _snapshot return. nil maps render
// as the empty string so customers can distinguish "no image"
// (INSERT lacks row_before, DELETE lacks row_after) from "empty
// image".
//
// ddlOrder=nil falls back to encoding/json's default
// alphabetical-key marshalling — same degraded path as
// imageToResult when no snapshot is available.
//
// Built by hand rather than via json.Marshal(map) because the stdlib
// encoder sorts map keys alphabetically with no override hook. The
// per-key json.Marshal calls reuse the stdlib encoder for the
// quoted key and the value, so string escaping (quotes, control
// chars, non-printable bytes) stays correct without a custom
// escaper here.
//
// Failure modes (all return ""):
//   - nil image (the documented "no image" sentinel).
//   - any inner json.Marshal error — e.g. a value of type chan,
//     func, NaN/Inf float, or a custom type whose MarshalJSON
//     returns an error. None of these can appear in a row image
//     decoded from MySQL JSON columns (parser rejects them on
//     INSERT, json.Unmarshal rejects them on read), so the failure
//     path is theoretical for production data; if it ever fires
//     the customer sees a missing row image rather than a partial
//     one, matching the original marshalImage behaviour.
func marshalImageOrdered(image map[string]any, ddlOrder []string) string {
	if image == nil {
		return ""
	}
	if len(ddlOrder) == 0 {
		b, err := json.Marshal(image)
		if err != nil {
			return ""
		}
		return string(b)
	}
	cols := orderColumns(image, ddlOrder)
	var sb strings.Builder
	sb.WriteByte('{')
	for i, c := range cols {
		if i > 0 {
			sb.WriteByte(',')
		}
		keyJSON, err := json.Marshal(c)
		if err != nil {
			return ""
		}
		sb.Write(keyJSON)
		sb.WriteByte(':')
		valJSON, err := json.Marshal(image[c])
		if err != nil {
			return ""
		}
		sb.Write(valJSON)
	}
	sb.WriteByte('}')
	return sb.String()
}

// imageToResult turns a single-row JSON object into a mysql.Result
// shaped for the wire protocol. Columns are emitted in ddlOrder
// (the source table's column ordinal_position from the latest
// schema_snapshots row) so a customer running
// `SELECT * FROM _flashback.orders ...` gets the same column
// ordering they'd get from a regular `SELECT * FROM orders` — no
// surprising reshuffling between the two.
//
// ddlOrder=nil signals "no snapshot available"; in that case we
// fall back to alphabetical key order, which is deterministic but
// won't match the table's natural DDL order.
func imageToResult(image map[string]any, ddlOrder []string) (*mysql.Result, error) {
	if len(image) == 0 {
		return emptyResult(), nil
	}

	cols := orderColumns(image, ddlOrder)
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

// orderColumns returns the column emission order for a row image:
//
//  1. Columns from ddlOrder that are present in image, in
//     ddlOrder sequence — this is the canonical case.
//  2. Then any columns present in image but not in ddlOrder, sorted
//     alphabetically. Catches the edge case where the binlog event
//     pre-dates a recent ALTER TABLE captured by the next snapshot,
//     so the image carries a column the snapshot doesn't know about
//     (or vice versa). Better to surface it at the end than to drop
//     it silently.
//
// Pure function — extracted from imageToResult specifically so the
// ordering rules can be unit-tested without spinning up MySQL.
func orderColumns(image map[string]any, ddlOrder []string) []string {
	if len(ddlOrder) == 0 {
		cols := make([]string, 0, len(image))
		for k := range image {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		return cols
	}

	cols := make([]string, 0, len(image))
	seen := make(map[string]bool, len(image))
	for _, c := range ddlOrder {
		if _, ok := image[c]; ok {
			cols = append(cols, c)
			seen[c] = true
		}
	}
	var extras []string
	for k := range image {
		if !seen[k] {
			extras = append(extras, k)
		}
	}
	sort.Strings(extras)
	return append(cols, extras...)
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

