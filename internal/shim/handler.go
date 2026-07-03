package shim

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
)

// showTablesFromVirtualRE matches the interactive `SHOW [FULL] TABLES
// FROM <virtual>` form against the three virtual schemas (#315). Without
// this interception the query falls through to ProxySQL's passthrough
// rule, hits the real MySQL, and gets ER_BAD_DB (1049 "Unknown database
// '_flashback'") — the #1 first-thing-a-DBA-types friction in interactive
// time-travel sessions. The shim answers with the table list from the
// newest schema snapshot per table (see runShowTablesFromVirtual).
//
// `SHOW TABLES FROM <realdb>` does NOT match this regex (the schema
// alternation is anchored to the three virtual prefixes) so legitimate
// real-database SHOW TABLES routed to the shim by mistake still falls
// through to the default unsupported-query path.
var showTablesFromVirtualRE = regexp.MustCompile(
	"(?i)^\\s*SHOW\\s+(?:FULL\\s+)?TABLES\\s+(?:FROM|IN)\\s+`?(_flashback|_diff|_snapshot)`?\\s*;?\\s*$",
)

// defaultFullTableRowCap bounds the buffered resultset for the
// full-table AS OF path (issue #276) so a query against a hot table
// with millions of distinct PKs cannot OOM the shim. 100k rows at a
// rough estimate of ~512 bytes per JSON row image ≈ 50 MB worst-case
// (k=1 archive source — multi-archive deployments multiply the
// transient pre-merge memory by the source count, but the post-merge
// enforcement still caps the resultset). A forensic shim instance can
// absorb that; exceeding it surfaces as ER_TOO_BIG_SELECT (1104) so
// monitoring can distinguish it from a real shim crash, and operators
// narrow the AS OF range or fall back to PK-filtered queries.
//
// Streaming the resultset (Conn.WriteFieldList + WriteRow per row,
// removing the cap) is deferred per the scoping comment on #276;
// cap+buffered is the bounded substitute for the MVP.
//
// Per-Handler override lives on Config.FullTableRowCap (zero =
// inherit this default) so unit tests can lower it without mutating
// global state, and a future per-tenant override is one struct field
// away.
const defaultFullTableRowCap = 100_000

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
	// resolverFn loads the whole-schema metadata.Resolver. Production
	// wires this to metadata.NewLatestPerTableResolver(indexDB) — the
	// newest snapshot PER TABLE, unioned across schema_snapshots — NOT
	// NewResolver(indexDB, 0) (single latest snapshot_id): a PostgreSQL
	// source writes ONE table per snapshot_id, so "latest snapshot"
	// would be just the last table that saw DML (#603); for MySQL the
	// union is a strict generalization (the latest whole-schema snapshot
	// already wins per table). The load materialises every table's
	// column metadata into memory — non-trivial under load. Tests inject
	// a fake to exercise the column-ordering paths without an indexDB.
	//
	// Wrapped by resolverCache below so successive queries share one
	// load for up to resolverCacheTTL; a fresh `bintrail snapshot`
	// becomes visible at the next cache miss without explicit
	// invalidation. Use resolver() (not resolverFn directly) from
	// production code so the cache + sticky-fallback policy applies.
	resolverFn    func() (*metadata.Resolver, error)
	resolverCache resolverCache

	// epochResolvers permanently caches per-snapshot resolvers for
	// epoch-aware ENUM/SET decoding (#475): snapshots are immutable, so
	// entries never go stale; epochResolverCacheCap bounds memory.
	epochMu        sync.Mutex
	epochResolvers map[int]*metadata.Resolver

	// The epoch LIST does grow (each `bintrail snapshot` appends), so it
	// gets the same TTL treatment as resolverCache: a fresh snapshot
	// becomes visible at the next expiry, and steady-state time-travel
	// queries pay no extra DB round-trip.
	epochListMu     sync.Mutex
	epochList       []metadata.SnapshotEpoch
	epochListLoaded time.Time

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
	// FullTableRowCap caps the buffered resultset for the full-table
	// AS OF path (issue #276). Zero (default) inherits
	// defaultFullTableRowCap (100k). Set non-zero to override per
	// Handler — primarily for unit tests that want to seed the
	// overflow path without materialising 100k rows. A future
	// per-tenant override would plumb through here.
	FullTableRowCap int
	// AuthMethod selects the MySQL auth plugin the shim advertises
	// during the handshake (issue #274). Valid values are listed by
	// NewMySQLServer; the four accepted spellings are: "" (default),
	// "mysql_native_password", "caching_sha2_password", and
	// "sha256_password". Empty keeps the historical mysql_native_password
	// path so existing deployments see no behaviour change.
	// mysql_native_password is deprecated in MySQL 8.0+ and disabled
	// by default in 8.4 — operators on a fresh 8.4+ instance with the
	// plugin disabled set this to "caching_sha2_password" (or
	// "sha256_password") to authenticate without re-enabling
	// deprecated auth. Requires ProxySQL 2.7+ upstream of the shim
	// (the LTS 2.6 line is not verified to negotiate SHA2 against
	// backends).
	AuthMethod string
	// BaselineDir / BaselineS3 point the _snapshot baseline-lookup path
	// (#355) at the Parquet snapshots produced by `bintrail baseline`.
	// BaselineS3 (an s3:// URL prefix) takes precedence over BaselineDir
	// (a local directory) when both are set. When neither is set,
	// _snapshot behaves exactly like _flashback (binlog-only) — the
	// pre-#355 behaviour — so existing deployments see no change until
	// they opt in by configuring a baseline source.
	//
	// Only _snapshot consults these; _flashback stays binlog-only by
	// design so the two virtual schemas have distinct, documented
	// semantics (a row that existed at AS OF but was never touched in
	// the retained binlog window appears under _snapshot, not under
	// _flashback).
	BaselineDir string
	BaselineS3  string
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
		resolverFn:     func() (*metadata.Resolver, error) { return metadata.NewLatestPerTableResolver(indexDB) },
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

	// SHOW TABLES FROM _flashback/_diff/_snapshot (#315). Intercepted
	// here, before Parse(), so the table list comes from the schema
	// snapshots rather than letting the query fall through to the
	// real MySQL (which returns ER_BAD_DB on the virtual schema).
	if m := showTablesFromVirtualRE.FindStringSubmatch(qstr); m != nil {
		return h.runShowTablesFromVirtual(currentDB, m[1])
	}

	q, perr := Parse(qstr, currentDB)
	if perr == nil {
		// Cross-cut PK validation (#296). Applied here, not inside each
		// runX, so all four parsed shapes (TypeFlashback, TypeSnapshot,
		// TypeDiff, and the hint-comment form which Parse normalises
		// into TypeFlashback) share one validation site. Returns a
		// *mysql.MyError typed at ER_PARSE_ERROR when the user's WHERE
		// column does not match the table's real PK — the bug the
		// issue describes was the shim silently using the literal value
		// against pk_values regardless of column name.
		if verr := h.validatePKColumn(q); verr != nil {
			return nil, verr
		}
		switch q.Type {
		case TypeFlashback:
			return h.runPointInTime(q)
		case TypeSnapshot:
			return h.runSnapshot(q)
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

// runPointInTime resolves a _flashback query (binlog-only) against the
// bintrail index + archives and reconstructs the row's state at q.AsOf.
//
// Two shapes are recognised, sharing this entry point:
//   - q.PKColumn != "": single-row point-lookup. Returns the latest
//     INSERT/UPDATE post-image, or an empty resultset when the latest
//     event at-or-before AsOf is a DELETE.
//   - q.PKColumn == "": full-table reconstruction (issue #276).
//     Dispatches to runFullTable.
//
// Dispatch is on PKColumn, not PKValue, so a query whose PK value is the
// empty string (legitimate against a NOT-NULL VARCHAR column) stays a
// single-row point-lookup instead of silently flipping to a 100k-row table scan.
//
// Both shapes treat DELETE as "row did not exist at AsOf" — the
// Oracle AS OF semantic the docs call out (docs/time-travel-sql.md).
// Forensic queries for the pre-delete image still work via _diff,
// which exposes the full per-PK event history including row_before.
//
// _snapshot no longer shares this entry point: it routes through
// runSnapshot, which adds baseline-lookup (#355) on top of this
// binlog-only path so rows that existed at AsOf but were never touched
// in the retained binlog window still resolve. _flashback deliberately
// stays binlog-only here.
func (h *Handler) runPointInTime(q TimeTravelQuery) (*mysql.Result, error) {
	if q.PKColumn == "" {
		return h.runFullTable(q)
	}

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

	// ENUM/SET ordinals → labels (#472/#475), so the historical row
	// renders the way the live table did when the event happened.
	h.mapEventImages(q.Schema, q.Table, rows)
	image := selectImage(rows)
	if image == nil {
		return emptyResult(), nil
	}
	// When q.Columns is set (#313 user-supplied projection), bypass
	// imageToResult's orderColumns step — orderColumns is designed for
	// SELECT * and DROPS missing-from-image columns + APPENDS image
	// columns absent from ddlOrder (alphabetically). Both behaviours
	// silently expand and reshuffle the user's explicit projection. The
	// multi-row path makes the same split via fullTableResult
	// (imagesToResultVerbatim vs imagesToResult).
	if q.Columns != nil {
		return imageToResultVerbatim(image, q.Columns)
	}
	return imageToResult(image, h.columnOrderFor(q.Schema, q.Table))
}

// imageToResultVerbatim is the user-projection sibling of imageToResult.
// Unlike imageToResult (which goes through orderColumns to drop missing
// keys and append snapshot-unknown extras), this function uses cols
// verbatim — exactly the columns the user listed, in the order they
// listed them, with NULL for any column missing from the image. That
// matches MySQL's behaviour for `SELECT <col>, <col> FROM <table>`
// after an ALTER TABLE DROP COLUMN: the column name stays, the value
// is NULL.
//
// Pure function — separated from imageToResult so the two semantics
// stay independently testable and the contract on each is single-purpose.
func imageToResultVerbatim(image map[string]any, cols []string) (*mysql.Result, error) {
	row := make([]any, len(cols))
	for i, c := range cols {
		row[i] = resultsetValue(image[c]) // nil for missing key → NULL on wire
	}
	rs, err := mysql.BuildSimpleTextResultset(cols, [][]any{row})
	if err != nil {
		return nil, fmt.Errorf("build verbatim resultset: %w", err)
	}
	return &mysql.Result{Resultset: rs}, nil
}

// fullTableResult builds the multi-row resultset for a full-table
// time-travel query, dispatching on the projection the user asked for —
// the exact split the single-row path makes between imageToResult and
// imageToResultVerbatim (see runPointInTime):
//
//   - explicit columns (#313, q.Columns != nil) → imagesToResultVerbatim:
//     project verbatim onto the user's list (NULL for any column an image
//     lacks); a column they did NOT list must not reappear. Appending
//     image-only keys here would silently widen the user's projection.
//   - SELECT * (q.Columns == nil) → imagesToResult: the table's newest
//     snapshot order as the base, plus any image-only keys — e.g. a column
//     dropped between AS OF and now whose value is still captured in the
//     index (#600).
//
// columnOrderFor stays newest-snapshot-per-table for SELECT * (#603): it
// also backs SHOW TABLES and PK validation, which must reflect the table's
// current (last-snapshotted) schema, not the shape at AS OF.
func (h *Handler) fullTableResult(q TimeTravelQuery, images []map[string]any) (*mysql.Result, error) {
	if q.Columns != nil {
		return imagesToResultVerbatim(images, q.Columns)
	}
	return imagesToResult(images, h.columnOrderFor(q.Schema, q.Table))
}

// runShowTablesFromVirtual answers `SHOW [FULL] TABLES FROM
// _flashback/_diff/_snapshot` (#315). The virtual schemas have no MySQL
// counterpart on the backend, so passing the query through ProxySQL would
// hit ER_BAD_DB. Instead we return every table of currentDB the index has
// schema knowledge of — the newest snapshot per table, unioned across
// schema_snapshots (#603; see metadata.NewLatestPerTableResolver). For a
// MySQL source that equals the latest whole-schema snapshot; for a
// PostgreSQL source (one table per snapshot_id) it is the only complete
// view. A table now dropped at the source still lists under its last-known
// shape — its indexed history is a legitimate target for
// `SELECT * FROM <virtualSchema>.<table> AS OF ...`, same rationale as the
// dropped-column surfacing in #600.
//
// `virtualSchema` is the literal `_flashback` / `_diff` / `_snapshot`
// captured from the SHOW; it's used only for the resultset column name
// (mirroring real MySQL's `Tables_in_<dbname>` convention).
func (h *Handler) runShowTablesFromVirtual(currentDB, virtualSchema string) (*mysql.Result, error) {
	colName := "Tables_in_" + virtualSchema
	if currentDB == "" {
		return nil, mysql.NewError(mysql.ER_NO_DB_ERROR, fmt.Sprintf(
			"no database selected; issue `USE <database>;` against your real schema first, then SHOW TABLES FROM %s",
			virtualSchema,
		))
	}

	r, err := h.resolverCache.get(time.Now, resolverCacheTTL, h.resolverFn, h.logger)
	if err != nil {
		// Pre-snapshot install: the virtual schemas exist but have no
		// indexable tables yet. Empty resultset rather than an error so
		// `SHOW TABLES` behaves like it would against a freshly-created
		// real database with no tables. Operators see "Empty set" — the
		// natural prompt to run `bintrail snapshot` is already in
		// ErrNoSnapshots' message text, surfaced elsewhere.
		if errors.Is(err, metadata.ErrNoSnapshots) {
			rs, rsErr := mysql.BuildSimpleTextResultset([]string{colName}, nil)
			if rsErr != nil {
				// BuildSimpleTextResultset on (1 column, nil values) is
				// empirically infallible today, but a future go-mysql
				// release could tighten the contract. Surface via Warn so
				// Sentry / structured logs catch it; the user still gets
				// the empty-set semantic.
				h.logger.Warn("shim: build empty SHOW TABLES resultset failed",
					"err", rsErr, "current_db", currentDB, "virtual_schema", virtualSchema)
				return nil, fmt.Errorf("build empty SHOW TABLES resultset: %w", rsErr)
			}
			return &mysql.Result{Resultset: rs}, nil
		}
		// Wrap with currentDB + virtualSchema so a multi-tenant shim log
		// lets oncall attribute the failure to a specific connection's
		// USE and virtual-schema target — same triage-friendly pattern
		// wrapFetchError uses for the data-fetch paths.
		return nil, fmt.Errorf("resolve schema snapshot for SHOW TABLES FROM %s (current_db=%s): %w",
			virtualSchema, currentDB, err)
	}

	tables := r.Tables(currentDB)
	values := make([][]any, 0, len(tables))
	for _, t := range tables {
		values = append(values, []any{t.Table})
	}
	rs, err := mysql.BuildSimpleTextResultset([]string{colName}, values)
	if err != nil {
		return nil, fmt.Errorf("build SHOW TABLES resultset: %w", err)
	}
	return &mysql.Result{Resultset: rs}, nil
}

// runFullTable reconstructs the full row state of a table at q.AsOf
// (issue #276). The SQL it issues is identical to the point-lookup
// path — query.Engine with LimitPerPK=1 and Until=q.AsOf — except
// PKValues is empty so the windowed query returns the latest event
// per PK across the whole table. Rows whose latest event is a DELETE
// are skipped (same semantic as the point-lookup path).
//
// Cost guardrail: queries are capped at fullTableRowCap rows. We
// fetch one extra row (cap+1) so the overflow is detectable; if the
// cap is exceeded we return ER_TOO_BIG_SELECT (1104). Operators
// narrow the AS OF range or fall back to PK-filtered queries.
//
// Cross-row column handling: column order is taken from the table's
// newest schema snapshot (same DDL order as point-lookup and _diff).
// Rows whose images carry columns missing from that snapshot get
// those columns dropped — same behaviour as a regular MySQL
// `SELECT *` after an ALTER TABLE that removed a column.
func (h *Handler) runFullTable(q TimeTravelQuery) (*mysql.Result, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cap := h.cfg.FullTableRowCap
	if cap <= 0 {
		cap = defaultFullTableRowCap
	}

	engine := query.New(h.indexDB)
	rows, _, err := query.FetchMerged(ctx, h.indexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:     q.Schema,
			Table:      q.Table,
			Until:      &q.AsOf,
			LimitPerPK: 1,
			Limit:      cap + 1,
		},
		DBName:         h.cfg.IndexDBName,
		NoArchive:      h.cfg.NoArchive,
		AllowGaps:      h.cfg.AllowGaps,
		ArchiveFetcher: h.archiveFetcher,
	})
	if err != nil {
		return nil, wrapFetchError(q.Type, err)
	}

	if len(rows) > cap {
		return nil, mysql.NewError(mysql.ER_TOO_BIG_SELECT, fmt.Sprintf(
			"resolve %s: %s.%s at %s would return more than %d rows; narrow the AS OF range or filter by PK",
			q.Type, q.Schema, q.Table, q.AsOf.Format("2006-01-02 15:04:05"), cap,
		))
	}

	// ENUM/SET ordinals → labels per event's snapshot epoch (#472/#475),
	// before the images are extracted.
	h.mapEventImages(q.Schema, q.Table, rows)
	return h.fullTableResult(q, extractFullTableImages(rows))
}

// extractFullTableImages picks the post-image of every non-DELETE
// event in rows. Skipping DELETEs is what makes the resultset
// represent the table's state at AS OF — rows whose latest event
// was a DELETE did not exist at that instant.
func extractFullTableImages(rows []query.ResultRow) []map[string]any {
	images := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		if r.EventType == event.EventDelete {
			continue
		}
		// Empty row_after (rare — corrupted index) is dropped silently
		// rather than emitting a row of nulls that would overstate the
		// table's row count.
		if len(r.RowAfter) == 0 {
			continue
		}
		images = append(images, r.RowAfter)
	}
	return images
}

// imagesToResult is the SELECT * multi-row sibling of imageToResult. The
// column list is computed once (by fullTableColumns: latest-snapshot order
// plus any image-only keys, #600) for the whole resultset so every row in
// the wire resultset has the same shape, with NULL where a row's image is
// missing a column. For an explicit user projection use the verbatim sibling.
//
// Empty input → empty resultset.
func imagesToResult(images []map[string]any, ddlOrder []string) (*mysql.Result, error) {
	if len(images) == 0 {
		return emptyResult(), nil
	}
	return buildImagesResult(images, fullTableColumns(images, ddlOrder))
}

// imagesToResultVerbatim is the multi-row sibling of imageToResultVerbatim:
// it projects the images onto cols EXACTLY as given — the user's listed
// columns (#313), in their order, with NULL for any column an image doesn't
// carry. Unlike imagesToResult it never appends image-only keys: the user
// asked for precisely these columns, so a column they did not list (e.g. one
// dropped after the AS OF instant) must not reappear.
//
// Empty input → empty resultset.
func imagesToResultVerbatim(images []map[string]any, cols []string) (*mysql.Result, error) {
	if len(images) == 0 {
		return emptyResult(), nil
	}
	return buildImagesResult(images, cols)
}

// buildImagesResult assembles the wire resultset: one row per image, each
// projected onto cols (missing key → NULL). Shared by imagesToResult and
// imagesToResultVerbatim so projection and serialization are identical; only
// the column-derivation policy differs between the two callers.
func buildImagesResult(images []map[string]any, cols []string) (*mysql.Result, error) {
	values := make([][]any, len(images))
	for i, img := range images {
		row := make([]any, len(cols))
		for j, c := range cols {
			row[j] = resultsetValue(img[c])
		}
		values[i] = row
	}

	rs, err := mysql.BuildSimpleTextResultset(cols, values)
	if err != nil {
		return nil, fmt.Errorf("build resultset: %w", err)
	}
	return &mysql.Result{Resultset: rs}, nil
}

// fullTableColumns computes the column emission order for a SELECT *
// full-table resultset. It is the multi-image relative of orderColumns:
// both APPEND image-only keys instead of strict-projecting them away, which
// is what fixes the #600 WHERE-clause asymmetry — adding `WHERE pk=` no
// longer hides a since-dropped column. (Full-table is a SUPERSET of the
// single-row column set, not strictly equal: it additionally NULL-fills
// ddlOrder columns no image carries, e.g. a column ADDED after AS OF. The
// two sets coincide exactly when every ddlOrder column is present in the
// images — the reported drop case.)
//
// Selection is snapshot-driven when possible:
//
//   - ddlOrder (the table's newest schema snapshot) is the base, used
//     verbatim — every column in it appears even if no image carries it
//     (NULL on the wire), matching how MySQL itself returns rows from a
//     table that was ALTER'd to ADD a column after some rows existed.
//   - Then any image-only keys (the union across every image, sorted)
//     are APPENDED. These are columns present in the captured row images
//     but absent from the table's newest snapshot — most importantly a column
//     DROPPED between the AS OF instant and now. Its value is still in
//     the index; strict-projecting onto ddlOrder alone (the pre-#600
//     behavior) silently hid it. Appending surfaces it instead, exactly
//     as the single-row path (orderColumns) already does.
//   - When ddlOrder is empty (no resolved snapshot for this table —
//     first install, or a snapshot that doesn't cover this schema/table),
//     we fall back to the union of every image's keys (sorted) — using
//     the first image alone would silently elide columns that appeared
//     only in later events of the same query.
//
// No-drift equivalence: when no schema change spans the query window,
// every image key is already in ddlOrder, so nothing is appended and the
// column list is byte-identical to the pre-#600 ddlOrder-verbatim output.
//
// Pure function — extracted so the ordering rules can be unit-tested
// without spinning up MySQL (mirrors orderColumns).
func fullTableColumns(images []map[string]any, ddlOrder []string) []string {
	if len(ddlOrder) == 0 {
		seen := make(map[string]struct{})
		for _, img := range images {
			for k := range img {
				seen[k] = struct{}{}
			}
		}
		cols := make([]string, 0, len(seen))
		for k := range seen {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		return cols
	}

	inDDL := make(map[string]struct{}, len(ddlOrder))
	for _, c := range ddlOrder {
		inDDL[c] = struct{}{}
	}
	extraSet := make(map[string]struct{})
	for _, img := range images {
		for k := range img {
			if _, ok := inDDL[k]; !ok {
				extraSet[k] = struct{}{}
			}
		}
	}
	if len(extraSet) == 0 {
		return ddlOrder
	}
	extras := make([]string, 0, len(extraSet))
	for k := range extraSet {
		extras = append(extras, k)
	}
	sort.Strings(extras)
	// slices.Concat allocates a fresh slice — never append into the caller's
	// ddlOrder backing array (it comes from a cached resolver and is shared).
	return slices.Concat(ddlOrder, extras)
}

// validatePKColumn rejects time-travel queries whose WHERE column
// does not match the table's declared primary key (#296). Without
// this check, a query like `WHERE customer_id=1` against a table
// PK'd on `id` silently returns the row with id=1 — the shim's
// fetch path joins the literal value against binlog_events.pk_values
// regardless of the column name the user typed, producing a
// schema-valid resultset for a question the user never asked.
//
// Validation policy (mirrors columnOrderFor's resolver semantics):
//
//   - q.PKColumn == "" → no WHERE clause was supplied (full-table
//     reconstruction path, #276). Nothing to validate; return nil.
//   - h.resolverFn == nil → constructor invariant violation; can only
//     happen in tests that build a bare &Handler{}. Permissive so
//     those tests stay representative of legacy paths.
//   - resolver load fails (DB blip, ErrNoSnapshots, sticky-fallback
//     mid-outage) → permissive. Preserves the columnOrderFor
//     graceful-degradation contract: a broken snapshot lookup must
//     not turn a working query into a 1064 error. The fetch still
//     runs, the wire response still degrades to alphabetical column
//     order, and operators see the same Debug/Warn split logs they
//     get from columnOrderFor for the same condition.
//   - table not in any snapshot → permissive, same rationale.
//     Tables created after the most recent snapshot (MySQL) or that
//     have not yet seen DML on the stream (PostgreSQL, whose
//     snapshots are written per RelationMessage) are common and
//     self-fix on the next snapshot / first DML.
//   - len(PKColumns) == 0 → table snapshot is present but has no PK
//     declared. The shim can't safely correlate row state without a
//     PK, so reject with 1064. validateTables enforces PK presence
//     at snapshot time, so this branch is reachable only via an
//     index that was rolled back from a stricter version, or a
//     hand-edited schema_snapshots row.
//   - len(PKColumns) > 1 → composite PK. The `WHERE col=val` shape
//     can only address a single-column PK; reject with 1064 so the
//     user gets a clear error instead of a silently-wrong row that
//     coincidentally matches the first PK column. A future iteration
//     may extend the parser to accept `WHERE (a,b)=(v1,v2)` — until
//     then, the right answer is "your filter shape is unsupported
//     for this table", not "your filter ran against the wrong column".
//   - single-column PK that matches q.PKColumn → nil (the only
//     accept-path).
//   - single-column PK that does NOT match → 1064 with an actionable
//     message naming both the expected and the user-supplied column.
func (h *Handler) validatePKColumn(q TimeTravelQuery) error {
	if q.PKColumn == "" {
		return nil
	}
	if h.resolverFn == nil {
		return nil
	}
	r, err := h.resolverCache.get(time.Now, resolverCacheTTL, h.resolverFn, h.logger)
	if err != nil {
		// Same split as columnOrderFor: ErrNoSnapshots is benign
		// first-install state; anything else is a real config/infra
		// problem the operator should see at default log-level info.
		if errors.Is(err, metadata.ErrNoSnapshots) {
			h.logger.Debug("shim: no snapshots yet; skipping PK validation",
				"schema", q.Schema, "table", q.Table)
		} else {
			h.logger.Warn("shim: schema_snapshots lookup failed; skipping PK validation",
				"err", err, "schema", q.Schema, "table", q.Table)
		}
		return nil
	}
	tm, err := r.Resolve(q.Schema, q.Table)
	if err != nil {
		h.logger.Debug("shim: table not in any snapshot; skipping PK validation",
			"err", err, "schema", q.Schema, "table", q.Table)
		return nil
	}
	if len(tm.PKColumns) == 0 {
		return mysql.NewError(mysql.ER_PARSE_ERROR, fmt.Sprintf(
			"%s.%s has no primary key declared in the indexed snapshot; cannot resolve %s by PK",
			q.Schema, q.Table, q.Type,
		))
	}
	if len(tm.PKColumns) > 1 {
		return mysql.NewError(mysql.ER_PARSE_ERROR, fmt.Sprintf(
			"%s.%s has a composite primary key (%s); WHERE %s = <value> shape supports only single-column PKs",
			q.Schema, q.Table, strings.Join(tm.PKColumns, ", "), q.PKColumn,
		))
	}
	if q.PKColumn != tm.PKColumns[0] {
		return mysql.NewError(mysql.ER_PARSE_ERROR, fmt.Sprintf(
			"WHERE column must be the primary key of %s.%s (expected %s, got %s)",
			q.Schema, q.Table, tm.PKColumns[0], q.PKColumn,
		))
	}
	return nil
}

// columnOrderFor returns the column names of schema.table in DDL
// (ordinal_position) order so the wire-protocol resultset matches
// what a regular MySQL `SELECT *` would emit. Returns nil when no
// snapshot is available or the table is missing from the latest
// snapshot — the caller falls back to alphabetical ordering of the
// JSON image keys. Degradation semantics and logging live in
// tableMetaFor.
func (h *Handler) columnOrderFor(schema, table string) []string {
	tm := h.tableMetaFor(schema, table)
	if tm == nil {
		return nil
	}
	cols := make([]string, 0, len(tm.Columns))
	for _, c := range tm.Columns {
		cols = append(cols, c.Name)
	}
	return cols
}

// tableMetaFor returns the table's metadata from its newest schema
// snapshot (per-table union across schema_snapshots, #603 — correct
// for PostgreSQL's one-table-per-snapshot_id layout as well as MySQL's
// whole-schema snapshots), or nil when it can't be resolved. nil is the
// graceful-degradation signal shared by every consumer (columnOrderFor's
// alphabetical fallback): a broken or absent snapshot must never turn
// a working query into an error.
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
//   - table not in any snapshot → Debug. Common for tables created
//     after the latest snapshot was taken (or, on a PG source, that
//     have not yet seen DML); benign and self-fixing once a fresh
//     snapshot runs.
//
// A degraded-but-deterministic fallback is strictly better than a
// hard failure on what is otherwise a working query — but the
// fallback should be loud when it's hiding a real outage.
func (h *Handler) tableMetaFor(schema, table string) *metadata.TableMeta {
	if h.resolverFn == nil {
		return nil
	}
	r, err := h.resolverCache.get(time.Now, resolverCacheTTL, h.resolverFn, h.logger)
	if err != nil {
		if errors.Is(err, metadata.ErrNoSnapshots) {
			h.logger.Debug("shim: no snapshots yet; proceeding without snapshot metadata",
				"schema", schema, "table", table)
		} else {
			h.logger.Warn("shim: schema_snapshots lookup failed; proceeding without snapshot metadata",
				"err", err, "schema", schema, "table", table)
		}
		return nil
	}
	tm, err := r.Resolve(schema, table)
	if err != nil {
		h.logger.Debug("shim: table not in any snapshot; proceeding without snapshot metadata",
			"err", err, "schema", schema, "table", table)
		return nil
	}
	return tm
}

// epochResolverCacheCap bounds the per-snapshot resolver cache: each
// entry holds a full snapshot in memory, and a long-lived shim on an
// index with years of snapshots must not accumulate them all. Real
// queries touch one or two epochs; eviction is arbitrary because any
// evicted entry reloads on demand.
const epochResolverCacheCap = 8

// mapEventImages rewrites ENUM/SET ordinals in every row's images back
// to labels (#472), decoding each EVENT with the snapshot in effect at
// its timestamp (#475): an enum reshaped between two events renders
// each event under its own definition instead of mislabeling old
// ordinals with the latest one. Degradation ladder: epoch lookup
// unavailable → the table's newest snapshot (the pre-#475 behavior);
// nothing resolvable → pass-through (raw ordinals, never a guessed label).
//
// Call this on fetched rows BEFORE any merge or text coercion — the
// _snapshot paths fold row_after wholesale into the reconstructed
// state, so pre-mapped events make the merged row carry labels, and
// fullTableTextCell would otherwise hide ordinals as text cells the
// mapper (correctly) refuses to touch.
func (h *Handler) mapEventImages(schema, table string, rows []query.ResultRow) {
	if len(rows) == 0 || h.resolverFn == nil {
		return
	}
	var fallback *metadata.Resolver
	if r, err := h.resolverCache.get(time.Now, resolverCacheTTL, h.resolverFn, h.logger); err == nil {
		fallback = r
	}
	epochs := h.loadEpochs()
	src := metadata.EnumMapperSource{
		Epochs:      epochs,
		ResolverFor: h.epochResolver,
		Fallback:    fallback,
	}
	// BLOB/TEXT columns are stored base64-encoded (marshalRow base64-encodes the
	// []byte go-mysql delivers); decode them back to raw bytes / strings before
	// emission so the wire resultset carries the real value, not its base64 text
	// (#661, sibling of recover #662 / reconstruct #663).
	//
	// Resolve the decodable columns at EACH event's epoch, not from the latest
	// snapshot, mirroring the ENUM/SET mapper beside it (#475). Whether a column
	// is base64-stored depends on whether go-mysql delivered it as []byte
	// (BLOB/TEXT) or string (VARCHAR/CHAR) when the event was captured — so a
	// latest-snapshot lookup would wrongly decode an old plain-string value that
	// happens to be valid base64 (e.g. "test") across a VARCHAR→TEXT widening,
	// silently corrupting it.
	//
	// Unlike the ENUM mapper, base64 decode has NO latest-snapshot fallback:
	// relabeling an ENUM by the latest definition is harmless (strings pass
	// through), but base64-decoding by the wrong schema is destructive, and the
	// degraded path is reachable in production — resolverCache is sticky on
	// failure (serves a stale latest resolver) while loadEpochs returns nil
	// uncached on a DB blip, so an empty epoch list can coincide with a non-nil
	// latest fallback. When the event's epoch typing is unavailable we therefore
	// leave the value as the base64 it was stored as (mirrors reconstruct #666).
	// The lone exception is a no-DB test handler (indexDB nil), where the
	// injected fallback is the sole schema source and decoding by it is intended.
	// The per-epoch column map is memoized.
	b64Memo := make(map[int]map[string]bool)
	base64ColsAt := func(t time.Time) map[string]bool {
		id, ok := metadata.EpochAt(epochs, t)
		if !ok {
			// Empty epoch list: in production decline (leave base64); only the
			// no-DB test handler decodes via the injected fallback (bucket -1).
			if h.indexDB != nil {
				return nil
			}
			id = -1
		}
		// Memoize per epoch id BEFORE attempting the load, so a snapshot whose
		// resolver consistently fails to load is probed at most once rather than
		// once per row at that epoch (mirrors EnumMapperSource.MapperAt, which
		// checks its memo before calling ResolverFor).
		if m, seen := b64Memo[id]; seen {
			return m
		}
		r := fallback // only reached for id == -1 (the no-DB test path)
		if id != -1 {
			// The event's epoch is known; type the column from THAT epoch's
			// resolver. If it fails to load, leave the value as base64 rather
			// than fall back to the latest snapshot — cross-epoch typing is the
			// corruption risk this closure exists to avoid.
			er, err := h.epochResolver(id)
			if err != nil || er == nil {
				b64Memo[id] = nil
				return nil
			}
			r = er
		}
		m := base64Cols(r, schema, table)
		b64Memo[id] = m
		return m
	}
	for i := range rows {
		m := src.MapperAt(schema, table, rows[i].EventTimestamp)
		m.MapImage(rows[i].RowBefore)
		m.MapImage(rows[i].RowAfter)
		// Decode AFTER the ENUM/SET map: the two passes touch disjoint columns
		// (ENUM/SET are never BLOB/TEXT), so order is immaterial, but keeping the
		// base64 decode last mirrors reconstruct.DecodeEventBinaries running
		// after MapEventEnumLabels. Event images only — never a baseline row, so
		// _snapshot decodes its deltas pre-merge and never double-decodes the
		// baseline value DuckDB scans straight to a Go string.
		b64 := base64ColsAt(rows[i].EventTimestamp)
		decodeImageBase64(rows[i].RowBefore, b64)
		decodeImageBase64(rows[i].RowAfter, b64)
	}
}

// base64StoredKind reports whether a column's DataType is in the BLOB or TEXT
// family — the ones go-mysql delivers as []byte so marshalRow base64-encodes
// them in storage — and if so whether it is binary (true → raw []byte) or text
// (false → string). Local copy of the predicate added for recover (#662) and
// reconstruct (#663); duplicated because those copies are unexported (#661 is
// the third consumer — a future refactor may hoist one shared copy).
//
// GEOMETRY/VECTOR are also delivered as []byte but deliberately out of scope
// here, matching the recover/reconstruct fixes.
func base64StoredKind(dataType string) (binary, ok bool) {
	switch strings.ToLower(dataType) {
	case "blob", "tinyblob", "mediumblob", "longblob":
		return true, true
	case "text", "tinytext", "mediumtext", "longtext":
		return false, true
	default:
		return false, false
	}
}

// decodeStoredBase64 reverses the storage-side base64 encoding of a BLOB/TEXT
// value. binary selects the decoded Go type (true → []byte, false → string).
// On the text resultset both render to the same wire bytes via
// BuildSimpleTextResultset, but the distinction is load-bearing for _diff, which
// JSON-marshals the image (marshalImageOrdered): a BLOB must stay []byte so it
// re-base64-encodes cleanly rather than emit raw, possibly invalid-UTF-8 bytes
// into the audit JSON. A value that is not a decodable base64 string is returned
// unchanged (defensive — NULL, a raw-JSON object/array blob promoted by
// marshalRow, or pre-existing non-base64 data).
func decodeStoredBase64(v any, binary bool) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return v
	}
	if binary {
		return b
	}
	return string(b)
}

// base64Cols maps each BLOB/TEXT column of schema.table to whether it is binary,
// using the SUPPLIED resolver — base64ColsAt passes the resolver in effect at
// the event's epoch (NOT necessarily the latest snapshot), which is what keeps
// an old VARCHAR value from being decoded across a VARCHAR→TEXT widening. Returns
// nil when the resolver is nil, the table is unknown, or no column needs
// decoding — in which case BLOB/TEXT values are left as the base64 they were
// stored as (no usable schema means no safe typing; this preserves the pre-fix
// behavior rather than guessing).
func base64Cols(r *metadata.Resolver, schema, table string) map[string]bool {
	if r == nil {
		return nil
	}
	tm, err := r.Resolve(schema, table)
	if err != nil {
		return nil
	}
	var m map[string]bool
	for _, c := range tm.Columns {
		if binary, ok := base64StoredKind(c.DataType); ok {
			if m == nil {
				m = make(map[string]bool)
			}
			m[c.Name] = binary
		}
	}
	return m
}

// decodeImageBase64 decodes the storage-side base64 of every BLOB/TEXT column in
// one event image, in place. No-op when binCols is empty or image is nil. Iterates
// binCols (typically a handful of columns) and only rewrites keys the image
// carries, so a partial image is left otherwise untouched.
func decodeImageBase64(image map[string]any, binCols map[string]bool) {
	if len(binCols) == 0 || image == nil {
		return
	}
	for col, binary := range binCols {
		if v, ok := image[col]; ok {
			image[col] = decodeStoredBase64(v, binary)
		}
	}
}

// loadEpochs fetches the snapshot history for epoch-aware ENUM/SET
// decoding, cached for resolverCacheTTL (the same freshness contract as
// the resolver: a fresh `bintrail snapshot` shows up at the next
// expiry). nil (on any failure, or in resolver-only test handlers
// without a DB) degrades MapperAt to the latest-snapshot Fallback —
// the pre-#475 behavior, logged at Debug since the wire response is
// still correct for any enum that was never reshaped. Failures are not
// cached: the next query retries.
func (h *Handler) loadEpochs() []metadata.SnapshotEpoch {
	if h.indexDB == nil {
		return nil
	}
	now := time.Now()
	h.epochListMu.Lock()
	if !h.epochListLoaded.IsZero() && now.Sub(h.epochListLoaded) < resolverCacheTTL {
		list := h.epochList
		h.epochListMu.Unlock()
		return list
	}
	h.epochListMu.Unlock()

	epochs, err := metadata.LoadSnapshotEpochs(h.indexDB)
	if err != nil {
		h.logger.Debug("shim: snapshot epoch lookup failed; decoding ENUM/SET with the table's newest snapshot", "err", err)
		return nil
	}
	h.epochListMu.Lock()
	h.epochList, h.epochListLoaded = epochs, now
	h.epochListMu.Unlock()
	return epochs
}

// epochResolver loads (and permanently caches) the resolver for one
// snapshot id. Snapshots are immutable, so entries never go stale;
// epochResolverCacheCap bounds memory on long-lived shims.
func (h *Handler) epochResolver(id int) (*metadata.Resolver, error) {
	h.epochMu.Lock()
	if r, ok := h.epochResolvers[id]; ok {
		h.epochMu.Unlock()
		return r, nil
	}
	h.epochMu.Unlock()

	r, err := metadata.NewResolver(h.indexDB, id)
	if err != nil {
		return nil, err
	}

	h.epochMu.Lock()
	defer h.epochMu.Unlock()
	if h.epochResolvers == nil {
		h.epochResolvers = make(map[int]*metadata.Resolver)
	}
	if len(h.epochResolvers) >= epochResolverCacheCap {
		for k := range h.epochResolvers {
			delete(h.epochResolvers, k)
			break
		}
	}
	h.epochResolvers[id] = r
	return r, nil
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
// Returns nil — meaning the caller responds with an empty resultset —
// in three cases: (1) no rows, (2) the latest event is a DELETE
// (the row did not exist at AsOf; matches docs/time-travel-sql.md's
// Oracle AS OF semantic and the full-table path), (3) both images
// are empty on an INSERT/UPDATE (corrupted index — treated as "no
// answer" rather than fabricating one).
//
// For non-DELETE events, row_after wins when present (the post-image
// is the row's state at the event — true for INSERT, UPDATE, and the
// EventSnapshot baseline rows _snapshot will surface in a future
// iteration). row_before is the fallback used only when row_after is
// missing — a path the regular indexer pipeline doesn't exercise for
// INSERT/UPDATE since marshalRow always emits row_after, but kept
// defensively.
//
// Extracted as a pure helper specifically so the rule can be
// unit-tested without spinning up a real MySQL: a future refactor
// that reintroduced the row_before fallback for DELETE would silently
// return stale data on every "what does this row look like AS OF
// after its deletion?" query, with the regression invisible to any
// test that doesn't exercise this exact branch.
func selectImage(rows []query.ResultRow) map[string]any {
	if len(rows) == 0 {
		return nil
	}
	latest := rows[0]
	if latest.EventType == event.EventDelete {
		return nil
	}
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
	// Map ENUM/SET ordinals to labels (#472/#475) so the audit JSON
	// shows each event under the definition in effect when it happened —
	// a _diff window crossing an enum ALTER renders each side correctly.
	h.mapEventImages(q.Schema, q.Table, rows)
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

// eventTypeName turns event.EventType (a uint8) into a human-readable
// string for the _diff resultset. The event package does not export a
// String() method so this lookup lives here.
func eventTypeName(t event.EventType) string {
	switch t {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
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

// numberToText renders a JSON-decoded numeric row-image value (#496) to the
// SAME wire bytes go-mysql's FormatTextValue produces for the equivalent native
// value. Two reasons it must pre-render to []byte rather than return a numeric:
//   - BuildSimpleTextResultset fixes a column's wire type from its first row and
//     rejects later rows of a different Go type ("row types aren't consistent").
//     Returning int64 for an integral DOUBLE and float64 for a fractional one in
//     the same column would crash the whole _flashback full-table query. A
//     uniform []byte makes every such column VAR_STRING.
//   - In the full-table _snapshot merge, baseline-origin INT/DOUBLE cells
//     (DuckDB-native, rendered via FormatTextValue) and event-origin cells emit
//     identical bytes because both route through FormatTextValue (a raw
//     json.Number literal would diverge, e.g. "1e+21" vs "1000…0"). FLOAT(32-bit)
//     is the known exception — see runSnapshotFullTable.
//
// The exact numeric type is recovered first so a BIGINT UNSIGNED > 2^63 stays
// exact (int64 → uint64 → float64), falling back to the literal text.
func numberToText(n json.Number) []byte {
	var v any
	if i, err := n.Int64(); err == nil {
		v = i
	} else if u, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
		v = u
	} else if f, err := n.Float64(); err == nil {
		v = f
	} else {
		return []byte(n.String())
	}
	if b, err := mysql.FormatTextValue(v); err == nil {
		return b
	}
	return []byte(n.String())
}

// resultsetValue normalizes a row-image cell for BuildSimpleTextResultset. A
// json.Number (#496) is pre-rendered to uniform text bytes via numberToText;
// every other value passes through unchanged.
func resultsetValue(v any) any {
	if n, ok := v.(json.Number); ok {
		return numberToText(n)
	}
	return v
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
		row[i] = resultsetValue(image[c])
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
