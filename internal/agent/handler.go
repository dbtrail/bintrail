package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/forensics"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// allowedForensicsQueries maps predefined query identifiers to safe SQL
// that runs against performance_schema and information_schema. Only these
// are allowed — the agent never executes arbitrary SQL from dbtrail.
// DO NOT modify at runtime — this is a security boundary.
//
// The forensics_* attribution commands (HandleForensicsCapabilities/
// Enrich/Activity/Users/AuditLog below) hold the same line: each maps to
// the fixed, parameterized queries inside internal/forensics — no SQL text
// ever crosses the WebSocket channel in either direction of those commands.
var allowedForensicsQueries = map[string]string{
	"recent_queries": `SELECT DIGEST_TEXT, COUNT_STAR, SUM_TIMER_WAIT/1e12 AS total_seconds,
		AVG_TIMER_WAIT/1e12 AS avg_seconds, LAST_SEEN
		FROM performance_schema.events_statements_summary_by_digest
		ORDER BY LAST_SEEN DESC LIMIT 50`,

	"lock_waits": `SELECT
		r.trx_id AS waiting_trx,
		r.trx_mysql_thread_id AS waiting_thread,
		b.trx_id AS blocking_trx,
		b.trx_mysql_thread_id AS blocking_thread,
		r.trx_query AS waiting_query
		FROM performance_schema.data_lock_waits w
		JOIN information_schema.innodb_trx b ON b.trx_id = w.BLOCKING_ENGINE_TRANSACTION_ID
		JOIN information_schema.innodb_trx r ON r.trx_id = w.REQUESTING_ENGINE_TRANSACTION_ID`,

	"table_io": `SELECT OBJECT_SCHEMA, OBJECT_NAME, OBJECT_TYPE,
		COUNT_READ, COUNT_WRITE, COUNT_FETCH, COUNT_INSERT, COUNT_UPDATE, COUNT_DELETE
		FROM performance_schema.table_io_waits_summary_by_table
		WHERE OBJECT_SCHEMA NOT IN ('mysql','performance_schema','information_schema','sys')
		ORDER BY COUNT_READ + COUNT_WRITE DESC LIMIT 50`,
}

// DefaultHandler implements Handler using the existing query, recovery,
// and parquetquery packages.
type DefaultHandler struct {
	// IndexDB is the index database connection. Nil disables MySQL-based
	// resolve_pk and recover; data sources fall back to the buffer
	// (if set) and Parquet archives.
	IndexDB *sql.DB

	// SourceDB is the source MySQL connection for forensics queries.
	// Nil disables forensics_query support.
	SourceDB *sql.DB

	// SourceHost is the resolved host[:port] of the source server (from the
	// agent's --source-dsn). It lets the audit-log tier reach the RDS/CloudWatch
	// remote sources for a managed RDS/Aurora source whose audit log is not on
	// a local filesystem. Empty => local-file audit reads only. A per-request
	// ForensicsAuditLogRequest.SourceHost overrides it.
	SourceHost string

	// ArchiveSources lists Parquet archive paths (local dirs or s3:// URLs).
	ArchiveSources []string

	// Buffer is the in-memory event buffer for BYOS mode. When set,
	// resolve_pk and recover check the buffer first (fastest path).
	Buffer *buffer.Buffer

	// Logger for handler operations. Nil falls back to slog.Default().
	Logger *slog.Logger
}

func (h *DefaultHandler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// HandleResolvePK looks up pk_values for a list of pk_hash values from
// the in-memory buffer, local MySQL index, and/or Parquet archives.
func (h *DefaultHandler) HandleResolvePK(ctx context.Context, req ResolvePKRequest) ([]PKResult, error) {
	if h.IndexDB == nil && len(h.ArchiveSources) == 0 && h.Buffer == nil {
		return nil, fmt.Errorf("no data sources configured (need --index-dsn, --archive-dir/--archive-s3, or buffer)")
	}

	results := make([]PKResult, len(req.Items))
	for i, item := range req.Items {
		results[i] = PKResult{PKHash: item.PKHash}

		// Try in-memory buffer first (fastest, most recent data).
		if h.Buffer != nil {
			if pkVal, ok := h.Buffer.ResolvePK(item.PKHash, item.Schema, item.Table); ok {
				results[i].PKValues = pkVal
				results[i].Found = true
				continue
			}
		}

		// Try MySQL index.
		if h.IndexDB != nil {
			pkVal, err := h.resolvePKFromMySQL(ctx, item)
			if err != nil {
				return nil, fmt.Errorf("resolve pk from index: %w", err)
			}
			if pkVal != "" {
				results[i].PKValues = pkVal
				results[i].Found = true
				continue
			}
		}

		// Fall back to Parquet archives.
		for _, src := range h.ArchiveSources {
			pkVal, err := h.resolvePKFromArchive(ctx, item, src)
			if err != nil {
				h.logger().Warn("archive query failed, skipping", "source", src, "error", err)
				continue
			}
			if pkVal != "" {
				results[i].PKValues = pkVal
				results[i].Found = true
				break
			}
		}
	}
	return results, nil
}

// resolvePKFromMySQL queries binlog_events for a single pk_hash.
//
// Note: the standard PK lookup pattern requires both pk_hash = SHA2(?, 256)
// AND pk_values = ? as a collision guard. Here we only have the hash (that's
// what we're resolving), so we query by pk_hash alone. SHA-256 collisions
// are astronomically unlikely; callers should verify results when critical.
func (h *DefaultHandler) resolvePKFromMySQL(ctx context.Context, item PKItem) (string, error) {
	var pkValues string
	err := h.IndexDB.QueryRowContext(ctx,
		`SELECT pk_values FROM binlog_events
		 WHERE pk_hash = ? AND schema_name = ? AND table_name = ?
		 LIMIT 1`,
		item.PKHash, item.Schema, item.Table,
	).Scan(&pkValues)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return pkValues, err
}

// resolvePKFromArchive scans Parquet archive rows for the given pk_hash.
// Since Parquet files have no SHA2 index, we fetch all rows for the
// schema.table and compute SHA-256 client-side to find the match.
func (h *DefaultHandler) resolvePKFromArchive(ctx context.Context, item PKItem, source string) (string, error) {
	opts := query.Options{
		Schema: item.Schema,
		Table:  item.Table,
		Limit:  0, // no limit — need to scan for the hash
	}
	rows, err := parquetquery.Fetch(ctx, opts, source)
	if err != nil {
		return "", err
	}
	for _, r := range rows {
		if byosPKHash(r.PKValues) == item.PKHash {
			return r.PKValues, nil
		}
	}
	return "", nil
}

// HandleRecover generates reversal SQL for the specified events.
//
// Scope precedence: when GTID is set the agent honours it as the precise
// filter and only applies time bounds if the caller supplied them.  When
// GTID is empty the time range becomes the primary scope and zero-value
// TimeStart/TimeEnd are still passed verbatim (callers must populate them
// or the query will match no events — same contract as before).
func (h *DefaultHandler) HandleRecover(ctx context.Context, req RecoverRequest) (string, error) {
	if h.IndexDB == nil && len(h.ArchiveSources) == 0 && h.Buffer == nil {
		return "", fmt.Errorf("no data sources configured (need --index-dsn, --archive-dir/--archive-s3, or buffer)")
	}
	// Fail-loud guard: every recover call must scope the events somehow.
	// Without GTID *and* without time bounds the previous code would have
	// happily generated reversal SQL for the last 1000 events in the index —
	// exactly the silent-fallback shape #1512 patched. Reject up front.
	if req.GTID == "" && req.TimeStart.IsZero() && req.TimeEnd.IsZero() {
		return "", fmt.Errorf("recover requires gtid or time bounds")
	}

	// Build query options from the recover request.
	opts := query.Options{
		Schema: req.Schema,
		Table:  req.Table,
		GTID:   req.GTID,
		Limit:  1000,
	}
	// Pass time bounds verbatim when the caller supplied them.  Skip
	// zero-value bounds entirely — passing year-1 to query.Fetch would
	// still match (TO_SECONDS('0001-01-01') is small) but it's cleaner
	// and avoids surprising parquet partition pruning.  See #1512.
	if !req.TimeStart.IsZero() {
		opts.Since = &req.TimeStart
	}
	if !req.TimeEnd.IsZero() {
		opts.Until = &req.TimeEnd
	}
	if len(req.EventTypes) > 1 {
		return "", fmt.Errorf("only one event type filter is supported, got %d", len(req.EventTypes))
	}
	if len(req.EventTypes) == 1 {
		et, err := cliutil.ParseEventType(req.EventTypes[0])
		if err != nil {
			return "", fmt.Errorf("invalid event type: %w", err)
		}
		opts.EventType = et
	}

	// Fetch events from buffer, MySQL, and/or archives.
	var rows []query.ResultRow
	if h.Buffer != nil {
		r := h.Buffer.Fetch(ctx, opts)
		rows = append(rows, r...)
	}
	if h.IndexDB != nil {
		engine := query.New(h.IndexDB)
		r, err := engine.Fetch(ctx, opts)
		if err != nil {
			return "", fmt.Errorf("query index: %w", err)
		}
		rows = append(rows, r...)
	}
	for _, src := range h.ArchiveSources {
		r, err := parquetquery.Fetch(ctx, opts, src)
		if err != nil {
			h.logger().Warn("archive query failed, skipping", "source", src, "error", err)
			continue
		}
		rows = append(rows, r...)
	}

	rows = query.MergeResults(rows, opts.Limit, opts.Order)

	// Filter to requested pk_hashes if specified.
	if len(req.PKHashes) > 0 {
		wanted := make(map[string]struct{}, len(req.PKHashes))
		for _, ph := range req.PKHashes {
			wanted[ph] = struct{}{}
		}
		filtered := rows[:0]
		for _, r := range rows {
			hash := byosPKHash(r.PKValues)
			if _, ok := wanted[hash]; ok {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}

	// Generate reversal SQL.
	var resolver *metadata.Resolver
	if h.IndexDB != nil {
		var err error
		resolver, err = metadata.NewResolver(h.IndexDB, 0)
		if err != nil {
			h.logger().Warn("could not load schema snapshot; WHERE clauses will use all columns", "error", err)
			resolver = nil
		}
	}
	// DialectForIndex is nil-safe: h.IndexDB may be nil here, in which case it returns
	// MySQLDialect (#533/#573).
	gen := recovery.NewForDialect(h.IndexDB, resolver, recovery.DialectForIndex(h.IndexDB))

	var buf bytes.Buffer
	_, err := gen.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		return "", fmt.Errorf("generate recovery SQL: %w", err)
	}
	return buf.String(), nil
}

// HandleForensicsQuery executes a predefined diagnostic query against
// MySQL system tables (performance_schema, information_schema).
func (h *DefaultHandler) HandleForensicsQuery(ctx context.Context, req ForensicsQueryRequest) (*ForensicsResult, error) {
	if h.SourceDB == nil {
		return nil, fmt.Errorf("forensics queries require --source-dsn")
	}

	q, ok := allowedForensicsQueries[req.Query]
	if !ok {
		return nil, fmt.Errorf("unknown forensics query %q; allowed: recent_queries, lock_waits, table_io", req.Query)
	}

	sqlRows, err := h.SourceDB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer sqlRows.Close()

	cols, err := sqlRows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}

	result := &ForensicsResult{Columns: cols}
	for sqlRows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := sqlRows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			v := values[i]
			// Convert []byte to string for JSON serialization.
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			row[col] = v
		}
		result.Rows = append(result.Rows, row)
	}
	return result, sqlRows.Err()
}

// ─── Forensics attribution commands ──────────────────────────────────────────
//
// Thin wrappers over internal/forensics: validate the connection, call the
// library, wrap the result. All validation of filters/caps lives in the
// library (single source of truth); the entitlement gate lives in dispatch.
// Results carry identity + statement text — see the payload/metadata note
// on the request types in command.go.

// requireSourceDB guards the forensics attribution handlers, which all
// inspect the live source server. Returns a clear error when the agent
// runs without --source-dsn (mirrors HandleForensicsQuery's guard).
func (h *DefaultHandler) requireSourceDB() error {
	if h.SourceDB == nil {
		return fmt.Errorf("forensics commands require --source-dsn")
	}
	return nil
}

// HandleForensicsCapabilities detects the forensic data sources available
// on the source server (performance_schema state, audit plugin variant and
// config, server version/variant).
func (h *DefaultHandler) HandleForensicsCapabilities(ctx context.Context) (forensics.Capabilities, error) {
	if err := h.requireSourceDB(); err != nil {
		return forensics.Capabilities{}, err
	}
	return forensics.DetectCapabilities(ctx, h.SourceDB)
}

// HandleForensicsEnrich looks up live thread/connection attribution for the
// requested connection IDs from performance_schema.
//
// LIVE-ONLY: sessions that have already disconnected come back in NotFound
// with fallback queries. The SaaS composes live results with its
// connection-cache history in the who-changed engine — not here.
func (h *DefaultHandler) HandleForensicsEnrich(ctx context.Context, req ForensicsEnrichRequest) (forensics.EnrichResult, error) {
	if err := h.requireSourceDB(); err != nil {
		return forensics.EnrichResult{}, err
	}
	return forensics.EnrichThreads(ctx, h.SourceDB, req.ThreadIDs)
}

// HandleForensicsActivity runs one of the three fixed activity queries
// (user_activity, connection_history, ddl_history) against
// performance_schema on the source server.
func (h *DefaultHandler) HandleForensicsActivity(ctx context.Context, req ForensicsActivityRequest) (forensics.ActivityResult, error) {
	if err := h.requireSourceDB(); err != nil {
		return forensics.ActivityResult{}, err
	}
	return forensics.Activity(ctx, h.SourceDB, forensics.ActivityQuery{
		Type:   req.QueryType,
		User:   req.User,
		Host:   req.Host,
		Schema: req.Schema,
		Since:  req.Since,
		Until:  req.Until,
		Limit:  req.Limit,
		Order:  req.Order,
	})
}

// HandleForensicsUsers lists the MySQL user accounts known to the source
// server (mysql.user merged with performance_schema.accounts).
func (h *DefaultHandler) HandleForensicsUsers(ctx context.Context) (ForensicsUsersResult, error) {
	if err := h.requireSourceDB(); err != nil {
		return ForensicsUsersResult{}, err
	}
	users, err := forensics.ListUsers(ctx, h.SourceDB)
	if err != nil {
		return ForensicsUsersResult{}, err
	}
	return ForensicsUsersResult{Users: users}, nil
}

// HandleForensicsAuditLog discovers and parses the audit log configured on the
// source server. In auto mode (req.Source==""), it reads local-filesystem files
// and falls back to the RDS file API when the resolved host is an RDS/Aurora
// endpoint; req.Source can force "local", "rds", or "cloudwatch". The host is
// resolved via resolveAuditSourceHost (per-request req.SourceHost, else the
// agent's own --source-dsn host), so a managed RDS/Aurora instance whose log is
// not on local disk is reachable without the caller supplying the endpoint.
func (h *DefaultHandler) HandleForensicsAuditLog(ctx context.Context, req ForensicsAuditLogRequest) (forensics.AuditReadResult, error) {
	if err := h.requireSourceDB(); err != nil {
		return forensics.AuditReadResult{}, err
	}
	host := resolveAuditSourceHost(req.SourceHost, h.SourceHost)
	return forensics.ReadAuditLog(ctx, h.SourceDB, forensics.AuditReadOptions{
		Since:              req.Since,
		Until:              req.Until,
		User:               req.User,
		EventType:          req.EventType,
		Limit:              req.Limit,
		Offset:             req.Offset,
		IncludeRotated:     req.IncludeRotated,
		TailLines:          req.TailLines,
		Source:             forensics.AuditSource(req.Source),
		SourceHost:         host,
		CloudWatchLogGroup: req.CloudWatchLogGroup,
	})
}

// resolveAuditSourceHost picks the host used to reach the RDS/CloudWatch remote
// audit sources: a per-request SourceHost wins, otherwise the agent's own source
// host (from --source-dsn). Without a non-empty result the audit tier stays on
// the local-file path — the wiring gap that made the RDS/Aurora reader dead code
// before it was threaded through, so this fallback is pinned by a test.
func resolveAuditSourceHost(reqHost, handlerHost string) string {
	if reqHost != "" {
		return reqHost
	}
	return handlerHost
}

// byosPKHash computes SHA-256 hex digest of pkValues, matching the
// byos.PKHash function and MySQL's SHA2(pk_values, 256).
func byosPKHash(pkValues string) string {
	h := sha256.Sum256([]byte(pkValues))
	return hex.EncodeToString(h[:])
}
