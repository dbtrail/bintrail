package forensics

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
)

// The capture cadences are constants, not knobs (#703): a 500ms poll makes
// even short-lived sessions likely to be seen at least once, and reading
// performance_schema.threads is an in-memory scan with no disk I/O on the
// source. Retention — how much identity history to keep — is the operator's
// choice (--attribution-retention); how we capture is ours.
const (
	pollInterval    = 500 * time.Millisecond
	cleanupInterval = time.Hour
)

// DefaultRetention is how long connection_cache rows outlive their last
// sighting when the operator does not choose otherwise (epic #701, decision
// D2 — the SaaS agent hardcoded 24h; here it is the --attribution-retention
// default and the sweep window is a parameter).
const DefaultRetention = 24 * time.Hour

// ConnCacheConfig configures one source's connection-identity poller.
type ConnCacheConfig struct {
	// SourceDSN is the watched MySQL server whose performance_schema is
	// polled. Strictly read-only — the poller never writes to the source.
	SourceDSN string
	// IndexDSN is the bintrail index database holding the connection_cache
	// table (created by `bintrail init` / indexer.EnsureSchema).
	IndexDSN string
	// Retention is how long a cached connection outlives its last sighting
	// before the hourly sweep deletes it. Zero or negative disables the
	// poller entirely.
	Retention time.Duration
}

// cachedConn holds a row from performance_schema.threads during polling.
type cachedConn struct {
	id      int64
	user    sql.NullString
	host    sql.NullString
	db      sql.NullString
	command sql.NullString
}

// StartConnCachePoller launches the connection-identity poller: every 500ms
// it reads the source's live foreground sessions (performance_schema.threads
// plus session_connect_attrs, scoped to the ids just seen) and upserts them
// into the connection_cache table in the index DB, so forensic attribution of
// a binlog event's connection_id still resolves after the session disconnects
// — performance_schema rows vanish the moment a session ends. An hourly sweep
// deletes rows unseen for cfg.Retention.
//
// Mirrors rotation.StartLoop's contract: returns immediately and is never
// fatal to the caller — connection failures and per-cycle panics are logged
// and retried, because attribution capture is a secondary job that must never
// take down the stream (the primary forensic capture). When the source has an
// active audit plugin the poller does not poll — audit logs carry better
// session history at lower cost — but it keeps running retention sweeps until
// ctx is cancelled, so connection_cache rows captured before the plugin was
// installed still age out. The loop stops when ctx is cancelled;
// the returned channel closes when it has fully exited (used by tests for
// deterministic shutdown; production callers may ignore it).
func StartConnCachePoller(ctx context.Context, cfg ConnCacheConfig) <-chan struct{} {
	done := make(chan struct{})
	if cfg.Retention <= 0 {
		slog.Info("connection-cache: attribution retention disabled; not caching session identity")
		close(done)
		return done
	}
	go func() {
		defer close(done)

		sourceDB, err := config.Connect(cfg.SourceDSN)
		if err != nil {
			slog.Warn("connection-cache: cannot connect to source; session identity will not be cached this run",
				"error", config.ScrubDSNText(err.Error(), cfg.SourceDSN, cfg.IndexDSN))
			return
		}
		defer sourceDB.Close()
		// Courtesy caps: the poller must never contend with the customer's
		// workload on the source.
		sourceDB.SetMaxOpenConns(2)
		sourceDB.SetMaxIdleConns(1)

		indexDB, err := config.Connect(cfg.IndexDSN)
		if err != nil {
			slog.Warn("connection-cache: cannot connect to index; session identity will not be cached this run",
				"error", config.ScrubDSNText(err.Error(), cfg.SourceDSN, cfg.IndexDSN))
			return
		}
		defer indexDB.Close()
		indexDB.SetMaxOpenConns(2)
		indexDB.SetMaxIdleConns(1)

		if auditProbe(ctx, sourceDB) {
			slog.Info("connection-cache: active audit plugin detected on the source — not polling (the audit log carries better session history at lower cost); running retention sweeps so any pre-audit cached rows still age out")
			// Polling is skipped, but connection_cache rows captured before the
			// audit plugin was installed must still be pruned per the retention
			// window — otherwise they persist forever and tier 2b would
			// attribute old events to a frozen, possibly reused identity.
			sweepLoop(ctx, indexDB, cfg.Retention)
			return
		}

		slog.Info("connection-cache: caching session identity for forensic attribution",
			"poll_interval", pollInterval.String(), "retention", cfg.Retention.String())
		pollLoop(ctx, sourceDB, indexDB, cfg.Retention)
	}()
	return done
}

// pollLoop drives the two tickers — the 500ms thread poll and the hourly
// retention sweep — until ctx is cancelled. Each cycle is recover-guarded so
// a panic can never take down the caller's stream (mirrors rotation.StartLoop).
func pollLoop(ctx context.Context, sourceDB, indexDB *sql.DB, retention time.Duration) {
	var consecutiveFailures int
	poll := func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("connection-cache: poll cycle panicked; polling continues next tick", "panic", r)
			}
		}()
		if err := pollOnce(ctx, sourceDB, indexDB); err != nil && ctx.Err() == nil {
			consecutiveFailures++
			// Log the first failure and then every 60th (≈ every 30s at the
			// 500ms cadence) so an outage doesn't flood the log.
			if consecutiveFailures == 1 || consecutiveFailures%60 == 0 {
				slog.Warn("connection-cache: poll error",
					"consecutive_failures", consecutiveFailures, "error", err)
			}
		} else if err == nil && consecutiveFailures > 0 {
			slog.Info("connection-cache: poll recovered", "after_failures", consecutiveFailures)
			consecutiveFailures = 0
		}
	}
	sweep := func() { sweepConnectionCache(ctx, indexDB, retention) }

	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTicker.C:
			poll()
		case <-cleanupTicker.C:
			sweep()
		}
	}
}

// pollOnce reads the current foreground connections from performance_schema
// and upserts them into connection_cache in the index DB.
func pollOnce(ctx context.Context, sourceDB, indexDB *sql.DB) error {
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 1. Read foreground threads from performance_schema (in-memory, no disk I/O).
	rows, err := sourceDB.QueryContext(pollCtx,
		"SELECT PROCESSLIST_ID, PROCESSLIST_USER, PROCESSLIST_HOST, "+
			"PROCESSLIST_DB, PROCESSLIST_COMMAND "+
			"FROM performance_schema.threads WHERE TYPE = 'FOREGROUND'")
	if err != nil {
		return fmt.Errorf("query threads: %w", err)
	}

	var conns []cachedConn
	for rows.Next() {
		var c cachedConn
		if err := rows.Scan(&c.id, &c.user, &c.host, &c.db, &c.command); err != nil {
			slog.Warn("connection-cache: scan thread row", "error", err)
			continue
		}
		conns = append(conns, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate threads: %w", err)
	}

	if len(conns) == 0 {
		return nil
	}

	// 2. Read session_connect_attrs only for the foreground connections we
	// found — never a blanket scan.
	connIDs := make([]int64, len(conns))
	for i, c := range conns {
		connIDs[i] = c.id
	}
	attrMap := pollConnAttrs(pollCtx, sourceDB, connIDs)

	// 3. Batch upsert into connection_cache.
	return upsertConnections(pollCtx, indexDB, conns, attrMap)
}

// pollConnAttrs reads session_connect_attrs for the given connection IDs and
// returns a map of connection_id → {attr_name: attr_value}. Failures are
// logged, not fatal — identity without client attributes still attributes.
func pollConnAttrs(ctx context.Context, sourceDB *sql.DB, connIDs []int64) map[int64]map[string]string {
	result := map[int64]map[string]string{}
	if len(connIDs) == 0 {
		return result
	}

	placeholders := make([]string, len(connIDs))
	args := make([]any, len(connIDs))
	for i, id := range connIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	rows, err := sourceDB.QueryContext(ctx, fmt.Sprintf(
		"SELECT PROCESSLIST_ID, ATTR_NAME, ATTR_VALUE "+
			"FROM performance_schema.session_connect_attrs "+
			"WHERE PROCESSLIST_ID IN (%s)",
		strings.Join(placeholders, ",")), args...)
	if err != nil {
		slog.Warn("connection-cache: query session_connect_attrs", "error", err)
		return result
	}
	defer rows.Close()

	for rows.Next() {
		var connID int64
		var name, value string
		if err := rows.Scan(&connID, &name, &value); err != nil {
			slog.Warn("connection-cache: scan session_connect_attrs row", "error", err)
			continue
		}
		if result[connID] == nil {
			result[connID] = map[string]string{}
		}
		result[connID][name] = value
	}
	if err := rows.Err(); err != nil {
		slog.Warn("connection-cache: iterate session_connect_attrs", "error", err)
	}
	return result
}

// upsertConnections batch-inserts connections into connection_cache using
// INSERT … ON DUPLICATE KEY UPDATE to minimize round trips.
func upsertConnections(ctx context.Context, indexDB *sql.DB, conns []cachedConn, attrMap map[int64]map[string]string) error {
	const batchSize = 50
	for i := 0; i < len(conns); i += batchSize {
		if err := upsertBatch(ctx, indexDB, conns[i:min(i+batchSize, len(conns))], attrMap); err != nil {
			return err
		}
	}
	return nil
}

func upsertBatch(ctx context.Context, indexDB *sql.DB, conns []cachedConn, attrMap map[int64]map[string]string) error {
	if len(conns) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO connection_cache " +
		"(connection_id, user, host, db, command, connection_attributes, cached_at, last_seen) VALUES ")

	args := make([]any, 0, len(conns)*6)
	for i, c := range conns {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?,?,?,NOW(),NOW())")

		var attrsJSON []byte
		if attrs, ok := attrMap[c.id]; ok && len(attrs) > 0 {
			var merr error
			attrsJSON, merr = json.Marshal(attrs)
			if merr != nil {
				slog.Warn("connection-cache: marshal connection attributes", "connection_id", c.id, "error", merr)
				attrsJSON = nil
			}
		}

		args = append(args, c.id, c.user, c.host, c.db, c.command, attrsJSON)
	}

	sb.WriteString(" ON DUPLICATE KEY UPDATE " +
		"user=VALUES(user), host=VALUES(host), db=VALUES(db), " +
		"command=VALUES(command), connection_attributes=VALUES(connection_attributes), " +
		"last_seen=NOW()")

	_, err := indexDB.ExecContext(ctx, sb.String(), args...)
	return err
}

// sweepConnectionCache runs one retention sweep, panic-recovered so a failure
// never takes down its caller's loop. Shared by pollLoop and sweepLoop.
func sweepConnectionCache(ctx context.Context, indexDB *sql.DB, retention time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("connection-cache: retention sweep panicked; sweep continues next tick", "panic", r)
		}
	}()
	if err := cleanupConnectionCache(ctx, indexDB, retention); err != nil && ctx.Err() == nil {
		slog.Warn("connection-cache: retention sweep error", "error", err)
	}
}

// sweepLoop runs only the retention sweep (no polling) until ctx is cancelled.
// Used when an audit plugin is active on the source: polling is skipped in
// favour of the audit log, but connection_cache rows captured before the plugin
// was installed must still age out per the retention window. It sweeps once
// immediately to prune the pre-audit backlog, then on each cleanup tick.
func sweepLoop(ctx context.Context, indexDB *sql.DB, retention time.Duration) {
	sweepConnectionCache(ctx, indexDB, retention)
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepConnectionCache(ctx, indexDB, retention)
		}
	}
}

// cleanupConnectionCache deletes cached entries whose last_seen timestamp is
// older than the retention window. Active connections keep their last_seen
// fresh via the poller, so only genuinely stale entries are removed.
func cleanupConnectionCache(ctx context.Context, indexDB *sql.DB, retention time.Duration) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	secs := int64(retention.Seconds())
	if secs < 1 {
		// Retention is positive here (StartConnCachePoller gates <= 0 before
		// polling), but under one second int64(Seconds()) truncates to 0 —
		// which would DELETE every row, including live sessions. last_seen is
		// second-precision, so round a sub-second window up to the minimum
		// meaningful value instead of wiping the cache.
		secs = 1
	}
	result, err := indexDB.ExecContext(cleanupCtx,
		"DELETE FROM connection_cache WHERE last_seen < NOW() - INTERVAL ? SECOND",
		secs)
	if err != nil {
		return fmt.Errorf("cleanup query: %w", err)
	}
	if n, _ := result.RowsAffected(); n > 0 {
		slog.Info("connection-cache: retention sweep removed stale entries",
			"rows", n, "retention", retention.String())
	}
	return nil
}

// ---------------------------------------------------------------------------
// Audit plugin check — skip polling when an audit log is available
// ---------------------------------------------------------------------------

// auditProbe is the audit-plugin check the poller runs once at start — a var,
// not a direct call, so tests can exercise the skip branch without installing
// an audit plugin (the rotation.escalateAfter seam pattern).
var auditProbe = hasAuditPlugin

// hasAuditPlugin reports whether an active audit plugin is installed on the
// source MySQL. When one is, the poller should not run: the audit log carries
// better historical connection data at lower cost (doctor and the setup guide,
// #702, carry the remediation). A probe failure returns false — "could not
// check" must degrade to capturing, never to a silent attribution gap.
func hasAuditPlugin(ctx context.Context, sourceDB *sql.DB) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := sourceDB.QueryContext(probeCtx,
		"SELECT PLUGIN_NAME FROM information_schema.PLUGINS "+
			"WHERE UPPER(PLUGIN_NAME) LIKE '%AUDIT%' AND PLUGIN_STATUS = 'ACTIVE'")
	if err != nil {
		slog.Warn("connection-cache: could not check for an audit plugin; poller will start", "error", err)
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			slog.Warn("connection-cache: scan audit plugin row", "error", err)
			continue
		}
		// The RDS internal audit plugin is not queryable via SQL and gives
		// the operator no session history — it must not suppress the poller.
		if strings.Contains(strings.ToUpper(name), "RDS_SECURITY") {
			continue
		}
		slog.Info("connection-cache: audit plugin detected", "plugin", name)
		return true
	}
	if err := rows.Err(); err != nil {
		slog.Warn("connection-cache: audit plugin check failed mid-read; poller will start", "error", err)
	}
	return false
}

// ---------------------------------------------------------------------------
// Cache lookup — the disconnected-session fallback for forensic enrichment
// ---------------------------------------------------------------------------

// CachedThread is one connection identity resolved from the connection_cache
// table — the disconnected-session sibling of a live performance_schema row.
type CachedThread struct {
	ConnectionID int64
	User         string
	Host         string
	DB           string
	Command      string
	ConnAttrs    map[string]string
}

// LookupCachedThreads resolves connection identities from the connection_cache
// table in the index DB — typically for connection IDs no longer present in
// live performance_schema because the session disconnected. Returns a map
// keyed by connection ID; IDs with no cached row are simply absent. An empty
// ids slice returns (nil, nil) without touching the database.
func LookupCachedThreads(ctx context.Context, indexDB *sql.DB, ids []int64) (map[int64]CachedThread, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	rows, err := indexDB.QueryContext(ctx, fmt.Sprintf(
		"SELECT connection_id, user, host, db, command, connection_attributes "+
			"FROM connection_cache WHERE connection_id IN (%s)",
		strings.Join(placeholders, ",")), args...)
	if err != nil {
		return nil, fmt.Errorf("query connection_cache: %w", err)
	}
	defer rows.Close()

	result := map[int64]CachedThread{}
	for rows.Next() {
		var ct CachedThread
		var user, host, dbName, command, attrsJSON sql.NullString
		if err := rows.Scan(&ct.ConnectionID, &user, &host, &dbName, &command, &attrsJSON); err != nil {
			slog.Warn("connection-cache: scan cached row", "error", err)
			continue
		}
		ct.User = user.String
		ct.Host = host.String
		ct.DB = dbName.String
		ct.Command = command.String
		if attrsJSON.Valid && attrsJSON.String != "" {
			var attrs map[string]string
			if err := json.Unmarshal([]byte(attrsJSON.String), &attrs); err != nil {
				slog.Warn("connection-cache: unmarshal connection attributes",
					"connection_id", ct.ConnectionID, "error", err)
			} else {
				ct.ConnAttrs = attrs
			}
		}
		result[ct.ConnectionID] = ct
	}
	return result, rows.Err()
}

// enrichSourceString labels where enrichment data came from: live
// performance_schema, the connection_cache fallback, or both. The enrichment
// engine (#706) surfaces it so an analyst can tell live identities apart from
// ones recovered after a disconnect.
func enrichSourceString(liveCount, cacheCount int) string {
	switch {
	case cacheCount > 0 && liveCount > 0:
		return "performance_schema+connection_cache"
	case cacheCount > 0:
		return "connection_cache"
	default:
		return "performance_schema"
	}
}
