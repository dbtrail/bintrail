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
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// allowedForensicsQueries maps predefined query identifiers to safe SQL
// that runs against performance_schema and information_schema. Only these
// are allowed — the agent never executes arbitrary SQL from dbtrail.
// DO NOT modify at runtime — this is a security boundary.
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
	// agent's --source-dsn). The agent runner forwards it to extension-
	// registered commands via ext.AgentDeps. Empty when no source is
	// configured.
	SourceHost string

	// ArchiveSources lists Parquet archive paths (local dirs or s3:// URLs).
	ArchiveSources []string

	// Buffer is the in-memory event buffer for BYOS mode. When set,
	// resolve_pk and recover check the buffer first (fastest path).
	Buffer *buffer.Buffer

	// ArchiveFetcher fetches events from one Parquet archive source. Nil →
	// parquetquery.Fetch (the container-safe DuckDB budget). A seam for tests.
	ArchiveFetcher query.ArchiveFetcher

	// Logger for handler operations. Nil falls back to slog.Default().
	Logger *slog.Logger
}

func (h *DefaultHandler) archiveFetcher() query.ArchiveFetcher {
	if h.ArchiveFetcher != nil {
		return h.ArchiveFetcher
	}
	return parquetquery.Fetch
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

	// pk_hash → pk_values per (source, schema, table), built from ONE
	// full-table archive fetch and reused for every batch item (#818):
	// Parquet archives have no SHA2 index, so the archive fallback scans
	// the whole table client-side — doing that per item multiplied the
	// full-table fetch by the batch size.
	archiveIdx := make(map[archiveTableKey]map[string]string)

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
			pkVal, err := h.resolvePKFromArchive(ctx, item, src, archiveIdx)
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
// Note: the standard PK lookup pattern pairs pk_hash = SHA2(?, 256) with
// pk_values = ? as a collision guard. Here pk_values is the ANSWER, not an
// input, so the guard cannot be applied and the hash stands alone. SHA-256
// collisions are astronomically unlikely; callers should verify results when
// critical.
//
// schema_name and table_name are still passed, and are not optional: they
// lead idx_pk_hash, so dropping them would turn this seek into a full scan.
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

// archiveTableKey identifies one archived table within one archive source —
// the memoization unit for resolve_pk's client-side hash index.
type archiveTableKey struct {
	source, schema, table string
}

// resolvePKFromArchive resolves a pk_hash against Parquet archive rows.
// Since Parquet files have no SHA2 index, the first lookup for a
// (source, schema, table) fetches all of the table's rows, hashes pk_values
// client-side into cache, and every later item in the batch resolves against
// that map instead of re-fetching the whole table (#818). Fetch errors are
// not cached — a later item retries the source.
func (h *DefaultHandler) resolvePKFromArchive(ctx context.Context, item PKItem, source string, cache map[archiveTableKey]map[string]string) (string, error) {
	key := archiveTableKey{source: source, schema: item.Schema, table: item.Table}
	idx, ok := cache[key]
	if !ok {
		opts := query.Options{
			Schema: item.Schema,
			Table:  item.Table,
			Limit:  0, // no limit — need every row's pk_values to hash
		}
		rows, err := h.archiveFetcher()(ctx, opts, source)
		if err != nil {
			return "", err
		}
		idx = make(map[string]string, len(rows))
		for _, r := range rows {
			hash := byosPKHash(r.PKValues)
			if _, seen := idx[hash]; !seen { // first match wins, as the pre-memoization scan did
				idx[hash] = r.PKValues
			}
		}
		// #1137 compat, second pass: a row persisted before the #1132 hex fix
		// stores the RAW spelling of a binary PK, whose hash can never match a
		// control-plane PKHash computed over the post-fix hex spelling. Index
		// the canonical spelling's hash as an ALIAS too, still resolving to
		// the stored value. Aliases are added only after every exact
		// stored-spelling hash is in, so an exact match always beats an alias
		// regardless of scan order (a raw row's alias could otherwise shadow
		// another row literally storing that hex text). No second hash is
		// computed when the spellings already match (the common case).
		for _, r := range rows {
			if canon := event.CanonicalPKValues(r.PKValues); canon != r.PKValues {
				canonHash := byosPKHash(canon)
				if _, seen := idx[canonHash]; !seen {
					idx[canonHash] = r.PKValues
				}
			}
		}
		cache[key] = idx
	}
	return idx[item.PKHash], nil
}

// recoverEventLimit caps the number of events a single recover call may
// reverse into one script. #763: silently trimming to this cap and
// generating reversal SQL from the truncated set produces a script the
// caller believes reverses the whole requested scope when it doesn't — the
// remaining events are left half-reverted with no signal. We instead fetch
// one row past the cap so an over-scoped request can be rejected outright
// (see the fetch+check below) rather than silently emitting a partial script.
const recoverEventLimit = 1000

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
		// Fetch one past the cap so we can detect (and reject) an
		// over-scoped request instead of silently truncating — see
		// recoverEventLimit and the check below.
		Limit: recoverEventLimit + 1,
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
	if len(h.ArchiveSources) > 0 && h.IndexDB != nil {
		// Misfiled archives (#1037): archives whose content-derived range
		// (archive_state.min/max_event_ts) overlaps the window despite an
		// out-of-range hour label must survive date/file pruning. Best-effort:
		// without an index DB (archive-only handlers) this stays nil and
		// pruning falls back to labels.
		// AllArchives (#1232). Misfiled hours are a WIDENING hint — "do not
		// prune these files" — so an over-broad set costs a few unpruned
		// reads while a narrow one silently skips backfilled rows. Unscoped is
		// the safe direction for a hint, the opposite of coverage.
		hours, mErr := query.MisfiledArchiveHours(ctx, h.IndexDB, opts.Since, opts.Until, query.AllArchives())
		if mErr != nil {
			h.logger().Warn("could not check archive_state for misfiled archives", "error", mErr)
		} else {
			opts.ExtraArchiveHours = hours
		}
	}
	for _, src := range h.ArchiveSources {
		r, err := parquetquery.Fetch(ctx, opts, src)
		if err != nil {
			h.logger().Warn("archive query failed, skipping", "source", src, "error", err)
			continue
		}
		rows = append(rows, r...)
	}

	// Dedup+sort without capping yet — capping here would hide the very
	// overflow we need to detect (#763).
	rows = query.MergeResults(rows, 0, opts.Order)
	if len(rows) > recoverEventLimit {
		return "", fmt.Errorf("recover scope matches more than %d events; narrow the time range/GTID or split the recovery into smaller windows (refusing to emit a partial reversal script)", recoverEventLimit)
	}

	// Filter to requested pk_hashes if specified.
	if len(req.PKHashes) > 0 {
		wanted := make(map[string]struct{}, len(req.PKHashes))
		for _, ph := range req.PKHashes {
			wanted[ph] = struct{}{}
		}
		filtered := rows[:0]
		for _, r := range rows {
			_, ok := wanted[byosPKHash(r.PKValues)]
			if !ok {
				// #1137 compat: an archive row persisted before the #1132 hex
				// fix stores the RAW spelling of a binary PK; the caller's
				// pk_hash is computed over the post-fix hex spelling. When the
				// spellings differ, also try the canonical spelling's hash.
				// In the residual collision formatPKValue's doc already
				// accepts (a VARBINARY PK literally holding the ASCII text of
				// another key's hex spelling), this OR of both hashes selects
				// BOTH rows, so the reversal can include a row the caller did
				// not name — that accepted ambiguity is made reachable here
				// for pre-fix raw-spelling rows.
				if canon := event.CanonicalPKValues(r.PKValues); canon != r.PKValues {
					_, ok = wanted[byosPKHash(canon)]
				}
			}
			if ok {
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

// byosPKHash computes SHA-256 hex digest of pkValues, matching the
// byos.PKHash function and MySQL's SHA2(pk_values, 256).
func byosPKHash(pkValues string) string {
	h := sha256.Sum256([]byte(pkValues))
	return hex.EncodeToString(h[:])
}
