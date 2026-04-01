package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/query"
	"github.com/dbtrail/bintrail/internal/recovery"
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
	// resolve_pk and recover (Parquet-only mode).
	IndexDB *sql.DB

	// SourceDB is the source MySQL connection for forensics queries.
	// Nil disables forensics_query support.
	SourceDB *sql.DB

	// ArchiveSources lists Parquet archive paths (local dirs or s3:// URLs).
	ArchiveSources []string

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
// the local MySQL index and/or Parquet archives.
func (h *DefaultHandler) HandleResolvePK(ctx context.Context, req ResolvePKRequest) ([]PKResult, error) {
	if h.IndexDB == nil && len(h.ArchiveSources) == 0 {
		return nil, fmt.Errorf("no data sources configured (need --index-dsn or --archive-dir/--archive-s3)")
	}

	results := make([]PKResult, len(req.Items))
	for i, item := range req.Items {
		results[i] = PKResult{PKHash: item.PKHash}

		// Try MySQL index first.
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
func (h *DefaultHandler) HandleRecover(ctx context.Context, req RecoverRequest) (string, error) {
	if h.IndexDB == nil && len(h.ArchiveSources) == 0 {
		return "", fmt.Errorf("no data sources configured (need --index-dsn or --archive-dir/--archive-s3)")
	}

	// Build query options from the recover request.
	opts := query.Options{
		Schema: req.Schema,
		Table:  req.Table,
		Since:  &req.TimeStart,
		Until:  &req.TimeEnd,
		Limit:  1000,
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

	// Fetch events from MySQL and/or archives.
	var rows []query.ResultRow
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

	rows = query.MergeResults(rows, opts.Limit)

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
	gen := recovery.New(h.IndexDB, resolver)

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
