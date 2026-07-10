// Package indexer consumes parsed binlog events and batch-inserts them into
// the binlog_events table in the index MySQL database.
package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"unicode/utf8"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// Indexer consumes event.Events from a channel and batch-inserts them into
// the binlog_events table.
type Indexer struct {
	db        *sql.DB
	batchSize int
	onDDL     func(ev event.Event) error
	// digestWarnOnce rate-limits the STATEMENT_DIGEST-unavailable warning to
	// one line per Indexer — without it a non-8.0 index would warn every batch.
	digestWarnOnce sync.Once
	// digestPartialWarnOnce rate-limits the some-statements-failed warning:
	// a systematic-but-partial condition (one application's statements always
	// tripping the digest) must be discoverable without waiting for the
	// all-failed case, but must not warn on every batch either.
	digestPartialWarnOnce sync.Once
	// digestUnavailable short-circuits digesting for the Indexer's lifetime
	// once the index has proven it lacks STATEMENT_DIGEST entirely (MySQL
	// error 1305, unknown function — e.g. a MariaDB index outside the 8.0+
	// contract). Without it a long-lived stream daemon would pay one failed
	// combined SELECT plus N failed per-text SELECTs on every batch, forever,
	// after its single warning line.
	digestUnavailable bool
}

// New creates an Indexer writing to db with the given batch size.
func New(db *sql.DB, batchSize int) *Indexer {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &Indexer{db: db, batchSize: batchSize}
}

// SetOnDDL registers a callback invoked when a DDL event is received.
// The current batch is flushed before the callback is called.
// DDL events are NOT inserted into binlog_events.
func (idx *Indexer) SetOnDDL(fn func(event.Event) error) {
	idx.onDDL = fn
}

// Run reads events from the channel until it is closed or ctx is cancelled,
// flushing to MySQL in batches. Returns the total number of rows inserted.
func (idx *Indexer) Run(ctx context.Context, events <-chan event.Event) (int64, error) {
	batch := make([]event.Event, 0, idx.batchSize)
	var total int64

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := idx.insertBatch(batch)
		if err != nil {
			return err
		}
		total += n
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case ev, ok := <-events:
			if !ok {
				// Channel closed — flush the final partial batch.
				return total, flush()
			}
			// DDL events: flush current batch, invoke callback, skip insertion.
			if ev.EventType == event.EventDDL {
				if err := flush(); err != nil {
					return total, err
				}
				if idx.onDDL != nil {
					if err := idx.onDDL(ev); err != nil {
						return total, fmt.Errorf("onDDL callback: %w", err)
					}
				}
				continue
			}
			batch = append(batch, ev)
			if len(batch) >= idx.batchSize {
				if err := flush(); err != nil {
					return total, err
				}
			}
		}
	}
}

// InsertBatch writes a batch of events and returns the count of rows inserted.
// This exported method allows callers (e.g. the stream command) that need
// manual checkpoint control between batches.
func (idx *Indexer) InsertBatch(batch []event.Event) (int64, error) {
	return idx.insertBatch(batch)
}

// BatchSize returns the configured batch size.
func (idx *Indexer) BatchSize() int {
	return idx.batchSize
}

// insertBatch writes a batch of events in a single multi-row INSERT.
// event_id and pk_hash are omitted — they are AUTO_INCREMENT and STORED
// generated respectively, so MySQL computes them on write.
func (idx *Indexer) insertBatch(batch []event.Event) (int64, error) {
	// 16 placeholders per row
	valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
	insertSQL := `INSERT INTO binlog_events ` +
		`(binlog_file, start_pos, end_pos, event_timestamp, gtid, connection_id, ` +
		`schema_name, table_name, event_type, pk_values, ` +
		`changed_columns, row_before, row_after, schema_version, query_text, query_hash) VALUES ` + valClause

	// Sanitize each event's captured statement text, then resolve the batch's
	// DISTINCT texts to STATEMENT_DIGEST hashes in one round trip (#699).
	// Sanitizing first keeps the stored hash consistent with the stored text.
	sanitized := make([]string, len(batch))
	var distinct []string
	seen := make(map[string]struct{})
	for i := range batch {
		if batch[i].QueryText == "" {
			continue
		}
		sanitized[i] = event.SanitizeQueryText(batch[i].QueryText)
		if _, ok := seen[sanitized[i]]; !ok {
			seen[sanitized[i]] = struct{}{}
			distinct = append(distinct, sanitized[i])
		}
	}
	digests := idx.digestStatements(distinct)

	args := make([]any, 0, len(batch)*16)
	for i := range batch {
		ev := &batch[i]

		changed, err := marshalJSON(event.ChangedColumns(ev.RowBefore, ev.RowAfter))
		if err != nil {
			return 0, fmt.Errorf("marshal changed_columns for %s.%s: %w", ev.Schema, ev.Table, err)
		}
		rowBefore, err := marshalRow(ev.RowBefore)
		if err != nil {
			return 0, fmt.Errorf("marshal row_before for %s.%s: %w", ev.Schema, ev.Table, err)
		}
		rowAfter, err := marshalRow(ev.RowAfter)
		if err != nil {
			return 0, fmt.Errorf("marshal row_after for %s.%s: %w", ev.Schema, ev.Table, err)
		}

		args = append(args,
			ev.BinlogFile,
			ev.StartPos,
			ev.EndPos,
			ev.Timestamp,
			nullOrString(ev.GTID),
			nullOrUint32(ev.ConnectionID),
			ev.Schema,
			ev.Table,
			uint8(ev.EventType),
			ev.PKValues,
			changed,
			rowBefore,
			rowAfter,
			ev.SchemaVersion,
			nullOrString(sanitized[i]),
			nullOrString(digests[sanitized[i]]),
		)
	}

	result, err := idx.db.Exec(insertSQL, args...)
	if err != nil {
		return 0, fmt.Errorf("batch INSERT of %d events failed: %w", len(batch), err)
	}
	n, _ := result.RowsAffected()
	return n, nil
}

// ─── Query-text enrichment (#699) ─────────────────────────────────────────────

// digestStatements resolves distinct statement texts to their
// STATEMENT_DIGEST() hashes on the index connection (STATEMENT_DIGEST exists
// on MySQL 8.0+, the index contract floor). Texts ending in the truncation
// marker are skipped up front: a truncated fragment misrepresents the
// statement's true shape — NULL is the honest value — and it usually ends
// mid-token, failing to parse anyway (MySQL error 3676).
//
// The happy path is ONE combined SELECT over all texts. That SELECT fails as
// a unit when ANY single text is unparseable, so on error it falls back to
// per-text digests — one bad statement can then never null the whole batch's
// hashes. The digest is an enrichment: every failure degrades to a missing
// map entry (NULL query_hash) while query_text is still stored. Failures are
// surfaced without flooding: per-statement details at debug, plus one
// warning per Indexer for the first partial failure and one for the first
// all-failed batch; a missing STATEMENT_DIGEST function (1305) additionally
// disables digesting for the Indexer's lifetime.
func (idx *Indexer) digestStatements(texts []string) map[string]string {
	if idx.digestUnavailable {
		return nil
	}
	candidates := make([]string, 0, len(texts))
	for _, t := range texts {
		if !strings.HasSuffix(t, event.QueryTextTruncationMarker) {
			candidates = append(candidates, t)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	out, combinedErr := idx.digestCombined(candidates)
	if combinedErr == nil {
		return out
	}
	// ER_SP_DOES_NOT_EXIST: the index simply has no STATEMENT_DIGEST function
	// — no per-text retry can succeed now or later. Warn once and stop trying.
	var myErr *mysql.MySQLError
	if errors.As(combinedErr, &myErr) && myErr.Number == 1305 {
		idx.digestUnavailable = true
		idx.digestWarnOnce.Do(func() {
			slog.Warn("STATEMENT_DIGEST is not available on the index connection — query_hash will be NULL for all events (query_text is unaffected)",
				"error", combinedErr)
		})
		return nil
	}
	slog.Debug("combined STATEMENT_DIGEST failed — falling back to per-text digests", "error", combinedErr)

	out = make(map[string]string, len(candidates))
	failures := 0
	var lastErr error
	for _, t := range candidates {
		var v sql.NullString
		if err := idx.db.QueryRow("SELECT STATEMENT_DIGEST(?)", t).Scan(&v); err != nil {
			failures++
			lastErr = err
			slog.Debug("STATEMENT_DIGEST failed for one statement — query_hash stays NULL for it",
				"error", err, "statement_prefix", truncateForLog(t))
			continue
		}
		if v.Valid {
			out[t] = v.String
		}
	}
	switch {
	case failures == len(candidates):
		idx.digestWarnOnce.Do(func() {
			slog.Warn("STATEMENT_DIGEST failed for every statement in a batch — query_hash will be NULL (query_text is unaffected)",
				"error", lastErr)
		})
	case failures > 0:
		// Partial failure: a systematic condition affecting one application's
		// statements would otherwise stay invisible until someone notices
		// NULL hashes in query results months later.
		idx.digestPartialWarnOnce.Do(func() {
			slog.Warn("STATEMENT_DIGEST failed for some statements — their query_hash stays NULL (per-statement details at debug level; this warning prints once)",
				"failed", failures, "of", len(candidates), "error", lastErr)
		})
	}
	return out
}

// truncateForLog bounds a statement text for a debug log line, cutting at a
// rune boundary (the input is sanitized UTF-8; keep it valid).
func truncateForLog(s string) string {
	const max = 120
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// digestCombined runs the single-round-trip form: one SELECT with one
// STATEMENT_DIGEST expression per text.
func (idx *Indexer) digestCombined(texts []string) (map[string]string, error) {
	var sb strings.Builder
	sb.WriteString("SELECT STATEMENT_DIGEST(?)")
	for range len(texts) - 1 {
		sb.WriteString(", STATEMENT_DIGEST(?)")
	}
	args := make([]any, len(texts))
	for i, t := range texts {
		args[i] = t
	}
	vals := make([]sql.NullString, len(texts))
	ptrs := make([]any, len(texts))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := idx.db.QueryRow(sb.String(), args...).Scan(ptrs...); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(texts))
	for i, t := range texts {
		if vals[i].Valid {
			out[t] = vals[i].String
		}
	}
	return out, nil
}

// ─── Serialisation helpers ────────────────────────────────────────────────────

// marshalRow encodes a named row map to JSON, returning nil for a nil map.
// []byte values that contain a JSON object/array (e.g. from MySQL JSON
// columns) are embedded as raw JSON rather than base64-encoded.
func marshalRow(row map[string]any) ([]byte, error) {
	if row == nil {
		return nil, nil
	}
	// Promote valid-JSON []byte values to json.RawMessage so they are embedded
	// rather than base64-encoded in the output JSON. Gated on
	// looksLikeJSONContainer, not bare json.Valid: go-mysql delivers
	// TEXT/BLOB columns as []byte too (MySQL's binlog row format has no
	// separate wire type for JSON vs. TEXT/BLOB), so a plain TEXT value that
	// happens to be the literal string "false"/"true"/"null" or a bare
	// numeric string ("0", "123") is also valid JSON — json.Valid alone would
	// silently turn that string into a JSON bool/null/number, corrupting it
	// (#736). Restricting to object/array payloads mirrors
	// query.looksLikeJSONContainer, which guards the same ambiguity on the
	// baseline/Parquet read side.
	//
	// go-mysql itself delivers only BLOB/TEXT, JSON, and GEOMETRY columns as
	// []byte (VARCHAR/CHAR/VARBINARY/BINARY arrive as Go string from go-mysql;
	// ENUM/SET as int64) — but metadata.MapRow now reinterprets BINARY/
	// VARBINARY as []byte too (#756, before this function ever sees the row),
	// so by the time a row reaches marshalRow, []byte covers those five kinds.
	// They all go through the same looksLikeJSONContainer/json.Valid gate
	// above: a BINARY/VARBINARY value whose first non-whitespace byte happens
	// to be '{'/'[' and whose full content happens to be valid JSON would be
	// promoted too, same residual ambiguity as a TEXT/BLOB value that
	// genuinely looks like JSON (documented below) — astronomically unlikely
	// for real binary content, and not fixable from bytes alone.
	//
	// Residual, accepted ambiguity (not fixed here, same limit as the
	// baseline-side guard above): a plain TEXT/BLOB value whose content
	// genuinely LOOKS like a JSON object/array (e.g. literal text
	// `{"a":1}`) is still promoted, same as before. Resolving this fully
	// would require tagging each captured value with its real column type
	// at the point it's read (available via metadata.Resolver.MapRow, but
	// not carried through to here) rather than guessing from content —
	// a deeper fix, out of scope for #736's reported corruption class.
	normalized := make(map[string]any, len(row))
	for k, v := range row {
		if b, ok := v.([]byte); ok && looksLikeJSONContainer(b) && json.Valid(b) {
			normalized[k] = json.RawMessage(b)
		} else {
			normalized[k] = v
		}
	}
	return json.Marshal(normalized)
}

// looksLikeJSONContainer reports whether b's first non-whitespace byte is '{'
// or '[' — a cheap prefix test that distinguishes JSON object/array payloads
// from bare string/numeric/bool/null literals that json.Valid would also
// accept. Deliberate duplicate of query.looksLikeJSONContainer (same
// rationale, different package — not worth a shared dependency for one
// pure function).
func looksLikeJSONContainer(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}

// marshalJSON encodes v to JSON, returning nil if v is nil.
func marshalJSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

// nullOrString returns nil when s is empty (stored as SQL NULL), else s.
func nullOrString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullOrUint32 returns nil when v is 0 (stored as SQL NULL), else v.
func nullOrUint32(v uint32) any {
	if v == 0 {
		return nil
	}
	return v
}

// EnsureSchema adds any columns introduced after the initial schema to
// binlog_events, schema_snapshots, and stream_state. It is idempotent — safe
// to call on every startup.
func EnsureSchema(db *sql.DB) error {
	if err := ensureColumn(db, "binlog_events", "connection_id",
		`ALTER TABLE binlog_events ADD COLUMN connection_id INT UNSIGNED DEFAULT NULL COMMENT 'MySQL connection ID (pseudo_thread_id) that produced this event' AFTER gtid`,
	); err != nil {
		return err
	}
	// column_type carries the full type declaration (e.g. "datetime(6)") so
	// full-table reconstruct (#187, #212) can tell the declared fractional
	// precision of DATETIME/TIMESTAMP PK columns, and so the shim can map
	// ENUM/SET ordinals back to labels (#472). Without this the PK
	// canonicalizer cannot distinguish DATETIME(0) from DATETIME(6) with
	// whole-second values.
	if err := ensureColumn(db, "schema_snapshots", "column_type",
		`ALTER TABLE schema_snapshots ADD COLUMN column_type TEXT DEFAULT NULL COMMENT 'full type from information_schema.COLUMNS.COLUMN_TYPE' AFTER data_type`,
	); err != nil {
		return err
	}
	// #212 created column_type as VARCHAR(128), which a realistic ENUM
	// declaration exceeds — and under strict mode the resulting 1406
	// ("Data too long") aborts the ENTIRE snapshot transaction, not just
	// one column. Widen pre-existing installs to TEXT (#472). Existing
	// values are preserved; the resolver already COALESCEs NULL to ''.
	if err := ensureColumnWidened(db, "schema_snapshots", "column_type", "text",
		`ALTER TABLE schema_snapshots MODIFY COLUMN column_type TEXT DEFAULT NULL COMMENT 'full type from information_schema.COLUMNS.COLUMN_TYPE'`,
	); err != nil {
		return err
	}
	// character_set_name carries information_schema.COLUMNS.CHARACTER_SET_NAME
	// (#756): NULL for BINARY/VARBINARY/numeric columns, populated for CHAR/
	// VARCHAR. go-mysql delivers those four types as a raw Go string with no
	// charset applied, and an invalid-UTF-8 value (a legacy latin1 table, a
	// binary UUID/digest) previously reached json.Marshal, which silently
	// replaces bad bytes with U+FFFD — at-rest data loss. Capturing the charset
	// lets MapRow transcode a latin1 CHAR/VARCHAR value to UTF-8 (MySQL's
	// "latin1" is actually cp1252) and fail loud instead of guess for any other
	// non-UTF-8 charset; a pre-#756 snapshot has this column NULL, so its
	// CHAR/VARCHAR columns keep failing loud on invalid UTF-8 until re-snapshotted.
	if err := ensureColumn(db, "schema_snapshots", "character_set_name",
		`ALTER TABLE schema_snapshots ADD COLUMN character_set_name VARCHAR(32) DEFAULT NULL COMMENT 'information_schema.COLUMNS.CHARACTER_SET_NAME; NULL for BINARY/VARBINARY/numeric columns; enables safe latin1-to-UTF8 transcoding of CHAR/VARCHAR values at index time instead of silent U+FFFD corruption (#756)' AFTER column_type`,
	); err != nil {
		return err
	}
	// pg_type_oid/pg_type_mod carry the PostgreSQL per-column type identity (pg_type
	// OID + atttypmod) from a pgoutput RelationMessage (#533). They are captured at
	// stream time because the offline recover path has no live PostgreSQL catalog to
	// rebuild them from. Nullable: MySQL snapshots leave them NULL (MySQL uses
	// data_type/column_type). WritePGSnapshot writes them; the type-faithful renderer
	// that reads them is a later #533 slice.
	if err := ensureColumn(db, "schema_snapshots", "pg_type_oid",
		`ALTER TABLE schema_snapshots ADD COLUMN pg_type_oid INT UNSIGNED DEFAULT NULL COMMENT 'PostgreSQL pg_type OID (pgoutput RelationMessage); NULL for MySQL snapshots (#533)' AFTER is_generated`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "schema_snapshots", "pg_type_mod",
		`ALTER TABLE schema_snapshots ADD COLUMN pg_type_mod INT DEFAULT NULL COMMENT 'PostgreSQL atttypmod (pgoutput RelationMessage); NULL for MySQL snapshots (#533)' AFTER pg_type_oid`,
	); err != nil {
		return err
	}
	// is_identity_always marks a PostgreSQL GENERATED ALWAYS AS IDENTITY column (#557).
	// Recovery emits OVERRIDING SYSTEM VALUE on a reverse-INSERT and omits the column
	// from a reverse-UPDATE SET (PostgreSQL rejects SET on it). NOT NULL DEFAULT 0 so
	// existing rows + MySQL snapshots read back as "not identity" with no migration.
	if err := ensureColumn(db, "schema_snapshots", "is_identity_always",
		`ALTER TABLE schema_snapshots ADD COLUMN is_identity_always TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1 if PostgreSQL GENERATED ALWAYS AS IDENTITY; 0 for MySQL (#557)' AFTER pg_type_mod`,
	); err != nil {
		return err
	}
	// query_text/query_hash carry the original SQL statement that produced each
	// row event (#699): the text from the binlog's ROWS_QUERY/ANNOTATE_ROWS
	// event (opt-in on the source via binlog_rows_query_log_events /
	// binlog_annotate_row_events), and its STATEMENT_DIGEST computed on the
	// index connection at insert time. Nullable: rows indexed before this
	// column existed, or while capture is off at the source, read back NULL.
	// event.SanitizeQueryText caps every text at event.MaxQueryTextBytes
	// (16 KiB) before it reaches this column; MEDIUMTEXT (not TEXT) is
	// headroom on top of that cap, so raising the cap — or any future path
	// that bypasses sanitization — cannot turn into a strict-mode 1406 that
	// aborts a whole batch INSERT.
	if err := ensureColumn(db, "binlog_events", "query_text",
		`ALTER TABLE binlog_events ADD COLUMN query_text MEDIUMTEXT DEFAULT NULL COMMENT 'original SQL statement from ROWS_QUERY/ANNOTATE_ROWS; NULL unless binlog_rows_query_log_events (MySQL) / binlog_annotate_row_events (MariaDB) is ON at the source (#699)' AFTER schema_version`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "binlog_events", "query_hash",
		`ALTER TABLE binlog_events ADD COLUMN query_hash CHAR(64) DEFAULT NULL COMMENT 'STATEMENT_DIGEST(query_text) computed on the index connection at index time; groups statements by normalized shape (#699)' AFTER query_text`,
	); err != nil {
		return err
	}
	// gap_lost_at/_detail record an unfillable-gap auto-advance durably
	// (#402): the advanced checkpoint is persisted, so without these columns
	// the only trace of the permanently lost events would be an in-memory
	// flag that a daemon restart silently discards.
	if err := ensureColumn(db, "stream_state", "gap_lost_at",
		`ALTER TABLE stream_state ADD COLUMN gap_lost_at DATETIME DEFAULT NULL COMMENT 'when an unfillable binlog gap forced an auto-advance (events permanently lost); cleared by an explicit monitor Stop or --reset' AFTER bintrail_id`,
	); err != nil {
		return err
	}
	if err := ensureColumn(db, "stream_state", "gap_lost_detail",
		`ALTER TABLE stream_state ADD COLUMN gap_lost_detail TEXT DEFAULT NULL COMMENT 'human-readable description of the lost gap' AFTER gap_lost_at`,
	); err != nil {
		return err
	}
	// source_health holds the latest source-side health snapshot a streaming
	// daemon polls (#599): for PostgreSQL, replication-slot wal_status/lag and
	// REPLICA IDENTITY coverage, with an embedded checked_at so the index-only
	// console can show staleness. Source-agnostic JSON payload (one index schema
	// for all source families); NULL on every index no daemon has polled.
	if err := ensureColumn(db, "stream_state", "source_health",
		`ALTER TABLE stream_state ADD COLUMN source_health JSON DEFAULT NULL COMMENT 'latest source-side health snapshot (PostgreSQL: replication-slot wal_status/lag + REPLICA IDENTITY coverage) with an embedded checked_at; serialized payload, source-agnostic column' AFTER gap_lost_detail`,
	); err != nil {
		return err
	}
	// flavor records the source database flavor (mysql/mariadb) so a resume
	// parses the saved gtid_set with the correct GTID parser. NOT NULL DEFAULT
	// 'mysql' means existing rows read back as mysql with no data migration,
	// keeping every pre-MariaDB install unchanged.
	if err := ensureColumn(db, "stream_state", "flavor",
		`ALTER TABLE stream_state ADD COLUMN flavor VARCHAR(16) NOT NULL DEFAULT 'mysql' COMMENT 'source flavor: mysql or mariadb; selects the GTID parser on resume' AFTER gtid_set`,
	); err != nil {
		return err
	}
	// delete_rule/update_rule carry each FK's referential action so cascade
	// recovery can tell which edges are ON DELETE CASCADE at recovery time.
	// `recover` is source-less, so the rule must live in the index rather than
	// be re-queried from the source. NOT NULL DEFAULT '' means pre-existing
	// rows read back as "unknown" (treated as non-cascade) with no data
	// migration; a fresh snapshot of the schema populates the real rule.
	//
	// fk_constraints post-dates the original schema and may be absent on very
	// old indexes (TakeSnapshot tolerates its absence). Only migrate it when
	// present; `bintrail init` (the DDL) creates the table with these columns,
	// and a snapshot then populates the rule once the table exists. The block is
	// gated by `if hasFK` (rather than an early return) so any migration added
	// after it still runs on an index that predates fk_constraints.
	hasFK, err := tableExists(db, "fk_constraints")
	if err != nil {
		return err
	}
	if hasFK {
		if err := ensureColumn(db, "fk_constraints", "delete_rule",
			`ALTER TABLE fk_constraints ADD COLUMN delete_rule VARCHAR(16) NOT NULL DEFAULT '' COMMENT 'ON DELETE rule (CASCADE/RESTRICT/SET NULL/NO ACTION); empty for pre-cascade-recovery snapshots' AFTER referenced_column_name`,
		); err != nil {
			return err
		}
		if err := ensureColumn(db, "fk_constraints", "update_rule",
			`ALTER TABLE fk_constraints ADD COLUMN update_rule VARCHAR(16) NOT NULL DEFAULT '' COMMENT 'ON UPDATE rule; empty for pre-cascade-recovery snapshots' AFTER delete_rule`,
		); err != nil {
			return err
		}
	}
	// connection_cache post-dates the original schema (#703): the forensics
	// poller persists session identity here so attribution survives
	// disconnects. A whole-table migration, so it is presence-checked first —
	// an up-to-date index keeps EnsureSchema write-free (ensureColumn's
	// contract), which matters for index users without CREATE privilege.
	hasConnCache, err := tableExists(db, "connection_cache")
	if err != nil {
		return err
	}
	if !hasConnCache {
		if _, err := db.Exec(ddlConnectionCache); err != nil {
			return fmt.Errorf("failed to create connection_cache: %w", err)
		}
	}
	// snapshot_id_seq post-dates the original schema (#844): a dedicated
	// AUTO_INCREMENT counter table that lets metadata.TakeSnapshot/
	// WritePGSnapshot allocate snapshot_id values without the deadlock-prone
	// `MAX(snapshot_id)+1 FOR UPDATE` pattern it replaces (metadata.
	// DDLSnapshotIDSeq has the full rationale). metadata.go's own callers
	// also self-heal this table lazily on first use — the standalone
	// `bintrail snapshot` command does not go through EnsureSchema — but
	// creating it eagerly here means the daemon-driven writers named in
	// #844 (the DDL hook, the console baseline trigger) never hit that lazy
	// path under load.
	hasSnapshotIDSeq, err := tableExists(db, "snapshot_id_seq")
	if err != nil {
		return err
	}
	if !hasSnapshotIDSeq {
		if _, err := db.Exec(metadata.DDLSnapshotIDSeq); err != nil {
			return fmt.Errorf("failed to create snapshot_id_seq: %w", err)
		}
	}
	return nil
}

// WrapSchemaMigrationErr rewrites an EnsureSchema failure for the READ plane
// (query, recover, verify, shim, reconstruct, whochanged, recover-cascade,
// the MCP tools) into an actionable error.
//
// EnsureSchema issues ALTER TABLE / CREATE TABLE statements, which a
// SELECT-only DSN — the natural pairing with the RBAC --profile feature —
// lacks the privilege for. MySQL then returns errno 1142
// (ER_TABLEACCESS_DENIED_ERROR) or 1044 (ER_DBACCESS_DENIED_ERROR), whose raw
// message names neither the privilege cause nor the fix, so the read plane
// refuses to read data it could otherwise serve with an unhelpful error.
// Detect those two errors specifically and rewrite them to name both cause
// and fix; every other EnsureSchema failure (connection drop, context
// deadline, etc.) is not a privilege issue and passes through with a plain
// "schema migration" wrap so it isn't misdiagnosed.
//
// The CAPTURE plane (index, stream, agent, rotate) legitimately requires a
// privileged DSN and must keep failing hard on any EnsureSchema error — those
// call sites must NOT use this helper.
func WrapSchemaMigrationErr(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1142 || mysqlErr.Number == 1044) {
		return fmt.Errorf("index schema is out of date and this user's DSN cannot migrate it (missing ALTER/CREATE privilege) — run any capture-plane command (index, stream, agent, or rotate) once with a privileged DSN to bring the schema up to date: %w", err)
	}
	return fmt.Errorf("schema migration: %w", err)
}

// tableExists reports whether a base table named `table` exists in the current
// database.
func tableExists(db *sql.DB, table string) (bool, error) {
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table).Scan(&n); err != nil {
		return false, fmt.Errorf("check table %s: %w", table, err)
	}
	return n > 0, nil
}

// ensureColumnWidened runs an idempotent ALTER TABLE MODIFY COLUMN: it
// checks the column's current information_schema DATA_TYPE and bails out
// when it already matches wantDataType (lowercase, e.g. "text"). A column
// that does not exist at all is also a no-op — ensureColumn owns creation;
// this helper only ever widens an existing one.
func ensureColumnWidened(db *sql.DB, table, column, wantDataType, alterSQL string) error {
	var dataType string
	err := db.QueryRow(`SELECT DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME   = ?
		  AND COLUMN_NAME  = ?`, table, column).Scan(&dataType)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check %s.%s type: %w", table, column, err)
	}
	if strings.EqualFold(dataType, wantDataType) {
		return nil
	}
	if _, err := db.Exec(alterSQL); err != nil {
		return fmt.Errorf("widen %s.%s to %s: %w", table, column, wantDataType, err)
	}
	return nil
}

// ensureColumn runs an idempotent ALTER TABLE ADD COLUMN: checks
// information_schema, bails out if the column already exists, and swallows
// the "duplicate column" error if a concurrent process added it between our
// check and the ALTER.
func ensureColumn(db *sql.DB, table, column, alterSQL string) error {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME   = ?
		  AND COLUMN_NAME  = ?`, table, column).Scan(&count)
	if err != nil {
		return fmt.Errorf("check %s.%s column: %w", table, column, err)
	}
	if count > 0 {
		return nil
	}
	if _, err := db.Exec(alterSQL); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1060 {
			return nil
		}
		return fmt.Errorf("add %s.%s column: %w", table, column, err)
	}
	return nil
}

// InsertSchemaChange records a DDL detection in the schema_changes table.
// snapshotID may be nil when no auto-snapshot was taken (file mode).
func InsertSchemaChange(db *sql.DB, ev event.Event, snapshotID *int) error {
	var snapArg any
	if snapshotID != nil {
		snapArg = *snapshotID
	}
	_, err := db.Exec(`
		INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, gtid, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.Timestamp, ev.BinlogFile, ev.EndPos,
		nullOrString(ev.GTID), ev.Schema, ev.Table, ev.DDLType, ev.DDLQuery, snapArg)
	return err
}
