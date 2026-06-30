// Package recovery generates reversal SQL from indexed binlog events.
// It reads events via the query engine and emits a transaction-wrapped SQL
// script that undoes each event in reverse chronological order.
package recovery

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Dialect selects the SQL dialect for generated reversal SQL. The index is
// single-source (stream_state.flavor), so the dialect is decided ONCE at
// construction from that authoritative signal — never inferred per-row from
// resolver/type presence, which would silently emit the wrong dialect for a row
// whose snapshot failed to load (the all-columns fallback path).
type Dialect int

const (
	// MySQLDialect emits MySQL/MariaDB SQL: backtick identifiers, X'..' hex blobs,
	// backslash string escaping. The default, and the only dialect the MySQL-source
	// path — and the reconstruct mydumper writer, via the exported FormatSQLValue/
	// QuoteName — has ever produced.
	MySQLDialect Dialect = iota
	// PostgresDialect emits PostgreSQL SQL: double-quoted identifiers and
	// standard-conforming-string escaping ('' doubling, no backslash). Values
	// captured from pgoutput are already in PostgreSQL's canonical text form, so a
	// quoted literal coerces into the target column's type on INSERT/UPDATE/WHERE —
	// the dialect difference is identifier quoting + string escaping, not per-type
	// literal forms (#533).
	PostgresDialect
)

// Generator produces reversal SQL from indexed binlog events.
type Generator struct {
	db       *sql.DB
	resolver *metadata.Resolver            // default resolver (latest snapshot); may be nil
	cache    map[uint32]*metadata.Resolver // per-snapshot resolvers, loaded lazily
	dialect  Dialect
	// maxScriptBytes bounds the estimated row payload GenerateSQLFromRows will
	// render before it refuses (#654). 0 = unlimited. Defaults to
	// DefaultMaxScriptBytes in the constructors so every caller — CLI, the MCP
	// recover tool, the console, recover-cascade — is guarded with zero config;
	// the recover CLI overrides it via SetMaxScriptBytes (--max-script-bytes).
	maxScriptBytes int64
}

// New creates a Generator emitting MySQL-dialect SQL. resolver may be nil — in that
// case, WHERE clauses for UPDATE and DELETE reversals will use ALL row columns
// instead of just PKs.
func New(db *sql.DB, resolver *metadata.Resolver) *Generator {
	return &Generator{db: db, resolver: resolver, dialect: MySQLDialect, maxScriptBytes: DefaultMaxScriptBytes}
}

// NewForDialect is New with an explicit SQL dialect. Callers that know the source
// flavor (e.g. `bintrail-pg recover` via DialectForIndex) pass PostgresDialect;
// everything else uses New (MySQL).
func NewForDialect(db *sql.DB, resolver *metadata.Resolver, dialect Dialect) *Generator {
	return &Generator{db: db, resolver: resolver, dialect: dialect, maxScriptBytes: DefaultMaxScriptBytes}
}

// DefaultMaxScriptBytes is the default ceiling on the estimated row payload of a
// reversal script. Above it, GenerateSQLFromRows refuses rather than buffering
// the whole script into RAM (#654): rendering builds a second full copy on top
// of the already-resident events, so a multi-gigabyte script roughly doubles
// peak memory. 2 GiB is deliberately generous — an ordinary recover renders
// kilobytes to megabytes, and a 2 GiB SQL script is already past being
// reviewable or appliable; only BLOB/TEXT-heavy windows approach it. The recover
// CLI exposes --max-script-bytes / BINTRAIL_RECOVER_MAX_BYTES to raise it or set
// 0 to disable. The bound is on the *rendered script*, not end-to-end memory:
// the events are fetched (row-count-bounded by --limit) before this runs, so it
// caps the render doubling, not the initial fetch.
const DefaultMaxScriptBytes int64 = 2 << 30

// SetMaxScriptBytes overrides the reversal-script payload budget enforced by
// GenerateSQLFromRows. n <= 0 disables the guard (unlimited). The recover CLI
// sets it from --max-script-bytes; other callers keep DefaultMaxScriptBytes.
func (g *Generator) SetMaxScriptBytes(n int64) { g.maxScriptBytes = n }

// CheckScriptBudget returns a *ScriptBudgetError when rendering rows would exceed
// the configured script-size budget (#654), or nil when within budget (or the
// budget is disabled). GenerateSQLFromRows calls it before rendering; callers
// that write their own preamble BEFORE GenerateSQLFromRows — e.g.
// cascaderecover.EmitSQL — call it first so a refusal writes nothing instead of
// leaving a dangling header (and, for cascade, a `SET FOREIGN_KEY_CHECKS=0`).
func (g *Generator) CheckScriptBudget(rows []query.ResultRow) error {
	if g.maxScriptBytes <= 0 {
		return nil
	}
	if est := EstimateScriptBytes(rows); est > g.maxScriptBytes {
		return &ScriptBudgetError{EstimatedBytes: est, Budget: g.maxScriptBytes}
	}
	return nil
}

// DialectForFlavor maps a stream_state.flavor value to the recovery SQL dialect.
// PostgreSQL gets its own dialect; MySQL, MariaDB, and an empty/unknown flavor all
// use MySQL (the established default — MariaDB recovery SQL is MySQL-dialect). It
// owns the canonical "postgres" flavor literal so callers don't re-derive it.
func DialectForFlavor(flavor string) Dialect {
	if flavor == "postgres" {
		return PostgresDialect
	}
	return MySQLDialect
}

// DialectForIndex returns the recovery dialect for an index database, read from the
// source flavor recorded in stream_state (the index is single-source). Best-effort:
// a nil db, or any read failure (no stream_state row on a file-indexed DB, very old
// schema), returns MySQLDialect and never blocks recovery. This is the authoritative
// selection every recover surface uses (cli/recover.go, console, MCP, agent). The nil
// guard lets a caller pass an as-yet-unopened handle (e.g. agent.IndexDB) directly.
func DialectForIndex(db *sql.DB) Dialect {
	if db == nil {
		return MySQLDialect
	}
	var flavor string
	if err := db.QueryRow("SELECT flavor FROM stream_state WHERE id = 1").Scan(&flavor); err != nil {
		return MySQLDialect
	}
	return DialectForFlavor(flavor)
}

// resolverForRow returns the resolver matching the row's schema version.
// It loads resolvers lazily and caches them. Falls back to the default
// resolver for SchemaVersion=0 (pre-migration data) or on load failure.
func (g *Generator) resolverForRow(row query.ResultRow) *metadata.Resolver {
	if row.SchemaVersion == 0 || g.db == nil {
		return g.resolver
	}
	if g.resolver != nil && uint32(g.resolver.SnapshotID()) == row.SchemaVersion {
		return g.resolver
	}
	if g.cache != nil {
		if r, ok := g.cache[row.SchemaVersion]; ok {
			return r
		}
	}
	r, err := metadata.NewResolver(g.db, int(row.SchemaVersion))
	if g.cache == nil {
		g.cache = make(map[uint32]*metadata.Resolver)
	}
	if err != nil {
		// Without the snapshot the per-column generated/identity skip-sets are unknown,
		// so reversal SQL for an affected table may fail to apply (PostgreSQL rejects an
		// INSERT/UPDATE that writes a GENERATED ALWAYS identity or generated column). The
		// failure is loud at apply time inside the BEGIN/COMMIT wrapper, not silent.
		slog.Warn("failed to load schema snapshot for schema_version; using default resolver — reversal SQL for tables with generated or identity columns may fail to apply",
			"schema_version", row.SchemaVersion, "error", err)
		// Cache the fallback so we don't repeat the DB query and warning
		// for every row with this version.
		g.cache[row.SchemaVersion] = g.resolver
		return g.resolver
	}
	g.cache[row.SchemaVersion] = r
	return r
}

// GenerateSQL fetches events matching opts, reverses their order (most-recent
// first), and writes a BEGIN/COMMIT-wrapped SQL script to w.
// Returns the number of SQL statements written (errors within a statement are
// emitted as SQL comments rather than halting generation).
func (g *Generator) GenerateSQL(ctx context.Context, opts query.Options, w io.Writer) (int, error) {
	rows, err := query.New(g.db).Fetch(ctx, opts)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch events: %w", err)
	}
	return g.GenerateSQLFromRows(rows, w)
}

// GenerateSQLFromRows generates reversal SQL from pre-fetched rows. Use this
// when rows have already been fetched and merged from multiple sources (e.g.
// live MySQL + Parquet archives). The rows are reversed so the most-recent
// event is undone first.
func (g *Generator) GenerateSQLFromRows(rows []query.ResultRow, w io.Writer) (int, error) {
	if len(rows) == 0 {
		fmt.Fprintln(w, "-- No events matched the specified criteria.")
		return 0, nil
	}

	// Bound peak memory before rendering (#654). The whole script is buffered
	// into RAM (body, below) on top of the already-resident events, so a
	// pathological BLOB/TEXT window roughly doubles peak. Refuse up front rather
	// than emit a truncated script: a partial reversal applied to production is a
	// silently-wrong recovery — the same fail-loud stance as the schema-drift
	// refusal below. The bound is on the rendered payload, not the initial fetch
	// (the events are already loaded, row-count-bounded by --limit).
	if err := g.CheckScriptBudget(rows); err != nil {
		return 0, err
	}

	// Reverse so the most-recent event is undone first.
	// For multiple UPDATEs on the same row this yields the correct
	// rollback order automatically.
	slices.Reverse(rows)

	// Build the body into a buffer first. The whole script is one BEGIN/COMMIT
	// transaction, so if any statement references a column dropped/renamed after the
	// event (schema drift, #601) the entire transaction would roll back at apply time —
	// emitting part of it is worse than emitting nothing. Detecting drift up front and
	// refusing before a single byte reaches w mirrors #602's pre-write fail-loud.
	var body bytes.Buffer
	drift := map[string]map[string]bool{} // "schema.table" -> set of drifted columns
	var driftOrder []string               // table keys in first-seen order, for a stable message

	written := 0
	for _, row := range rows {
		fmt.Fprintln(&body)

		gtidSuffix := ""
		if row.GTID != nil {
			gtidSuffix = " gtid=" + *row.GTID
		}
		fmt.Fprintf(&body, "-- [%d] reverse %s on %s.%s pk=%s at %s%s\n",
			row.EventID,
			eventTypeName(row.EventType),
			row.SchemaName, row.TableName,
			row.PKValues,
			row.EventTimestamp.Format("2006-01-02 15:04:05"),
			gtidSuffix,
		)

		stmt, cols, err := g.buildStatement(row)
		if err != nil {
			// Emit error as a SQL comment so the script remains runnable
			// (the transaction will roll back on first error anyway).
			fmt.Fprintf(&body, "-- ERROR generating reversal for event %d: %v\n", row.EventID, err)
			continue
		}
		if d := g.driftedEmitted(row, cols); len(d) > 0 {
			key := row.SchemaName + "." + row.TableName
			if drift[key] == nil {
				drift[key] = map[string]bool{}
				driftOrder = append(driftOrder, key)
			}
			for _, c := range d {
				drift[key][c] = true
			}
		}
		fmt.Fprintln(&body, stmt+";")
		written++
	}

	if len(driftOrder) > 0 {
		return 0, schemaDriftError(drift, driftOrder)
	}

	fmt.Fprintf(w, "-- Generated by bintrail recover at %s\n", time.Now().UTC().Format("2006-01-02 15:04:05 UTC"))
	fmt.Fprintf(w, "-- Events to reverse: %d\n", len(rows))
	fmt.Fprintln(w, "-- IMPORTANT: Review carefully before applying to production.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "BEGIN;")
	if g.dialect == PostgresDialect {
		// escapePGString relies on standard_conforming_strings=on (PostgreSQL's
		// default), under which a backslash is literal. If the operator applies this
		// script in a session with it OFF, an unescaped backslash would be reinterpreted
		// (silent corruption). SET LOCAL pins it for this transaction only, so the
		// script defends its own escaping regardless of the target session's setting.
		fmt.Fprintln(w, "SET LOCAL standard_conforming_strings = on;")
	}
	if _, err := io.Copy(w, &body); err != nil {
		return 0, err
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "COMMIT;")
	return written, nil
}

// schemaDriftError builds the fail-loud error for #601: the reversal SQL references
// columns the latest schema snapshot no longer carries. It names every affected
// schema.table and its drifted columns so the operator knows exactly what to reconcile.
func schemaDriftError(drift map[string]map[string]bool, order []string) error {
	parts := make([]string, 0, len(order))
	for _, key := range order {
		cols := make([]string, 0, len(drift[key]))
		for c := range drift[key] {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		parts = append(parts, fmt.Sprintf("%s (%s)", key, strings.Join(cols, ", ")))
	}
	return fmt.Errorf("recover: refusing to emit reversal SQL — it references column(s) that the latest schema "+
		"snapshot no longer has (dropped or renamed after the event was captured), so the SQL would not apply to "+
		"the current table: %s. Re-snapshot if the table actually still has these columns; otherwise reconcile the "+
		"column(s) by hand", strings.Join(parts, "; "))
}

// ─── Script-size budget (#654) ─────────────────────────────────────────────────

// ScriptBudgetError is returned by GenerateSQLFromRows when the estimated row
// payload of the matched events exceeds the configured script-size budget
// (#654). It is a fail-loud refusal, not a truncation: a partial reversal script
// applied to a production database is a silently-wrong recovery, so recover
// refuses to emit anything. The recover CLI wraps it with command-specific
// guidance; the typed fields let other callers report it however they like.
type ScriptBudgetError struct {
	EstimatedBytes int64 // estimated row payload to render
	Budget         int64 // the configured ceiling that was exceeded
}

func (e *ScriptBudgetError) Error() string {
	return fmt.Sprintf("recover: refusing to generate the reversal script — the matched events hold ~%s "+
		"of row data, over the script-size budget of %s. Rendering the SQL would roughly double peak memory "+
		"(the events are already loaded). Narrow the recovery window, or raise/disable the budget (0 = unlimited)",
		humanizeBytes(e.EstimatedBytes), humanizeBytes(e.Budget))
}

// EstimateScriptBytes returns a cheap estimate of the row payload
// GenerateSQLFromRows will render into the in-memory script buffer (#654). It
// sums the resident before- and after-image bytes plus the PK across every row,
// walking the already-decoded maps (allocation-free). BOTH images are counted
// because the reversal can reference either, and a reverse UPDATE references
// both: DELETE→INSERT renders the before image; INSERT→DELETE renders the after
// image in its WHERE clause; UPDATE renders the before image in SET AND the after
// image in WHERE (see buildUpdate/buildDelete — the WHERE keys on row_after, the
// nil-resolver fallback using every after column). Summing both never
// under-counts the referenced payload, so the guard fails loud rather than
// letting an oversized script through. The rendered SQL adds escaping (binary →
// hex can roughly double a value) plus identifiers and comments, so this is a
// payload proxy, not byte-exact — adequate for the GB-scale pathological windows
// the guard targets. (A reverse DELETE of an INSERT whose PK is known renders
// only the key, so this is then a conservative over-estimate — the safe, fail-
// loud, recoverable direction.)
func EstimateScriptBytes(rows []query.ResultRow) int64 {
	var total int64
	for i := range rows {
		total += int64(len(rows[i].PKValues))
		total += estimateRowBytes(rows[i].RowBefore)
		total += estimateRowBytes(rows[i].RowAfter)
	}
	return total
}

func estimateRowBytes(m map[string]any) int64 {
	var t int64
	for k, v := range m {
		t += int64(len(k)) + estimateValueBytes(v)
	}
	return t
}

// estimateValueBytes approximates the in-memory footprint of a JSON-decoded
// value. Strings ([]byte base64 / TEXT / hex) and json.Number dominate the
// payload of fat rows, so their exact length is counted; scalars get a small
// constant. After json.Unmarshal into map[string]any, numbers are float64 (see
// the codebase's JSON round-trip note), with json.Number covered for UseNumber
// callers.
func estimateValueBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return int64(len(x))
	case []byte:
		return int64(len(x))
	case json.Number:
		return int64(len(x))
	case bool:
		return 1
	case float64:
		return 8
	case map[string]any:
		return estimateRowBytes(x)
	case []any:
		var t int64
		for _, e := range x {
			t += estimateValueBytes(e)
		}
		return t
	default:
		return 16
	}
}

// humanizeBytes formats a byte count with a binary KB/MB/GB suffix, matching the
// units cliutil.ParseByteSize accepts on the --max-script-bytes flag.
func humanizeBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fGB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fKB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// ─── Statement generators ─────────────────────────────────────────────────────

// buildStatement is generateStatement plus the set of columns the emitted SQL actually
// references — schema-drift detection (#601) compares exactly those against the latest
// snapshot, so the check can never diverge from what is emitted. generateStatement is a
// thin wrapper kept for the test call sites that only need the SQL (no production caller
// uses it — GenerateSQLFromRows calls buildStatement directly).
func (g *Generator) buildStatement(row query.ResultRow) (string, []string, error) {
	switch row.EventType {
	case event.EventDelete:
		return g.buildInsert(row) // DELETE → INSERT (restore the deleted row)
	case event.EventUpdate:
		return g.buildUpdate(row) // UPDATE → reverse UPDATE (restore before state)
	case event.EventInsert:
		return g.buildDelete(row) // INSERT → DELETE (remove the inserted row)
	case event.EventSnapshot:
		// Snapshot rows are read-only baseline state, not change events, so
		// reversal SQL is undefined for them. Reject with a clear message
		// instead of falling through to the generic "unknown event type"
		// error — this path is only reachable if future code wires snapshots
		// into the recover pipeline.
		return "", nil, fmt.Errorf("cannot generate reversal SQL for SNAPSHOT event %d (baseline rows are read-only)", row.EventID)
	default:
		return "", nil, fmt.Errorf("unknown event type %d", row.EventType)
	}
}

func (g *Generator) generateStatement(row query.ResultRow) (string, error) {
	stmt, _, err := g.buildStatement(row)
	return stmt, err
}

// driftedEmitted returns the subset of emitted columns that existed in the event-time
// snapshot but are absent from the latest snapshot — columns dropped or renamed after
// the event was captured, whose presence in the reversal SQL would make it fail to apply
// against the current table (#601). Returns nil (detection off) whenever the schema
// knowledge for the comparison is unavailable: no latest-snapshot resolver, or the table
// is missing from either snapshot. The event-time membership gate is what prevents a
// false positive on a column ADDED after the event and not re-snapshotted — such a column
// is absent from the event-time snapshot too, so it is never flagged.
func (g *Generator) driftedEmitted(row query.ResultRow, emitted []string) []string {
	if g.resolver == nil || len(emitted) == 0 {
		return nil
	}
	evtR := g.resolverForRow(row)
	if evtR == nil {
		return nil
	}
	evtTM, err := evtR.Resolve(row.SchemaName, row.TableName)
	if err != nil {
		// Event-time table not in its snapshot — can't establish the event's columns,
		// so drift is undeterminable. Warn rather than go dark: every other resolver
		// path in this file logs on a resolve failure, and this detector's whole job is
		// to surface drift.
		slog.Warn("cannot resolve event-time schema for drift check; reversal SQL not checked for dropped/renamed columns",
			"schema", row.SchemaName, "table", row.TableName, "schema_version", row.SchemaVersion, "error", err)
		return nil
	}
	curTM, err := g.resolver.Resolve(row.SchemaName, row.TableName)
	if err != nil {
		// Table absent from the LATEST snapshot — usually a table dropped after the
		// event (its reversal would fail to apply), but also a legitimately scoped
		// --schemas/--tables snapshot. Ambiguous, so warn-and-continue rather than
		// refuse (mirrors pkWhereClause's all-columns fallback); apply-time stays loud.
		slog.Warn("cannot resolve current schema for drift check; reversal SQL not checked for dropped/renamed columns (table may have been dropped after the event)",
			"schema", row.SchemaName, "table", row.TableName, "error", err)
		return nil
	}
	return driftedColumns(emitted, evtTM, curTM)
}

// driftedColumns returns the columns in `emitted` that the event-time schema had but the
// current schema no longer has (dropped or renamed after the event). Pure and
// dialect-agnostic — the result preserves `emitted` order and is deduplicated. #601.
func driftedColumns(emitted []string, eventTime, current *metadata.TableMeta) []string {
	if eventTime == nil || current == nil {
		return nil
	}
	evtSet := columnNameSet(eventTime.Columns)
	curSet := columnNameSet(current.Columns)
	var drifted []string
	seen := map[string]bool{}
	for _, c := range emitted {
		if !seen[c] && evtSet[c] && !curSet[c] {
			drifted = append(drifted, c)
			seen[c] = true
		}
	}
	return drifted
}

func columnNameSet(cols []metadata.ColumnMeta) map[string]bool {
	s := make(map[string]bool, len(cols))
	for _, c := range cols {
		s[c.Name] = true
	}
	return s
}

// base64Cols maps each column of a table that go-mysql delivers as []byte — the
// BLOB and TEXT families (both binlog type MYSQL_TYPE_BLOB) — to whether it is
// binary. marshalRow base64-encodes those non-JSON []byte values into the stored
// JSON, so on recovery the value comes back as a base64 STRING that must be
// decoded before emission or the reversal SQL writes the base64 text verbatim
// (#653): binary → X'hex', text → a string literal.
//
// Returns nil (no coercion) for a non-MySQL dialect, a nil resolver, or an
// unresolvable table. In the last two cases BLOB/TEXT values are still emitted as
// base64 — recover without usable schema metadata cannot type its columns; the
// caller has already warned about the missing snapshot (resolverForRow /
// pkWhereClause), so this does not warn again.
func (g *Generator) base64Cols(r *metadata.Resolver, schema, table string) map[string]bool {
	if g.dialect != MySQLDialect || r == nil {
		return nil
	}
	tm, err := r.Resolve(schema, table)
	if err != nil {
		return nil
	}
	var m map[string]bool
	for _, c := range tm.Columns {
		binary, ok := base64StoredKind(c.DataType)
		if !ok {
			continue
		}
		if m == nil {
			m = make(map[string]bool)
		}
		m[c.Name] = binary
	}
	return m
}

// base64StoredKind reports whether a column's DataType is in the BLOB or TEXT
// family — the ones go-mysql delivers as []byte so marshalRow base64-encodes them
// in storage — and if so whether it is binary (true → emit X'hex') or text
// (false → emit a string literal). go-mysql also delivers GEOMETRY/VECTOR as
// []byte, but they are deliberately out of scope: a bare X'hex'/string literal
// can't load them (they need ST_GeomFromWKB / STRING_TO_VECTOR), so reversing
// their base64 is no better than leaving it — a separate concern from #653.
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
// value (see base64Cols). binary selects the decoded Go type so FormatSQLValue
// emits X'hex' (binary) vs a quoted string (text). A value that is not a
// decodable base64 string is returned unchanged (defensive — NULL, a raw-JSON
// object/array blob, or pre-existing non-base64 data).
//
// Known lossy edge (pre-existing, NOT introduced here): marshalRow stores a
// BLOB/TEXT whose bytes are valid JSON RAW, not base64. A valid-JSON OBJECT/ARRAY
// blob comes back as a map/[]any (the string guard skips it). But a valid-JSON
// SCALAR-STRING blob (bytes literally `"YWJj"`) comes back as the Go string
// `YWJj`, indistinguishable from a base64-stored blob, and is wrongly decoded.
// That value was already corrupted at capture (the outer quotes were stripped by
// the raw-JSON promotion), so this only changes the flavor of an existing bug; a
// real fix belongs at the storage encoding, out of scope for #653.
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

// generateInsert reverses a DELETE event: reconstruct the deleted row from
// row_before with a full INSERT, skipping STORED/VIRTUAL generated columns. On the
// PostgreSQL path it emits OVERRIDING SYSTEM VALUE so a GENERATED ALWAYS AS IDENTITY
// column accepts its restored value (#557); the clause is a harmless no-op on tables
// without such a column and on GENERATED BY DEFAULT identity (verified against live
// PG 14–17 by the integration suite), so it is emitted unconditionally rather than
// gated on identity metadata — keeping the highest-frequency recovery op robust.
// Identity columns are KEPT (the real id is the point of recovery); only generated
// columns are omitted.
func (g *Generator) generateInsert(row query.ResultRow) (string, error) {
	stmt, _, err := g.buildInsert(row)
	return stmt, err
}

func (g *Generator) buildInsert(row query.ResultRow) (string, []string, error) {
	if row.RowBefore == nil {
		return "", nil, fmt.Errorf("row_before is nil for DELETE event (event_id=%d)", row.EventID)
	}
	r := g.resolverForRow(row)
	genCols := generatedColsFromResolver(r, row.SchemaName, row.TableName)
	b64 := g.base64Cols(r, row.SchemaName, row.TableName)
	var cols, colParts, valParts []string
	for _, col := range sortedKeys(row.RowBefore) {
		if genCols[col] {
			continue
		}
		cols = append(cols, col)
		colParts = append(colParts, g.quoteName(col))
		v := row.RowBefore[col]
		if binary, ok := b64[col]; ok {
			v = decodeStoredBase64(v, binary)
		}
		valParts = append(valParts, g.formatValue(v))
	}
	overriding := ""
	if g.dialect == PostgresDialect {
		overriding = " OVERRIDING SYSTEM VALUE"
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s)%s VALUES (%s)",
		g.quoteName(row.SchemaName), g.quoteName(row.TableName),
		strings.Join(colParts, ", "),
		overriding,
		strings.Join(valParts, ", "),
	), cols, nil
}

// generateUpdate reverses an UPDATE event: SET all columns to row_before values
// (skipping generated and GENERATED ALWAYS identity columns, #557), WHERE identifies
// the row using row_after PK values.
func (g *Generator) generateUpdate(row query.ResultRow) (string, error) {
	stmt, _, err := g.buildUpdate(row)
	return stmt, err
}

func (g *Generator) buildUpdate(row query.ResultRow) (string, []string, error) {
	if row.RowBefore == nil {
		return "", nil, fmt.Errorf("row_before is nil for UPDATE event (event_id=%d)", row.EventID)
	}
	if row.RowAfter == nil {
		return "", nil, fmt.Errorf("row_after is nil for UPDATE event (event_id=%d)", row.EventID)
	}

	// SET clause: restore before-image values, omitting columns PostgreSQL forbids in
	// a SET — STORED/VIRTUAL generated columns AND GENERATED ALWAYS identity columns
	// (#557). PostgreSQL permits only SET <col> = DEFAULT on a GENERATED ALWAYS column,
	// never an explicit value, so a reverse-UPDATE (which has no OVERRIDING clause)
	// cannot restore its before-image regardless — omitting it is the only valid choice.
	// The WHERE clause still PK-targets the column.
	r := g.resolverForRow(row)
	skipCols := updateSetSkipCols(r, row.SchemaName, row.TableName)
	b64 := g.base64Cols(r, row.SchemaName, row.TableName)
	var cols, setParts []string
	for _, col := range sortedKeys(row.RowBefore) {
		if skipCols[col] {
			continue
		}
		cols = append(cols, col)
		v := row.RowBefore[col]
		if binary, ok := b64[col]; ok {
			v = decodeStoredBase64(v, binary)
		}
		setParts = append(setParts, g.quoteName(col)+" = "+g.formatValue(v))
	}

	// WHERE uses row_after (current state), so the UPDATE finds the right row
	// even if the PK itself was changed in the original UPDATE.
	whereParts, whereCols := g.pkWhereClause(r, row.SchemaName, row.TableName, row.RowAfter)
	cols = append(cols, whereCols...)

	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		g.quoteName(row.SchemaName), g.quoteName(row.TableName),
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " AND "),
	), cols, nil
}

// generateDelete reverses an INSERT event: delete the inserted row using its
// row_after PK values (the current DB state).
func (g *Generator) generateDelete(row query.ResultRow) (string, error) {
	stmt, _, err := g.buildDelete(row)
	return stmt, err
}

func (g *Generator) buildDelete(row query.ResultRow) (string, []string, error) {
	if row.RowAfter == nil {
		return "", nil, fmt.Errorf("row_after is nil for INSERT event (event_id=%d)", row.EventID)
	}
	r := g.resolverForRow(row)
	whereParts, whereCols := g.pkWhereClause(r, row.SchemaName, row.TableName, row.RowAfter)
	return fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		g.quoteName(row.SchemaName), g.quoteName(row.TableName),
		strings.Join(whereParts, " AND "),
	), whereCols, nil
}

// generatedColsFromResolver returns the set of STORED/VIRTUAL generated column
// names for a table, using the provided resolver. Returns nil when the resolver
// is absent or the table is not in the snapshot — callers treat nil as an empty set.
func generatedColsFromResolver(resolver *metadata.Resolver, schema, table string) map[string]bool {
	if resolver == nil {
		return nil
	}
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		slog.Warn("cannot determine generated columns; reversal INSERT may include generated columns",
			"schema", schema, "table", table, "error", err)
		return nil
	}
	var gen map[string]bool
	for _, c := range tm.Columns {
		if c.IsGenerated {
			if gen == nil {
				gen = make(map[string]bool)
			}
			gen[c.Name] = true
		}
	}
	return gen
}

// updateSetSkipCols returns the columns to omit from a reverse-UPDATE SET clause:
// STORED/VIRTUAL generated columns AND PostgreSQL GENERATED ALWAYS identity columns
// (#557) — PostgreSQL rejects `SET <col> = <value>` on either ("column can only be
// updated to DEFAULT"). A GENERATED ALWAYS column accepts only `SET <col> = DEFAULT`,
// never an explicit value, so a reverse-UPDATE can never restore its before-image and
// omitting it is the only valid choice. (A GENERATED BY DEFAULT identity is NOT
// skipped — PostgreSQL allows an explicit value there, which is required to reverse a
// PK-changing UPDATE.) Returns nil when the resolver is absent or the table is not in
// the snapshot.
func updateSetSkipCols(resolver *metadata.Resolver, schema, table string) map[string]bool {
	if resolver == nil {
		return nil
	}
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		slog.Warn("cannot determine generated/identity columns; reversal UPDATE may SET a generated or identity column",
			"schema", schema, "table", table, "error", err)
		return nil
	}
	var skip map[string]bool
	for _, c := range tm.Columns {
		if c.IsGenerated || c.IsIdentityAlways {
			if skip == nil {
				skip = make(map[string]bool)
			}
			skip[c.Name] = true
		}
	}
	return skip
}

// pkWhereClause builds "pk_col = val AND ..." from the given resolver, in the
// Generator's dialect. Falls back to ALL columns if the table cannot be resolved
// (e.g. table was dropped, or no snapshot was loaded). Note: on the PostgreSQL
// path the all-columns fallback can emit `"col" = '...'` for a json column, which
// has no `=` operator in PostgreSQL (jsonb does) — PK-scoped (the #533 norm) avoids
// it since PKs are scalars.
//
// It returns both the rendered clauses and the column names they reference, so
// schema-drift detection (#601) checks exactly the columns the WHERE emits — a PK-scoped
// WHERE whose PK still exists carries no drifted column even when some other column was
// dropped, so that recovery is (correctly) not refused.
func (g *Generator) pkWhereClause(resolver *metadata.Resolver, schema, table string, row map[string]any) (clauses []string, cols []string) {
	if resolver != nil {
		tm, err := resolver.Resolve(schema, table)
		if err != nil {
			slog.Warn("cannot resolve table for PK lookup; using all-columns WHERE",
				"schema", schema, "table", table, "error", err)
		} else {
			pkCols := tm.PKColumnMetas()
			if len(pkCols) > 0 {
				// A BLOB/TEXT column can be a PK with a prefix length, and its
				// row value is the same base64 string as elsewhere — decode it or
				// the WHERE matches zero rows (silent no-op recovery, #653).
				b64 := g.base64Cols(resolver, schema, table)
				parts := make([]string, 0, len(pkCols))
				names := make([]string, 0, len(pkCols))
				allFound := true
				for _, pk := range pkCols {
					v, ok := row[pk.Name]
					if !ok {
						allFound = false
						break
					}
					if binary, isB64 := b64[pk.Name]; isB64 {
						v = decodeStoredBase64(v, binary)
					}
					parts = append(parts, g.quoteName(pk.Name)+" = "+g.formatValue(v))
					names = append(names, pk.Name)
				}
				if allFound {
					return parts, names
				}
			}
		}
	}
	// Fallback: all columns — verbose but always uniquely identifies the row
	// (assuming the table has no duplicates, which is true for well-formed data).
	return g.allColsWhere(row)
}

func (g *Generator) allColsWhere(row map[string]any) (clauses []string, cols []string) {
	cols = sortedKeys(row)
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = g.quoteName(col) + " = " + g.formatValue(row[col])
	}
	return parts, cols
}

// ─── Value formatting ─────────────────────────────────────────────────────────

// FormatSQLValue renders a Go value as a MySQL literal suitable for embedding
// in a generated SQL statement. Exported so other packages (notably the
// mydumper writer in internal/reconstruct, #187) can reuse the exact same
// formatting and escaping.
//
// Binlog-event values arrive here after a JSON round-trip — row_before/row_after
// are decoded via query.UnmarshalRowImage, so numeric values are json.Number
// (the exact literal, no float64 rounding — #496); the json.Number case emits
// them verbatim.
//
// DuckDB's database/sql driver (used by the full-table reconstruct path) returns
// int64 / float64 / time.Time / []byte natively — those cases are also handled
// here so the same function formats both JSON-round-tripped binlog values and
// direct DuckDB scan values from baseline Parquet rows. The float64 case is now
// reached only by DuckDB-origin DOUBLE/FLOAT columns, not binlog integers.
func FormatSQLValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case bool:
		if val {
			return "1"
		}
		return "0"

	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int:
		return strconv.FormatInt(int64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)

	case json.Number:
		// Row images read from binlog_events JSON come back as json.Number
		// (query.UnmarshalRowImage uses UseNumber), preserving the exact literal.
		// Emit it verbatim as a SQL numeric literal — integers above 2^53 survive
		// instead of being rounded through float64 (#496). JSON number syntax is a
		// valid SQL numeric literal (integer, decimal, or exponent).
		return string(val)

	case float64:
		// DuckDB-origin DOUBLE/FLOAT columns (baseline reconstruct path). Whole
		// numbers are emitted as integers, fractional ones as decimals. Binlog
		// integers no longer reach here — they arrive as json.Number, handled
		// above. math.Abs guard prevents int64 overflow for very large floats.
		if !math.IsInf(val, 0) && !math.IsNaN(val) &&
			val == math.Trunc(val) && math.Abs(val) < 1e15 {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)

	case time.Time:
		// MySQL DATETIME literal with microsecond precision. UTC matches
		// the indexer's storage convention for event_timestamp.
		return "'" + val.UTC().Format("2006-01-02 15:04:05.000000") + "'"

	case []byte:
		// Binary/blob column. Emit as MySQL hex literal to survive
		// arbitrary non-UTF-8 bytes. Empty slices become X'' which MySQL
		// accepts as a zero-length BLOB.
		return "X'" + hex.EncodeToString(val) + "'"

	case string:
		return "'" + EscapeString(val) + "'"

	case map[string]any:
		// MySQL JSON column: re-serialise to JSON and store as a string literal.
		b, _ := json.Marshal(val)
		return "'" + EscapeString(string(b)) + "'"

	case []any:
		// JSON array column.
		b, _ := json.Marshal(val)
		return "'" + EscapeString(string(b)) + "'"

	case json.RawMessage:
		return "'" + EscapeString(string(val)) + "'"

	default:
		return "'" + EscapeString(fmt.Sprintf("%v", val)) + "'"
	}
}

// EscapeString escapes a string for safe embedding inside a MySQL
// single-quoted literal.
func EscapeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	s = strings.ReplaceAll(s, "\x00", `\0`)
	return s
}

// QuoteName wraps a MySQL identifier (schema, table, column) in backticks,
// escaping any backticks in the name itself.
func QuoteName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// ─── Dialect dispatch (#533) ───────────────────────────────────────────────────

// quoteName quotes an identifier in the Generator's dialect.
func (g *Generator) quoteName(name string) string {
	if g.dialect == PostgresDialect {
		return quoteNamePG(name)
	}
	return QuoteName(name)
}

// formatValue renders a value as a literal in the Generator's dialect.
func (g *Generator) formatValue(v any) string {
	if g.dialect == PostgresDialect {
		return formatValuePG(v)
	}
	return FormatSQLValue(v)
}

// quoteNamePG wraps a PostgreSQL identifier in double quotes, doubling any embedded
// double quote.
func quoteNamePG(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// escapePGString escapes a string for a PostgreSQL single-quoted literal under
// standard_conforming_strings=on (PostgreSQL's default for 15+ years; the emitted
// SQL is portable under it): only the single quote is doubled. A backslash is a
// LITERAL backslash and must NOT be doubled — doubling it (as MySQL escaping does)
// would silently store two backslashes. PostgreSQL text cannot contain a NUL byte
// and pgoutput never delivers one, so none is handled.
func escapePGString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// formatValuePG renders a Go value as a PostgreSQL literal. Values captured from
// pgoutput arrive here as Go strings (pgoutput text mode) or nil — and a quoted,
// standard-conforming-escaped string coerces into the target column's type on
// INSERT/UPDATE/WHERE, so no per-type literal forms are needed (a numeric, uuid,
// bytea '\x..', jsonb, bool 't', timestamptz, etc. all coerce from their canonical
// text). The non-string cases are DEFENSIVE: they should not occur on the
// PostgreSQL path (which stores every value as text), but are handled so a stray
// value never emits invalid SQL.
func formatValuePG(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + escapePGString(val) + "'"
	case json.Number:
		// Defensive: PG values are stored as strings, not json.Number; if one
		// appears, emit it verbatim (a valid numeric literal, no float64 rounding).
		return string(val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case map[string]any, []any, json.RawMessage:
		// Defensive, mirroring FormatSQLValue: a structured value → JSON, quoted. On
		// the PG path the only structured value a row image can carry is the
		// unchanged-TOAST sentinel map, reachable only via the all-columns WHERE
		// fallback under a weaker-than-FULL replica identity (out of support; RI FULL
		// resolves it at decode). JSON-marshalling keeps it valid, collision-distinct SQL.
		b, _ := json.Marshal(val)
		return "'" + escapePGString(string(b)) + "'"
	default:
		// Defensive: any other Go type → its text form, quoted + escaped.
		return "'" + escapePGString(fmt.Sprintf("%v", val)) + "'"
	}
}

// FormatSetNullRestore emits an idempotent UPDATE that restores a foreign-key
// column an ON DELETE SET NULL cascade nulled (MySQL ≤8.x never logs it). It
// sets fkCol back to value, but ONLY for the row still in the nulled state
// (WHERE pk… AND fkCol IS NULL) — so a re-run, a manual fix, or a later re-point
// of the child is never clobbered (the cascade synthesis can't tell a
// re-pointed child from a still-nulled one, because the re-point event doesn't
// match the fk=parent scan that found the candidate). pkCols + row supply the
// PK predicate; value is the parent key (typed, so it renders as a numeric
// literal for an integer FK rather than a quoted string).
func FormatSetNullRestore(schema, table, fkCol string, value any, pkCols []metadata.ColumnMeta, row map[string]any) (string, error) {
	if len(pkCols) == 0 {
		return "", fmt.Errorf("no PK columns for %s.%s SET NULL restore", schema, table)
	}
	where := make([]string, 0, len(pkCols)+1)
	for _, c := range pkCols {
		v, ok := row[c.Name]
		if !ok {
			return "", fmt.Errorf("PK column %q absent from %s.%s row for SET NULL restore", c.Name, schema, table)
		}
		where = append(where, QuoteName(c.Name)+" = "+FormatSQLValue(v))
	}
	where = append(where, QuoteName(fkCol)+" IS NULL")
	return fmt.Sprintf("UPDATE %s.%s SET %s = %s WHERE %s",
		QuoteName(schema), QuoteName(table), QuoteName(fkCol), FormatSQLValue(value),
		strings.Join(where, " AND ")), nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func eventTypeName(et event.EventType) string {
	switch et {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}
