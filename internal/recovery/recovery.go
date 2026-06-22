// Package recovery generates reversal SQL from indexed binlog events.
// It reads events via the query engine and emits a transaction-wrapped SQL
// script that undoes each event in reverse chronological order.
package recovery

import (
	"context"
	"database/sql"
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
}

// New creates a Generator emitting MySQL-dialect SQL. resolver may be nil — in that
// case, WHERE clauses for UPDATE and DELETE reversals will use ALL row columns
// instead of just PKs.
func New(db *sql.DB, resolver *metadata.Resolver) *Generator {
	return &Generator{db: db, resolver: resolver, dialect: MySQLDialect}
}

// NewForDialect is New with an explicit SQL dialect. Callers that know the source
// flavor (e.g. `bintrail-pg recover` via DialectForIndex) pass PostgresDialect;
// everything else uses New (MySQL).
func NewForDialect(db *sql.DB, resolver *metadata.Resolver, dialect Dialect) *Generator {
	return &Generator{db: db, resolver: resolver, dialect: dialect}
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

	// Reverse so the most-recent event is undone first.
	// For multiple UPDATEs on the same row this yields the correct
	// rollback order automatically.
	slices.Reverse(rows)

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

	written := 0
	for _, row := range rows {
		fmt.Fprintln(w)

		gtidSuffix := ""
		if row.GTID != nil {
			gtidSuffix = " gtid=" + *row.GTID
		}
		fmt.Fprintf(w, "-- [%d] reverse %s on %s.%s pk=%s at %s%s\n",
			row.EventID,
			eventTypeName(row.EventType),
			row.SchemaName, row.TableName,
			row.PKValues,
			row.EventTimestamp.Format("2006-01-02 15:04:05"),
			gtidSuffix,
		)

		stmt, err := g.generateStatement(row)
		if err != nil {
			// Emit error as a SQL comment so the script remains runnable
			// (the transaction will roll back on first error anyway).
			fmt.Fprintf(w, "-- ERROR generating reversal for event %d: %v\n", row.EventID, err)
			continue
		}
		fmt.Fprintln(w, stmt+";")
		written++
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "COMMIT;")
	return written, nil
}

// ─── Statement generators ─────────────────────────────────────────────────────

func (g *Generator) generateStatement(row query.ResultRow) (string, error) {
	switch row.EventType {
	case event.EventDelete:
		return g.generateInsert(row) // DELETE → INSERT (restore the deleted row)
	case event.EventUpdate:
		return g.generateUpdate(row) // UPDATE → reverse UPDATE (restore before state)
	case event.EventInsert:
		return g.generateDelete(row) // INSERT → DELETE (remove the inserted row)
	case event.EventSnapshot:
		// Snapshot rows are read-only baseline state, not change events, so
		// reversal SQL is undefined for them. Reject with a clear message
		// instead of falling through to the generic "unknown event type"
		// error — this path is only reachable if future code wires snapshots
		// into the recover pipeline.
		return "", fmt.Errorf("cannot generate reversal SQL for SNAPSHOT event %d (baseline rows are read-only)", row.EventID)
	default:
		return "", fmt.Errorf("unknown event type %d", row.EventType)
	}
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
	if row.RowBefore == nil {
		return "", fmt.Errorf("row_before is nil for DELETE event (event_id=%d)", row.EventID)
	}
	r := g.resolverForRow(row)
	genCols := generatedColsFromResolver(r, row.SchemaName, row.TableName)
	var colParts, valParts []string
	for _, col := range sortedKeys(row.RowBefore) {
		if genCols[col] {
			continue
		}
		colParts = append(colParts, g.quoteName(col))
		valParts = append(valParts, g.formatValue(row.RowBefore[col]))
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
	), nil
}

// generateUpdate reverses an UPDATE event: SET all columns to row_before values
// (skipping generated and GENERATED ALWAYS identity columns, #557), WHERE identifies
// the row using row_after PK values.
func (g *Generator) generateUpdate(row query.ResultRow) (string, error) {
	if row.RowBefore == nil {
		return "", fmt.Errorf("row_before is nil for UPDATE event (event_id=%d)", row.EventID)
	}
	if row.RowAfter == nil {
		return "", fmt.Errorf("row_after is nil for UPDATE event (event_id=%d)", row.EventID)
	}

	// SET clause: restore before-image values, omitting columns PostgreSQL forbids in
	// a SET — STORED/VIRTUAL generated columns AND GENERATED ALWAYS identity columns
	// (#557). PostgreSQL permits only SET <col> = DEFAULT on a GENERATED ALWAYS column,
	// never an explicit value, so a reverse-UPDATE (which has no OVERRIDING clause)
	// cannot restore its before-image regardless — omitting it is the only valid choice.
	// The WHERE clause still PK-targets the column.
	r := g.resolverForRow(row)
	skipCols := updateSetSkipCols(r, row.SchemaName, row.TableName)
	var setParts []string
	for _, col := range sortedKeys(row.RowBefore) {
		if skipCols[col] {
			continue
		}
		setParts = append(setParts, g.quoteName(col)+" = "+g.formatValue(row.RowBefore[col]))
	}

	// WHERE uses row_after (current state), so the UPDATE finds the right row
	// even if the PK itself was changed in the original UPDATE.
	whereParts := g.pkWhereClause(r, row.SchemaName, row.TableName, row.RowAfter)

	return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		g.quoteName(row.SchemaName), g.quoteName(row.TableName),
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " AND "),
	), nil
}

// generateDelete reverses an INSERT event: delete the inserted row using its
// row_after PK values (the current DB state).
func (g *Generator) generateDelete(row query.ResultRow) (string, error) {
	if row.RowAfter == nil {
		return "", fmt.Errorf("row_after is nil for INSERT event (event_id=%d)", row.EventID)
	}
	r := g.resolverForRow(row)
	whereParts := g.pkWhereClause(r, row.SchemaName, row.TableName, row.RowAfter)
	return fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		g.quoteName(row.SchemaName), g.quoteName(row.TableName),
		strings.Join(whereParts, " AND "),
	), nil
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
func (g *Generator) pkWhereClause(resolver *metadata.Resolver, schema, table string, row map[string]any) []string {
	if resolver != nil {
		tm, err := resolver.Resolve(schema, table)
		if err != nil {
			slog.Warn("cannot resolve table for PK lookup; using all-columns WHERE",
				"schema", schema, "table", table, "error", err)
		} else {
			pkCols := tm.PKColumnMetas()
			if len(pkCols) > 0 {
				parts := make([]string, 0, len(pkCols))
				allFound := true
				for _, pk := range pkCols {
					v, ok := row[pk.Name]
					if !ok {
						allFound = false
						break
					}
					parts = append(parts, g.quoteName(pk.Name)+" = "+g.formatValue(v))
				}
				if allFound {
					return parts
				}
			}
		}
	}
	// Fallback: all columns — verbose but always uniquely identifies the row
	// (assuming the table has no duplicates, which is true for well-formed data).
	return g.allColsWhere(row)
}

func (g *Generator) allColsWhere(row map[string]any) []string {
	cols := sortedKeys(row)
	parts := make([]string, len(cols))
	for i, col := range cols {
		parts[i] = g.quoteName(col) + " = " + g.formatValue(row[col])
	}
	return parts
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
