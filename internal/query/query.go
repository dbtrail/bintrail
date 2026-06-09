// Package query implements the binlog_events query engine — dynamic SQL
// construction from filter options and multi-format result rendering.
// It is also used by the recovery package, which calls Fetch directly.
package query

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// mysqlToSecondsConst is the value of MySQL's TO_SECONDS('1970-01-01 00:00:00').
// MySQL counts seconds from the proleptic Gregorian year 0, not the Unix epoch;
// the difference is exactly 719528 days (62167219200 seconds).
// TO_SECONDS(t) == t.Unix() + mysqlToSecondsConst for any datetime t expressed in UTC.
const mysqlToSecondsConst = int64(62167219200)

// mysqlToSeconds returns the MySQL TO_SECONDS() value for t, matching the
// RANGE(TO_SECONDS(event_timestamp)) partition expression stored as integers.
// t is normalised to UTC, so callers do not need to convert in advance.
func mysqlToSeconds(t time.Time) int64 {
	return t.UTC().Unix() + mysqlToSecondsConst
}

// ─── RBAC types ───────────────────────────────────────────────────────────────

// SchemaTable identifies a schema+table pair used in RBAC deny rules.
type SchemaTable struct {
	Schema string
	Table  string
}

// SchemaTableColumn identifies a specific column used in RBAC redaction rules.
type SchemaTableColumn struct {
	Schema string
	Table  string
	Column string
}

// LoadProfileRules loads the RBAC deny rules for a named profile and returns
// the set of tables whose events should be excluded (table-level deny) and the
// set of columns whose values should be nulled out in query results (column-level deny).
func LoadProfileRules(ctx context.Context, db *sql.DB, profile string) ([]SchemaTable, []SchemaTableColumn, error) {
	// Table-level deny rules: tables flagged for 'deny' by this profile.
	tableRows, err := db.QueryContext(ctx, `
		SELECT DISTINCT tf.schema_name, tf.table_name
		FROM access_rules ar
		JOIN profiles p ON ar.profile_id = p.id
		JOIN table_flags tf ON tf.flag = ar.flag AND tf.column_name = ''
		WHERE p.name = ? AND ar.permission = 'deny'`, profile)
	if err != nil {
		return nil, nil, fmt.Errorf("load table deny rules: %w", err)
	}
	defer tableRows.Close()

	var denyTables []SchemaTable
	for tableRows.Next() {
		var st SchemaTable
		if err := tableRows.Scan(&st.Schema, &st.Table); err != nil {
			return nil, nil, err
		}
		denyTables = append(denyTables, st)
	}
	if err := tableRows.Err(); err != nil {
		return nil, nil, err
	}

	// Column-level deny rules: specific columns to redact in query results.
	colRows, err := db.QueryContext(ctx, `
		SELECT DISTINCT tf.schema_name, tf.table_name, tf.column_name
		FROM access_rules ar
		JOIN profiles p ON ar.profile_id = p.id
		JOIN table_flags tf ON tf.flag = ar.flag AND tf.column_name != ''
		WHERE p.name = ? AND ar.permission = 'deny'`, profile)
	if err != nil {
		return nil, nil, fmt.Errorf("load column redact rules: %w", err)
	}
	defer colRows.Close()

	var redactCols []SchemaTableColumn
	for colRows.Next() {
		var stc SchemaTableColumn
		if err := colRows.Scan(&stc.Schema, &stc.Table, &stc.Column); err != nil {
			return nil, nil, err
		}
		redactCols = append(redactCols, stc)
	}
	if err := colRows.Err(); err != nil {
		return nil, nil, err
	}

	return denyTables, redactCols, nil
}

// ─── Options ─────────────────────────────────────────────────────────────────

// Options specifies the filter criteria for querying binlog_events.
// All fields are optional; nil / zero values are ignored when building SQL.
type Options struct {
	Schema        string
	Table         string
	PKValues      string            // pipe-delimited PK, e.g. "12345" or "12345|2"
	PKValuesIn    []string          // multi-PK lookup (mutually exclusive with PKValues)
	EventType     *parser.EventType // nil = all types
	GTID          string
	Since         *time.Time
	Until         *time.Time
	ChangedColumn string // column name; matched via JSON_CONTAINS
	ColumnEq      []ColumnEq // match against values inside row_after / row_before
	Flag          string     // return events from tables/columns carrying this flag
	Limit         int        // 0 → no limit (no LIMIT clause emitted)
	// LimitPerPK caps the number of latest events returned per pk_values value.
	// 0 = unlimited. Applied via ROW_NUMBER OVER (PARTITION BY pk_values
	// ORDER BY event_timestamp DESC, event_id DESC) so the kept events are
	// the most recent ones per PK. The inner DESC ordering is fixed (it
	// selects "latest N per PK"); only the outer ORDER BY direction follows
	// Order.
	LimitPerPK int
	// Order controls the direction of the outer ORDER BY applied before
	// LIMIT. "DESC" (case-insensitive) selects descending order; any other
	// value (including empty) defaults to ascending — this preserves the
	// pre-#1511 behavior for callers that don't set Order. Both sort keys
	// (event_timestamp, event_id) get the same direction so the ordering
	// is total and deterministic regardless of timestamp collisions.
	Order string

	DenyTables    []SchemaTable       // tables excluded by RBAC profile
	RedactColumns []SchemaTableColumn // column values nulled out by RBAC profile
}

// ─── ResultRow ────────────────────────────────────────────────────────────────

// ResultRow is one decoded row from binlog_events.
type ResultRow struct {
	EventID        uint64
	BinlogFile     string
	StartPos       uint64
	EndPos         uint64
	EventTimestamp time.Time
	GTID           *string // nil when GTID not enabled on the source
	ConnectionID   *uint32 // nil for events indexed before this column was added
	SchemaName     string
	TableName      string
	EventType      parser.EventType
	PKValues       string
	ChangedColumns []string
	RowBefore      map[string]any // nil for INSERT
	RowAfter       map[string]any // nil for DELETE
	SchemaVersion  uint32         // snapshot_id at index time; 0 for pre-migration data
}

// OrderDirection normalises an Options.Order value to a SQL direction keyword
// ("ASC" or "DESC"). It is case-insensitive on "DESC"; anything else — empty,
// "ASC", garbage — returns "ASC" so the default behavior matches pre-#1511
// (ascending by event_timestamp, event_id). Exposed so the parquetquery and
// merge paths use the same normalisation rule as the MySQL SQL builder.
func OrderDirection(order string) string {
	if strings.EqualFold(order, "DESC") {
		return "DESC"
	}
	return "ASC"
}

// ─── Engine ───────────────────────────────────────────────────────────────────

// Engine executes queries against the index database.
type Engine struct {
	db *sql.DB
}

// New creates a query Engine backed by db.
func New(db *sql.DB) *Engine { return &Engine{db: db} }

// Fetch executes the query and returns raw result rows.
// This is the shared entry point used by both the query and recover commands.
func (e *Engine) Fetch(ctx context.Context, opts Options) ([]ResultRow, error) {
	q, args := buildQuery(opts)
	rows, err := e.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	results, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	if len(opts.RedactColumns) > 0 {
		applyRedaction(results, opts.RedactColumns)
	}
	return results, nil
}

// Run executes the query and writes formatted results to w.
// format must be one of "table", "json", or "csv"; defaults to "table".
// Returns the number of rows written.
func (e *Engine) Run(ctx context.Context, opts Options, format string, w io.Writer) (int, error) {
	results, err := e.Fetch(ctx, opts)
	if err != nil {
		return 0, err
	}
	return Format(results, format, w)
}

// Format writes rows to w in the chosen format (table, json, or csv).
// It is exported so callers that fetch from multiple sources (e.g. MySQL + Parquet
// archives) can merge rows before formatting.
func Format(rows []ResultRow, format string, w io.Writer) (int, error) {
	switch strings.ToLower(format) {
	case "json":
		return writeJSON(rows, w)
	case "csv":
		return writeCSV(rows, w)
	default:
		return writeTable(rows, w)
	}
}

// ─── SQL builder ──────────────────────────────────────────────────────────────

func buildQuery(opts Options) (string, []any) {
	var where []string
	var args []any

	if opts.Schema != "" {
		where = append(where, "schema_name = ?")
		args = append(args, opts.Schema)
	}
	if opts.Table != "" {
		where = append(where, "table_name = ?")
		args = append(args, opts.Table)
	}
	if opts.PKValues != "" {
		// Use pk_hash for the index scan; pk_values for the collision guard.
		where = append(where, "pk_hash = SHA2(?, 256) AND pk_values = ?")
		args = append(args, opts.PKValues, opts.PKValues)
	} else if len(opts.PKValuesIn) > 0 {
		// Multi-PK lookup. The pk_hash generated column index can't help with
		// IN-lists, so the planner falls back to per-partition scans pruned by
		// (schema_name, table_name, event_timestamp). Callers supply schema
		// and table to keep the scan bounded.
		placeholders := make([]string, len(opts.PKValuesIn))
		for i, v := range opts.PKValuesIn {
			placeholders[i] = "?"
			args = append(args, v)
		}
		where = append(where, "pk_values IN ("+strings.Join(placeholders, ",")+")")
	}
	if opts.EventType != nil {
		where = append(where, "event_type = ?")
		args = append(args, uint8(*opts.EventType))
	}
	if opts.GTID != "" {
		where = append(where, "gtid = ?")
		args = append(args, opts.GTID)
	}
	if opts.Since != nil {
		since := *opts.Since
		// Add an hour-aligned lower bound as a TO_SECONDS integer literal so
		// MySQL can prune to the correct partition(s) at parse time. This hint
		// is always required — MySQL cannot infer partition pruning from
		// parameterised datetime comparisons, even when the value is hour-aligned.
		outerSince := mysqlToSeconds(since.Truncate(time.Hour))
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", outerSince))
		where = append(where, "event_timestamp >= ?")
		args = append(args, since)
	}
	if opts.Until != nil {
		until := *opts.Until
		// Add an hour-aligned upper bound (exclusive) as a TO_SECONDS literal
		// for partition pruning. Truncate to the hour, then advance one hour.
		// E.g. 15:13 → 16:00, 15:00 → 16:00.
		outerUntil := mysqlToSeconds(until.Truncate(time.Hour).Add(time.Hour))
		where = append(where, fmt.Sprintf("TO_SECONDS(event_timestamp) < %d", outerUntil))
		where = append(where, "event_timestamp <= ?")
		args = append(args, until)
	}
	if opts.ChangedColumn != "" {
		// json.Marshal produces the JSON string representation (with quotes),
		// which is exactly what MySQL's JSON_CONTAINS expects as the needle.
		needle, _ := json.Marshal(opts.ChangedColumn)
		where = append(where, "JSON_CONTAINS(changed_columns, ?)")
		args = append(args, string(needle))
	}
	for _, ce := range opts.ColumnEq {
		// Defense-in-depth: ParseColumnEq is the canonical entry, but
		// Options.ColumnEq is exported and crosses package/process boundaries
		// (CLI, MCP, library callers). MySQL does not accept bind parameters
		// for JSON paths, so the column name MUST be interpolated into the SQL
		// string — re-validate here so a hand-built ColumnEq cannot reach the
		// concatenation. On failure, emit "1=0" so the result set is provably
		// empty rather than silently broader (a dropped filter would scoop
		// rows the operator never asked for).
		if !IsSafeColumnName(ce.Column) {
			slog.Error("query.buildQuery: rejected unsafe column name in ColumnEq filter; emitting no-match clause",
				"column", ce.Column)
			where = append(where, "1=0")
			continue
		}
		path := "$." + ce.Column
		if ce.IsNull {
			where = append(where, fmt.Sprintf(
				"(JSON_TYPE(JSON_EXTRACT(row_after, '%s')) = 'NULL' "+
					"OR JSON_TYPE(JSON_EXTRACT(row_before, '%s')) = 'NULL')",
				path, path))
			continue
		}
		where = append(where, fmt.Sprintf(
			"(JSON_UNQUOTE(JSON_EXTRACT(row_after, '%s')) = ? "+
				"OR JSON_UNQUOTE(JSON_EXTRACT(row_before, '%s')) = ?)",
			path, path))
		args = append(args, ce.Value, ce.Value)
	}
	if opts.Flag != "" {
		// EXISTS subquery: match events from tables (or columns) carrying the
		// given flag. The explicit table qualifiers (table_flags.schema_name,
		// binlog_events.schema_name) prevent MySQL from resolving unqualified
		// names against the subquery's own columns rather than the outer table.
		where = append(where, `EXISTS (
			SELECT 1 FROM table_flags
			WHERE table_flags.schema_name = binlog_events.schema_name
			  AND table_flags.table_name  = binlog_events.table_name
			  AND table_flags.flag        = ?)`)
		args = append(args, opts.Flag)
	}
	for _, dt := range opts.DenyTables {
		where = append(where, "NOT (schema_name = ? AND table_name = ?)")
		args = append(args, dt.Schema, dt.Table)
	}

	cols := `event_id, binlog_file, start_pos, end_pos, event_timestamp,
	         gtid, connection_id, schema_name, table_name, event_type, pk_values,
	         changed_columns, row_before, row_after, schema_version`

	dir := OrderDirection(opts.Order)
	outerOrderBy := " ORDER BY event_timestamp " + dir + ", event_id " + dir

	var q string
	if opts.LimitPerPK > 0 {
		// Per-PK cap via ROW_NUMBER. Inner ORDER BY DESC is fixed: it
		// selects "latest N events per pk_values" regardless of the
		// requested outer direction. Only the outer ORDER BY follows
		// opts.Order so callers can pick the most recent or the earliest
		// page across all PKs.
		inner := "SELECT " + cols + ", ROW_NUMBER() OVER (PARTITION BY pk_values" +
			" ORDER BY event_timestamp DESC, event_id DESC) AS bt_rn FROM binlog_events"
		if len(where) > 0 {
			inner += " WHERE " + strings.Join(where, " AND ")
		}
		q = "SELECT " + cols + " FROM (" + inner + ") AS t WHERE bt_rn <= ?"
		args = append(args, opts.LimitPerPK)
		q += outerOrderBy
	} else {
		q = "SELECT " + cols + " FROM binlog_events"
		if len(where) > 0 {
			q += " WHERE " + strings.Join(where, " AND ")
		}
		q += outerOrderBy
	}
	if opts.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, opts.Limit)
	}

	return q, args
}

// applyRedaction nulls out denied column values in RowBefore and RowAfter maps.
func applyRedaction(rows []ResultRow, redact []SchemaTableColumn) {
	type colKey struct{ schema, table, column string }
	set := make(map[colKey]struct{}, len(redact))
	for _, r := range redact {
		set[colKey{r.Schema, r.Table, r.Column}] = struct{}{}
	}
	for i := range rows {
		r := &rows[i]
		for col := range r.RowBefore {
			if _, ok := set[colKey{r.SchemaName, r.TableName, col}]; ok {
				r.RowBefore[col] = nil
			}
		}
		for col := range r.RowAfter {
			if _, ok := set[colKey{r.SchemaName, r.TableName, col}]; ok {
				r.RowAfter[col] = nil
			}
		}
	}
}

// ─── Row scanner ─────────────────────────────────────────────────────────────

func scanRows(rows *sql.Rows) ([]ResultRow, error) {
	var results []ResultRow
	for rows.Next() {
		var r ResultRow
		// Every NOT NULL column is scanned defensively. The migrations
		// declare them NOT NULL, but production has shown that customer
		// indexes can carry NULL in multiple columns simultaneously —
		// likely from external pipelines, partial-write paths, or
		// pre-constraint backfills. The first sighting (#318) was
		// binlog_file; #1484's deploy verification surfaced start_pos
		// on the same byos-202 tenant. Defending the entire Scan closes
		// the pattern. event_id stays a bare uint64 because
		// AUTO_INCREMENT cannot return NULL on read.
		var (
			binlogFile     sql.NullString
			startPos       sql.NullInt64
			endPos         sql.NullInt64
			eventTimestamp sql.NullTime
			gtid           sql.NullString
			connID         sql.NullInt64
			schemaName     sql.NullString
			tableName      sql.NullString
			eventType      sql.NullInt32
			pkValues       sql.NullString
			schemaVersion  sql.NullInt32
		)
		var changedCols, rowBefore, rowAfter []byte

		if err := rows.Scan(
			&r.EventID, &binlogFile, &startPos, &endPos, &eventTimestamp,
			&gtid, &connID, &schemaName, &tableName, &eventType, &pkValues,
			&changedCols, &rowBefore, &rowAfter, &schemaVersion,
		); err != nil {
			return nil, fmt.Errorf("failed to scan result row: %w", err)
		}
		if binlogFile.Valid {
			r.BinlogFile = binlogFile.String
		}
		if startPos.Valid {
			r.StartPos = uint64(startPos.Int64)
		}
		if endPos.Valid {
			r.EndPos = uint64(endPos.Int64)
		}
		if eventTimestamp.Valid {
			r.EventTimestamp = eventTimestamp.Time
		}
		if gtid.Valid {
			r.GTID = &gtid.String
		}
		if connID.Valid {
			v := uint32(connID.Int64)
			r.ConnectionID = &v
		}
		if schemaName.Valid {
			r.SchemaName = schemaName.String
		}
		if tableName.Valid {
			r.TableName = tableName.String
		}
		if eventType.Valid {
			r.EventType = parser.EventType(eventType.Int32)
		}
		if pkValues.Valid {
			r.PKValues = pkValues.String
		}
		if schemaVersion.Valid {
			r.SchemaVersion = uint32(schemaVersion.Int32)
		}
		if changedCols != nil {
			_ = json.Unmarshal(changedCols, &r.ChangedColumns)
		}
		if rowBefore != nil {
			_ = json.Unmarshal(rowBefore, &r.RowBefore)
		}
		if rowAfter != nil {
			_ = json.Unmarshal(rowAfter, &r.RowAfter)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// ─── Formatters ───────────────────────────────────────────────────────────────

const tsFormat = "2006-01-02 15:04:05"

// writeTable renders results as a human-readable aligned table.
// row_before and row_after are omitted to keep the output scannable;
// use --format json for full row data.
func writeTable(rows []ResultRow, w io.Writer) (int, error) {
	if len(rows) == 0 {
		fmt.Fprintln(w, "No results.")
		return 0, nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "ID\tTIMESTAMP\tTYPE\tSCHEMA\tTABLE\tPK_VALUES\tCHANGED_COLS\tGTID\tCONN_ID")
	fmt.Fprintln(tw, "──\t─────────\t────\t──────\t─────\t─────────\t────────────\t────\t───────")

	for i := range rows {
		r := &rows[i]
		gtid := "-"
		if r.GTID != nil {
			gtid = *r.GTID
		}
		connID := "-"
		if r.ConnectionID != nil {
			connID = fmt.Sprintf("%d", *r.ConnectionID)
		}
		changed := strings.Join(r.ChangedColumns, ",")
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			r.EventID,
			r.EventTimestamp.Format(tsFormat),
			eventTypeName(r.EventType),
			r.SchemaName,
			r.TableName,
			r.PKValues,
			changed,
			gtid,
			connID,
		)
	}
	return len(rows), nil
}

// jsonRow is the JSON-serialisable view of a ResultRow with string event type.
type jsonRow struct {
	EventID        uint64         `json:"event_id"`
	BinlogFile     string         `json:"binlog_file"`
	StartPos       uint64         `json:"start_pos"`
	EndPos         uint64         `json:"end_pos"`
	EventTimestamp string         `json:"event_timestamp"`
	GTID           *string        `json:"gtid"`
	ConnectionID   *uint32        `json:"connection_id"`
	SchemaName     string         `json:"schema_name"`
	TableName      string         `json:"table_name"`
	EventType      string         `json:"event_type"`
	PKValues       string         `json:"pk_values"`
	ChangedColumns []string       `json:"changed_columns"`
	RowBefore      map[string]any `json:"row_before"`
	RowAfter       map[string]any `json:"row_after"`
}

func writeJSON(rows []ResultRow, w io.Writer) (int, error) {
	out := make([]jsonRow, len(rows))
	for i, r := range rows {
		out[i] = jsonRow{
			EventID:        r.EventID,
			BinlogFile:     r.BinlogFile,
			StartPos:       r.StartPos,
			EndPos:         r.EndPos,
			EventTimestamp: r.EventTimestamp.Format(tsFormat),
			GTID:           r.GTID,
			ConnectionID:   r.ConnectionID,
			SchemaName:     r.SchemaName,
			TableName:      r.TableName,
			EventType:      eventTypeName(r.EventType),
			PKValues:       r.PKValues,
			ChangedColumns: r.ChangedColumns,
			RowBefore:      r.RowBefore,
			RowAfter:       r.RowAfter,
		}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return 0, fmt.Errorf("JSON encode failed: %w", err)
	}
	return len(rows), nil
}

// csvHeaders is the fixed column order for CSV output.
var csvHeaders = []string{
	"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
	"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
	"changed_columns", "row_before", "row_after",
}

func writeCSV(rows []ResultRow, w io.Writer) (int, error) {
	cw := csv.NewWriter(w)
	if err := cw.Write(csvHeaders); err != nil {
		return 0, err
	}
	for i := range rows {
		r := &rows[i]
		gtid := ""
		if r.GTID != nil {
			gtid = *r.GTID
		}
		connID := ""
		if r.ConnectionID != nil {
			connID = fmt.Sprintf("%d", *r.ConnectionID)
		}
		changed := ""
		if r.ChangedColumns != nil {
			b, _ := json.Marshal(r.ChangedColumns)
			changed = string(b)
		}
		before := ""
		if r.RowBefore != nil {
			b, _ := json.Marshal(r.RowBefore)
			before = string(b)
		}
		after := ""
		if r.RowAfter != nil {
			b, _ := json.Marshal(r.RowAfter)
			after = string(b)
		}
		record := []string{
			fmt.Sprintf("%d", r.EventID),
			r.BinlogFile,
			fmt.Sprintf("%d", r.StartPos),
			fmt.Sprintf("%d", r.EndPos),
			r.EventTimestamp.Format(tsFormat),
			gtid,
			connID,
			r.SchemaName,
			r.TableName,
			eventTypeName(r.EventType),
			r.PKValues,
			changed,
			before,
			after,
		}
		if err := cw.Write(record); err != nil {
			return i, err
		}
	}
	cw.Flush()
	return len(rows), cw.Error()
}

// ─── Utility ─────────────────────────────────────────────────────────────────

func eventTypeName(et parser.EventType) string {
	switch et {
	case parser.EventInsert:
		return "INSERT"
	case parser.EventUpdate:
		return "UPDATE"
	case parser.EventDelete:
		return "DELETE"
	case parser.EventSnapshot:
		return "SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}
