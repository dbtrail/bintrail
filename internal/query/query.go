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
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
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
	EventType     *parser.EventType // nil = all types
	GTID          string
	Since         *time.Time
	Until         *time.Time
	ChangedColumn string // column name; matched via JSON_CONTAINS
	ColumnEq      []ColumnEq // match against values inside row_after / row_before
	Flag          string     // return events from tables/columns carrying this flag
	Limit         int        // 0 → default 100

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
		// Column name is allowlisted by ParseColumnEq to [A-Za-z0-9_], so
		// interpolating it into the JSON path literal is safe. MySQL does not
		// accept bind parameters for JSON paths, so the column name MUST be
		// embedded in the SQL string.
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

	q := `SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp,
	             gtid, connection_id, schema_name, table_name, event_type, pk_values,
	             changed_columns, row_before, row_after, schema_version
	      FROM binlog_events`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY event_timestamp, event_id"
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
		var gtid sql.NullString
		var connID sql.NullInt64
		var changedCols, rowBefore, rowAfter []byte

		if err := rows.Scan(
			&r.EventID, &r.BinlogFile, &r.StartPos, &r.EndPos, &r.EventTimestamp,
			&gtid, &connID, &r.SchemaName, &r.TableName, &r.EventType, &r.PKValues,
			&changedCols, &rowBefore, &rowAfter, &r.SchemaVersion,
		); err != nil {
			return nil, fmt.Errorf("failed to scan result row: %w", err)
		}
		if gtid.Valid {
			r.GTID = &gtid.String
		}
		if connID.Valid {
			v := uint32(connID.Int64)
			r.ConnectionID = &v
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
	default:
		return "UNKNOWN"
	}
}
