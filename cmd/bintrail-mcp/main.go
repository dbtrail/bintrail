// bintrail-mcp is an MCP (Model Context Protocol) server that exposes
// read-only Bintrail operations as tools: query, recover, and status.
//
// It communicates over stdio and is designed to be launched as a subprocess
// by an MCP-compatible client (e.g. Claude Code, Cursor, etc.).
//
// Configuration:
//
//	Set BINTRAIL_INDEX_DSN to the MySQL DSN for the index database,
//	or pass index_dsn as a parameter on each tool call.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/parser"
	"github.com/bintrail/bintrail/internal/query"
	"github.com/bintrail/bintrail/internal/recovery"
)

func main() {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "bintrail",
		Version: "v1.0.0",
	}, &mcp.ServerOptions{
		Instructions: "Bintrail MCP server for querying indexed MySQL binlog events, " +
			"generating recovery SQL, and viewing index status. " +
			"Set BINTRAIL_INDEX_DSN environment variable or pass index_dsn on each tool call.",
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "query",
		Description: "Search indexed MySQL binlog events with filters. " +
			"Returns matching events showing row changes (before/after images), timestamps, and metadata. " +
			"Use json format for full row data or table format for a human-readable summary.",
	}, queryTool)

	mcp.AddTool(server, &mcp.Tool{
		Name: "recover",
		Description: "Generate reversal SQL to undo matching binlog events (dry-run only). " +
			"Produces a BEGIN/COMMIT-wrapped SQL script that reverses events in chronological order: " +
			"DELETE->INSERT, UPDATE->reverse UPDATE, INSERT->DELETE. " +
			"Review carefully before applying to production.",
	}, recoverTool)

	mcp.AddTool(server, &mcp.Tool{
		Name: "status",
		Description: "Show the current state of the binlog index: " +
			"which files have been indexed, partition layout with estimated row counts, " +
			"and aggregate summary of indexed events.",
	}, statusTool)

	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatal(err)
	}
}

// ─── Tool argument types ─────────────────────────────────────────────────────

type queryArgs struct {
	IndexDSN      string `json:"index_dsn,omitempty" jsonschema:"description=MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var."`
	Schema        string `json:"schema,omitempty" jsonschema:"description=Filter by database schema name"`
	Table         string `json:"table,omitempty" jsonschema:"description=Filter by table name"`
	PK            string `json:"pk,omitempty" jsonschema:"description=Filter by primary key value (pipe-delimited for composite keys e.g. 123 or 123|2)"`
	EventType     string `json:"event_type,omitempty" jsonschema:"description=Filter by event type: INSERT UPDATE or DELETE"`
	GTID          string `json:"gtid,omitempty" jsonschema:"description=Filter by GTID (e.g. uuid:42)"`
	Since         string `json:"since,omitempty" jsonschema:"description=Filter events at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until         string `json:"until,omitempty" jsonschema:"description=Filter events at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	ChangedColumn string `json:"changed_column,omitempty" jsonschema:"description=Filter UPDATE events that modified this column"`
	Format        string `json:"format,omitempty" jsonschema:"description=Output format: json table or csv (default: json)"`
	Limit         int    `json:"limit,omitempty" jsonschema:"description=Maximum number of events to return (default: 100)"`
}

type recoverArgs struct {
	IndexDSN      string `json:"index_dsn,omitempty" jsonschema:"description=MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var."`
	Schema        string `json:"schema,omitempty" jsonschema:"description=Filter by database schema name"`
	Table         string `json:"table,omitempty" jsonschema:"description=Filter by table name"`
	PK            string `json:"pk,omitempty" jsonschema:"description=Filter by primary key value (pipe-delimited for composite keys)"`
	EventType     string `json:"event_type,omitempty" jsonschema:"description=Filter by event type: INSERT UPDATE or DELETE"`
	GTID          string `json:"gtid,omitempty" jsonschema:"description=Filter by GTID (e.g. uuid:42)"`
	Since         string `json:"since,omitempty" jsonschema:"description=Filter events at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until         string `json:"until,omitempty" jsonschema:"description=Filter events at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	ChangedColumn string `json:"changed_column,omitempty" jsonschema:"description=Filter UPDATE events that modified this column"`
	Limit         int    `json:"limit,omitempty" jsonschema:"description=Maximum number of events to reverse (default: 1000)"`
}

type statusArgs struct {
	IndexDSN string `json:"index_dsn,omitempty" jsonschema:"description=MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var."`
}

// ─── Tool handlers ───────────────────────────────────────────────────────────

func queryTool(ctx context.Context, req *mcp.CallToolRequest, args queryArgs) (*mcp.CallToolResult, any, error) {
	db, err := connectIndex(args.IndexDSN)
	if err != nil {
		return errorResult(err), nil, nil
	}
	defer db.Close()

	opts, err := buildQueryOptions(args.Schema, args.Table, args.PK, args.EventType,
		args.GTID, args.Since, args.Until, args.ChangedColumn, args.Limit, 100)
	if err != nil {
		return errorResult(err), nil, nil
	}

	format := args.Format
	if format == "" {
		format = "json"
	}
	if !isValidFormat(format) {
		return errorResult(fmt.Errorf("invalid format %q; must be json, table, or csv", format)), nil, nil
	}

	var buf bytes.Buffer
	engine := query.New(db)
	n, err := engine.Run(ctx, opts, format, &buf)
	if err != nil {
		return errorResult(err), nil, nil
	}

	text := buf.String()
	if n > 0 && format != "json" {
		text += fmt.Sprintf("\n%d row(s)\n", n)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: text},
		},
	}, nil, nil
}

func recoverTool(ctx context.Context, req *mcp.CallToolRequest, args recoverArgs) (*mcp.CallToolResult, any, error) {
	db, err := connectIndex(args.IndexDSN)
	if err != nil {
		return errorResult(err), nil, nil
	}
	defer db.Close()

	defaultLimit := 1000
	opts, err := buildQueryOptions(args.Schema, args.Table, args.PK, args.EventType,
		args.GTID, args.Since, args.Until, args.ChangedColumn, args.Limit, defaultLimit)
	if err != nil {
		return errorResult(err), nil, nil
	}

	// Load schema resolver best-effort for PK-only WHERE clauses.
	resolver, resolverErr := metadata.NewResolver(db, 0)
	if resolverErr != nil {
		resolver = nil
	}

	gen := recovery.New(db, resolver)
	var buf bytes.Buffer
	n, err := gen.GenerateSQL(ctx, opts, &buf)
	if err != nil {
		return errorResult(err), nil, nil
	}

	text := buf.String()
	if resolverErr != nil {
		text += fmt.Sprintf("\n-- Note: schema snapshot unavailable (%v); WHERE clauses use all columns.\n", resolverErr)
	}
	if n > 0 {
		text += fmt.Sprintf("\n-- %d reversal statement(s) generated.\n", n)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: text},
		},
	}, nil, nil
}

func statusTool(ctx context.Context, req *mcp.CallToolRequest, args statusArgs) (*mcp.CallToolResult, any, error) {
	dsn, err := resolveDSN(args.IndexDSN)
	if err != nil {
		return errorResult(err), nil, nil
	}

	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return errorResult(fmt.Errorf("invalid DSN: %w", err)), nil, nil
	}
	dbName := cfg.DBName
	if dbName == "" {
		return errorResult(fmt.Errorf("DSN must include a database name")), nil, nil
	}

	db, err := config.Connect(dsn)
	if err != nil {
		return errorResult(fmt.Errorf("failed to connect: %w", err)), nil, nil
	}
	defer db.Close()

	files, err := loadIndexState(ctx, db)
	if err != nil {
		return errorResult(fmt.Errorf("failed to load index state: %w", err)), nil, nil
	}

	parts, err := loadPartitionStats(ctx, db, dbName)
	if err != nil {
		return errorResult(fmt.Errorf("failed to load partition info: %w", err)), nil, nil
	}

	var buf bytes.Buffer
	writeStatus(&buf, files, parts)

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: buf.String()},
		},
	}, nil, nil
}

// ─── Shared helpers ──────────────────────────────────────────────────────────

func resolveDSN(override string) (string, error) {
	if override != "" {
		return override, nil
	}
	dsn := os.Getenv("BINTRAIL_INDEX_DSN")
	if dsn == "" {
		return "", fmt.Errorf("no index DSN: set BINTRAIL_INDEX_DSN env var or pass index_dsn parameter")
	}
	return dsn, nil
}

func connectIndex(override string) (*sql.DB, error) {
	dsn, err := resolveDSN(override)
	if err != nil {
		return nil, err
	}
	return config.Connect(dsn)
}

func errorResult(err error) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: err.Error()},
		},
		IsError: true,
	}
}

func buildQueryOptions(schema, table, pk, eventType, gtid, since, until, changedCol string, limit, defaultLimit int) (query.Options, error) {
	if pk != "" && (schema == "" || table == "") {
		return query.Options{}, fmt.Errorf("pk requires both schema and table")
	}
	if changedCol != "" && (schema == "" || table == "") {
		return query.Options{}, fmt.Errorf("changed_column requires both schema and table")
	}

	et, err := parseEventType(eventType)
	if err != nil {
		return query.Options{}, err
	}
	sinceT, err := parseTime(since)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid since: %w", err)
	}
	untilT, err := parseTime(until)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid until: %w", err)
	}

	if limit <= 0 {
		limit = defaultLimit
	}

	return query.Options{
		Schema:        schema,
		Table:         table,
		PKValues:      pk,
		EventType:     et,
		GTID:          gtid,
		Since:         sinceT,
		Until:         untilT,
		ChangedColumn: changedCol,
		Limit:         limit,
	}, nil
}

func parseEventType(s string) (*parser.EventType, error) {
	switch strings.ToUpper(s) {
	case "":
		return nil, nil
	case "INSERT":
		et := parser.EventInsert
		return &et, nil
	case "UPDATE":
		et := parser.EventUpdate
		return &et, nil
	case "DELETE":
		et := parser.EventDelete
		return &et, nil
	default:
		return nil, fmt.Errorf("invalid event_type %q; must be INSERT, UPDATE, or DELETE", s)
	}
}

func parseTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	// Try MySQL datetime format first.
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local)
	if err == nil {
		return &t, nil
	}
	// Try RFC 3339.
	t, err = time.Parse(time.RFC3339, s)
	if err == nil {
		return &t, nil
	}
	// Try date-only.
	t, err = time.ParseInLocation("2006-01-02", s, time.Local)
	if err == nil {
		return &t, nil
	}
	return nil, fmt.Errorf("invalid time %q; expected YYYY-MM-DD HH:MM:SS, RFC 3339, or YYYY-MM-DD", s)
}

func isValidFormat(s string) bool {
	switch strings.ToLower(s) {
	case "table", "json", "csv":
		return true
	}
	return false
}

// ─── Status helpers (adapted from cmd/bintrail/status.go) ────────────────────

type indexStateRow struct {
	BinlogFile    string
	Status        string
	EventsIndexed int64
	FileSize      int64
	LastPosition  int64
	StartedAt     time.Time
	CompletedAt   sql.NullTime
	ErrorMessage  sql.NullString
}

type partitionStat struct {
	Name        string
	Description string
	TableRows   int64
	Ordinal     int
}

func loadIndexState(ctx context.Context, db *sql.DB) ([]indexStateRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT binlog_file, status, events_indexed, file_size, last_position,
		       started_at, completed_at, error_message
		FROM index_state
		ORDER BY started_at, binlog_file`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []indexStateRow
	for rows.Next() {
		var r indexStateRow
		if err := rows.Scan(
			&r.BinlogFile, &r.Status, &r.EventsIndexed, &r.FileSize, &r.LastPosition,
			&r.StartedAt, &r.CompletedAt, &r.ErrorMessage,
		); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

func loadPartitionStats(ctx context.Context, db *sql.DB, dbName string) ([]partitionStat, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, IFNULL(PARTITION_DESCRIPTION, ''),
		       PARTITION_ORDINAL_POSITION, COALESCE(TABLE_ROWS, 0)
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'
		ORDER BY PARTITION_ORDINAL_POSITION`,
		dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []partitionStat
	for rows.Next() {
		var p partitionStat
		if err := rows.Scan(&p.Name, &p.Description, &p.Ordinal, &p.TableRows); err != nil {
			return nil, err
		}
		stats = append(stats, p)
	}
	return stats, rows.Err()
}

const statusTSFmt = "2006-01-02 15:04:05"

func writeStatus(buf *bytes.Buffer, files []indexStateRow, parts []partitionStat) {
	fmt.Fprintln(buf, "=== Indexed Files ===")
	if len(files) == 0 {
		fmt.Fprintln(buf, "  (no files indexed yet)")
	} else {
		tw := tabwriter.NewWriter(buf, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "FILE\tSTATUS\tEVENTS\tSTARTED_AT\tCOMPLETED_AT\tERROR")
		fmt.Fprintln(tw, "----\t------\t------\t----------\t------------\t-----")
		for _, f := range files {
			completedAt := "-"
			if f.CompletedAt.Valid {
				completedAt = f.CompletedAt.Time.Format(statusTSFmt)
			}
			errMsg := "-"
			if f.ErrorMessage.Valid && f.ErrorMessage.String != "" {
				errMsg = truncate(f.ErrorMessage.String, 60)
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n",
				f.BinlogFile, f.Status, f.EventsIndexed,
				f.StartedAt.Format(statusTSFmt),
				completedAt, errMsg,
			)
		}
		tw.Flush()
	}

	fmt.Fprintln(buf)
	fmt.Fprintln(buf, "=== Partitions ===")
	if len(parts) == 0 {
		fmt.Fprintln(buf, "  (no partitions found -- run 'bintrail init' first)")
	} else {
		tw := tabwriter.NewWriter(buf, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "PARTITION\tLESS_THAN\tROWS (est.)")
		fmt.Fprintln(tw, "---------\t---------\t-----------")
		var totalRows int64
		for _, p := range parts {
			fmt.Fprintf(tw, "%s\t%s\t%d\n", p.Name, descriptionToHuman(p.Description), p.TableRows)
			totalRows += p.TableRows
		}
		tw.Flush()
		fmt.Fprintf(buf, "Total events (est.): %d\n", totalRows)
	}

	if len(files) > 0 {
		counts := map[string]int{}
		var totalEvents int64
		for _, f := range files {
			counts[f.Status]++
			totalEvents += f.EventsIndexed
		}
		fmt.Fprintln(buf)
		fmt.Fprintln(buf, "=== Summary ===")
		fmt.Fprintf(buf, "Files:  %d completed, %d in_progress, %d failed\n",
			counts["completed"], counts["in_progress"], counts["failed"])
		fmt.Fprintf(buf, "Events: %d indexed\n", totalEvents)
	}
}

func descriptionToHuman(desc string) string {
	if desc == "" || strings.EqualFold(desc, "MAXVALUE") {
		return "MAXVALUE"
	}
	days, err := strconv.ParseInt(desc, 10, 64)
	if err != nil {
		return desc
	}
	return time.Unix((days-719528)*86400, 0).UTC().Format("2006-01-02 UTC")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
