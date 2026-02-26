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
	"log/slog"
	"os"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/bintrail/bintrail/internal/cliutil"
	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/query"
	"github.com/bintrail/bintrail/internal/recovery"
	"github.com/bintrail/bintrail/internal/status"
)

func newServer() *mcp.Server {
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
		Annotations: &mcp.ToolAnnotations{
			Title:          "Search binlog events",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, queryTool)

	mcp.AddTool(server, &mcp.Tool{
		Name: "recover",
		Description: "Generate reversal SQL to undo matching binlog events (dry-run only). " +
			"Produces a BEGIN/COMMIT-wrapped SQL script that reverses events in reverse chronological order (most recent first): " +
			"DELETE->INSERT, UPDATE->reverse UPDATE, INSERT->DELETE. " +
			"Review carefully before applying to production.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Generate recovery SQL",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, recoverTool)

	mcp.AddTool(server, &mcp.Tool{
		Name: "status",
		Description: "Show the current state of the binlog index: " +
			"which files have been indexed, partition layout with estimated row counts, " +
			"and aggregate summary of indexed events.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Index status",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, statusTool)

	return server
}

func main() {
	if err := newServer().Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		slog.Error("MCP server error", "error", err)
		os.Exit(1)
	}
}

// ─── Tool argument types ─────────────────────────────────────────────────────

type queryArgs struct {
	IndexDSN      string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var."`
	Schema        string `json:"schema,omitempty" jsonschema:"Filter by database schema name"`
	Table         string `json:"table,omitempty" jsonschema:"Filter by table name"`
	PK            string `json:"pk,omitempty" jsonschema:"Filter by primary key value (pipe-delimited for composite keys e.g. 123 or 123|2)"`
	EventType     string `json:"event_type,omitempty" jsonschema:"Filter by event type: INSERT UPDATE or DELETE"`
	GTID          string `json:"gtid,omitempty" jsonschema:"Filter by GTID (e.g. uuid:42)"`
	Since         string `json:"since,omitempty" jsonschema:"Filter events at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until         string `json:"until,omitempty" jsonschema:"Filter events at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	ChangedColumn string `json:"changed_column,omitempty" jsonschema:"Filter UPDATE events that modified this column"`
	Format        string `json:"format,omitempty" jsonschema:"Output format: json table or csv (default: json)"`
	Limit         int    `json:"limit,omitempty" jsonschema:"Maximum number of events to return (default: 100)"`
}

type recoverArgs struct {
	IndexDSN      string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var."`
	Schema        string `json:"schema,omitempty" jsonschema:"Filter by database schema name"`
	Table         string `json:"table,omitempty" jsonschema:"Filter by table name"`
	PK            string `json:"pk,omitempty" jsonschema:"Filter by primary key value (pipe-delimited for composite keys)"`
	EventType     string `json:"event_type,omitempty" jsonschema:"Filter by event type: INSERT UPDATE or DELETE"`
	GTID          string `json:"gtid,omitempty" jsonschema:"Filter by GTID (e.g. uuid:42)"`
	Since         string `json:"since,omitempty" jsonschema:"Filter events at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until         string `json:"until,omitempty" jsonschema:"Filter events at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	ChangedColumn string `json:"changed_column,omitempty" jsonschema:"Filter UPDATE events that modified this column"`
	Limit         int    `json:"limit,omitempty" jsonschema:"Maximum number of events to reverse (default: 1000)"`
}

type statusArgs struct {
	IndexDSN string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var."`
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
	if !cliutil.IsValidFormat(format) {
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

	files, err := status.LoadIndexState(ctx, db)
	if err != nil {
		return errorResult(fmt.Errorf("failed to load index state: %w", err)), nil, nil
	}

	parts, err := status.LoadPartitionStats(ctx, db, dbName)
	if err != nil {
		return errorResult(fmt.Errorf("failed to load partition info: %w", err)), nil, nil
	}

	var buf bytes.Buffer
	status.WriteStatus(&buf, files, parts)

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

	et, err := cliutil.ParseEventType(eventType)
	if err != nil {
		return query.Options{}, err
	}
	sinceT, err := cliutil.ParseTime(since)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid since: %w", err)
	}
	untilT, err := cliutil.ParseTime(until)
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

