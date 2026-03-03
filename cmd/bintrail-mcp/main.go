// bintrail-mcp is an MCP (Model Context Protocol) server that exposes
// read-only Bintrail operations as tools: query, recover, and status.
//
// By default it communicates over stdio (for use as a subprocess by Claude
// Code, Cursor, etc.). Pass --http <addr> to start an HTTP server instead,
// allowing any MCP client on the network to connect without a local binary.
//
//	bintrail-mcp                   # stdio (default)
//	bintrail-mcp --http :8080      # HTTP on all interfaces, port 8080
//
// Multi-tenant mode (for shared backends behind mcp-gateway):
//
//	bintrail-mcp --http :8080 --tenant-dsns tenant-dsns.json
//
// The gateway sends X-Bintrail-Tenant headers; the server resolves the
// tenant's index DSN from the provided JSON map file.
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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/bintrail/bintrail/internal/cliutil"
	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/query"
	"github.com/bintrail/bintrail/internal/recovery"
	"github.com/bintrail/bintrail/internal/status"
)

// mcpVersion is injected at build time via -ldflags.
var mcpVersion = "dev"

// tenantDSNs maps tenant IDs to their MySQL index DSNs. Loaded at startup
// from the --tenant-dsns JSON file when running in multi-tenant mode.
var tenantDSNs map[string]string

func newServer() *mcp.Server {
	return newServerWithDSN("")
}

// newServerWithDSN creates an MCP server. If dsnOverride is non-empty, all
// tool handlers will use it as the index DSN, ignoring the environment
// variable and tool-level index_dsn parameter. This is used in multi-tenant
// mode where the gateway resolves the DSN from the X-Bintrail-Tenant header.
func newServerWithDSN(dsnOverride string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "bintrail",
		Version: mcpVersion,
	}, &mcp.ServerOptions{
		Instructions: "Bintrail MCP server for querying indexed MySQL binlog events, " +
			"generating recovery SQL, and viewing index status. " +
			"Set BINTRAIL_INDEX_DSN environment variable or pass index_dsn on each tool call.",
	})

	// resolveFn picks the DSN: forced override > tool arg > env var.
	resolveFn := func(argDSN string) (string, error) {
		if dsnOverride != "" {
			return dsnOverride, nil
		}
		return resolveDSN(argDSN)
	}
	connectFn := func(argDSN string) (*sql.DB, error) {
		dsn, err := resolveFn(argDSN)
		if err != nil {
			return nil, err
		}
		return config.Connect(dsn)
	}

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
	}, makeQueryTool(connectFn))

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
	}, makeRecoverTool(connectFn))

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
	}, makeStatusTool(resolveFn))

	return server
}

func main() {
	httpAddr := flag.String("http", "", "HTTP listen address (e.g. :8080); omit to use stdio")
	tenantDSNsFile := flag.String("tenant-dsns", "", "JSON file mapping tenant IDs to index DSNs (multi-tenant mode)")
	flag.Parse()

	// Load tenant DSN map if provided.
	if *tenantDSNsFile != "" {
		data, err := os.ReadFile(*tenantDSNsFile)
		if err != nil {
			slog.Error("failed to read tenant DSNs file", "path", *tenantDSNsFile, "error", err)
			os.Exit(1)
		}
		if err := json.Unmarshal(data, &tenantDSNs); err != nil {
			slog.Error("failed to parse tenant DSNs file", "path", *tenantDSNsFile, "error", err)
			os.Exit(1)
		}
		slog.Info("loaded tenant DSN map", "tenants", len(tenantDSNs))
	}

	ctx := context.Background()

	if *httpAddr != "" {
		handler := mcp.NewStreamableHTTPHandler(
			func(r *http.Request) *mcp.Server {
				// In multi-tenant mode, resolve DSN from the X-Bintrail-Tenant header.
				if tenantDSNs != nil {
					tenant := r.Header.Get("X-Bintrail-Tenant")
					if tenant != "" {
						if dsn, ok := tenantDSNs[tenant]; ok {
							slog.Debug("resolved tenant DSN", "tenant", tenant)
							return newServerWithDSN(dsn)
						}
						slog.Warn("unknown tenant", "tenant", tenant)
						// Return a server that will error on every tool call
						// rather than falling through to the env-var DSN.
						return newServerWithDSN("unknown-tenant:invalid")
					}
				}
				return newServer()
			},
			nil,
		)
		mux := http.NewServeMux()
		mux.Handle("/mcp", handler)

		srv := &http.Server{Addr: *httpAddr, Handler: mux}

		// Shut down gracefully on SIGINT/SIGTERM.
		sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		go func() {
			<-sigCtx.Done()
			slog.Info("MCP HTTP server shutting down")
			if err := srv.Shutdown(context.Background()); err != nil {
				slog.Error("MCP HTTP server shutdown error", "error", err)
			}
		}()

		slog.Info("MCP HTTP server starting", "addr", *httpAddr, "endpoint", "/mcp")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("MCP HTTP server error", "error", err)
			os.Exit(1)
		}
		return
	}

	if err := newServer().Run(ctx, &mcp.StdioTransport{}); err != nil {
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
	Flag          string `json:"flag,omitempty" jsonschema:"Filter events from tables or columns carrying this flag"`
	Format        string `json:"format,omitempty" jsonschema:"Output format: json table or csv (default: json)"`
	Limit         int    `json:"limit,omitempty" jsonschema:"Maximum number of events to return (default: 100)"`
	Profile       string `json:"profile,omitempty" jsonschema:"Apply RBAC access rules for this profile (table-level deny and column-level redaction)"`
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
	Flag          string `json:"flag,omitempty" jsonschema:"Filter events from tables or columns carrying this flag"`
	Limit         int    `json:"limit,omitempty" jsonschema:"Maximum number of events to reverse (default: 1000)"`
	Profile       string `json:"profile,omitempty" jsonschema:"Apply RBAC access rules for this profile (table-level deny and column-level redaction)"`
}

type statusArgs struct {
	IndexDSN string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var."`
}

// connectFunc abstracts DSN resolution and DB connection for tool handlers.
type connectFunc func(argDSN string) (*sql.DB, error)

// resolveFunc abstracts DSN resolution for handlers that need the raw DSN.
type resolveFunc func(argDSN string) (string, error)

// ─── Tool handler factories ──────────────────────────────────────────────────
//
// Each factory returns a closure that captures the DSN resolution function.
// In single-tenant mode, the resolution falls through to the env var.
// In multi-tenant mode, the DSN override is baked in at server creation time.

func makeQueryTool(connect connectFunc) func(context.Context, *mcp.CallToolRequest, queryArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args queryArgs) (*mcp.CallToolResult, any, error) {
		db, err := connect(args.IndexDSN)
		if err != nil {
			return errorResult(err), nil, nil
		}
		defer db.Close()

		opts, err := buildQueryOptions(args.Schema, args.Table, args.PK, args.EventType,
			args.GTID, args.Since, args.Until, args.ChangedColumn, args.Flag, args.Limit, 100)
		if err != nil {
			return errorResult(err), nil, nil
		}

		if args.Profile != "" {
			denyTables, redactCols, err := query.LoadProfileRules(ctx, db, args.Profile)
			if err != nil {
				return errorResult(fmt.Errorf("load profile rules: %w", err)), nil, nil
			}
			opts.DenyTables = denyTables
			opts.RedactColumns = redactCols
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
}

func makeRecoverTool(connect connectFunc) func(context.Context, *mcp.CallToolRequest, recoverArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args recoverArgs) (*mcp.CallToolResult, any, error) {
		db, err := connect(args.IndexDSN)
		if err != nil {
			return errorResult(err), nil, nil
		}
		defer db.Close()

		defaultLimit := 1000
		opts, err := buildQueryOptions(args.Schema, args.Table, args.PK, args.EventType,
			args.GTID, args.Since, args.Until, args.ChangedColumn, args.Flag, args.Limit, defaultLimit)
		if err != nil {
			return errorResult(err), nil, nil
		}

		if args.Profile != "" {
			denyTables, redactCols, err := query.LoadProfileRules(ctx, db, args.Profile)
			if err != nil {
				return errorResult(fmt.Errorf("load profile rules: %w", err)), nil, nil
			}
			opts.DenyTables = denyTables
			opts.RedactColumns = redactCols
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
}

func makeStatusTool(resolve resolveFunc) func(context.Context, *mcp.CallToolRequest, statusArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args statusArgs) (*mcp.CallToolResult, any, error) {
		dsn, err := resolve(args.IndexDSN)
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

		// Best-effort: servers info from bintrail_servers table.
		servers, serversErr := status.LoadServers(ctx, db)
		if serversErr != nil {
			slog.Warn("could not load servers", "error", serversErr)
			servers = nil
		}

		// Best-effort: stream state from stream_state table.
		stream, streamErr := status.LoadStreamState(ctx, db)
		if streamErr != nil {
			slog.Warn("could not load stream state", "error", streamErr)
			stream = nil
		}

		// Best-effort: coverage info from schema_changes table.
		coverage, coverageErr := status.LoadCoverage(ctx, db)
		if coverageErr != nil {
			slog.Warn("could not load coverage info", "error", coverageErr)
			coverage = nil
		}

		var buf bytes.Buffer
		status.WriteStatus(&buf, files, parts, nil, coverage, servers, stream)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: buf.String()},
			},
		}, nil, nil
	}
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

func errorResult(err error) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: err.Error()},
		},
		IsError: true,
	}
}

func buildQueryOptions(schema, table, pk, eventType, gtid, since, until, changedCol, flagVal string, limit, defaultLimit int) (query.Options, error) {
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
		Flag:          flagVal,
		Limit:         limit,
	}, nil
}
