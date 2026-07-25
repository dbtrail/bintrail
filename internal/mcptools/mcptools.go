// Package mcptools implements the read-only Bintrail MCP tools — query,
// recover, status, list_schema_changes, and the opt-in reconstruct (#953) —
// decoupled from how the index connection is obtained.
//
// Two surfaces consume it:
//
//   - cmd/bintrail-mcp (standalone server): the index is resolved per tool
//     call from a DSN (tool-level index_dsn parameter, BINTRAIL_INDEX_DSN env
//     var, or a multi-tenant override), a fresh connection is opened and
//     closed around every call, and the idempotent schema migration runs on
//     it (the server owns no long-lived connection).
//   - internal/console (/mcp endpoint): the index is a long-lived connManager
//     bundle owned by the console, the DSN parameters are rejected (an
//     authenticated MCP client must not be able to point the console at an
//     arbitrary DSN), and the console's read boundary applies — result caps,
//     process-global RBAC posture, and query_text/query_hash withheld to
//     match the events API's eventDTO.
//
// The seam is ResolveTarget: a callback mapping the (possibly empty) tool
// index_dsn argument to a Target carrying the connection plus the posture the
// serving surface imposes on it.
package mcptools

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/status"
)

// DefaultQueryLimit and DefaultRecoverLimit are the per-tool defaults applied
// when the caller passes no limit (or a non-positive one). They match the CLI
// defaults and the console API's default caps.
const (
	DefaultQueryLimit   = 100
	DefaultRecoverLimit = 1000
)

// Target is one resolved index a tool call runs against: the open connection
// plus the read posture the serving surface imposes on it.
type Target struct {
	// DB is the open index connection. Never nil on a successful resolve.
	DB *sql.DB
	// DBName is the index database name, required by the status tool (the
	// query planner and partition inspection scope to it). May be empty on
	// the standalone surface when the DSN carries no database name — the
	// status tool then refuses with an actionable error.
	DBName string
	// CloseDB is true when the connection was opened for this call and the
	// handler must close it (standalone). False when the connection is owned
	// by a long-lived pool (console connManager bundles).
	CloseDB bool
	// EnsureSchema runs the idempotent indexer schema migration before the
	// query/recover engines touch binlog_events (they SELECT
	// post-initial-schema columns). True on the standalone surface, which
	// opens a fresh connection per call. False on the console: the boot entry
	// is migrated by the cmd layer at startup, and registry servers are
	// deliberately NEVER migrated (the console's read-only contract confines
	// DDL to the command-line DSN).
	EnsureSchema bool
	// EnvArchiveDiscovery consults BINTRAIL_ARCHIVE_S3 + BINTRAIL_ID before
	// falling back to archive_state auto-discovery (standalone behavior).
	// False on the console, whose archive posture is per-server.
	EnvArchiveDiscovery bool
	// NoArchive disables Parquet archive auto-discovery outright — the
	// console bundle's per-server posture (its own flag OR an active RBAC
	// profile, which archives do not enforce).
	NoArchive bool
	// DenyTables / RedactColumns / ProfileActive are RBAC rules the surface
	// attaches to every query (the console's process-global profile). The
	// standalone surface leaves them empty and loads rules from the tool's
	// profile parameter instead.
	DenyTables    []query.SchemaTable
	RedactColumns []query.SchemaTableColumn
	ProfileActive bool
	// Resolver is a preloaded schema resolver for recovery WHERE clauses.
	// Consulted only when ResolverLoaded is true (the console preloads its
	// per-bundle resolver, which may legitimately be nil); otherwise the
	// recover tool loads the latest snapshot best-effort per call.
	Resolver       *metadata.Resolver
	ResolverLoaded bool
	// RedactStatementText blanks query_text/query_hash on every fetched row
	// before formatting, so the query tool's output carries the same field
	// content as the console events API (whose eventDTO omits statement text,
	// #699). Set by the console surface only.
	RedactStatementText bool
	// FindBaseline is the reconstruct tool's baseline lookup on surfaces that
	// own baseline routing (the console binds its bundle's findBaseline, which
	// carries the #766 local→S3 fallback). Left nil on the standalone surface,
	// which binds a source from the tool parameters or the environment instead
	// — see resolveBaselineLookup.
	FindBaseline FindBaselineFunc
	// BaselineConfigured gates the reconstruct tool for THIS target on surfaces
	// that own baseline routing: the console's per-server signal, which already
	// folds in "a baseline location is set" AND "archives are enabled" AND "no
	// RBAC profile is active" (see internal/console's newBundleDerived). Unused
	// on the standalone surface, whose gate is whether a baseline source
	// resolved at all.
	BaselineConfigured bool
}

// release closes the connection when this call owns it.
func (t *Target) release() {
	if t.CloseDB && t.DB != nil {
		_ = t.DB.Close()
	}
}

// stripStatementText enforces RedactStatementText on a fetched row set.
func (t *Target) stripStatementText(rows []query.ResultRow) {
	if !t.RedactStatementText {
		return
	}
	for i := range rows {
		rows[i].QueryText = nil
		rows[i].QueryHash = nil
	}
}

// archiveSources returns Parquet archive source paths for this target, or nil
// when archives are disabled or discovery fails (the MCP tools are
// deliberately permissive: serve without archives rather than fail the call).
func (t *Target) archiveSources(ctx context.Context) []string {
	if t.NoArchive {
		return nil
	}
	if t.EnvArchiveDiscovery {
		return EnvArchiveSources(ctx, t.DB)
	}
	return stateArchiveSources(ctx, t.DB)
}

// ResolveTarget maps the tool-level index_dsn argument (empty on surfaces
// that reject it) to the Target the call runs against.
type ResolveTarget func(ctx context.Context, argDSN string) (*Target, error)

// Config assembles an MCP server over the read-only tools.
type Config struct {
	// Version is the reported mcp.Implementation version.
	Version string
	// Instructions is the server-level usage hint sent to clients.
	Instructions string
	// Resolve obtains the Target for each tool call.
	Resolve ResolveTarget
	// AllowDSNParam accepts the tool-level index_dsn parameter (standalone).
	// When false the parameter is rejected with an explicit error — the
	// serving surface owns connection routing (console).
	AllowDSNParam bool
	// AllowProfileParam accepts the tool-level profile parameter
	// (standalone). When false it is rejected — the surface's RBAC posture
	// is fixed at process start (console).
	AllowProfileParam bool
	// Reconstruct registers the reconstruct (time-travel) tool. Opt-in per
	// surface rather than unconditional in NewServer: a surface that cannot
	// supply a baseline lookup (neither Target.FindBaseline nor the
	// baseline_dir/baseline_s3 parameters) would otherwise advertise a tool
	// that can only ever error.
	Reconstruct bool
	// AllowBaselineParams accepts the reconstruct tool's baseline_dir /
	// baseline_s3 parameters, with BINTRAIL_BASELINE_DIR / BINTRAIL_BASELINE_S3
	// as the fallback (standalone). When false they are rejected exactly like
	// index_dsn is, and the lookup comes from Target.FindBaseline instead — the
	// serving surface owns baseline routing (console).
	AllowBaselineParams bool
	// QueryMaxLimit returns the hard row ceiling for the query tool. nil
	// means EnvQueryMaxLimit (the standalone default, #654).
	QueryMaxLimit func() int
	// RecoverMaxLimit caps the recover tool's limit; 0 means uncapped
	// (standalone behavior — the CLI is the bounded-review path there).
	RecoverMaxLimit int
	// MaxScriptBytes overrides the reversal-script payload budget
	// (recovery.Generator.SetMaxScriptBytes) the recover tool enforces before
	// rendering. 0 means "leave it alone" — the Generator already defaults to
	// recovery.DefaultMaxScriptBytes (2 GiB, #654) on its own, so standalone
	// bintrail-mcp's behavior is unchanged whether or not this field is set.
	// The console sets it to its own tighter, shared-daemon budget (#849,
	// internal/console's recoverMaxScriptBytes) — the console's /mcp endpoint
	// is the same 2 GiB-by-default gap #849 closed for /api/recover, reached
	// by the SAME row-count cap (RecoverMaxLimit) without a byte cap.
	MaxScriptBytes int64
	// AuditSurface tags ext.Record audit events; "" means "mcp".
	AuditSurface string
}

func (c Config) queryMaxLimit() int {
	if c.QueryMaxLimit != nil {
		return c.QueryMaxLimit()
	}
	return EnvQueryMaxLimit()
}

func (c Config) auditSurface() string {
	if c.AuditSurface == "" {
		return "mcp"
	}
	return c.AuditSurface
}

// scriptBudgetOverride reports whether the recover tool should call
// Generator.SetMaxScriptBytes, and with what value. ok is false when
// c.MaxScriptBytes is unset (<= 0) — the caller must then leave the
// Generator's own constructor default (recovery.DefaultMaxScriptBytes, 2 GiB)
// untouched, which is what standalone bintrail-mcp relies on.
//
// This is deliberately its own method, not an inline `if cfg.MaxScriptBytes >
// 0 { gen.SetMaxScriptBytes(...) }` at the call site (#849 code review): "0 =
// don't touch it" and "0 = call SetMaxScriptBytes(0)" are NOT the same thing
// — SetMaxScriptBytes(0) explicitly DISABLES the budget guard
// (recovery.Generator.SetMaxScriptBytes: "n <= 0 disables the guard
// (unlimited)"). A future refactor that simplified the call site to
// `gen.SetMaxScriptBytes(cfg.MaxScriptBytes)` unconditionally would silently
// make the standalone posture (MaxScriptBytes never set) render with NO
// budget at all, re-opening the exact class of bug #654/#849 close. Isolating
// the decision here lets TestConfigScriptBudgetOverride pin it directly
// (Config{}.scriptBudgetOverride() must be (0, false)) without needing an
// impractical multi-gigabyte test payload to observe the difference
// end-to-end.
func (c Config) scriptBudgetOverride() (int64, bool) {
	if c.MaxScriptBytes > 0 {
		return c.MaxScriptBytes, true
	}
	return 0, false
}

// NewServer builds an MCP server exposing the read-only tools bound to cfg:
// query, recover, status and list_schema_changes always, plus reconstruct when
// cfg.Reconstruct is set. All tools are annotated read-only + idempotent.
func NewServer(cfg Config) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "bintrail",
		Version: cfg.Version,
	}, &mcp.ServerOptions{
		Instructions: cfg.Instructions,
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
	}, MakeQueryTool(cfg))

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
	}, MakeRecoverTool(cfg))

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
	}, MakeStatusTool(cfg))

	mcp.AddTool(server, &mcp.Tool{
		Name: "list_schema_changes",
		Description: "List DDL schema changes (CREATE, ALTER, DROP, RENAME, TRUNCATE) " +
			"recorded during binlog indexing or streaming. " +
			"Returns the full DDL statement, binlog coordinates, and timestamp for each change.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "List schema changes",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, MakeSchemaChangesTool(cfg))

	// Opt-in (#953): only surfaces that can supply a baseline lookup advertise
	// it — see Config.Reconstruct.
	if cfg.Reconstruct {
		mcp.AddTool(server, &mcp.Tool{
			Name: "reconstruct",
			Description: "Reconstruct a single row's full state at a point in time (time travel). " +
				"Folds a baseline snapshot with the indexed events after it, so columns never touched " +
				"in the retained window resolve correctly — unlike `recover`, which only reverses events it has. " +
				"Use history=true for every state transition up to that time. " +
				"Requires a baseline snapshot produced by `bintrail baseline`.",
			Annotations: &mcp.ToolAnnotations{
				Title:          "Reconstruct row state at a point in time",
				ReadOnlyHint:   true,
				IdempotentHint: true,
			},
		}, MakeReconstructTool(cfg))
	}

	return server
}

// DSNTarget adapts a DSN resolution function (override > tool arg > env var,
// on the standalone surface) into a ResolveTarget that opens a fresh
// connection per call with the standalone posture: caller-closed, schema
// migration on, env-var archive discovery.
func DSNTarget(resolve func(argDSN string) (string, error)) ResolveTarget {
	return func(ctx context.Context, argDSN string) (*Target, error) {
		dsn, err := resolve(argDSN)
		if err != nil {
			return nil, err
		}
		cfg, err := mysqldriver.ParseDSN(dsn)
		if err != nil {
			return nil, fmt.Errorf("invalid DSN: %w", err)
		}
		db, err := config.Connect(dsn)
		if err != nil {
			return nil, err
		}
		return &Target{
			DB:                  db,
			DBName:              cfg.DBName,
			CloseDB:             true,
			EnsureSchema:        true,
			EnvArchiveDiscovery: true,
		}, nil
	}
}

// ─── Tool argument types ─────────────────────────────────────────────────────

// QueryArgs are the query tool's parameters.
type QueryArgs struct {
	IndexDSN      string   `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
	Schema        string   `json:"schema,omitempty" jsonschema:"Filter by database schema name"`
	Table         string   `json:"table,omitempty" jsonschema:"Filter by table name"`
	PK            string   `json:"pk,omitempty" jsonschema:"Filter by primary key value (pipe-delimited for composite keys e.g. 123 or 123|2)"`
	EventType     string   `json:"event_type,omitempty" jsonschema:"Filter by event type: INSERT UPDATE or DELETE"`
	GTID          string   `json:"gtid,omitempty" jsonschema:"Filter by GTID (e.g. uuid:42)"`
	Since         string   `json:"since,omitempty" jsonschema:"Filter events at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until         string   `json:"until,omitempty" jsonschema:"Filter events at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	ChangedColumn string   `json:"changed_column,omitempty" jsonschema:"Filter UPDATE events that modified this column"`
	ColumnEq      []string `json:"column_eq,omitempty" jsonschema:"Filter events where a column in row_after or row_before equals the given value. Each entry is column=value. Repeat for AND. Literal NULL matches JSON null."`
	Flag          string   `json:"flag,omitempty" jsonschema:"Filter events from tables or columns carrying this flag"`
	Format        string   `json:"format,omitempty" jsonschema:"Output format: json table or csv (default: json)"`
	Limit         int      `json:"limit,omitempty" jsonschema:"Maximum number of events to return (default: 100)"`
	Profile       string   `json:"profile,omitempty" jsonschema:"Apply RBAC access rules for this profile (table-level deny and column-level redaction)"`
	NoArchive     bool     `json:"no_archive,omitempty" jsonschema:"Disable auto-routing to Parquet archives (MySQL-only results)"`
}

// RecoverArgs are the recover tool's parameters.
type RecoverArgs struct {
	IndexDSN      string   `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
	Schema        string   `json:"schema,omitempty" jsonschema:"Filter by database schema name"`
	Table         string   `json:"table,omitempty" jsonschema:"Filter by table name"`
	PK            string   `json:"pk,omitempty" jsonschema:"Filter by primary key value (pipe-delimited for composite keys)"`
	EventType     string   `json:"event_type,omitempty" jsonschema:"Filter by event type: INSERT UPDATE or DELETE"`
	GTID          string   `json:"gtid,omitempty" jsonschema:"Filter by GTID (e.g. uuid:42)"`
	Since         string   `json:"since,omitempty" jsonschema:"Filter events at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until         string   `json:"until,omitempty" jsonschema:"Filter events at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	ChangedColumn string   `json:"changed_column,omitempty" jsonschema:"Filter UPDATE events that modified this column"`
	ColumnEq      []string `json:"column_eq,omitempty" jsonschema:"Filter events where a column in row_after or row_before equals the given value. Each entry is column=value. Repeat for AND. Literal NULL matches JSON null."`
	Flag          string   `json:"flag,omitempty" jsonschema:"Filter events from tables or columns carrying this flag"`
	Limit         int      `json:"limit,omitempty" jsonschema:"Maximum number of events to reverse (default: 1000)"`
	Profile       string   `json:"profile,omitempty" jsonschema:"Apply RBAC access rules for this profile (table-level deny and column-level redaction)"`
	NoArchive     bool     `json:"no_archive,omitempty" jsonschema:"Disable auto-routing to Parquet archives (MySQL-only results)"`
}

// StatusArgs are the status tool's parameters.
type StatusArgs struct {
	IndexDSN string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
}

// SchemaChangesArgs are the list_schema_changes tool's parameters.
type SchemaChangesArgs struct {
	IndexDSN string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
	Schema   string `json:"schema,omitempty" jsonschema:"Filter by database schema name"`
	Table    string `json:"table,omitempty" jsonschema:"Filter by table name"`
	DDLType  string `json:"ddl_type,omitempty" jsonschema:"Filter by DDL type: CREATE ALTER DROP RENAME or TRUNCATE"`
	Since    string `json:"since,omitempty" jsonschema:"Filter changes at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until    string `json:"until,omitempty" jsonschema:"Filter changes at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Limit    int    `json:"limit,omitempty" jsonschema:"Maximum number of changes to return (default: 100)"`
}

// rejectSurfaceParams enforces the surface's parameter policy: a non-nil
// result is the tool error to return. The messages are deliberately explicit
// — an agent must learn the surface's routing model, not retry blindly.
func rejectSurfaceParams(cfg Config, indexDSN, profile string) *mcp.CallToolResult {
	if !cfg.AllowDSNParam && indexDSN != "" {
		return ErrorResult(errors.New(
			"index_dsn is not accepted here: this server routes connections itself " +
				"(select a server via the /mcp/{id-or-name} URL path; connections are managed in the console)"))
	}
	if !cfg.AllowProfileParam && profile != "" {
		return ErrorResult(errors.New(
			"profile is not accepted here: the RBAC posture is fixed by the serving process configuration"))
	}
	return nil
}

// ─── Tool handler factories ──────────────────────────────────────────────────
//
// Each factory returns a closure over the Config: the target resolution seam
// plus the surface's caps and parameter policy.

// MakeQueryTool returns the query tool handler.
func MakeQueryTool(cfg Config) func(context.Context, *mcp.CallToolRequest, QueryArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args QueryArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, args.Profile); res != nil {
			return res, nil, nil
		}
		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		// Same idempotent migration as the recover tool below: the query
		// engine SELECTs post-initial-schema binlog_events columns
		// (query_text/query_hash, #699). Standalone-only — the console never
		// migrates its servers (see Target.EnsureSchema).
		if t.EnsureSchema {
			if err := indexer.EnsureSchema(t.DB); err != nil {
				return ErrorResult(indexer.WrapSchemaMigrationErr(err)), nil, nil
			}
		}

		opts, err := BuildQueryOptions(args.Schema, args.Table, args.PK, args.EventType,
			args.GTID, args.Since, args.Until, args.ChangedColumn, args.ColumnEq, args.Flag, args.Limit, DefaultQueryLimit)
		if err != nil {
			return ErrorResult(err), nil, nil
		}

		// Hard ceiling on an explicit, oversized limit (#654). BuildQueryOptions
		// already coerces limit<=0 to the default, so this only bounds a large
		// EXPLICIT value an agent might pass. It is applied here, per-tool, on the
		// local opts — NOT in the shared BuildQueryOptions, because the recover
		// tool must refuse on size, not silently cap a read.
		ceiling := cfg.queryMaxLimit()
		requestedLimit := opts.Limit
		ceilingApplied := false
		if c, did := ApplyQueryCeiling(opts.Limit, ceiling); did {
			opts.Limit = c
			ceilingApplied = true
		}

		// The surface's RBAC posture always applies (the console's
		// process-global profile); the per-call profile parameter — standalone
		// only — layers the named profile's rules on top.
		opts.DenyTables = t.DenyTables
		opts.RedactColumns = t.RedactColumns
		opts.ProfileActive = t.ProfileActive
		if args.Profile != "" {
			denyTables, redactCols, err := query.LoadProfileRules(ctx, t.DB, args.Profile)
			if err != nil {
				return ErrorResult(fmt.Errorf("load profile rules: %w", err)), nil, nil
			}
			opts.DenyTables = denyTables
			opts.RedactColumns = redactCols
			opts.ProfileActive = true
		}

		format := args.Format
		if format == "" {
			format = "json"
		}
		if !cliutil.IsValidFormat(format) {
			return ErrorResult(fmt.Errorf("invalid format %q; must be json, table, or csv", format)), nil, nil
		}

		engine := query.New(t.DB)

		// Skip archive auto-discovery when no_archive is set or when an RBAC
		// profile is active (archive queries do not enforce rules). The
		// target's own posture (console per-server no-archive, which already
		// folds the process profile in) is enforced inside archiveSources.
		var archSources []string
		if !args.NoArchive && args.Profile == "" {
			archSources = t.archiveSources(ctx)
		}

		var buf bytes.Buffer
		var n int

		if len(archSources) == 0 && !t.RedactStatementText {
			// Fast path: no archives and no post-fetch redaction — fetch and
			// format in one step.
			n, err = engine.Run(ctx, opts, format, &buf)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
		} else {
			// Fetch from live index (+ archives when present), merge, redact,
			// then format.
			fetchOpts := opts
			results, err := engine.Fetch(ctx, fetchOpts)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
			for _, src := range archSources {
				ar, err := parquetquery.Fetch(ctx, fetchOpts, src)
				if err != nil {
					slog.Warn("archive query failed, skipping", "source", src, "error", err)
					continue
				}
				results = append(results, ar...)
			}
			if len(archSources) > 0 {
				results = query.MergeResults(results, opts.Limit, opts.Order)
			}
			t.stripStatementText(results)
			n, err = query.Format(results, format, &buf)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
		}

		text := buf.String()
		if n > 0 && format != "json" {
			text += fmt.Sprintf("\n%d row(s)\n", n)
		}
		text += QueryResultNotice(ceilingApplied, requestedLimit, ceiling, n, opts.Limit)

		ext.Record(ctx, ext.AuditEvent{
			Surface: cfg.auditSurface(),
			Action:  "query.run",
			Actor:   ext.ProcessActor(args.Profile),
			Schema:  args.Schema,
			Table:   args.Table,
			Detail:  map[string]string{"results": strconv.Itoa(n), "format": format},
		})

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
			},
		}, nil, nil
	}
}

// MakeRecoverTool returns the recover tool handler.
func MakeRecoverTool(cfg Config) func(context.Context, *mcp.CallToolRequest, RecoverArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args RecoverArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, args.Profile); res != nil {
			return res, nil, nil
		}
		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		// Run the idempotent schema migration before NewResolver. Since
		// #212 NewResolver reads schema_snapshots.column_type and fails on
		// pre-migration databases with Error 1054. Standalone-only — see
		// Target.EnsureSchema.
		if t.EnsureSchema {
			if err := indexer.EnsureSchema(t.DB); err != nil {
				return ErrorResult(indexer.WrapSchemaMigrationErr(err)), nil, nil
			}
		}

		opts, err := BuildQueryOptions(args.Schema, args.Table, args.PK, args.EventType,
			args.GTID, args.Since, args.Until, args.ChangedColumn, args.ColumnEq, args.Flag, args.Limit, DefaultRecoverLimit)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		// The surface's hard cap on the reversal window (console: same cap as
		// the /api/recover endpoint). The truncation warning below fires when
		// the capped limit is reached, so a clipped window is never silent.
		if cfg.RecoverMaxLimit > 0 && opts.Limit > cfg.RecoverMaxLimit {
			opts.Limit = cfg.RecoverMaxLimit
		}
		// When --limit truncates the matched window it must keep the most
		// RECENT events (#785/#927): the ASC default would keep the OLDEST
		// prefix, undoing old events underneath later un-reverted ones — a
		// state that never historically existed. Rows are re-sorted ascending
		// after the fetch, before generation (see below).
		opts.Order = "DESC"

		opts.DenyTables = t.DenyTables
		opts.RedactColumns = t.RedactColumns
		opts.ProfileActive = t.ProfileActive
		if args.Profile != "" {
			denyTables, redactCols, err := query.LoadProfileRules(ctx, t.DB, args.Profile)
			if err != nil {
				return ErrorResult(fmt.Errorf("load profile rules: %w", err)), nil, nil
			}
			opts.DenyTables = denyTables
			opts.RedactColumns = redactCols
			opts.ProfileActive = true
		}

		// Schema resolver for PK-only WHERE clauses: preloaded by the surface
		// (console bundles), else loaded best-effort per call.
		resolver := t.Resolver
		var resolverErr error
		if !t.ResolverLoaded {
			resolver, resolverErr = metadata.NewResolver(t.DB, 0)
			if resolverErr != nil {
				resolver = nil
			}
		}

		// Fetch events from live index + archives. Archive skip conditions
		// mirror the query tool above.
		engine := query.New(t.DB)
		var archSources []string
		if !args.NoArchive && args.Profile == "" {
			archSources = t.archiveSources(ctx)
		}

		var rows []query.ResultRow
		if len(archSources) > 0 {
			fetchOpts := opts
			rows, err = engine.Fetch(ctx, fetchOpts)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
			for _, src := range archSources {
				ar, err := parquetquery.Fetch(ctx, fetchOpts, src)
				if err != nil {
					slog.Warn("archive query failed, skipping", "source", src, "error", err)
					continue
				}
				rows = append(rows, ar...)
			}
			rows = query.MergeResults(rows, opts.Limit, opts.Order)
		} else {
			rows, err = engine.Fetch(ctx, opts)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
		}

		// The fetch above ran Order=DESC so --limit kept the newest suffix of
		// the window (#785/#927). Restore ascending order: GenerateSQLFromRows
		// expects ASC input and reverses it internally to undo most-recent
		// first.
		rows = query.MergeResults(rows, 0, "ASC")

		gen := recovery.NewForDialect(t.DB, resolver, recovery.DialectForIndex(t.DB))
		// #849: the console sets cfg.MaxScriptBytes to its own shared-daemon
		// budget; standalone bintrail-mcp leaves it 0, so the Generator keeps
		// its own DefaultMaxScriptBytes (2 GiB) exactly as before this field
		// existed. See Config.scriptBudgetOverride for why this must stay an
		// "only call when set" gate rather than an unconditional
		// SetMaxScriptBytes(cfg.MaxScriptBytes).
		if v, ok := cfg.scriptBudgetOverride(); ok {
			gen.SetMaxScriptBytes(v)
		}
		var buf bytes.Buffer
		n, err := gen.GenerateSQLFromRows(rows, &buf)
		if err != nil {
			// A *recovery.ScriptBudgetError gets a message built from its typed
			// fields rather than its own Error() verbatim — that text ends with
			// "raise/disable the budget (0 = unlimited)", advice that presumes a
			// Go caller of SetMaxScriptBytes, not an MCP client. Point at the one
			// escape hatch that's actually reachable from here on EITHER surface
			// (console or standalone): `bintrail recover` from the CLI, which does
			// expose --max-script-bytes (#849 code review: writeRecoverError in
			// internal/console/api.go does the equivalent for the HTTP endpoints;
			// this is the MCP-tool sibling of that fix).
			var be *recovery.ScriptBudgetError
			if errors.As(err, &be) {
				return ErrorResult(fmt.Errorf(
					"refusing to generate the reversal script — the matched events hold ~%.1f MiB of row data, "+
						"over the %.0f MiB budget for a single recovery. Narrow the recovery filter "+
						"(schema/table/pk/time range) to shrink the window, or use `bintrail recover` from the CLI "+
						"for large recoveries (it supports --max-script-bytes to raise or disable this budget)",
					float64(be.EstimatedBytes)/(1<<20), float64(be.Budget)/(1<<20))), nil, nil
			}
			return ErrorResult(err), nil, nil
		}

		text := buf.String()
		if resolverErr != nil {
			text += fmt.Sprintf("\n-- Note: schema snapshot unavailable (%v); WHERE clauses use all columns.\n", resolverErr)
		}
		if n > 0 {
			text += fmt.Sprintf("\n-- %d reversal statement(s) generated.\n", n)
		}
		if n >= opts.Limit {
			text += fmt.Sprintf("\n-- Warning: results truncated at %d rows. Use a narrower since/until range or increase the limit to see more.\n", opts.Limit)
		}

		ext.Record(ctx, ext.AuditEvent{
			Surface: cfg.auditSurface(),
			Action:  "recover.generate",
			Actor:   ext.ProcessActor(args.Profile),
			Schema:  args.Schema,
			Table:   args.Table,
			Detail: map[string]string{
				"statements": strconv.Itoa(n),
				"dry_run":    "true", // MCP recover always returns the script, never applies it
				"gtid":       args.GTID,
			},
		})

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
			},
		}, nil, nil
	}
}

// MakeStatusTool returns the status tool handler.
func MakeStatusTool(cfg Config) func(context.Context, *mcp.CallToolRequest, StatusArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args StatusArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, ""); res != nil {
			return res, nil, nil
		}
		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		if t.DBName == "" {
			return ErrorResult(fmt.Errorf("DSN must include a database name")), nil, nil
		}

		data, err := status.CollectStatus(ctx, t.DB, t.DBName)
		if err != nil {
			return ErrorResult(err), nil, nil
		}

		var buf bytes.Buffer
		data.Write(&buf)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: buf.String()},
			},
		}, nil, nil
	}
}

// MakeSchemaChangesTool returns the list_schema_changes tool handler.
func MakeSchemaChangesTool(cfg Config) func(context.Context, *mcp.CallToolRequest, SchemaChangesArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args SchemaChangesArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, ""); res != nil {
			return res, nil, nil
		}
		// Validate inputs before connecting.
		sinceT, err := cliutil.ParseTime(args.Since)
		if err != nil {
			return ErrorResult(fmt.Errorf("invalid since: %w", err)), nil, nil
		}
		untilT, err := cliutil.ParseTime(args.Until)
		if err != nil {
			return ErrorResult(fmt.Errorf("invalid until: %w", err)), nil, nil
		}

		if args.DDLType != "" {
			upper := strings.ToUpper(args.DDLType)
			switch upper {
			case "CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE":
				// valid
			default:
				return ErrorResult(fmt.Errorf("invalid ddl_type %q; must be CREATE, ALTER, DROP, RENAME, or TRUNCATE", args.DDLType)), nil, nil
			}
		}

		limit := args.Limit
		if limit <= 0 {
			limit = 100
		}

		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		q := "SELECT id, detected_at, schema_name, table_name, ddl_type, ddl_query, binlog_file, binlog_pos, gtid FROM schema_changes WHERE 1=1"
		var params []any

		if args.Schema != "" {
			q += " AND schema_name = ?"
			params = append(params, args.Schema)
		}
		if args.Table != "" {
			q += " AND table_name = ?"
			params = append(params, args.Table)
		}
		if args.DDLType != "" {
			// Match prefix: "ALTER" matches "ALTER TABLE", etc.
			q += " AND ddl_type LIKE ?"
			params = append(params, strings.ToUpper(args.DDLType)+"%")
		}
		if sinceT != nil {
			q += " AND detected_at >= ?"
			params = append(params, *sinceT)
		}
		if untilT != nil {
			q += " AND detected_at <= ?"
			params = append(params, *untilT)
		}
		q += " ORDER BY detected_at DESC LIMIT ?"
		params = append(params, limit)

		rows, err := t.DB.QueryContext(ctx, q, params...)
		if err != nil {
			if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "1146") {
				return ErrorResult(fmt.Errorf("schema_changes table not found; run `bintrail init` to create it")), nil, nil
			}
			return ErrorResult(fmt.Errorf("query schema_changes: %w", err)), nil, nil
		}
		defer rows.Close()

		type schemaChange struct {
			ID         int64  `json:"id"`
			DetectedAt string `json:"detected_at"`
			Schema     string `json:"schema_name"`
			Table      string `json:"table_name"`
			DDLType    string `json:"ddl_type"`
			Statement  string `json:"statement"`
			BinlogFile string `json:"binlog_file"`
			BinlogPos  int64  `json:"binlog_pos"`
			GTID       string `json:"gtid,omitempty"`
		}

		var results []schemaChange
		for rows.Next() {
			var sc schemaChange
			var detectedAt time.Time
			var gtid sql.NullString
			if err := rows.Scan(&sc.ID, &detectedAt, &sc.Schema, &sc.Table, &sc.DDLType, &sc.Statement, &sc.BinlogFile, &sc.BinlogPos, &gtid); err != nil {
				return ErrorResult(fmt.Errorf("scan: %w", err)), nil, nil
			}
			sc.DetectedAt = detectedAt.UTC().Format("2006-01-02 15:04:05")
			if gtid.Valid {
				sc.GTID = gtid.String
			}
			results = append(results, sc)
		}
		if err := rows.Err(); err != nil {
			return ErrorResult(fmt.Errorf("rows iteration: %w", err)), nil, nil
		}

		out, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			return ErrorResult(fmt.Errorf("marshal: %w", err)), nil, nil
		}

		text := string(out)
		n := len(results)
		if n > 0 {
			text += fmt.Sprintf("\n\n%d schema change(s)", n)
		} else {
			text = "No schema changes found."
		}
		if n >= limit {
			text += fmt.Sprintf("\nWarning: results truncated at %d rows. Use a narrower since/until range or increase the limit to see more.", limit)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
			},
		}, nil, nil
	}
}

// ─── Shared helpers ──────────────────────────────────────────────────────────

// ErrorResult wraps an error as a tool-level MCP error result.
func ErrorResult(err error) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: err.Error()},
		},
		IsError: true,
	}
}

// EnvArchiveSources returns Parquet archive source paths for use with
// parquetquery.Fetch. It checks env vars BINTRAIL_ARCHIVE_S3 + BINTRAIL_ID
// first (for explicit configuration — both must be set), then falls back to
// auto-discovery from archive_state in the index database.
func EnvArchiveSources(ctx context.Context, db *sql.DB) []string {
	archiveS3 := os.Getenv("BINTRAIL_ARCHIVE_S3")
	bintrailID := os.Getenv("BINTRAIL_ID")
	if archiveS3 != "" && bintrailID != "" {
		base := strings.TrimSuffix(archiveS3, "/") + "/bintrail_id=" + bintrailID
		return []string{base}
	}
	if archiveS3 != "" || bintrailID != "" {
		slog.Warn("partial archive env var config; both BINTRAIL_ARCHIVE_S3 and BINTRAIL_ID must be set",
			"BINTRAIL_ARCHIVE_S3", archiveS3, "BINTRAIL_ID", bintrailID)
	}
	return stateArchiveSources(ctx, db)
}

// stateArchiveSources auto-discovers archive sources from archive_state,
// warn-and-continue on failure (the MCP tools are deliberately permissive —
// their own per-source fetch loops warn-and-continue too).
func stateArchiveSources(ctx context.Context, db *sql.DB) []string {
	sources, err := query.ResolveArchiveSources(ctx, db)
	if err != nil {
		slog.Warn("archive auto-discovery failed; proceeding without archives", "error", err)
		return nil
	}
	return sources
}

// DefaultQueryMaxLimit is the hard row ceiling for the MCP query tool (#654):
// a backstop against a pathological explicit limit OOMing the long-lived server.
// ~1M rows is multiple GB worst-case at the project's per-row sizing — far above
// any legitimate agent query, yet bounded. The unbounded escape hatch is the
// `bintrail query` CLI, so this is deliberately not disengageable via env.
const DefaultQueryMaxLimit = 1_000_000

// EnvQueryMaxLimit returns the standalone MCP query-tool row ceiling.
// BINTRAIL_MCP_QUERY_MAX_LIMIT raises or lowers it; an empty/invalid/<=0 value
// falls back to the default rather than disabling the ceiling (the CLI is the
// unbounded path, not the agent-facing tool).
func EnvQueryMaxLimit() int {
	if v := os.Getenv("BINTRAIL_MCP_QUERY_MAX_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return DefaultQueryMaxLimit
}

// ApplyQueryCeiling caps limit to max, returning the (possibly capped) limit and
// whether a cap was applied. max <= 0 disables capping.
func ApplyQueryCeiling(limit, max int) (int, bool) {
	if max > 0 && limit > max {
		return max, true
	}
	return limit, false
}

// QueryResultNotice composes the trailing warning appended to a query-tool
// result. When the ceiling fired it SUPERSEDES the generic truncation notice —
// telling an agent to "increase the limit" would be wrong, since the limit it
// asked for is exactly the one that was capped (and after a cap n == limit makes
// the generic arm true too, so order matters). Returns "" when no notice applies.
func QueryResultNotice(ceilingApplied bool, requestedLimit, ceiling, n, limit int) string {
	switch {
	case ceilingApplied:
		return fmt.Sprintf("\nWarning: requested limit %d exceeds the MCP query ceiling of %d rows; capped to %d. "+
			"Narrow your filters/time range, or run the `bintrail query` CLI for an unbounded export.\n", requestedLimit, ceiling, ceiling)
	case n >= limit:
		return fmt.Sprintf("\nWarning: results truncated at %d rows. Use a narrower since/until range or increase the limit to see more.\n", limit)
	}
	return ""
}

// BuildQueryOptions converts the shared tool filter parameters into a
// query.Options, validating cross-field requirements and applying the default
// limit to a non-positive request.
func BuildQueryOptions(schema, table, pk, eventType, gtid, since, until, changedCol string, columnEq []string, flagVal string, limit, defaultLimit int) (query.Options, error) {
	if pk != "" && (schema == "" || table == "") {
		return query.Options{}, fmt.Errorf("pk requires both schema and table")
	}
	if changedCol != "" && (schema == "" || table == "") {
		return query.Options{}, fmt.Errorf("changed_column requires both schema and table")
	}
	if len(columnEq) > 0 && (schema == "" || table == "") {
		return query.Options{}, fmt.Errorf("column_eq requires both schema and table")
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
	parsedEq, err := query.ParseColumnEqs(columnEq)
	if err != nil {
		return query.Options{}, err
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
		ColumnEq:      parsedEq,
		Flag:          flagVal,
		Limit:         limit,
	}, nil
}
