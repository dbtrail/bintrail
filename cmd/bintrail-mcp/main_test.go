package main

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/bintrail/bintrail/internal/parser"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const defaultLimit = 100

// ─── buildQueryOptions ───────────────────────────────────────────────────────

func TestBuildQueryOptions_empty(t *testing.T) {
	opts, err := buildQueryOptions("", "", "", "", "", "", "", "", "", 0, defaultLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Limit != defaultLimit {
		t.Errorf("expected limit %d, got %d", defaultLimit, opts.Limit)
	}
	if opts.EventType != nil {
		t.Errorf("expected nil EventType, got %v", opts.EventType)
	}
	if opts.Since != nil {
		t.Errorf("expected nil Since, got %v", opts.Since)
	}
	if opts.Until != nil {
		t.Errorf("expected nil Until, got %v", opts.Until)
	}
}

func TestBuildQueryOptions_allFields(t *testing.T) {
	opts, err := buildQueryOptions(
		"mydb", "orders", "12345", "INSERT",
		"abc:1", "2026-02-19 14:00:00", "2026-02-19 15:00:00", "status", "",
		50, defaultLimit,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Schema != "mydb" {
		t.Errorf("expected schema mydb, got %q", opts.Schema)
	}
	if opts.Table != "orders" {
		t.Errorf("expected table orders, got %q", opts.Table)
	}
	if opts.PKValues != "12345" {
		t.Errorf("expected pk 12345, got %q", opts.PKValues)
	}
	if opts.EventType == nil || *opts.EventType != parser.EventInsert {
		t.Errorf("expected EventInsert, got %v", opts.EventType)
	}
	if opts.GTID != "abc:1" {
		t.Errorf("expected gtid abc:1, got %q", opts.GTID)
	}
	if opts.Since == nil {
		t.Error("expected non-nil Since")
	}
	if opts.Until == nil {
		t.Error("expected non-nil Until")
	}
	if opts.ChangedColumn != "status" {
		t.Errorf("expected changed_column status, got %q", opts.ChangedColumn)
	}
	if opts.Limit != 50 {
		t.Errorf("expected limit 50, got %d", opts.Limit)
	}
}

func TestBuildQueryOptions_pkWithoutSchemaTable(t *testing.T) {
	_, err := buildQueryOptions("", "", "12345", "", "", "", "", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error when pk is set without schema/table")
	}
	if !strings.Contains(err.Error(), "schema") {
		t.Errorf("expected schema mention in error, got: %v", err)
	}
}

func TestBuildQueryOptions_pkWithSchemaOnly(t *testing.T) {
	_, err := buildQueryOptions("mydb", "", "12345", "", "", "", "", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error when pk is set with schema but no table")
	}
}

func TestBuildQueryOptions_changedColumnWithoutSchemaTable(t *testing.T) {
	_, err := buildQueryOptions("", "", "", "", "", "", "", "status", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error when changed_column is set without schema/table")
	}
	if !strings.Contains(err.Error(), "schema") {
		t.Errorf("expected schema mention in error, got: %v", err)
	}
}

func TestBuildQueryOptions_invalidEventType(t *testing.T) {
	_, err := buildQueryOptions("mydb", "orders", "", "UPSERT", "", "", "", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error for invalid event_type")
	}
}

func TestBuildQueryOptions_invalidSince(t *testing.T) {
	_, err := buildQueryOptions("", "", "", "", "", "not-a-date", "", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error for invalid since")
	}
	if !strings.Contains(err.Error(), "since") {
		t.Errorf("expected 'since' in error, got: %v", err)
	}
}

func TestBuildQueryOptions_invalidUntil(t *testing.T) {
	_, err := buildQueryOptions("", "", "", "", "", "", "not-a-date", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error for invalid until")
	}
	if !strings.Contains(err.Error(), "until") {
		t.Errorf("expected 'until' in error, got: %v", err)
	}
}

func TestBuildQueryOptions_limitZeroUsesDefault(t *testing.T) {
	opts, err := buildQueryOptions("", "", "", "", "", "", "", "", "", 0, defaultLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Limit != defaultLimit {
		t.Errorf("expected default limit %d, got %d", defaultLimit, opts.Limit)
	}
}

func TestBuildQueryOptions_negativeLimitUsesDefault(t *testing.T) {
	opts, err := buildQueryOptions("", "", "", "", "", "", "", "", "", -5, defaultLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Limit != defaultLimit {
		t.Errorf("expected default limit %d, got %d", defaultLimit, opts.Limit)
	}
}

func TestBuildQueryOptions_flagPassedThrough(t *testing.T) {
	opts, err := buildQueryOptions("", "", "", "", "", "", "", "", "billing", 0, defaultLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Flag != "billing" {
		t.Errorf("expected Flag %q, got %q", "billing", opts.Flag)
	}
}

// ─── resolveDSN ──────────────────────────────────────────────────────────────

func TestResolveDSN_overrideProvided(t *testing.T) {
	dsn := "user:pass@tcp(localhost:3306)/mydb"
	got, err := resolveDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != dsn {
		t.Errorf("expected %q, got %q", dsn, got)
	}
}

func TestResolveDSN_envVarFallback(t *testing.T) {
	dsn := "user:pass@tcp(localhost:3306)/mydb"
	t.Setenv("BINTRAIL_INDEX_DSN", dsn)
	got, err := resolveDSN("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != dsn {
		t.Errorf("expected %q from env var, got %q", dsn, got)
	}
}

func TestResolveDSN_noOverrideNoEnv(t *testing.T) {
	t.Setenv("BINTRAIL_INDEX_DSN", "")
	_, err := resolveDSN("")
	if err == nil {
		t.Fatal("expected error when no DSN available")
	}
	if !strings.Contains(err.Error(), "BINTRAIL_INDEX_DSN") {
		t.Errorf("expected error to mention BINTRAIL_INDEX_DSN, got: %v", err)
	}
}

// ─── errorResult ─────────────────────────────────────────────────────────────

func TestErrorResult(t *testing.T) {
	result := errorResult(fmt.Errorf("broke"))
	if !result.IsError {
		t.Error("expected IsError to be true")
	}
	if len(result.Content) != 1 {
		t.Fatalf("expected 1 content item, got %d", len(result.Content))
	}
	tc, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected *mcp.TextContent, got %T", result.Content[0])
	}
	if tc.Text != "broke" {
		t.Errorf("expected text %q, got %q", "broke", tc.Text)
	}
}

// ─── resolveArchiveSources ──────────────────────────────────────────────────

func TestResolveArchiveSources_envVars(t *testing.T) {
	t.Setenv("BINTRAIL_ARCHIVE_S3", "s3://my-bucket/archives/")
	t.Setenv("BINTRAIL_ID", "97adaf56-fe9e-4c1b-9794-b042f7faf197")

	// resolveArchiveSources should use env vars without touching the DB.
	// Passing nil as db is safe because the env var path returns early.
	sources := resolveArchiveSources(t.Context(), nil)
	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}
	want := "s3://my-bucket/archives/bintrail_id=97adaf56-fe9e-4c1b-9794-b042f7faf197"
	if sources[0] != want {
		t.Errorf("expected %q, got %q", want, sources[0])
	}
}

func TestResolveArchiveSources_envVarsTrailingSlash(t *testing.T) {
	t.Setenv("BINTRAIL_ARCHIVE_S3", "s3://bucket/prefix")
	t.Setenv("BINTRAIL_ID", "abc-123")

	sources := resolveArchiveSources(t.Context(), nil)
	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}
	want := "s3://bucket/prefix/bintrail_id=abc-123"
	if sources[0] != want {
		t.Errorf("expected %q, got %q", want, sources[0])
	}
}

func TestResolveArchiveSources_noEnvNoDBReturnsNil(t *testing.T) {
	t.Setenv("BINTRAIL_ARCHIVE_S3", "")
	t.Setenv("BINTRAIL_ID", "")

	// With no env vars and a nil DB, should return nil (no archives).
	sources := resolveArchiveSources(t.Context(), nil)
	if len(sources) != 0 {
		t.Errorf("expected 0 sources, got %d", len(sources))
	}
}

func TestResolveArchiveSources_partialEnvVarsIgnored(t *testing.T) {
	// Only one env var set — should fall through to DB auto-discovery.
	t.Setenv("BINTRAIL_ARCHIVE_S3", "s3://bucket/prefix")
	t.Setenv("BINTRAIL_ID", "")

	sources := resolveArchiveSources(t.Context(), nil)
	if len(sources) != 0 {
		t.Errorf("expected 0 sources with partial env vars, got %d", len(sources))
	}
}

// ─── list_schema_changes tool ────────────────────────────────────────────────

func TestNewServer_registersSchemaChangesTool(t *testing.T) {
	// Verifies newServer() doesn't panic with the list_schema_changes tool
	// registered (catches jsonschema tag issues at construction time).
	server := newServer()
	if server == nil {
		t.Fatal("newServer() returned nil")
	}
}

func TestSchemaChangesArgs_invalidDDLType(t *testing.T) {
	connect := func(dsn string) (*sql.DB, error) {
		return nil, fmt.Errorf("should not connect")
	}
	handler := makeSchemaChangesTool(connect)
	result, _, _ := handler(t.Context(), nil, schemaChangesArgs{
		IndexDSN: "user:pass@tcp(localhost)/db",
		DDLType:  "UPSERT",
	})
	if !result.IsError {
		t.Error("expected error for invalid ddl_type")
	}
	tc := result.Content[0].(*mcp.TextContent)
	if !strings.Contains(tc.Text, "invalid ddl_type") {
		t.Errorf("expected 'invalid ddl_type' in error, got: %s", tc.Text)
	}
}

func TestSchemaChangesArgs_invalidSince(t *testing.T) {
	connect := func(dsn string) (*sql.DB, error) {
		return nil, fmt.Errorf("should not connect")
	}
	handler := makeSchemaChangesTool(connect)
	result, _, _ := handler(t.Context(), nil, schemaChangesArgs{
		IndexDSN: "user:pass@tcp(localhost)/db",
		Since:    "not-a-date",
	})
	if !result.IsError {
		t.Error("expected error for invalid since")
	}
	tc := result.Content[0].(*mcp.TextContent)
	if !strings.Contains(tc.Text, "since") {
		t.Errorf("expected 'since' in error, got: %s", tc.Text)
	}
}

func TestSchemaChangesArgs_invalidUntil(t *testing.T) {
	connect := func(dsn string) (*sql.DB, error) {
		return nil, fmt.Errorf("should not connect")
	}
	handler := makeSchemaChangesTool(connect)
	result, _, _ := handler(t.Context(), nil, schemaChangesArgs{
		IndexDSN: "user:pass@tcp(localhost)/db",
		Until:    "not-a-date",
	})
	if !result.IsError {
		t.Error("expected error for invalid until")
	}
	tc := result.Content[0].(*mcp.TextContent)
	if !strings.Contains(tc.Text, "until") {
		t.Errorf("expected 'until' in error, got: %s", tc.Text)
	}
}

func TestSchemaChangesArgs_validDDLTypes(t *testing.T) {
	for _, ddlType := range []string{"CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE", "create", "alter"} {
		connect := func(dsn string) (*sql.DB, error) {
			// Return connection error — the validation should pass before connecting.
			return nil, fmt.Errorf("expected connect")
		}
		handler := makeSchemaChangesTool(connect)
		result, _, _ := handler(t.Context(), nil, schemaChangesArgs{
			IndexDSN: "user:pass@tcp(localhost)/db",
			DDLType:  ddlType,
		})
		if !result.IsError {
			t.Errorf("expected connect error for valid ddl_type %q (not validation error)", ddlType)
			continue
		}
		tc := result.Content[0].(*mcp.TextContent)
		if strings.Contains(tc.Text, "invalid ddl_type") {
			t.Errorf("ddl_type %q should be valid, got: %s", ddlType, tc.Text)
		}
	}
}
