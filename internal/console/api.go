package console

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/query"
	"github.com/dbtrail/bintrail/internal/recovery"
	"github.com/dbtrail/bintrail/internal/status"
)

// Result caps. Every endpoint applies a default and a hard maximum; the limit
// is never 0/unlimited. A read-only browser must not be able to ask the index
// for an unbounded result set.
const (
	eventsDefaultLimit  = 100
	eventsMaxLimit      = 1000
	recoverDefaultLimit = 1000
	recoverMaxLimit     = 10000
)

// filterParams is the source-agnostic set of query filters parsed from either
// a URL query string (events) or a JSON body (recover).
type filterParams struct {
	Schema        string
	Table         string
	PK            string
	EventType     string
	GTID          string
	Since         string
	Until         string
	ChangedColumn string
	Order         string
	Limit         int
}

// recoverRequest is the JSON body accepted by POST /api/recover.
type recoverRequest struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	PK            string `json:"pk"`
	EventType     string `json:"event_type"`
	GTID          string `json:"gtid"`
	Since         string `json:"since"`
	Until         string `json:"until"`
	ChangedColumn string `json:"changed_column"`
	// Order is accepted for request symmetry with /api/events but IGNORED by
	// recover: handleRecover forces oldest-first (ASC) input, which the undo
	// generator requires. A client-supplied value has no effect.
	Order string `json:"order"`
	Limit int    `json:"limit"`
}

type eventsResponse struct {
	Events   []eventDTO `json:"events"`
	Count    int        `json:"count"`
	Limit    int        `json:"limit"`
	Warnings []string   `json:"warnings,omitempty"`
}

type recoverResponse struct {
	SQL            string   `json:"sql"`
	StatementCount int      `json:"statement_count"`
	RowCount       int      `json:"row_count"`
	Warnings       []string `json:"warnings,omitempty"`
}

type schemasResponse struct {
	Schemas []string `json:"schemas"`
}

type tablesResponse struct {
	Schema string   `json:"schema"`
	Tables []string `json:"tables"`
}

// buildOptions converts shared filter params into a query.Options, validating
// cross-field requirements and clamping the limit. RBAC rules (deny tables /
// redact columns) resolved at startup are always attached so every query the
// console runs is bound by the operator's profile.
func (s *Server) buildOptions(p filterParams, defaultLimit, maxLimit int) (query.Options, error) {
	et, err := cliutil.ParseEventType(p.EventType)
	if err != nil {
		return query.Options{}, err
	}
	since, err := cliutil.ParseTime(p.Since)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid since: %w", err)
	}
	until, err := cliutil.ParseTime(p.Until)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid until: %w", err)
	}

	// A PK or changed-column filter is only meaningful when scoped to one
	// table; mirror the MCP/CLI validation so the index isn't scanned blindly.
	if p.PK != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, errors.New("pk filter requires both schema and table")
	}
	if p.ChangedColumn != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, errors.New("changed_column filter requires both schema and table")
	}

	// Default to newest-first, the natural order for a browsing UI.
	order := strings.ToUpper(strings.TrimSpace(p.Order))
	if order != "ASC" {
		order = "DESC"
	}

	return query.Options{
		Schema:        p.Schema,
		Table:         p.Table,
		PKValues:      p.PK,
		EventType:     et,
		GTID:          p.GTID,
		Since:         since,
		Until:         until,
		ChangedColumn: p.ChangedColumn,
		Limit:         clampLimit(p.Limit, defaultLimit, maxLimit),
		Order:         order,
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
	}, nil
}

// fetch runs the shared cross-source fetch (live MySQL + Parquet archives).
//
// AllowGaps is true for both events and recover, matching the CLI `recover`
// (warn-and-continue — a human reviews the script). Coverage gaps the planner
// detects are returned in the QueryPlan and surfaced to the caller as warnings
// via gapWarnings(plan); the recover UI renders them prominently, so an
// incomplete-coverage undo is never presented as a clean success.
//
// One residual case FetchMerged does not expose: when several archive sources
// are configured and only SOME fail to load, it logs the failure server-side
// and continues (again, matching the CLI). The console cannot surface that to
// the browser today because FetchMerged returns no per-source failure signal;
// this limitation is documented in docs/console.md.
func (s *Server) fetch(ctx context.Context, opts query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
	return query.FetchMerged(ctx, s.db, s.engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         s.dbName,
		NoArchive:      s.noArchive,
		AllowGaps:      true,
		ArchiveFetcher: parquetquery.Fetch,
	})
}

// handleEvents serves GET /api/events — the events browser.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p := filterParams{
		Schema:        q.Get("schema"),
		Table:         q.Get("table"),
		PK:            q.Get("pk"),
		EventType:     q.Get("event_type"),
		GTID:          q.Get("gtid"),
		Since:         q.Get("since"),
		Until:         q.Get("until"),
		ChangedColumn: q.Get("changed_column"),
		Order:         q.Get("order"),
		Limit:         atoiDefault(q.Get("limit"), 0),
	}
	opts, err := s.buildOptions(p, eventsDefaultLimit, eventsMaxLimit)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	rows, plan, err := s.fetch(r.Context(), opts)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, eventsResponse{
		Events:   toEventDTOs(rows),
		Count:    len(rows),
		Limit:    opts.Limit,
		Warnings: gapWarnings(plan),
	})
}

// handleRecover serves POST /api/recover — generates undo SQL. It NEVER
// executes the SQL: rows are fetched (read-only), reversed into a buffer, and
// the script is returned as text for the operator to review and apply.
func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	var body recoverRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	p := filterParams{
		Schema:        body.Schema,
		Table:         body.Table,
		PK:            body.PK,
		EventType:     body.EventType,
		GTID:          body.GTID,
		Since:         body.Since,
		Until:         body.Until,
		ChangedColumn: body.ChangedColumn,
		Order:         body.Order,
		Limit:         body.Limit,
	}
	opts, err := s.buildOptions(p, recoverDefaultLimit, recoverMaxLimit)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Refuse to generate an undo script for the entire index; a recovery must
	// be scoped to at least one schema.
	if opts.Schema == "" {
		writeJSONError(w, http.StatusBadRequest, "recover requires at least a schema filter")
		return
	}

	// Recovery requires chronological (oldest-first) input: GenerateSQLFromRows
	// reverses internally so the most-recent event is undone first. The browsing
	// default (DESC) would invert the undo order for a PK touched multiple times.
	// Forcing ASC here also makes the LIMIT select the oldest N, matching the CLI.
	opts.Order = ""

	// Coverage gaps come back in plan.GapHours and are surfaced as warnings
	// below — the recover UI renders them, so an incomplete-coverage undo is
	// flagged to the operator rather than silently presented as complete.
	rows, plan, err := s.fetch(r.Context(), opts)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var buf bytes.Buffer
	n, err := recovery.New(s.db, s.resolver).GenerateSQLFromRows(rows, &buf)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, recoverResponse{
		SQL:            buf.String(),
		StatementCount: n,
		RowCount:       len(rows),
		Warnings:       gapWarnings(plan),
	})
}

// handleStatus serves GET /api/status — index health, partitions, coverage,
// stream lag, archives. Reuses status.CollectStatus + WriteJSON verbatim;
// that surface exposes only aggregate server metadata, never per-event actor
// attribution, so it stays inside the free query_explorer boundary.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	data, err := status.CollectStatus(r.Context(), s.db, s.dbName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Encode into a buffer first: status data is already in memory, so this is
	// free and avoids committing a 200 then emitting a truncated body if the
	// encode fails partway (mirrors handleRecover).
	var buf bytes.Buffer
	if err := data.WriteJSON(&buf); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(buf.Bytes()); err != nil {
		slog.Error("console: status write failed", "error", err)
	}
}

// handleSchemas serves GET /api/schemas. Without a ?schema= param it returns
// the distinct schemas present in the index; with one it returns that schema's
// tables (snapshot-authoritative, falling back to distinct observed tables).
func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	schema := r.URL.Query().Get("schema")
	if schema == "" {
		names, err := s.distinctSchemas(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, schemasResponse{Schemas: names})
		return
	}
	tables, err := s.tablesForSchema(r.Context(), schema)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tablesResponse{Schema: schema, Tables: tables})
}

// handleHealthz serves GET /api/healthz — an unauthenticated liveness probe.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// distinctSchemas lists the schemas observed in binlog_events.
func (s *Server) distinctSchemas(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT DISTINCT schema_name FROM binlog_events ORDER BY schema_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStrings(rows)
}

// tablesForSchema lists the tables of one schema. It prefers the latest schema
// snapshot (authoritative, includes tables with no recent events) and falls
// back to the distinct tables observed in binlog_events when no snapshot covers
// the schema.
func (s *Server) tablesForSchema(ctx context.Context, schema string) ([]string, error) {
	if s.resolver != nil {
		if metas := s.resolver.Tables(schema); len(metas) > 0 {
			out := make([]string, len(metas))
			for i, m := range metas {
				out[i] = m.Table
			}
			return out, nil
		}
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT DISTINCT table_name FROM binlog_events WHERE schema_name = ? ORDER BY table_name", schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStrings(rows)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// scanStrings collects a single-column string result set. The returned slice
// is non-nil so it JSON-encodes as [] rather than null.
func scanStrings(rows *sql.Rows) ([]string, error) {
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// clampLimit enforces the default/maximum result caps: a non-positive request
// becomes the default; an oversized request is capped at the maximum.
func clampLimit(n, def, maxLimit int) int {
	if n <= 0 {
		return def
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

// atoiDefault parses s as an int, returning def when s is empty or invalid.
func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

// gapWarnings renders coverage-gap hours from a query plan into a warning list
// for the API response, or nil when there are none.
func gapWarnings(plan *query.QueryPlan) []string {
	if plan == nil || len(plan.GapHours) == 0 {
		return nil
	}
	return []string{query.FormatGapWarning(plan.GapHours)}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("console: JSON encode failed", "error", err)
	}
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
