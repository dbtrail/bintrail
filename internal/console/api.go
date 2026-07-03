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

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/status"
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
	// Cascade fields are set only when the recover target was auto-detected as a
	// foreign-key parent whose DELETE cascaded below the binlog (the script then
	// also re-creates the invisible children). Zero/false/empty for a plain
	// recover, so existing clients are unaffected.
	CascadeDetected bool `json:"cascade_detected,omitempty"`
	VictimCount     int  `json:"victim_count,omitempty"`
	SetNullCount    int  `json:"set_null_count,omitempty"`
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
		return query.Options{}, errors.New("the PK filter needs both a schema and a table")
	}
	if p.ChangedColumn != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, errors.New("the changed-column filter needs both a schema and a table")
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

// fetch runs the shared cross-source fetch (live MySQL + Parquet archives)
// against the request's selected server bundle.
//
// AllowGaps is true for both events and recover, matching the CLI `recover`
// (warn-and-continue — a human reviews the script). Coverage gaps the planner
// detects are returned in the QueryPlan and surfaced to the caller as warnings
// via gapWarnings(plan); the recover UI renders them prominently, so an
// incomplete-coverage undo is never presented as a clean success.
//
// One residual case for these permissive endpoints: when several archive
// sources are configured and only SOME fail to load, FetchMerged logs the
// failure server-side and continues (again, matching the CLI). That is a
// deliberate AllowGaps=true trade-off, not a missing signal — under
// AllowGaps=false (the reconstruct endpoint) any source failure aborts the
// fetch (#377). The trade-off is documented in docs/console.md.
func (s *Server) fetch(ctx context.Context, b *bundle, opts query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
	return query.FetchMerged(ctx, b.db, b.engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         b.dbName,
		NoArchive:      b.noArchive,
		AllowGaps:      true,
		ArchiveFetcher: parquetquery.Fetch,
	})
}

// handleEvents serves GET /api/events — the events browser.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
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
	rows, plan, err := s.fetch(r.Context(), b, opts)
	if err != nil {
		writeFetchError(w, err)
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
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
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
		writeJSONError(w, http.StatusBadRequest, "choose at least a schema to search")
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
	rows, plan, err := s.fetch(r.Context(), b, opts)
	if err != nil {
		writeFetchError(w, err)
		return
	}
	warnings := gapWarnings(plan)

	// Per-bundle dialect (the console is multi-server): MySQLDialect covers MySQL +
	// MariaDB, PostgresDialect a PG-flavored index. Read once and reused below.
	dialect := recovery.DialectForIndex(b.db)

	// Cascade auto-detection. Undoing a DELETE on a foreign-key parent, the plain
	// reversal is a strict SUBSET: it re-inserts the parent but not the child rows
	// InnoDB cascade-deleted below the binlog (MySQL Bug #32506). When the target
	// is such a parent, synthesize those invisible victims and fold them into ONE
	// script — the operator never has to know their FK topology or visit a separate
	// tab. Gated to MySQL/MariaDB: it is a binlog blind-spot fix, and PostgreSQL
	// logical replication captures cascade deletes as real events (no blind spot to
	// synthesize — firing here would only surface a misleading "0 victims" banner).
	// Otherwise only meaningful when a single table is in scope and the matched rows
	// actually contain a DELETE on it (an INSERT/UPDATE undo never cascades).
	if dialect == recovery.MySQLDialect && body.Table != "" && rowsContainDeleteOn(rows, body.Table) {
		isParent, derr := s.cascadeParentDetect(b, body.Schema, body.Table)
		switch {
		case derr != nil:
			// Detection is best-effort: a probe failure must never block a plain
			// recover — but it must NOT silently downgrade one either. If this table
			// IS a cascade parent we couldn't tell, so warn that any cascade-deleted
			// children may be missing (mirrors the RBAC arm below), then fall through
			// to the plain path.
			slog.Warn("console: cascade parent detection failed; recover proceeds without cascade synthesis", "error", derr)
			warnings = append([]string{
				"Could not check whether this table is a foreign-key parent (detection failed: " + derr.Error() + "). If it is, any cascade-deleted child rows are NOT included in the script below — retry, or use recover-cascade to reconstruct them.",
			}, warnings...)
		case isParent && s.rbacActive():
			// Synthesis can't honor redaction (it would leak denied/redacted child
			// rows), so it stays disabled under a profile — but SAY so, so a
			// parent-only script is never silently presented as a full restore.
			warnings = append([]string{
				"This table has ON DELETE CASCADE / SET NULL children, but cascade synthesis is disabled while an RBAC redaction profile is active — the script below re-creates the parent only; cascade-deleted child rows are NOT included.",
			}, warnings...)
		case isParent:
			cres, cerr := s.cascadeRecover(r.Context(), b, body, opts, rows)
			if cerr != nil {
				// Cascade synthesis is an ENHANCEMENT of the plain recover, not a
				// precondition — the base rows were already fetched. A synthesis
				// failure must not deny the recover the operator can still get;
				// degrade to the plain path with a loud warning rather than 500ing
				// the whole request (which would block even the parent-only undo).
				slog.Warn("console: cascade synthesis failed; falling back to plain recover", "error", cerr)
				warnings = append([]string{
					"Cascade synthesis failed (" + cerr.Error() + "); the script below re-creates the parent only — cascade-deleted child rows are NOT included.",
				}, warnings...)
				break // out of the switch → plain recover below
			}
			cw := warnings
			if len(cres.Caveats) > 0 {
				cw = append([]string{
					"Cascade recovery is provably partial — review the caveats below; some cascade-deleted rows may be missing.",
				}, cres.Caveats...)
				cw = append(cw, warnings...)
			}
			writeJSON(w, http.StatusOK, recoverResponse{
				SQL:             cres.SQL,
				StatementCount:  cres.StatementCount,
				RowCount:        len(rows),
				Warnings:        cw,
				CascadeDetected: true,
				VictimCount:     cres.VictimCount,
				SetNullCount:    cres.SetNullCount,
			})
			return
		}
	}

	var buf bytes.Buffer
	// Per-bundle dialect (read above): a PG-flavored index → PostgreSQL reversal SQL.
	// DialectForIndex defaults to MySQL on any read failure (#533/#573).
	n, err := recovery.NewForDialect(b.db, b.resolver, dialect).GenerateSQLFromRows(rows, &buf)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, recoverResponse{
		SQL:            buf.String(),
		StatementCount: n,
		RowCount:       len(rows),
		Warnings:       warnings,
	})
}

// handleStatus serves GET /api/status — index health, partitions, coverage,
// stream lag, archives. Reuses status.CollectStatus + WriteJSON verbatim;
// that surface exposes only aggregate server metadata, never per-event actor
// attribution, so it stays inside the free query_explorer boundary.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	data, err := status.CollectStatus(r.Context(), b.db, b.dbName)
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
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	schema := r.URL.Query().Get("schema")
	if schema == "" {
		names, err := b.distinctSchemas(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, schemasResponse{Schemas: names})
		return
	}
	tables, err := b.tablesForSchema(r.Context(), schema)
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

// distinctSchemas lists the schemas observed in this server's binlog_events.
func (b *bundle) distinctSchemas(ctx context.Context) ([]string, error) {
	rows, err := b.db.QueryContext(ctx, "SELECT DISTINCT schema_name FROM binlog_events ORDER BY schema_name")
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
func (b *bundle) tablesForSchema(ctx context.Context, schema string) ([]string, error) {
	if b.resolver != nil {
		if metas := b.resolver.Tables(schema); len(metas) > 0 {
			out := make([]string, len(metas))
			for i, m := range metas {
				out[i] = m.Table
			}
			return out, nil
		}
	}
	rows, err := b.db.QueryContext(ctx,
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

// writeFetchError maps a cross-source fetch failure onto the right HTTP
// response. The interesting case: a registry index that predates one of the
// post-initial-schema binlog_events columns (connection_id, or #699's
// query_text/query_hash) fails the events SELECT with MySQL error 1054. The
// console deliberately never migrates registry servers (EnsureSchema — an
// ALTER — is confined to the command-line DSN), so instead of a cryptic 500 we
// return an actionable 422 telling the operator how to migrate.
func writeFetchError(w http.ResponseWriter, err error) {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && myErr.Number == 1054 &&
		(strings.Contains(myErr.Message, "connection_id") ||
			strings.Contains(myErr.Message, "query_text") ||
			strings.Contains(myErr.Message, "query_hash")) {
		col := "connection_id"
		for _, c := range []string{"query_text", "query_hash"} {
			if strings.Contains(myErr.Message, c) {
				col = c
			}
		}
		writeJSONError(w, http.StatusUnprocessableEntity,
			"this index predates the "+col+" column, and the console never migrates servers added in the UI; "+
				"run a writer command against it once (bintrail index / stream / agent), or start a console with --index-dsn pointing at it")
		return
	}
	writeJSONError(w, http.StatusInternalServerError, err.Error())
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
