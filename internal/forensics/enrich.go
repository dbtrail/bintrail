package forensics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// maxEnrichThreadIDs caps a single enrichment batch. Larger requests must be
// chunked by the caller — an unbounded IN clause against performance_schema
// is both slow and a memory hazard.
const maxEnrichThreadIDs = 500

// ThreadInfo holds forensic metadata about a MySQL connection.
type ThreadInfo struct {
	User          string            `json:"user"`
	Host          string            `json:"host"`
	ConnectionID  int64             `json:"connection_id"`
	ProcesslistDB *string           `json:"db,omitempty"`
	Command       string            `json:"command"`
	State         string            `json:"state"`
	ConnAttrs     map[string]string `json:"connection_attributes,omitempty"`
}

// FallbackQuery is an executable SQL query returned to the caller when the
// requested forensic data is not directly available — the caller (a human, or
// an MCP client) can run it manually against the source server.
type FallbackQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// EnrichResult maps "connection_id" (stringified) to live thread metadata.
// IDs with no live session appear in NotFound together with fallback queries
// for manual investigation.
type EnrichResult struct {
	Threads         map[string]*ThreadInfo `json:"threads"`
	Source          string                 `json:"source"`
	NotFound        []int64                `json:"not_found,omitempty"`
	FallbackQueries []FallbackQuery        `json:"fallback_queries,omitempty"`
}

// EnrichThreads looks up forensic metadata for the given thread/connection IDs
// from performance_schema (threads + session_connect_attrs). At most
// maxEnrichThreadIDs may be requested per call.
//
// LIVE-ONLY: this looks at currently-connected sessions. The SaaS falls back
// to a connection_cache table for disconnected sessions; that cache is ported
// in the sibling connection-cache poller (#703), and the live→cache
// composition happens in the who-changed engine (#706) — not here.
func EnrichThreads(ctx context.Context, sourceDB *sql.DB, threadIDs []int64) (EnrichResult, error) {
	if len(threadIDs) == 0 {
		return EnrichResult{}, errors.New("thread_ids is required and must not be empty")
	}
	if len(threadIDs) > maxEnrichThreadIDs {
		return EnrichResult{}, fmt.Errorf("thread_ids must not exceed %d entries", maxEnrichThreadIDs)
	}

	threads, err := lookupThreads(ctx, sourceDB, threadIDs)
	if err != nil {
		return EnrichResult{}, err
	}

	// Generate fallback queries for thread IDs not found.
	var notFound []int64
	for _, tid := range threadIDs {
		key := fmt.Sprintf("%d", tid)
		if _, ok := threads[key]; !ok {
			notFound = append(notFound, tid)
		}
	}

	res := EnrichResult{
		Threads:  threads,
		Source:   "performance_schema",
		NotFound: notFound,
	}
	if len(notFound) > 0 {
		res.FallbackQueries = generateThreadFallbackQueries(notFound)
	}
	return res, nil
}

// lookupThreads queries performance_schema.threads and session_connect_attrs
// for the given connection IDs. Returns a map of "connection_id" → ThreadInfo.
func lookupThreads(ctx context.Context, db *sql.DB, threadIDs []int64) (map[string]*ThreadInfo, error) {
	result := map[string]*ThreadInfo{}

	// Build IN clause with positional args.
	placeholders := make([]string, len(threadIDs))
	args := make([]any, len(threadIDs))
	for i, id := range threadIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	inClause := strings.Join(placeholders, ",")

	// Query the threads table — PROCESSLIST_ID is the connection_id used in
	// binlog events.
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"SELECT PROCESSLIST_ID, PROCESSLIST_USER, PROCESSLIST_HOST, "+
			"PROCESSLIST_DB, PROCESSLIST_COMMAND, PROCESSLIST_STATE "+
			"FROM performance_schema.threads "+
			"WHERE TYPE = 'FOREGROUND' AND PROCESSLIST_ID IN (%s)", inClause), args...)
	if err != nil {
		return nil, fmt.Errorf("query performance_schema.threads: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ti ThreadInfo
		var processlistDB sql.NullString
		var state sql.NullString
		if err := rows.Scan(&ti.ConnectionID, &ti.User, &ti.Host,
			&processlistDB, &ti.Command, &state); err != nil {
			slog.Warn("forensics: scan thread row", "error", err)
			continue
		}
		if processlistDB.Valid {
			ti.ProcesslistDB = &processlistDB.String
		}
		if state.Valid {
			ti.State = state.String
		}
		key := fmt.Sprintf("%d", ti.ConnectionID)
		result[key] = &ti
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread rows: %w", err)
	}

	// Enrich with session_connect_attrs (client program, OS, pid, etc.).
	enrichWithConnAttrs(ctx, db, result, inClause, args)

	return result, nil
}

// enrichWithConnAttrs adds session_connect_attrs data to thread info entries.
func enrichWithConnAttrs(ctx context.Context, db *sql.DB, threads map[string]*ThreadInfo, inClause string, args []any) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"SELECT PROCESSLIST_ID, ATTR_NAME, ATTR_VALUE "+
			"FROM performance_schema.session_connect_attrs "+
			"WHERE PROCESSLIST_ID IN (%s)", inClause), args...)
	if err != nil {
		slog.Warn("forensics: could not query session_connect_attrs", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var connID int64
		var attrName, attrValue string
		if err := rows.Scan(&connID, &attrName, &attrValue); err != nil {
			slog.Warn("forensics: scan connect_attr row", "error", err)
			continue
		}
		key := fmt.Sprintf("%d", connID)
		ti, ok := threads[key]
		if !ok {
			// Connection found in attrs but not in threads (might have disconnected).
			ti = &ThreadInfo{ConnectionID: connID}
			threads[key] = ti
		}
		if ti.ConnAttrs == nil {
			ti.ConnAttrs = map[string]string{}
		}
		ti.ConnAttrs[attrName] = attrValue
	}
	if err := rows.Err(); err != nil {
		slog.Warn("forensics: iterate connect_attr rows", "error", err)
	}
}

// generateThreadFallbackQueries returns SQL queries the user can run manually
// to investigate thread IDs that are no longer in performance_schema (historical).
func generateThreadFallbackQueries(threadIDs []int64) []FallbackQuery {
	ids := make([]string, len(threadIDs))
	for i, id := range threadIDs {
		ids[i] = fmt.Sprintf("%d", id)
	}
	idList := strings.Join(ids, ", ")

	return []FallbackQuery{
		{
			Description: "Check processlist for active connections (MySQL)",
			SQL:         fmt.Sprintf("SELECT * FROM information_schema.PROCESSLIST WHERE ID IN (%s)", idList),
		},
		{
			Description: "Check recent statement history (requires events_statements_history consumer)",
			SQL: fmt.Sprintf(
				"SELECT t.PROCESSLIST_ID, t.PROCESSLIST_USER, t.PROCESSLIST_HOST, "+
					"esh.SQL_TEXT, esh.TIMER_START, esh.TIMER_END, esh.ROWS_AFFECTED "+
					"FROM performance_schema.events_statements_history esh "+
					"JOIN performance_schema.threads t ON t.THREAD_ID = esh.THREAD_ID "+
					"WHERE t.PROCESSLIST_ID IN (%s) "+
					"ORDER BY esh.TIMER_START DESC LIMIT 50", idList),
		},
		{
			Description: "Check general log for historical queries (if enabled)",
			SQL: fmt.Sprintf(
				"SELECT event_time, user_host, thread_id, command_type, argument "+
					"FROM mysql.general_log "+
					"WHERE thread_id IN (%s) "+
					"ORDER BY event_time DESC LIMIT 50", idList),
		},
	}
}
