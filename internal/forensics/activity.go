package forensics

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

// Activity query types.
const (
	QueryUserActivity      = "user_activity"
	QueryConnectionHistory = "connection_history"
)

// ActivityQuery selects one of the two forensic activity query modes and
// carries its filters. Fields not used by the selected Type are ignored.
type ActivityQuery struct {
	// Type is one of QueryUserActivity, QueryConnectionHistory.
	Type string
	User string // required for user_activity; optional filter for connection_history
	Host string // optional filter for connection_history (one of User/Host required there)
	// Since/Until accept MySQL DATETIME or ISO 8601 strings. performance_schema
	// ring buffers have no wall-clock column, so these only shape the generated
	// fallback SQL (general_log filters) — they never filter the live query.
	Since string
	Until string
	Limit int    // defaults to 50; capped at 1000
	Order string // "ASC" for ascending; anything else means descending
}

// ActivityResult is the outcome of an activity query. Events is populated for
// user_activity; Connections for connection_history. When the
// needed performance_schema data is unavailable, Source is "fallback" and
// FallbackQueries carries executable SQL for manual investigation —
// fallback-over-error is the point: a degraded source is an answer, not an
// error.
type ActivityResult struct {
	Events          []map[string]any `json:"events,omitempty"`
	Connections     []map[string]any `json:"connections,omitempty"`
	Source          string           `json:"source"`
	Count           int              `json:"count"`
	Note            string           `json:"note,omitempty"`
	Diagnostics     map[string]any   `json:"diagnostics,omitempty"`
	FallbackQueries []FallbackQuery  `json:"fallback_queries,omitempty"`
}

// Activity runs a general forensic query (user activity or connection
// history) against performance_schema on the source server. It returns
// an error only for invalid parameters or an unknown query type; data-source
// failures degrade to fallback SQL inside the result.
func Activity(ctx context.Context, sourceDB *sql.DB, q ActivityQuery) (ActivityResult, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}
	ascending := strings.EqualFold(q.Order, "ASC")

	switch q.Type {
	case QueryUserActivity:
		if q.User == "" {
			return ActivityResult{}, fmt.Errorf("user is required for %s query", QueryUserActivity)
		}
		return handleUserActivity(ctx, sourceDB, q.User, q.Since, q.Until, limit, ascending), nil
	case QueryConnectionHistory:
		if q.User == "" && q.Host == "" {
			return ActivityResult{}, fmt.Errorf("user or host is required for %s query", QueryConnectionHistory)
		}
		return handleConnectionHistory(ctx, sourceDB, q.User, q.Host, limit, ascending), nil
	default:
		return ActivityResult{}, fmt.Errorf("unknown query_type %q: must be %s or %s",
			q.Type, QueryUserActivity, QueryConnectionHistory)
	}
}

// perfSchemaGrantNote returns a user-friendly message when performance_schema
// queries fail, including the actual MySQL error.
func perfSchemaGrantNote(queryErr error) string {
	return fmt.Sprintf("performance_schema query failed: %v", queryErr)
}

// normalizeTimestamp converts ISO 8601 "T" separators to spaces so that
// lexicographic comparison works against MySQL DATETIME format
// ("2006-01-02 15:04:05") in the generated fallback SQL.
func normalizeTimestamp(ts string) string {
	return strings.Replace(ts, "T", " ", 1)
}

// handleUserActivity returns recent statement history for a MySQL user.
// Falls back to providing SQL queries when performance_schema data is unavailable.
func handleUserActivity(ctx context.Context, db *sql.DB, user, since, until string, limit int, ascending bool) ActivityResult {
	since = normalizeTimestamp(since)
	until = normalizeTimestamp(until)

	// Try events_statements_history_long first (larger history).
	events, err := queryUserStatements(ctx, db, user, limit, ascending)
	if err != nil {
		slog.Warn("forensics: user_activity query failed", "error", err)
		return ActivityResult{
			Events:          []map[string]any{},
			Source:          "fallback",
			FallbackQueries: generateUserActivityFallback(user, since, until, limit),
			Note:            perfSchemaGrantNote(err),
		}
	}

	// events_statements_history_long returned 0 rows — try the per-thread
	// history buffer as a fallback (smaller but still useful).
	if len(events) == 0 {
		var shortErr error
		events, shortErr = queryUserStatementsShort(ctx, db, user, limit, ascending)
		if shortErr != nil {
			slog.Warn("forensics: events_statements_history fallback failed", "error", shortErr)
		}
	}

	if len(events) > 0 {
		return ActivityResult{
			Events: events,
			Source: "performance_schema",
			Count:  len(events),
		}
	}

	// Still empty — diagnose why and include actionable info.
	diag := diagnoseEmptyUserActivity(ctx, db, user)
	slog.Info("forensics: user_activity returned 0 rows", "user", user, "diagnostics", diag)
	res := ActivityResult{
		Events:          []map[string]any{},
		Source:          "fallback",
		Diagnostics:     diag,
		FallbackQueries: generateUserActivityFallback(user, since, until, limit),
	}
	if note, ok := diag["note"].(string); ok {
		res.Note = note
	}
	return res
}

// queryUserStatements queries performance_schema for recent statements by a user.
// Note: events_statements_history_long doesn't have a timestamp column,
// but TIMER_START is in picoseconds since server start. We can't easily
// filter by wall-clock time, so time filters are best-effort (fallback SQL only).
func queryUserStatements(ctx context.Context, db *sql.DB, user string, limit int, ascending bool) ([]map[string]any, error) {
	orderDir := "DESC"
	if ascending {
		orderDir = "ASC"
	}
	query := `SELECT
		t.PROCESSLIST_ID AS connection_id,
		t.PROCESSLIST_USER AS user,
		t.PROCESSLIST_HOST AS host,
		esh.SQL_TEXT AS sql_text,
		esh.DIGEST_TEXT AS digest,
		esh.ROWS_AFFECTED AS rows_affected,
		esh.ROWS_EXAMINED AS rows_examined,
		esh.CREATED_TMP_TABLES AS tmp_tables,
		esh.NO_INDEX_USED AS no_index_used,
		esh.TIMER_WAIT / 1000000000 AS duration_ms
	FROM performance_schema.events_statements_history_long esh
	JOIN performance_schema.threads t ON t.THREAD_ID = esh.THREAD_ID
	WHERE t.PROCESSLIST_USER = ?
	ORDER BY esh.TIMER_START ` + orderDir + fmt.Sprintf(" LIMIT %d", limit)

	rows, err := db.QueryContext(ctx, query, user)
	if err != nil {
		return nil, fmt.Errorf("query events_statements_history_long: %w", err)
	}
	defer rows.Close()
	return scanStatementRows(rows)
}

// queryUserStatementsShort queries the per-thread events_statements_history
// buffer. It's smaller (typically last 10 statements per thread) but can
// succeed when _history_long is empty because its consumer is off.
func queryUserStatementsShort(ctx context.Context, db *sql.DB, user string, limit int, ascending bool) ([]map[string]any, error) {
	orderDir := "DESC"
	if ascending {
		orderDir = "ASC"
	}
	query := `SELECT
		t.PROCESSLIST_ID AS connection_id,
		t.PROCESSLIST_USER AS user,
		t.PROCESSLIST_HOST AS host,
		esh.SQL_TEXT AS sql_text,
		esh.DIGEST_TEXT AS digest,
		esh.ROWS_AFFECTED AS rows_affected,
		esh.ROWS_EXAMINED AS rows_examined,
		esh.CREATED_TMP_TABLES AS tmp_tables,
		esh.NO_INDEX_USED AS no_index_used,
		esh.TIMER_WAIT / 1000000000 AS duration_ms
	FROM performance_schema.events_statements_history esh
	JOIN performance_schema.threads t ON t.THREAD_ID = esh.THREAD_ID
	WHERE t.PROCESSLIST_USER = ?
	ORDER BY esh.TIMER_START ` + orderDir + fmt.Sprintf(" LIMIT %d", limit)

	rows, err := db.QueryContext(ctx, query, user)
	if err != nil {
		return nil, fmt.Errorf("query events_statements_history: %w", err)
	}
	defer rows.Close()
	return scanStatementRows(rows)
}

// scanStatementRows scans rows from either events_statements_history or
// events_statements_history_long into a slice of event maps.
// Both tables share the same column layout in our SELECT.
func scanStatementRows(rows *sql.Rows) ([]map[string]any, error) {
	var events []map[string]any
	for rows.Next() {
		var connID int64
		var sqlUser, host string
		var sqlText, digest sql.NullString
		var rowsAffected, rowsExamined, tmpTables, noIndexUsed int64
		var durationMS float64

		if err := rows.Scan(&connID, &sqlUser, &host, &sqlText, &digest,
			&rowsAffected, &rowsExamined, &tmpTables, &noIndexUsed, &durationMS); err != nil {
			slog.Warn("forensics: scan statement row", "error", err)
			continue
		}

		event := map[string]any{
			"connection_id": connID,
			"user":          sqlUser,
			"host":          host,
			"rows_affected": rowsAffected,
			"rows_examined": rowsExamined,
			"duration_ms":   durationMS,
		}
		if sqlText.Valid && sqlText.String != "" {
			event["sql_text"] = sqlText.String
		}
		if digest.Valid && digest.String != "" {
			event["digest"] = digest.String
		}
		events = append(events, event)
	}
	if events == nil {
		events = []map[string]any{}
	}
	return events, rows.Err()
}

// diagnoseEmptyUserActivity checks why both the _history_long and _history
// queries returned 0 rows for a given user, and returns actionable diagnostics.
func diagnoseEmptyUserActivity(ctx context.Context, db *sql.DB, user string) map[string]any {
	diag := map[string]any{}

	// Track which queries succeeded so the switch doesn't operate on zero-values.
	var consumerOK, historyOK, threadsOK, fgOK bool

	// 1. Is the history_long consumer enabled?
	var consumerEnabled string
	err := db.QueryRowContext(ctx,
		"SELECT ENABLED FROM performance_schema.setup_consumers WHERE NAME = 'events_statements_history_long'",
	).Scan(&consumerEnabled)
	if err != nil {
		slog.Warn("forensics: diagnose setup_consumers query failed", "error", err)
	} else {
		consumerOK = true
		diag["history_long_consumer"] = consumerEnabled
	}

	// 2. How many rows are in the global history buffer?
	var historyCount int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM performance_schema.events_statements_history_long",
	).Scan(&historyCount)
	if err != nil {
		slog.Warn("forensics: diagnose history_long count query failed", "error", err)
	} else {
		historyOK = true
		diag["history_long_rows"] = historyCount
	}

	// 3. Can we see the target user's threads? (requires PROCESS privilege)
	var userThreads int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_USER = ?", user,
	).Scan(&userThreads)
	if err != nil {
		slog.Warn("forensics: diagnose user threads query failed", "error", err)
	} else {
		threadsOK = true
		diag["user_threads_visible"] = userThreads
	}

	// 4. Total foreground threads visible (to detect privilege restrictions).
	var totalFG int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM performance_schema.threads WHERE TYPE = 'FOREGROUND'",
	).Scan(&totalFG)
	if err != nil {
		slog.Warn("forensics: diagnose foreground threads query failed", "error", err)
	} else {
		fgOK = true
		diag["total_foreground_threads"] = totalFG
	}

	// 5. Does the accounts table confirm the user exists?
	var totalConns sql.NullInt64
	err = db.QueryRowContext(ctx,
		"SELECT TOTAL_CONNECTIONS FROM performance_schema.accounts WHERE USER = ?", user,
	).Scan(&totalConns)
	if err != nil {
		slog.Warn("forensics: diagnose accounts query failed", "error", err)
	} else if totalConns.Valid {
		diag["user_total_connections"] = totalConns.Int64
	}

	// Build an actionable note — only reference values from queries that succeeded.
	switch {
	case consumerOK && !strings.EqualFold(consumerEnabled, "YES"):
		diag["note"] = "The events_statements_history_long consumer is disabled. " +
			"Enable it with: UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' " +
			"WHERE NAME = 'events_statements_history_long'"
	case historyOK && historyCount == 0 && consumerOK && strings.EqualFold(consumerEnabled, "YES"):
		diag["note"] = "Consumer is enabled but history buffer is empty. " +
			"The server may have been recently restarted, or the monitoring user may lack SELECT on performance_schema."
	case threadsOK && userThreads == 0 && fgOK && totalFG <= 1:
		diag["note"] = fmt.Sprintf("Only %d foreground thread(s) visible — the monitoring user likely lacks PROCESS privilege. "+
			"GRANT PROCESS ON *.* TO '<monitoring_user>'@'%%' to see all users' activity.", totalFG)
	case threadsOK && userThreads == 0 && fgOK && totalFG > 1:
		diag["note"] = fmt.Sprintf("No active threads found for user %q (but %d other foreground threads are visible). "+
			"The user may not be currently connected, or statements were evicted from the ring buffer.", user, totalFG)
	case !consumerOK && !historyOK && !threadsOK:
		diag["note"] = "All diagnostic queries failed — the monitoring user may lack SELECT privilege on performance_schema."
	default:
		diag["note"] = fmt.Sprintf("No statements found for user %q. "+
			"The user may have had no recent activity, or statements were evicted from the ring buffer.", user)
	}

	return diag
}

// handleConnectionHistory returns connection/account information from performance_schema.
func handleConnectionHistory(ctx context.Context, db *sql.DB, user, host string, limit int, ascending bool) ActivityResult {
	connections, err := queryConnections(ctx, db, user, host, limit, ascending)
	if err != nil {
		slog.Warn("forensics: connection_history query failed", "error", err)
		return ActivityResult{
			Connections:     []map[string]any{},
			Source:          "fallback",
			FallbackQueries: generateConnectionFallback(user, host, limit),
			Note:            perfSchemaGrantNote(err),
		}
	}

	return ActivityResult{
		Connections: connections,
		Source:      "performance_schema",
		Count:       len(connections),
	}
}

// queryConnections queries performance_schema for connection metadata.
func queryConnections(ctx context.Context, db *sql.DB, user, host string, limit int, ascending bool) ([]map[string]any, error) {
	// Get current connections matching the filter.
	query := `SELECT
		t.PROCESSLIST_ID AS connection_id,
		t.PROCESSLIST_USER AS user,
		t.PROCESSLIST_HOST AS host,
		t.PROCESSLIST_DB AS current_db,
		t.PROCESSLIST_COMMAND AS command,
		t.PROCESSLIST_STATE AS state,
		t.PROCESSLIST_TIME AS time_seconds,
		t.PROCESSLIST_INFO AS current_query
	FROM performance_schema.threads t
	WHERE t.TYPE = 'FOREGROUND'`

	var args []any
	if user != "" {
		query += " AND t.PROCESSLIST_USER = ?"
		args = append(args, user)
	}
	if host != "" {
		query += " AND t.PROCESSLIST_HOST LIKE ?"
		args = append(args, "%"+host+"%")
	}
	orderDir := "DESC"
	if ascending {
		orderDir = "ASC"
	}
	query += fmt.Sprintf(" ORDER BY t.PROCESSLIST_TIME %s LIMIT %d", orderDir, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query threads for connections: %w", err)
	}
	defer rows.Close()

	var connections []map[string]any
	for rows.Next() {
		var connID int64
		var connUser, connHost string
		var connDB, command, state, currentQuery sql.NullString
		var timeSeconds sql.NullInt64

		if err := rows.Scan(&connID, &connUser, &connHost, &connDB,
			&command, &state, &timeSeconds, &currentQuery); err != nil {
			slog.Warn("forensics: scan connection row", "error", err)
			continue
		}

		conn := map[string]any{
			"connection_id": connID,
			"user":          connUser,
			"host":          connHost,
		}
		if connDB.Valid {
			conn["current_db"] = connDB.String
		}
		if command.Valid {
			conn["command"] = command.String
		}
		if state.Valid {
			conn["state"] = state.String
		}
		if timeSeconds.Valid {
			conn["time_seconds"] = timeSeconds.Int64
		}
		if currentQuery.Valid && currentQuery.String != "" {
			conn["current_query"] = currentQuery.String
		}
		connections = append(connections, conn)
	}
	if connections == nil {
		connections = []map[string]any{}
	}
	return connections, rows.Err()
}

// ---------------------------------------------------------------------------
// Fallback query generators — used when performance_schema data is unavailable
// ---------------------------------------------------------------------------

// sqlEscape escapes user-supplied values for safe interpolation into fallback
// SQL queries. These queries are returned as text (not executed by bintrail),
// but may be executed by MCP clients — so we prevent injection.
//
// Uses ANSI SQL standard quote-doubling (every embedded single quote is
// doubled) rather than backslash escaping (\'), which is immune to
// backslash-based bypass attacks in MySQL.
func sqlEscape(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "''")
	return s
}

func generateUserActivityFallback(user, since, until string, limit int) []FallbackQuery {
	safeUser := sqlEscape(user)
	queries := []FallbackQuery{
		{
			Description: "Check current connections for this user",
			SQL: fmt.Sprintf(
				"SELECT * FROM information_schema.PROCESSLIST WHERE USER = '%s'", safeUser),
		},
		{
			Description: "Recent statement history for this user (requires events_statements_history consumer)",
			SQL: fmt.Sprintf(
				"SELECT t.PROCESSLIST_ID, t.PROCESSLIST_HOST, "+
					"esh.SQL_TEXT, esh.DIGEST_TEXT, esh.ROWS_AFFECTED, "+
					"esh.TIMER_WAIT/1000000000 AS duration_ms "+
					"FROM performance_schema.events_statements_history esh "+
					"JOIN performance_schema.threads t ON t.THREAD_ID = esh.THREAD_ID "+
					"WHERE t.PROCESSLIST_USER = '%s' "+
					"ORDER BY esh.TIMER_START DESC LIMIT %d", safeUser, limit),
		},
	}
	if since != "" || until != "" {
		timeFilter := ""
		if since != "" {
			timeFilter += fmt.Sprintf(" AND event_time >= '%s'", sqlEscape(since))
		}
		if until != "" {
			timeFilter += fmt.Sprintf(" AND event_time <= '%s'", sqlEscape(until))
		}
		queries = append(queries, FallbackQuery{
			Description: "Check general log for historical queries (if general_log is ON)",
			SQL: fmt.Sprintf(
				"SELECT event_time, user_host, thread_id, command_type, argument "+
					"FROM mysql.general_log "+
					"WHERE user_host LIKE '%s@%%'%s "+
					"ORDER BY event_time DESC LIMIT %d", safeUser, timeFilter, limit),
		})
	}
	return queries
}

func generateConnectionFallback(user, host string, limit int) []FallbackQuery {
	safeUser := sqlEscape(user)
	safeHost := sqlEscape(host)
	var filter string
	if user != "" && host != "" {
		filter = fmt.Sprintf("WHERE USER = '%s' AND HOST LIKE '%%%s%%'", safeUser, safeHost)
	} else if user != "" {
		filter = fmt.Sprintf("WHERE USER = '%s'", safeUser)
	} else {
		filter = fmt.Sprintf("WHERE HOST LIKE '%%%s%%'", safeHost)
	}

	return []FallbackQuery{
		{
			Description: "Current connections matching filter",
			SQL:         fmt.Sprintf("SELECT * FROM information_schema.PROCESSLIST %s LIMIT %d", filter, limit),
		},
		{
			Description: "Account summary from performance_schema (cumulative, survives disconnection)",
			SQL: fmt.Sprintf(
				"SELECT USER, HOST, CURRENT_CONNECTIONS, TOTAL_CONNECTIONS "+
					"FROM performance_schema.accounts %s", filter),
		},
		{
			Description: "Host connection summary",
			SQL: "SELECT HOST, CURRENT_CONNECTIONS, TOTAL_CONNECTIONS " +
				"FROM performance_schema.hosts WHERE HOST IS NOT NULL ORDER BY TOTAL_CONNECTIONS DESC",
		},
	}
}
