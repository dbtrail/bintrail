package forensics

import (
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// statementCols is the column list scanStatementRows expects, shared by the
// events_statements_history and events_statements_history_long SELECTs.
var statementCols = []string{
	"connection_id", "user", "host", "sql_text", "digest",
	"rows_affected", "rows_examined", "tmp_tables", "no_index_used", "duration_ms",
}

func TestActivityValidation(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		query   ActivityQuery
		wantErr string
	}{
		{
			name:    "unknown query type",
			query:   ActivityQuery{Type: "bogus"},
			wantErr: "bogus",
		},
		{
			name:    "empty query type",
			query:   ActivityQuery{},
			wantErr: "unknown query_type",
		},
		{
			name:    "user_activity requires user",
			query:   ActivityQuery{Type: QueryUserActivity},
			wantErr: "user is required",
		},
		{
			name:    "connection_history requires user or host",
			query:   ActivityQuery{Type: QueryConnectionHistory},
			wantErr: "user or host is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Activity(t.Context(), db, tt.query)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Activity error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestActivityUserActivityFallbackOnQueryError pins fallback-over-error: a
// failing performance_schema query is an answer (source=fallback + executable
// SQL + note), never an error. Limit 0 must surface as the default 50 in the
// generated fallback SQL.
func TestActivityUserActivityFallbackOnQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("events_statements_history_long esh").
		WillReturnError(errors.New("SELECT command denied to user"))

	res, err := Activity(t.Context(), db, ActivityQuery{Type: QueryUserActivity, User: "app_user"})
	if err != nil {
		t.Fatalf("Activity must not error on a data-source failure: %v", err)
	}
	if res.Source != "fallback" {
		t.Errorf("Source = %q, want fallback", res.Source)
	}
	if len(res.Events) != 0 {
		t.Errorf("Events = %v, want empty", res.Events)
	}
	if !strings.Contains(res.Note, "performance_schema query failed") {
		t.Errorf("Note = %q, want the perf-schema failure note", res.Note)
	}
	if len(res.FallbackQueries) == 0 {
		t.Fatal("expected fallback queries")
	}
	foundLimit := false
	for _, q := range res.FallbackQueries {
		if !strings.Contains(q.SQL, "app_user") && !strings.Contains(q.Description, "user") {
			t.Errorf("fallback query does not reference the user: %+v", q)
		}
		if strings.Contains(q.SQL, "LIMIT 50") {
			foundLimit = true
		}
	}
	if !foundLimit {
		t.Error("expected the default limit 50 in the fallback SQL")
	}
}

func TestActivityUserActivityHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("events_statements_history_long esh").
		WithArgs("app_user").
		WillReturnRows(sqlmock.NewRows(statementCols).
			AddRow(42, "app_user", "10.0.0.5:33060", "UPDATE orders SET status = 1", "UPDATE `orders` ...", 3, 3, 0, 0, 12.5))

	res, err := Activity(t.Context(), db, ActivityQuery{Type: QueryUserActivity, User: "app_user"})
	if err != nil {
		t.Fatalf("Activity: %v", err)
	}
	if res.Source != "performance_schema" || res.Count != 1 || len(res.Events) != 1 {
		t.Fatalf("result = source=%q count=%d events=%d, want performance_schema/1/1", res.Source, res.Count, len(res.Events))
	}
	ev := res.Events[0]
	if ev["connection_id"] != int64(42) || ev["user"] != "app_user" {
		t.Errorf("event identity fields wrong: %v", ev)
	}
	if ev["sql_text"] != "UPDATE orders SET status = 1" {
		t.Errorf("sql_text = %v", ev["sql_text"])
	}
	if ev["rows_affected"] != int64(3) {
		t.Errorf("rows_affected = %v, want 3", ev["rows_affected"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestActivityUserActivityShortHistoryFallback pins the two-tier read: when
// the global history_long buffer has nothing (consumer off), the per-thread
// events_statements_history buffer is tried before giving up.
func TestActivityUserActivityShortHistoryFallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("events_statements_history_long esh").
		WithArgs("app_user").
		WillReturnRows(sqlmock.NewRows(statementCols))
	mock.ExpectQuery("events_statements_history esh").
		WithArgs("app_user").
		WillReturnRows(sqlmock.NewRows(statementCols).
			AddRow(7, "app_user", "localhost", "DELETE FROM t WHERE id = 9", nil, 1, 1, 0, 0, 3.25))

	res, err := Activity(t.Context(), db, ActivityQuery{Type: QueryUserActivity, User: "app_user"})
	if err != nil {
		t.Fatalf("Activity: %v", err)
	}
	if res.Source != "performance_schema" || res.Count != 1 {
		t.Fatalf("result = source=%q count=%d, want performance_schema/1", res.Source, res.Count)
	}
	if _, hasDigest := res.Events[0]["digest"]; hasDigest {
		t.Error("NULL digest must be omitted from the event map")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestActivityUserActivityDiagnostics pins the diagnose path: both history
// buffers empty → the result carries diagnostics and an actionable note (here:
// the history_long consumer is disabled → the exact UPDATE to run).
func TestActivityUserActivityDiagnostics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("events_statements_history_long esh").
		WillReturnRows(sqlmock.NewRows(statementCols))
	mock.ExpectQuery("events_statements_history esh").
		WillReturnRows(sqlmock.NewRows(statementCols))
	// diagnoseEmptyUserActivity probes, in order:
	mock.ExpectQuery("SELECT ENABLED FROM performance_schema.setup_consumers").
		WillReturnRows(sqlmock.NewRows([]string{"ENABLED"}).AddRow("NO"))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM performance_schema.events_statements_history_long").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(0))
	mock.ExpectQuery("FROM performance_schema.threads WHERE PROCESSLIST_USER").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(0))
	mock.ExpectQuery("FROM performance_schema.threads WHERE TYPE = 'FOREGROUND'").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT"}).AddRow(4))
	mock.ExpectQuery("SELECT TOTAL_CONNECTIONS FROM performance_schema.accounts").
		WillReturnRows(sqlmock.NewRows([]string{"TOTAL_CONNECTIONS"}))

	res, err := Activity(t.Context(), db, ActivityQuery{Type: QueryUserActivity, User: "app_user"})
	if err != nil {
		t.Fatalf("Activity: %v", err)
	}
	if res.Source != "fallback" {
		t.Errorf("Source = %q, want fallback", res.Source)
	}
	if res.Diagnostics["history_long_consumer"] != "NO" {
		t.Errorf("diagnostics = %v, want history_long_consumer=NO", res.Diagnostics)
	}
	if !strings.Contains(res.Note, "UPDATE performance_schema.setup_consumers") {
		t.Errorf("Note = %q, want the consumer-enable UPDATE", res.Note)
	}
	if len(res.FallbackQueries) == 0 {
		t.Error("expected fallback queries")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestActivityConnectionHistoryHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	connectionCols := []string{
		"connection_id", "user", "host", "current_db",
		"command", "state", "time_seconds", "current_query",
	}
	mock.ExpectQuery("FROM performance_schema.threads t").
		WithArgs("root").
		WillReturnRows(sqlmock.NewRows(connectionCols).
			AddRow(11, "root", "localhost", "shop", "Query", "executing", 120, "SELECT 1"))

	res, err := Activity(t.Context(), db, ActivityQuery{Type: QueryConnectionHistory, User: "root"})
	if err != nil {
		t.Fatalf("Activity: %v", err)
	}
	if res.Source != "performance_schema" || res.Count != 1 || len(res.Connections) != 1 {
		t.Fatalf("result = source=%q count=%d connections=%d, want performance_schema/1/1",
			res.Source, res.Count, len(res.Connections))
	}
	conn := res.Connections[0]
	if conn["connection_id"] != int64(11) || conn["current_db"] != "shop" || conn["current_query"] != "SELECT 1" {
		t.Errorf("connection = %v", conn)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestActivityConnectionHistoryFallbackOnQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM performance_schema.threads t").
		WillReturnError(errors.New("denied"))

	res, err := Activity(t.Context(), db, ActivityQuery{Type: QueryConnectionHistory, Host: "10.0.1.50"})
	if err != nil {
		t.Fatalf("Activity must not error on a data-source failure: %v", err)
	}
	if res.Source != "fallback" || len(res.FallbackQueries) == 0 || res.Note == "" {
		t.Errorf("want fallback+queries+note, got %+v", res)
	}
}

func TestActivityDDLHistoryHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	ddlCols := []string{"connection_id", "user", "host", "sql_text", "duration_ms"}
	mock.ExpectQuery("events_statements_history_long esh").
		WithArgs("shop").
		WillReturnRows(sqlmock.NewRows(ddlCols).
			AddRow(3, "admin", "10.0.0.9", "ALTER TABLE orders ADD COLUMN note TEXT", 88.0))

	res, err := Activity(t.Context(), db, ActivityQuery{Type: QueryDDLHistory, Schema: "shop"})
	if err != nil {
		t.Fatalf("Activity: %v", err)
	}
	if res.Source != "performance_schema" || res.Count != 1 {
		t.Fatalf("result = source=%q count=%d, want performance_schema/1", res.Source, res.Count)
	}
	if res.Events[0]["sql_text"] != "ALTER TABLE orders ADD COLUMN note TEXT" {
		t.Errorf("sql_text = %v", res.Events[0]["sql_text"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestActivityDDLHistoryFallbackCapsLimit exercises the DDL fallback path and
// pins the limit cap: 5000 must clamp to 1000 in the generated SQL.
func TestActivityDDLHistoryFallbackCapsLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("events_statements_history_long esh").
		WillReturnError(errors.New("denied"))

	res, err := Activity(t.Context(), db, ActivityQuery{Type: QueryDDLHistory, Limit: 5000})
	if err != nil {
		t.Fatalf("Activity: %v", err)
	}
	if res.Source != "fallback" || len(res.FallbackQueries) == 0 {
		t.Fatalf("want fallback with queries, got %+v", res)
	}
	capped := false
	for _, q := range res.FallbackQueries {
		if strings.Contains(q.SQL, "LIMIT 1000") {
			capped = true
		}
		if strings.Contains(q.SQL, "LIMIT 5000") {
			t.Errorf("limit was not capped: %s", q.SQL)
		}
	}
	if !capped {
		t.Error("expected the capped LIMIT 1000 in fallback SQL")
	}
}

// ---------------------------------------------------------------------------
// Fallback query generator tests (ported from the SaaS agent's forensics_test.go)
// ---------------------------------------------------------------------------

func TestSqlEscape(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"root", "root"},
		{"it's", "it''s"},
		{`\' OR 1=1 --`, `\\'' OR 1=1 --`},
		{`admin\`, `admin\\`},
		{"a'b'c", "a''b''c"},
		{"", ""},
	}
	for _, tc := range tests {
		got := sqlEscape(tc.input)
		if got != tc.want {
			t.Errorf("sqlEscape(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestGenerateUserActivityFallback(t *testing.T) {
	queries := generateUserActivityFallback("app_user", "2026-03-10 14:00:00", "", 50)

	if len(queries) < 2 {
		t.Fatalf("expected at least 2 fallback queries, got %d", len(queries))
	}

	found := false
	for _, q := range queries {
		if strings.Contains(q.SQL, "app_user") {
			found = true
		}
	}
	if !found {
		t.Error("fallback queries should reference the user")
	}

	// A since/until filter adds the general_log query with the time filter.
	last := queries[len(queries)-1]
	if !strings.Contains(last.SQL, "2026-03-10 14:00:00") {
		t.Errorf("expected the since filter in the general_log query: %s", last.SQL)
	}

	// User-supplied values must be escaped in the generated SQL.
	escaped := generateUserActivityFallback("it's", "", "", 50)
	for _, q := range escaped {
		if strings.Contains(q.SQL, "it's") {
			t.Errorf("unescaped quote in fallback SQL: %s", q.SQL)
		}
	}
}

func TestGenerateConnectionFallback(t *testing.T) {
	queries := generateConnectionFallback("root", "10.0.1.50", 50)

	if len(queries) < 2 {
		t.Fatalf("expected at least 2 fallback queries, got %d", len(queries))
	}
	if !strings.Contains(queries[0].SQL, "USER = 'root'") || !strings.Contains(queries[0].SQL, "10.0.1.50") {
		t.Errorf("combined user+host filter missing: %s", queries[0].SQL)
	}

	hostOnly := generateConnectionFallback("", "10.0.1.50", 50)
	if strings.Contains(hostOnly[0].SQL, "USER =") {
		t.Errorf("host-only filter must not reference USER: %s", hostOnly[0].SQL)
	}
}

func TestGenerateDDLFallback(t *testing.T) {
	queries := generateDDLFallback("mydb", "2026-03-01", "2026-03-10", 50)

	if len(queries) < 1 {
		t.Fatalf("expected at least 1 fallback query, got %d", len(queries))
	}
	genLog := queries[len(queries)-1]
	for _, want := range []string{"mydb", "2026-03-01", "2026-03-10", "general_log"} {
		if !strings.Contains(genLog.SQL, want) {
			t.Errorf("general_log DDL fallback missing %q: %s", want, genLog.SQL)
		}
	}
}

func TestNormalizeTimestamp(t *testing.T) {
	tests := []struct{ in, want string }{
		{"2026-03-10T14:00:00", "2026-03-10 14:00:00"},
		{"2026-03-10 14:00:00", "2026-03-10 14:00:00"},
		{"", ""},
		// Only the first T (the date/time separator) is replaced.
		{"2026-03-10T14:00:00TZ", "2026-03-10 14:00:00TZ"},
	}
	for _, tc := range tests {
		if got := normalizeTimestamp(tc.in); got != tc.want {
			t.Errorf("normalizeTimestamp(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
