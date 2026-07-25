package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

func TestClampLimit(t *testing.T) {
	cases := []struct{ n, def, max, want int }{
		{0, 100, 1000, 100},     // unset → default
		{-5, 100, 1000, 100},    // negative → default
		{50, 100, 1000, 50},     // in range → unchanged
		{5000, 100, 1000, 1000}, // over max → capped
		{1000, 100, 1000, 1000}, // at max → unchanged
	}
	for _, c := range cases {
		if got := clampLimit(c.n, c.def, c.max); got != c.want {
			t.Errorf("clampLimit(%d,%d,%d) = %d, want %d", c.n, c.def, c.max, got, c.want)
		}
	}
}

func TestBuildOptionsValidation(t *testing.T) {
	s := &Server{}
	bad := []struct {
		name string
		p    filterParams
	}{
		{"pk without schema/table", filterParams{PK: "1"}},
		{"changed_column without schema/table", filterParams{ChangedColumn: "x"}},
		{"invalid event type", filterParams{EventType: "BOGUS"}},
		{"invalid since", filterParams{Since: "not-a-time"}},
		{"invalid until", filterParams{Until: "nope"}},
	}
	for _, tc := range bad {
		if _, err := s.buildOptions(tc.p, 100, 1000); err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
	}
}

func TestBuildOptionsValues(t *testing.T) {
	s := &Server{}

	opts, err := s.buildOptions(filterParams{
		Schema: "app", Table: "users", PK: "42", EventType: "update", Limit: 0,
	}, 100, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Limit != 100 {
		t.Errorf("Limit = %d, want default 100", opts.Limit)
	}
	if opts.Order != "DESC" {
		t.Errorf("Order = %q, want DESC (browsing default)", opts.Order)
	}
	if opts.PKValues != "42" {
		t.Errorf("PKValues = %q, want 42", opts.PKValues)
	}
	if opts.EventType == nil || *opts.EventType != parser.EventUpdate {
		t.Error("EventType not parsed to UPDATE")
	}

	capped, _ := s.buildOptions(filterParams{Schema: "app", Limit: 99999}, 100, 1000)
	if capped.Limit != 1000 {
		t.Errorf("Limit = %d, want capped 1000", capped.Limit)
	}

	asc, _ := s.buildOptions(filterParams{Order: "asc"}, 100, 1000)
	if asc.Order != "ASC" {
		t.Errorf("Order = %q, want ASC", asc.Order)
	}
}

// TestBuildOptionsAttachesRBAC guards that the server's profile rules (deny
// tables / redact columns) are attached to every query.Options buildOptions
// produces — a refactor that drops this wiring would silently bypass RBAC.
func TestBuildOptionsAttachesRBAC(t *testing.T) {
	deny := []query.SchemaTable{{Schema: "app", Table: "secrets"}}
	redact := []query.SchemaTableColumn{{Schema: "app", Table: "users", Column: "ssn"}}
	s := &Server{denyTables: deny, redactCols: redact}

	opts, err := s.buildOptions(filterParams{Schema: "app"}, 100, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(opts.DenyTables, deny) {
		t.Errorf("DenyTables not attached to options: %+v", opts.DenyTables)
	}
	if !reflect.DeepEqual(opts.RedactColumns, redact) {
		t.Errorf("RedactColumns not attached to options: %+v", opts.RedactColumns)
	}
}

// TestBuildOptionsThreadsProfileActive guards the #838 fix: a named profile —
// even one that resolved to ZERO deny/redact rules — must set
// query.Options.ProfileActive so the redaction pass fires and QueryText/
// QueryHash are withheld (#699). Without a profile it must stay false so
// query_text is visible on an unrestricted console.
func TestBuildOptionsThreadsProfileActive(t *testing.T) {
	// profileActive true even with no deny/redact rules → a zero-rule named
	// profile still withholds query_text.
	active := &Server{profileActive: true}
	opts, err := active.buildOptions(filterParams{Schema: "app"}, 100, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.ProfileActive {
		t.Error("ProfileActive must be true when a profile name was supplied (zero-rule profile still withholds query_text)")
	}

	// No profile → ProfileActive false (query_text visible).
	none := &Server{}
	opts, err = none.buildOptions(filterParams{Schema: "app"}, 100, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if opts.ProfileActive {
		t.Error("ProfileActive must be false with no profile configured")
	}
}

// TestEventsHandlerIncludesConnectionID exercises handleEvents at the HTTP
// layer with sqlmock and asserts the #701 D1 boundary move holds over real
// data flow: the source row carries connection_id, and the response now does
// too — while query_text/query_hash (#699, untouched by this epic) still
// never reach the wire.
func TestEventsHandlerIncludesConnectionID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rows := sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, int64(4242), "app", "users", int64(parser.EventUpdate), "7",
		[]byte(`["email"]`), []byte(`{"email":"a@x"}`), []byte(`{"email":"b@x"}`), int64(0),
		"UPDATE users SET email='b@x'", "cafe0000", int64(1767322445000123),
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

	s := newBootServer(db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/events?schema=app&table=users", nil)
	s.handleEvents(rec, req)

	if rec.Code != 200 {
		t.Fatalf("events status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	// #699 statement-capture fields (query_text/query_hash): the mock row
	// above feeds a statement + digest through the handler, and neither key
	// nor value may reach the wire.
	for _, banned := range []string{"query_text", "query_hash", "UPDATE users SET", "cafe0000"} {
		if strings.Contains(body, banned) {
			t.Errorf("events response must not contain %q: %s", banned, body)
		}
	}
	if !strings.Contains(body, "connection_id") || !strings.Contains(body, "4242") {
		t.Errorf("events HTTP response must carry connection_id (#701 D1): %s", body)
	}
	var resp eventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Count != 1 {
		t.Errorf("count = %d, want 1", resp.Count)
	}
	if resp.Limit != eventsDefaultLimit {
		t.Errorf("limit = %d, want default %d", resp.Limit, eventsDefaultLimit)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestRecoverInvalidJSON: a malformed (non-empty) body is a 400, not a panic.
func TestRecoverInvalidJSON(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{bad`))
	s.handleRecover(rec, req)
	if rec.Code != 400 {
		t.Errorf("invalid JSON body: code = %d, want 400", rec.Code)
	}
}

// TestRecoverIsReadOnly asserts the read-only invariant: the recover handler
// fetches with a single SELECT and generates SQL text — it never executes any
// statement. The sqlmock registers ONLY an ExpectQuery; any write the handler
// attempted would fail (no matching expectation) and break the clean 200.
func TestRecoverIsReadOnly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	// An INSERT event (event_type=1); its reversal is a DELETE built from
	// row_after. schema_version=0 keeps recovery on the default resolver (nil),
	// so no per-row resolver query touches the DB.
	resultRows := sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(parser.EventInsert), "42",
		nil, nil, []byte(`{"id":42,"email":"a@x"}`), int64(0),
		nil, nil, nil,
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(resultRows)

	// newBootServer leaves dbName empty (disables the planner → no
	// archive_state query) and the resolver nil (all-column WHERE fallback).
	s := newBootServer(db)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{"schema":"app","table":"users"}`))
	s.handleRecover(rec, req)

	if rec.Code != 200 {
		t.Fatalf("recover status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp recoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	if !strings.Contains(resp.SQL, "DELETE FROM") {
		t.Errorf("expected a DELETE in the undo SQL, got:\n%s", resp.SQL)
	}
	if !strings.Contains(resp.SQL, "BEGIN;") || !strings.Contains(resp.SQL, "COMMIT;") {
		t.Errorf("expected a transaction-wrapped script, got:\n%s", resp.SQL)
	}
	if resp.StatementCount != 1 {
		t.Errorf("StatementCount = %d, want 1", resp.StatementCount)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("read-only invariant violated — unexpected DB interaction: %v", err)
	}
}

// TestRecoverUnderByteBudget is TestRecoverIsReadOnly's sibling, added for
// #849: an ordinary small recovery must sail through the new
// recoverMaxScriptBytes guard unaffected — a 200 with generated SQL, not an
// accidental refusal from an overzealous budget.
func TestRecoverUnderByteBudget(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	resultRows := sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(parser.EventInsert), "42",
		nil, nil, []byte(`{"id":42,"email":"a@x"}`), int64(0),
		nil, nil, nil,
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(resultRows)

	s := newBootServer(db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{"schema":"app","table":"users"}`))
	s.handleRecover(rec, req)

	if rec.Code != 200 {
		t.Fatalf("recover status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp recoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	if resp.StatementCount != 1 {
		t.Errorf("StatementCount = %d, want 1", resp.StatementCount)
	}
	// Strengthened per code review (#849 item 3): StatementCount alone doesn't
	// prove the budget guard left the actual SQL generation untouched — inspect
	// the rendered script the way TestRecoverIsReadOnly does.
	if !strings.Contains(resp.SQL, "DELETE FROM") {
		t.Errorf("expected a DELETE in the undo SQL, got:\n%s", resp.SQL)
	}
	if !strings.Contains(resp.SQL, "BEGIN;") || !strings.Contains(resp.SQL, "COMMIT;") {
		t.Errorf("expected a transaction-wrapped script, got:\n%s", resp.SQL)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestRecoverOverByteBudget is the #849 repro: MANY moderately wide rows —
// not one pathological giant row — whose COMBINED decoded row_after payload
// exceeds recoverMaxScriptBytes must refuse with an actionable 422, not a 200
// carrying a giant script and not a bare 500. #849's actual complaint was that
// recoverMaxLimit (10,000) bounds row COUNT while row SIZE stayed unbounded —
// a many-rows-of-a-few-MB shape is exactly what that gap allowed through, so
// the repro uses 40 x 1 MiB rows (well under the 10,000-row cap) rather than a
// single oversized row, to prove the estimate is summed ACROSS rows and not
// just checked per-row. The refusal happens INSIDE GenerateSQLFromRows before
// any byte reaches the response buffer (recovery's pre-render
// CheckScriptBudget, #654), so this also guards that the console actually
// wired the tightened budget through (SetMaxScriptBytes) rather than relying
// on the CLI-sized 2 GiB default.
func TestRecoverOverByteBudget(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	const (
		numRows    = 40
		rowBlobLen = 1 << 20 // 1 MiB each; 40 MiB total, over the 32 MiB budget
	)
	blob := strings.Repeat("x", rowBlobLen)
	resultRows := sqlmock.NewRows(cols)
	for i := 0; i < numRows; i++ {
		rowAfter, err := json.Marshal(map[string]any{"id": i, "blob": blob})
		if err != nil {
			t.Fatal(err)
		}
		resultRows.AddRow(
			int64(i+1), "bin.000001", int64(4), int64(40), ts,
			nil, nil, "app", "users", int64(parser.EventInsert), fmt.Sprintf("%d", i),
			nil, nil, rowAfter, int64(0),
			nil, nil, nil,
		)
	}
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(resultRows)

	s := newBootServer(db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{"schema":"app","table":"users"}`))
	s.handleRecover(rec, req)

	if rec.Code != 422 {
		t.Fatalf("recover over budget: code = %d, want 422, body = %s", rec.Code, rec.Body.String())
	}
	var errBody map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("decode error body: %v (body=%s)", err, rec.Body.String())
	}
	msg := errBody["error"]
	for _, want := range []string{"MiB budget", "Narrow the recovery filter", "bintrail recover"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q: %s", want, msg)
		}
	}
	// The message must not point the operator at a knob the console doesn't
	// expose — the CLI-only escape hatch (--max-script-bytes) is offered
	// explicitly above, not the bare "0 = unlimited" phrasing from
	// ScriptBudgetError.Error(), which reads as a console setting that
	// doesn't exist.
	if strings.Contains(msg, "0 = unlimited") {
		t.Errorf("error message must not reference the CLI's raw '0 = unlimited' phrasing: %s", msg)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestRecoverRequiresSchema ensures recover refuses to undo the whole index.
func TestRecoverRequiresSchema(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{}`))
	s.handleRecover(rec, req)
	if rec.Code != 400 {
		t.Errorf("recover without schema: code = %d, want 400", rec.Code)
	}
}

// TestDistinctSchemasUnionsSnapshot is the #1065 repro at unit tier: once
// rotate has archived every partition to Parquet/S3 the live binlog_events is
// empty, yet /api/events and /api/recover still answer from the archives. The
// schema dropdown is a <select> with no free-text fallback, so an empty list
// makes the recover page unusable against archive-only data. The schema
// snapshot outlives the events (schema_snapshots is never partitioned and
// rotate never touches it), so it must fill the gap.
func TestDistinctSchemasUnionsSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	// Archive-only index: no live rows at all.
	mock.ExpectQuery("SELECT DISTINCT schema_name FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"schema_name"}))

	b := &bundle{db: db, resolver: metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders":    {Schema: "app", Table: "orders"},
		"app.customers": {Schema: "app", Table: "customers"},
		"shop.items":    {Schema: "shop", Table: "items"},
	})}
	got, err := b.distinctSchemas(context.Background())
	if err != nil {
		t.Fatalf("distinctSchemas: %v", err)
	}
	if want := []string{"app", "shop"}; !reflect.DeepEqual(got, want) {
		t.Errorf("archive-only schemas = %v, want %v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestMergeSchemaNames pins the merge itself: dedup across the two sources, a
// sorted result, a live-only schema kept even when it is absent from the latest
// snapshot (dropped from the source but still recoverable from archives), and
// an untouched passthrough when no snapshot is loaded.
func TestMergeSchemaNames(t *testing.T) {
	snap := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders": {Schema: "app", Table: "orders"},
		"shop.items": {Schema: "shop", Table: "items"},
	})
	cases := []struct {
		name string
		live []string
		res  *metadata.Resolver
		want []string
	}{
		{"nil resolver passes live through", []string{"b", "a"}, nil, []string{"b", "a"}},
		{"snapshot only", []string{}, snap, []string{"app", "shop"}},
		{"dedup and sort", []string{"shop", "app"}, snap, []string{"app", "shop"}},
		{"live-only schema survives", []string{"legacy"}, snap, []string{"app", "legacy", "shop"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := mergeSchemaNames(c.live, c.res); !reflect.DeepEqual(got, c.want) {
				t.Errorf("mergeSchemaNames(%v) = %v, want %v", c.live, got, c.want)
			}
		})
	}
}

// TestDistinctSchemasNoArchiveKeepsLiveOnly pins the gate: with archives
// unreachable (--no-archive, or any active RBAC profile) the snapshot half is
// skipped, so behaviour is byte-identical to pre-#1065.
func TestDistinctSchemasNoArchiveKeepsLiveOnly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT DISTINCT schema_name FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"schema_name"}).AddRow("live"))

	b := &bundle{db: db, noArchive: true, resolver: metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"archived.orders": {Schema: "archived", Table: "orders"},
	})}
	got, err := b.distinctSchemas(context.Background())
	if err != nil {
		t.Fatalf("distinctSchemas: %v", err)
	}
	if want := []string{"live"}; !reflect.DeepEqual(got, want) {
		t.Errorf("--no-archive schemas = %v, want %v (snapshot-only schema is unreachable)", got, want)
	}
}
