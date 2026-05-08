package shim

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	_ "github.com/go-sql-driver/mysql" // database/sql driver registration

	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// TestHandlerHandshakeNoise verifies the small allow-list for queries
// MySQL clients send during connection setup — these shouldn't be
// rejected as "non-flashback" because that would abort the handshake
// before the customer ever runs a real query.
func TestHandlerHandshakeNoise(t *testing.T) {
	h := NewHandler(nil, nil)

	cases := []string{
		"SET NAMES 'utf8mb4'",
		"SET autocommit=1",
		"SET session transaction isolation level read committed",
		"SET sql_mode = 'TRADITIONAL'",
		"SELECT @@version",
		"SELECT @@session.tx_isolation",
		"SHOW WARNINGS",
		"select database()",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			res, err := h.HandleQuery(q)
			if err != nil {
				t.Errorf("expected handshake noise to succeed, got %v", err)
			}
			if res == nil {
				t.Error("expected non-nil result")
			}
		})
	}
}

// TestHandlerHandshakeNoiseRejectsPrivileged — narrow allow-listing
// matters: an over-broad `set ` prefix would let a caller smuggle
// privileged DDL past the shim with a fake-success response. Verify
// the dangerous shapes are NOT silently accepted.
func TestHandlerHandshakeNoiseRejectsPrivileged(t *testing.T) {
	h := NewHandler(nil, nil)
	h.UseDB("myapp")

	cases := []string{
		"SET PASSWORD = 'x'",
		"SET ROLE admin",
		"SET GLOBAL read_only = 0",
		"DROP TABLE orders",
		"INSERT INTO orders VALUES (1)",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			_, err := h.HandleQuery(q)
			if err == nil {
				t.Errorf("query %q should NOT be silently accepted as handshake noise", q)
			}
		})
	}
}

// TestHandlerRejectsNonFlashbackQuery — anything that's not a
// _flashback statement and not handshake noise should fail with a
// clear error to the client.
func TestHandlerRejectsNonFlashbackQuery(t *testing.T) {
	h := NewHandler(nil, nil)
	h.UseDB("myapp")

	_, err := h.HandleQuery("SELECT * FROM orders WHERE id = 1")
	if err == nil {
		t.Fatal("expected error for non-flashback query")
	}
	if !strings.Contains(err.Error(), "_flashback") {
		t.Errorf("error should mention _flashback, got %v", err)
	}
}

// TestHandlerWireErrorCodes pins the wire codes the shim returns to
// MySQL clients. ORMs and monitoring rely on these to tell user input
// errors apart from server crashes; an untyped `fmt.Errorf` collapses
// to ER_UNKNOWN_ERROR (1105) which is the wrong signal.
//
//   - malformed time-travel (recognised virtual schema, bad shape) → 1064
//   - non-time-travel routed to the shim                            → 1235
func TestHandlerWireErrorCodes(t *testing.T) {
	h := NewHandler(nil, nil)
	h.UseDB("myapp")

	cases := []struct {
		name    string
		query   string
		wantErr uint16
	}{
		{
			name:    "malformed_flashback_missing_as_of",
			query:   "SELECT * FROM _flashback.orders WHERE id = 1",
			wantErr: gomysql.ER_PARSE_ERROR,
		},
		{
			name:    "malformed_diff_bad_between",
			query:   "SELECT * FROM _diff.orders WHERE id = 1",
			wantErr: gomysql.ER_PARSE_ERROR,
		},
		{
			name:    "non_time_travel_query",
			query:   "SELECT * FROM orders WHERE id = 1",
			wantErr: gomysql.ER_NOT_SUPPORTED_YET,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.HandleQuery(tc.query)
			if err == nil {
				t.Fatalf("expected error for %q", tc.query)
			}
			var myErr *gomysql.MyError
			if !errors.As(err, &myErr) {
				t.Fatalf("expected *mysql.MyError so server emits a typed wire code, got %T: %v", err, err)
			}
			if myErr.Code != tc.wantErr {
				t.Errorf("wire code = %d, want %d (msg=%q)", myErr.Code, tc.wantErr, myErr.Message)
			}
		})
	}

	// The "no USE <db>" path is structurally distinct: parser sees a
	// virtual schema prefix but no default DB and returns its own
	// error before even attempting the regex match. Pin this case
	// separately so a future split between "syntax error" (1064) and
	// "session-state error" (e.g. 1046) is a deliberate decision, not
	// a silent regression.
	t.Run("missing_use_db_returns_1064", func(t *testing.T) {
		h := NewHandler(nil, nil) // deliberately no UseDB
		_, err := h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01' WHERE id = 1")
		if err == nil {
			t.Fatal("expected error when no schema is selected")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) {
			t.Fatalf("expected *mysql.MyError, got %T: %v", err, err)
		}
		if myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Errorf("wire code = %d, want %d (msg=%q)", myErr.Code, gomysql.ER_PARSE_ERROR, myErr.Message)
		}
		// The operator hint ("USE <database>") is the actionable
		// part of this error — without it the 1064 is correctly
		// typed but useless to the human reading it.
		if !strings.Contains(myErr.Message, "USE") {
			t.Errorf("error should hint at USE <database>; got %q", myErr.Message)
		}
	})
}

// TestHandlerInternalErrorsKeepDefaultWireCode pins the *inverse* half
// of #277's contract: failures inside runPointInTime / runDiff (DB
// timeouts, FetchMerged errors, archive_state lookup failures) must
// NOT be wrapped in *mysql.MyError so go-mysql/server emits the
// catch-all ER_UNKNOWN_ERROR (1105). A future refactor that wraps
// these in mysql.NewError(ER_PARSE_ERROR, ...) would silently flip
// "the server is broken" into "your query is malformed" — exactly
// the user-vs-server-fault confusion #277 was filed to eliminate.
func TestHandlerInternalErrorsKeepDefaultWireCode(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// We force *some* internal failure inside FetchMerged by failing
	// the archive_state lookup. The exact propagation path is
	// implementation detail (ResolveArchiveSources may swallow the
	// first hit; the planner's archive_state re-query may carry it;
	// an unmocked information_schema query may surface it instead).
	// What matters for the contract is that whatever error reaches
	// HandleQuery's caller is a plain Go error, not a pre-typed
	// *mysql.MyError. The ExpectationsWereMet check below ensures
	// the archive_state query was actually issued — a refactor that
	// stops touching FetchMerged would fail the test loudly rather
	// than silently keeping a passing assertion.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnError(errors.New("simulated archive_state lookup failure"))
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock expectation not met — the FetchMerged path "+
				"this test pins is no longer being exercised: %v", err)
		}
	})

	h := &Handler{
		indexDB: db,
		// AllowGaps=false is the production default and is the mode
		// that propagates archive errors rather than degrading to a
		// Warn. Pin it explicitly so a flip in the zero-value default
		// doesn't quietly change which path this test exercises.
		cfg:    Config{IndexDBName: "bintrail_index", AllowGaps: false},
		logger: slog.Default(),
		archiveFetcher: func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
			return nil, nil
		},
	}
	h.UseDB("myapp")

	_, err = h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00' WHERE id = 1")
	if err == nil {
		t.Fatal("expected error from failing archive_state lookup")
	}
	var myErr *gomysql.MyError
	if errors.As(err, &myErr) {
		t.Errorf("internal failure must NOT be wrapped in *mysql.MyError "+
			"(go-mysql/server would then emit %d instead of the catch-all 1105); "+
			"got code=%d msg=%q", myErr.Code, myErr.Code, myErr.Message)
	}
}

// TestWrapFetchErrorClassifiesGapAsTyped pins the error-classification
// contract that runPointInTime / runDiff use for FetchMerged failures
// (issue #283). Coverage gaps are client-input concerns and must wire
// as ER_NO_PARTITION_FOR_GIVEN_VALUE (1526); everything else stays a
// plain Go error so go-mysql/server emits the catch-all 1105.
//
// Testing the helper directly (not via sqlmock + the planner) keeps
// the contract pinned to the *classification rule*, not to whatever
// query path happens to surface a GapError today.
func TestWrapFetchErrorClassifiesGapAsTyped(t *testing.T) {
	gap := &query.GapError{GapHours: []time.Time{time.Date(2026, 5, 4, 18, 0, 0, 0, time.UTC)}}

	cases := []struct {
		name     string
		in       error
		isTyped  bool
		wantCode uint16
	}{
		{
			name:     "bare_gap_error",
			in:       gap,
			isTyped:  true,
			wantCode: gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE,
		},
		{
			// errors.As must unwrap through %w wrappers — protects against
			// a future refactor that wraps the planner's GapError before
			// it reaches the shim.
			name:     "wrapped_gap_error",
			in:       fmt.Errorf("planner reported gaps: %w", gap),
			isTyped:  true,
			wantCode: gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE,
		},
		{
			name:    "internal_db_failure_stays_untyped",
			in:      errors.New("connection reset by peer"),
			isTyped: false,
		},
		{
			name:    "wrapped_internal_failure_stays_untyped",
			in:      fmt.Errorf("planner failed: %w", errors.New("information_schema unavailable")),
			isTyped: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := wrapFetchError(TypeFlashback, tc.in)
			var myErr *gomysql.MyError
			gotTyped := errors.As(out, &myErr)
			if gotTyped != tc.isTyped {
				t.Fatalf("isTyped = %v, want %v (out=%v)", gotTyped, tc.isTyped, out)
			}
			if !tc.isTyped {
				return
			}
			if myErr.Code != tc.wantCode {
				t.Errorf("wire code = %d, want %d (msg=%q)", myErr.Code, tc.wantCode, myErr.Message)
			}
			// Pin the qType prefix on both branches: operators with
			// concurrent shim sessions need to attribute the error to
			// a specific query type without correlating logs.
			if !strings.Contains(myErr.Message, "flashback") {
				t.Errorf("wire message should include qType context; got %q", myErr.Message)
			}
		})
	}
}

// TestHandlerUseDBStoresSchema — the schema set via UseDB is held
// for use by subsequent HandleQuery calls. The end-to-end coverage
// for "UseDB then run flashback" lives in TestEndToEndHandshake; here
// we just validate the storage step in isolation.
func TestHandlerUseDBStoresSchema(t *testing.T) {
	h := NewHandler(nil, nil)
	if err := h.UseDB("myapp"); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	got := h.db
	h.mu.Unlock()
	if got != "myapp" {
		t.Errorf("stored schema = %q, want %q", got, "myapp")
	}
}

// TestImageToResultColumnOrder — when no DDL order is supplied
// (snapshot missing or table unknown), columns fall back to
// alphabetical order so the wire output stays deterministic.
func TestImageToResultColumnOrder(t *testing.T) {
	res, err := imageToResult(map[string]any{
		"name":  "alice",
		"id":    int64(42),
		"email": "a@b.com",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Resultset == nil {
		t.Fatal("nil resultset")
	}
	want := []string{"email", "id", "name"}
	got := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		got[i] = string(f.Name)
	}
	if !slices.Equal(got, want) {
		t.Errorf("column order = %v, want %v", got, want)
	}
}

// TestImageToResultRespectsDDLOrder — when ddlOrder is supplied,
// the wire output emits columns in DDL position so customers see
// the same column ordering they'd get from a regular `SELECT *`.
// Without this the time-travel queries return alphabetised columns
// which mismatches the source table's natural order, surprising
// any side-by-side comparison the user might run.
func TestImageToResultRespectsDDLOrder(t *testing.T) {
	res, err := imageToResult(
		map[string]any{
			"id":   int64(42),
			"sku":  "ABC-1",
			"qty":  int64(2),
			"note": "initial",
		},
		[]string{"id", "sku", "qty", "note"},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "sku", "qty", "note"}
	got := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		got[i] = string(f.Name)
	}
	if !slices.Equal(got, want) {
		t.Errorf("column order = %v, want %v", got, want)
	}
}

// TestOrderColumnsEdgeCases pins the merge behaviour for the
// "image and snapshot disagree" cases. Each branch is a real
// path the production code can hit when an ALTER TABLE happens
// between the binlog event being indexed and the snapshot being
// taken (or vice versa).
func TestOrderColumnsEdgeCases(t *testing.T) {
	cases := []struct {
		name     string
		image    map[string]any
		ddlOrder []string
		want     []string
	}{
		{
			name:     "nil_ddl_order_falls_back_to_alphabetical",
			image:    map[string]any{"sku": 1, "id": 2, "qty": 3},
			ddlOrder: nil,
			want:     []string{"id", "qty", "sku"},
		},
		{
			name:     "empty_ddl_order_falls_back_to_alphabetical",
			image:    map[string]any{"b": 1, "a": 2},
			ddlOrder: []string{},
			want:     []string{"a", "b"},
		},
		{
			name:     "ddl_columns_missing_from_image_are_skipped",
			image:    map[string]any{"id": 1, "qty": 3},
			ddlOrder: []string{"id", "sku", "qty", "note"},
			want:     []string{"id", "qty"},
		},
		{
			name: "image_columns_missing_from_ddl_are_appended_alphabetically",
			image: map[string]any{
				"id": 1, "sku": 2, "qty": 3, "added_after": 4, "another_new": 5,
			},
			ddlOrder: []string{"id", "sku", "qty"},
			want:     []string{"id", "sku", "qty", "added_after", "another_new"},
		},
		{
			name:     "exact_match_preserves_ddl_order",
			image:    map[string]any{"note": 4, "id": 1, "qty": 3, "sku": 2},
			ddlOrder: []string{"id", "sku", "qty", "note"},
			want:     []string{"id", "sku", "qty", "note"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := orderColumns(tc.image, tc.ddlOrder)
			if !slices.Equal(got, tc.want) {
				t.Errorf("orderColumns = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSelectImage covers every branch of the row-image priority rule
// used by runPointInTime. The function is intentionally pure so it
// can be exercised without sqlmock or a real MySQL: the rest of the
// _flashback / _snapshot pipeline (sort, LimitPerPK, archive merge)
// is covered by the query package's own tests.
//
// A future refactor that swaps the row_after / row_before priority,
// or that mishandles a DELETE event (row_after empty by definition),
// would silently return wrong row state to the customer. The
// "delete_fallback" case is the tripwire for that regression.
func TestSelectImage(t *testing.T) {
	after := map[string]any{"id": int64(1), "name": "after"}
	before := map[string]any{"id": int64(1), "name": "before"}

	cases := []struct {
		name string
		rows []query.ResultRow
		want map[string]any
	}{
		{
			name: "empty_input",
			rows: nil,
			want: nil,
		},
		{
			name: "insert_returns_row_after",
			rows: []query.ResultRow{{
				EventType: parser.EventInsert,
				RowAfter:  after,
			}},
			want: after,
		},
		{
			name: "update_prefers_row_after",
			rows: []query.ResultRow{{
				EventType: parser.EventUpdate,
				RowBefore: before,
				RowAfter:  after,
			}},
			want: after,
		},
		{
			name: "delete_fallback_to_row_before",
			rows: []query.ResultRow{{
				EventType: parser.EventDelete,
				RowBefore: before,
			}},
			want: before,
		},
		{
			// Pin the len() > 0 vs != nil distinction. A future
			// refactor that swapped len() for a nil-check would
			// silently regress DELETE handling if the indexer ever
			// emitted an empty non-nil RowAfter (defensive map
			// allocation upstream, redaction blanking every column,
			// etc.). Without this case the regression slips through
			// both "delete_fallback" (RowAfter is nil there) and
			// "both_empty" (RowBefore is also empty there).
			name: "row_after_empty_map_falls_back_to_row_before",
			rows: []query.ResultRow{{
				EventType: parser.EventDelete,
				RowAfter:  map[string]any{},
				RowBefore: before,
			}},
			want: before,
		},
		{
			name: "both_empty_returns_nil",
			rows: []query.ResultRow{{
				EventType: parser.EventUpdate,
				RowBefore: map[string]any{},
				RowAfter:  map[string]any{},
			}},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := selectImage(tc.rows)
			if !equalMaps(got, tc.want) {
				t.Errorf("selectImage = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestExtractFullTableImagesPinsDivergence locks in the contract
// divergence between full-table reconstruction (#276) and the existing
// point-lookup path. Given the SAME DELETE event:
//   - selectImage returns the row_before — the row's last known state.
//   - extractFullTableImages skips it — the row didn't exist at AS OF.
//
// A future refactor that "unifies" the two paths would silently break
// either point-lookup forensics or full-table correctness. Pinning
// both behaviors against the same input here turns that into a loud
// test failure.
func TestExtractFullTableImagesPinsDivergence(t *testing.T) {
	deleteRow := query.ResultRow{
		EventType: parser.EventDelete,
		RowBefore: map[string]any{"id": int64(1), "qty": int64(5)},
	}
	insertRow := query.ResultRow{
		EventType: parser.EventInsert,
		RowAfter:  map[string]any{"id": int64(2), "qty": int64(7)},
	}
	// Empty row_after is the "rare — corrupted index" branch. Drop
	// it so the resultset doesn't overstate the row count with an
	// all-null phantom row.
	emptyAfterRow := query.ResultRow{
		EventType: parser.EventInsert,
		RowAfter:  nil,
	}

	if got := selectImage([]query.ResultRow{deleteRow}); got == nil {
		t.Errorf("selectImage([DELETE]) must keep row_before for point-lookup; got nil")
	}
	if got := extractFullTableImages([]query.ResultRow{deleteRow}); len(got) != 0 {
		t.Errorf("extractFullTableImages([DELETE]) must skip the row; got %v", got)
	}
	if got := extractFullTableImages([]query.ResultRow{emptyAfterRow}); len(got) != 0 {
		t.Errorf("extractFullTableImages([INSERT with empty row_after]) must skip the row; got %v", got)
	}

	mixed := []query.ResultRow{insertRow, deleteRow, emptyAfterRow, insertRow}
	images := extractFullTableImages(mixed)
	if len(images) != 2 {
		t.Errorf("extractFullTableImages: DELETE + empty_after must both be skipped, kept rows = %d, want 2", len(images))
	}
}

// TestRunPointInTimeDispatchesByPKColumn pins the fix for the empty-
// string PK collision: `WHERE id = ''` is a legitimate single-row
// query against a NOT-NULL VARCHAR with empty default, and a dispatch
// on q.PKValue would silently flip it into a 100k-row table scan.
//
// The previous version of this test re-implemented `q.PKColumn == ""`
// inline and asserted the predicate against itself — a tautology that
// would still pass even if runPointInTime ignored its argument
// entirely. This rewrite drives runPointInTime through sqlmock and
// observes which SQL pattern reaches the index DB, which is the
// only thing that proves the dispatch is correct.
//
// Path detection:
//   - Point-lookup SQL contains `pk_hash = SHA2` (the hash + value
//     guard the SQL builder emits when Options.PKValues != "").
//   - Full-table SQL omits that filter entirely. We detect it via
//     the cost-cap behavioural signature: only runFullTable performs
//     the >cap check, so seeding cap+1 rows surfaces 1104 iff
//     dispatch reached runFullTable.
func TestRunPointInTimeDispatchesByPKColumn(t *testing.T) {
	t.Run("PKColumn_set_runs_point_lookup_sql", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("information_schema.PARTITIONS").
			WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
		// Anchor on `pk_hash = SHA2` — unique to the point-lookup SQL.
		// If runPointInTime wrongly dispatched to runFullTable, the
		// emitted SQL would lack pk_hash and ExpectationsWereMet would
		// fail with "expected query was not matched".
		mock.ExpectQuery("pk_hash = SHA2").
			WillReturnRows(emptyBinlogEventsRows())

		h := &Handler{
			indexDB: db,
			cfg:     Config{AllowGaps: true, IndexDBName: "bintrail_index", NoArchive: true},
			logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
			archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
				return nil, nil
			},
		}
		h.UseDB("myapp")

		q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "id", PKValue: "42", AsOf: time.Now().UTC()}
		_, _ = h.runPointInTime(q) // result irrelevant; mock matching is the assertion
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("expected point-lookup SQL with pk_hash filter; got: %v", err)
		}
	})

	t.Run("empty_PKColumn_dispatches_to_full_table", func(t *testing.T) {
		// Behavioural proof of dispatch: only runFullTable performs
		// the cap check. Lower the cap to 1 via Config (no global
		// state), seed 2 rows, expect ER_TOO_BIG_SELECT. If
		// runPointInTime stayed on the point-lookup branch despite
		// PKColumn="", no cap check would fire and the test would
		// fail loud.
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("information_schema.PARTITIONS").
			WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
		rows := sqlmock.NewRows(binlogEventsColumns())
		now := time.Now().UTC()
		for i := 0; i < 2; i++ {
			rows.AddRow(int64(i+1), "binlog.000001", int64(100), int64(200), now,
				nil, nil, "myapp", "orders", parser.EventInsert,
				fmt.Sprintf("%d", i+1), nil, nil,
				fmt.Sprintf(`{"id":%d,"sku":"X"}`, i+1), 0)
		}
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

		h := &Handler{
			indexDB: db,
			cfg: Config{AllowGaps: true, IndexDBName: "bintrail_index",
				NoArchive: true, FullTableRowCap: 1},
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
				return nil, nil
			},
		}
		h.UseDB("myapp")

		q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "orders",
			AsOf: now} // PKColumn / PKValue both empty
		_, err = h.runPointInTime(q)
		if err == nil {
			t.Fatal("expected ER_TOO_BIG_SELECT (proves dispatch reached runFullTable); got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_TOO_BIG_SELECT {
			t.Errorf("expected ER_TOO_BIG_SELECT (1104), got %v", err)
		}
	})
}

// binlogEventsColumns is the column list scanned by query.Engine.Fetch.
// Extracted so the cap-overflow tests don't duplicate the literal.
func binlogEventsColumns() []string {
	return []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type",
		"pk_values", "changed_columns", "row_before", "row_after", "schema_version",
	}
}

// emptyBinlogEventsRows is the empty-resultset stub for query.Engine.Fetch.
func emptyBinlogEventsRows() *sqlmock.Rows {
	return sqlmock.NewRows(binlogEventsColumns())
}

// TestImagesToResultColumnsFromUnionWhenNoDDLOrder pins the union-
// across-images behavior for the no-snapshot fallback. Using only
// images[0]'s keys would silently drop a column added by a later
// event in the same query (e.g. a row captured pre-ALTER followed
// by a row captured post-ALTER).
func TestImagesToResultColumnsFromUnionWhenNoDDLOrder(t *testing.T) {
	images := []map[string]any{
		{"id": 1, "sku": "A"},                           // pre-ALTER
		{"id": 2, "sku": "B", "added_after_alter": "X"}, // post-ALTER
	}
	res, err := imagesToResult(images, nil)
	if err != nil {
		t.Fatal(err)
	}
	gotCols := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		gotCols[i] = string(f.Name)
	}
	wantCols := []string{"added_after_alter", "id", "sku"}
	if !slices.Equal(gotCols, wantCols) {
		t.Errorf("cols = %v, want %v (no-ddlOrder fallback must union image keys, "+
			"not pick from images[0])", gotCols, wantCols)
	}
}

// TestImagesToResultDDLOrderStrictWhenSnapshotPresent pins the
// snapshot-driven semantic the docstring describes: when ddlOrder is
// supplied, every column in it appears in the resultset even if no
// image carries it (NULL on the wire). A future refactor that
// reverted to "intersect ddlOrder with images[0] keys" would silently
// elide post-ALTER columns from queries that span the ALTER.
func TestImagesToResultDDLOrderStrictWhenSnapshotPresent(t *testing.T) {
	images := []map[string]any{
		{"id": 1, "sku": "A"}, // missing the post-ALTER column
	}
	ddlOrder := []string{"id", "sku", "qty", "note"}
	res, err := imagesToResult(images, ddlOrder)
	if err != nil {
		t.Fatal(err)
	}
	gotCols := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		gotCols[i] = string(f.Name)
	}
	if !slices.Equal(gotCols, ddlOrder) {
		t.Errorf("cols = %v, want %v (ddlOrder must be honored verbatim)", gotCols, ddlOrder)
	}
}

// TestImagesToResultBuildsResultset covers the multi-row resultset
// builder added for #276. The single-row imageToResult path is
// covered by TestImageToResultColumnOrder / TestImageToResultRespectsDDLOrder.
func TestImagesToResultBuildsResultset(t *testing.T) {
	cases := []struct {
		name     string
		images   []map[string]any
		ddlOrder []string
		wantRows int
		wantCols []string
	}{
		{
			name:     "empty_input_returns_empty_resultset",
			images:   nil,
			wantRows: 0,
			wantCols: []string{"_flashback"},
		},
		{
			name:     "single_row_uses_ddl_order",
			images:   []map[string]any{{"id": 1, "sku": "ABC", "qty": 3}},
			ddlOrder: []string{"id", "sku", "qty"},
			wantRows: 1,
			wantCols: []string{"id", "sku", "qty"},
		},
		{
			name: "multi_row_uses_first_image_for_columns",
			images: []map[string]any{
				{"id": 1, "sku": "A", "qty": 1},
				{"id": 2, "sku": "B", "qty": 2},
				{"id": 3, "sku": "C", "qty": 3},
			},
			ddlOrder: []string{"id", "sku", "qty"},
			wantRows: 3,
			wantCols: []string{"id", "sku", "qty"},
		},
		{
			// A row missing a column known to ddlOrder gets a NULL
			// in that position rather than failing the whole query —
			// this mirrors how MySQL itself handles a column added
			// after some rows already existed.
			name: "row_missing_column_yields_null",
			images: []map[string]any{
				{"id": 1, "sku": "A", "qty": 1},
				{"id": 2, "sku": "B"},
			},
			ddlOrder: []string{"id", "sku", "qty"},
			wantRows: 2,
			wantCols: []string{"id", "sku", "qty"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := imagesToResult(tc.images, tc.ddlOrder)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(res.Resultset.RowDatas); got != tc.wantRows {
				t.Errorf("rows = %d, want %d", got, tc.wantRows)
			}
			gotCols := make([]string, len(res.Resultset.Fields))
			for i, f := range res.Resultset.Fields {
				gotCols[i] = string(f.Name)
			}
			if !slices.Equal(gotCols, tc.wantCols) {
				t.Errorf("cols = %v, want %v", gotCols, tc.wantCols)
			}
		})
	}
}

// TestImageToResultEmpty — an empty image (zero-key map) should
// produce a resultset with no rows.
func TestImageToResultEmpty(t *testing.T) {
	res, err := imageToResult(map[string]any{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Resultset == nil {
		t.Fatal("nil resultset")
	}
	if got := len(res.Resultset.RowDatas); got != 0 {
		t.Errorf("expected 0 rows, got %d", got)
	}
}

// TestNewHandlerWiresArchiveFetcher locks in the issue #255 fix at the
// construction boundary. Both NewHandler and NewHandlerWithConfig must
// install a non-nil archiveFetcher; otherwise every virtual-schema
// query crashes with "ArchiveFetcher is required when NoArchive is
// false" because the FetchMerged contract demands either NoArchive=true
// or a non-nil fetcher.
//
// A failure here means a refactor dropped the archiveFetcher wiring
// from the constructor — the same regression class /proxysql-e2e
// would catch end-to-end, but at unit-test speed.
func TestNewHandlerWiresArchiveFetcher(t *testing.T) {
	h := NewHandler(nil, nil)
	if h.archiveFetcher == nil {
		t.Error("NewHandler must wire a non-nil archiveFetcher; got nil")
	}
	h2 := NewHandlerWithConfig(nil, Config{}, nil)
	if h2.archiveFetcher == nil {
		t.Error("NewHandlerWithConfig must wire a non-nil archiveFetcher; got nil")
	}
}

// TestNewHandlerWiresResolverFn — same boundary check as for the
// archive fetcher: both constructors must install a non-nil
// resolverFn or every time-travel query falls back to alphabetical
// column order silently. A failure here means a refactor dropped
// the schema_snapshots wiring; the e2e/shim test would catch it
// end-to-end but at much higher cost.
func TestNewHandlerWiresResolverFn(t *testing.T) {
	if h := NewHandler(nil, nil); h.resolverFn == nil {
		t.Error("NewHandler must wire a non-nil resolverFn; got nil")
	}
	if h := NewHandlerWithConfig(nil, Config{}, nil); h.resolverFn == nil {
		t.Error("NewHandlerWithConfig must wire a non-nil resolverFn; got nil")
	}
}

// TestColumnOrderForFallsBackOnResolverError pins the resilience
// contract: when the resolver fails to load (no snapshot yet, DB
// blip, ALTER TABLE the snapshot doesn't know about), columnOrderFor
// returns nil so imageToResult silently degrades to alphabetical
// order rather than failing the customer's query. The opposite
// behaviour (hard-failing on resolver error) would make brand-new
// installs that haven't run `bintrail snapshot` yet unable to
// answer any time-travel query.
func TestColumnOrderForFallsBackOnResolverError(t *testing.T) {
	cases := []struct {
		name       string
		resolverFn func() (*metadata.Resolver, error)
		want       []string
	}{
		{
			name:       "resolver_load_fails",
			resolverFn: func() (*metadata.Resolver, error) { return nil, errors.New("snapshot table missing") },
			want:       nil,
		},
		{
			name: "resolver_loads_but_table_unknown",
			resolverFn: func() (*metadata.Resolver, error) {
				return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{}), nil
			},
			want: nil,
		},
		{
			name: "resolver_returns_table_in_ddl_order",
			resolverFn: func() (*metadata.Resolver, error) {
				return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
					"appdb.orders": {
						Schema: "appdb", Table: "orders",
						Columns: []metadata.ColumnMeta{
							{Name: "id", OrdinalPosition: 1},
							{Name: "sku", OrdinalPosition: 2},
							{Name: "qty", OrdinalPosition: 3},
							{Name: "note", OrdinalPosition: 4},
						},
					},
				}), nil
			},
			want: []string{"id", "sku", "qty", "note"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{
				logger:     slog.Default(),
				resolverFn: tc.resolverFn,
			}
			got := h.columnOrderFor("appdb", "orders")
			if !slices.Equal(got, tc.want) {
				t.Errorf("columnOrderFor = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolverCacheBehaviour pins five properties of the resolver
// cache that columnOrderFor relies on. Each is documented in
// handler.go's resolverCache type comment; this test enforces them.
//
//  1. Hit-within-TTL: a second columnOrderFor call within the TTL
//     window must NOT invoke resolverFn — the resolver load is the
//     expensive operation we're caching.
//  2. Expiry-triggers-reload: a call after the TTL window invokes
//     resolverFn again, so a fresh `bintrail snapshot` is picked up
//     without restarting the shim.
//  3. Sticky-fallback: when a refresh fails AND the cache holds a
//     prior good resolver, we keep serving the stale resolver. This
//     prevents transient index-DB blips from oscillating wire-
//     protocol column order between DDL and alphabetical for the
//     same customer connection.
//  4. Sticky-fallback emits a Warn the first time it fires, so a
//     persistent index-DB outage is operator-visible. Without this,
//     a 2-hour outage is invisible because the wire response still
//     looks healthy.
//  5. Sticky-fallback Warns are rate-limited to one per TTL window
//     so a hot shim doesn't spam the log under sustained outage.
func TestResolverCacheBehaviour(t *testing.T) {
	tableMeta := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"appdb.orders": {
			Schema: "appdb", Table: "orders",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1},
				{Name: "sku", OrdinalPosition: 2},
			},
		},
	})
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("hit_within_ttl_skips_loader", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		calls := 0
		c := resolverCache{}
		load := func() (*metadata.Resolver, error) { calls++; return tableMeta, nil }

		if _, err := c.get(func() time.Time { return now }, time.Minute, load, silentLogger); err != nil {
			t.Fatalf("first get: %v", err)
		}
		if _, err := c.get(func() time.Time { return now.Add(30 * time.Second) }, time.Minute, load, silentLogger); err != nil {
			t.Fatalf("second get: %v", err)
		}
		if calls != 1 {
			t.Errorf("expected exactly 1 loader call within TTL, got %d", calls)
		}
	})

	t.Run("ttl_expiry_triggers_reload", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		calls := 0
		c := resolverCache{}
		load := func() (*metadata.Resolver, error) { calls++; return tableMeta, nil }

		if _, err := c.get(func() time.Time { return now }, time.Minute, load, silentLogger); err != nil {
			t.Fatalf("first get: %v", err)
		}
		if _, err := c.get(func() time.Time { return now.Add(2 * time.Minute) }, time.Minute, load, silentLogger); err != nil {
			t.Fatalf("second get: %v", err)
		}
		if calls != 2 {
			t.Errorf("expected 2 loader calls after TTL expiry, got %d", calls)
		}
	})

	t.Run("sticky_fallback_on_load_error", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		c := resolverCache{}
		ok := func() (*metadata.Resolver, error) { return tableMeta, nil }
		fail := func() (*metadata.Resolver, error) { return nil, errors.New("transient db blip") }

		if _, err := c.get(func() time.Time { return now }, time.Minute, ok, silentLogger); err != nil {
			t.Fatalf("warm-up: %v", err)
		}
		got, err := c.get(func() time.Time { return now.Add(2 * time.Minute) }, time.Minute, fail, silentLogger)
		if err != nil {
			t.Fatalf("expected sticky fallback to mask error, got: %v", err)
		}
		if got != tableMeta {
			t.Errorf("expected sticky fallback to return prior resolver, got %v", got)
		}
	})

	t.Run("error_with_no_prior_cache_surfaces", func(t *testing.T) {
		c := resolverCache{}
		want := errors.New("first-time db unreachable")
		_, err := c.get(time.Now, time.Minute, func() (*metadata.Resolver, error) { return nil, want }, silentLogger)
		if !errors.Is(err, want) {
			t.Errorf("expected first-time error to surface, got: %v", err)
		}
	})

	t.Run("sticky_fallback_warns_first_time", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		c := resolverCache{}
		ok := func() (*metadata.Resolver, error) { return tableMeta, nil }
		fail := func() (*metadata.Resolver, error) { return nil, errors.New("db gone") }
		rec := newRecordingHandler()
		logger := slog.New(rec)

		// Warm the cache so the next failure triggers sticky fallback.
		if _, err := c.get(func() time.Time { return now }, time.Minute, ok, logger); err != nil {
			t.Fatalf("warm-up: %v", err)
		}
		// Push past TTL with a failing load. Expect Warn.
		if _, err := c.get(func() time.Time { return now.Add(2 * time.Minute) }, time.Minute, fail, logger); err != nil {
			t.Fatalf("get during outage: %v", err)
		}

		warns := rec.atLevel(slog.LevelWarn)
		if len(warns) != 1 {
			t.Fatalf("expected 1 Warn record on first sticky-fallback, got %d: %v", len(warns), rec.records)
		}
		if !strings.Contains(warns[0].Message, "stale snapshot") {
			t.Errorf("expected Warn about stale snapshot, got %q", warns[0].Message)
		}
	})

	t.Run("sticky_fallback_warn_is_rate_limited_to_one_per_ttl", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		c := resolverCache{}
		ok := func() (*metadata.Resolver, error) { return tableMeta, nil }
		fail := func() (*metadata.Resolver, error) { return nil, errors.New("db gone") }
		rec := newRecordingHandler()
		logger := slog.New(rec)
		ttl := time.Minute

		if _, err := c.get(func() time.Time { return now }, ttl, ok, logger); err != nil {
			t.Fatalf("warm-up: %v", err)
		}
		// Three failing gets close together — only the first should Warn.
		for i, dt := range []time.Duration{2 * time.Minute, 2*time.Minute + 5*time.Second, 2*time.Minute + 30*time.Second} {
			if _, err := c.get(func() time.Time { return now.Add(dt) }, ttl, fail, logger); err != nil {
				t.Fatalf("get #%d during outage: %v", i, err)
			}
		}

		if got := len(rec.atLevel(slog.LevelWarn)); got != 1 {
			t.Errorf("expected 1 Warn within TTL window, got %d", got)
		}

		// Push past the rate-limit window — expect a second Warn.
		if _, err := c.get(func() time.Time { return now.Add(2*time.Minute + 70*time.Second) }, ttl, fail, logger); err != nil {
			t.Fatalf("get past rate-limit: %v", err)
		}
		if got := len(rec.atLevel(slog.LevelWarn)); got != 2 {
			t.Errorf("expected 2 Warns after TTL window expires, got %d", got)
		}
	})
}

// TestColumnOrderForDistinguishesNoSnapshotFromRealError pins the
// log-level split documented in columnOrderFor: ErrNoSnapshots is
// the benign first-install state (Debug log only) while any other
// resolver-load error is a real config/infra problem (Warn log).
// Both still return nil so the alphabetical fallback path runs.
//
// Without this test a future refactor that collapsed both error
// paths back into the same Debug log would silently un-fix the
// observability gap that motivated the sentinel — the recording
// handler asserts on the actual emitted level rather than reading
// the source.
func TestColumnOrderForDistinguishesNoSnapshotFromRealError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantLevel slog.Level
		wantMsg   string
	}{
		{
			name:      "no_snapshots_logs_debug",
			err:       metadata.ErrNoSnapshots,
			wantLevel: slog.LevelDebug,
			wantMsg:   "no snapshots",
		},
		{
			name:      "real_error_logs_warn",
			err:       errors.New("connection refused"),
			wantLevel: slog.LevelWarn,
			wantMsg:   "schema_snapshots lookup failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecordingHandler()
			h := &Handler{
				logger:     slog.New(rec),
				resolverFn: func() (*metadata.Resolver, error) { return nil, tc.err },
			}
			if got := h.columnOrderFor("appdb", "orders"); got != nil {
				t.Errorf("expected nil fallback, got %v", got)
			}
			records := rec.atLevel(tc.wantLevel)
			if len(records) != 1 {
				t.Fatalf("expected exactly 1 record at level %s, got %d (all records: %v)",
					tc.wantLevel, len(records), rec.records)
			}
			if !strings.Contains(records[0].Message, tc.wantMsg) {
				t.Errorf("expected message containing %q, got %q", tc.wantMsg, records[0].Message)
			}
		})
	}
}

// TestColumnOrderForUsesCache pins the wiring between columnOrderFor
// and resolverCache. Without this test, a refactor that bypassed the
// cache (e.g. called h.resolverFn() directly) would invalidate every
// property TestResolverCacheBehaviour pins — the cache subtests would
// still pass because they exercise the cache type directly, not the
// integration. The test counts loader invocations across two
// columnOrderFor calls within the TTL window and asserts the count
// is exactly 1.
func TestColumnOrderForUsesCache(t *testing.T) {
	calls := 0
	tableMeta := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"appdb.orders": {
			Schema: "appdb", Table: "orders",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1},
				{Name: "sku", OrdinalPosition: 2},
			},
		},
	})
	h := &Handler{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		resolverFn: func() (*metadata.Resolver, error) {
			calls++
			return tableMeta, nil
		},
	}

	if got := h.columnOrderFor("appdb", "orders"); !slices.Equal(got, []string{"id", "sku"}) {
		t.Fatalf("first call: %v", got)
	}
	if got := h.columnOrderFor("appdb", "orders"); !slices.Equal(got, []string{"id", "sku"}) {
		t.Fatalf("second call: %v", got)
	}
	if calls != 1 {
		t.Errorf("expected resolverFn to be invoked exactly once across two columnOrderFor calls "+
			"within the TTL window (cache wiring regression?), got %d calls", calls)
	}
}

// TestMarshalImageOrderedDDL pins the contract that _diff JSON keys
// follow the source table's DDL order — without this, runDiff's
// row_before/row_after columns alphabetise (the json.Marshal(map)
// default), creating an inconsistency with _flashback's reconstructed
// row.
func TestMarshalImageOrderedDDL(t *testing.T) {
	cases := []struct {
		name     string
		image    map[string]any
		ddlOrder []string
		want     string
	}{
		{
			name:     "ddl_order_respected",
			image:    map[string]any{"id": 42, "sku": "ABC", "qty": 1, "note": "init"},
			ddlOrder: []string{"id", "sku", "qty", "note"},
			want:     `{"id":42,"sku":"ABC","qty":1,"note":"init"}`,
		},
		{
			name:     "nil_image_renders_empty_string",
			image:    nil,
			ddlOrder: []string{"id"},
			want:     "",
		},
		{
			name:     "nil_ddl_order_falls_back_to_alphabetical",
			image:    map[string]any{"id": 42, "sku": "ABC"},
			ddlOrder: nil,
			want:     `{"id":42,"sku":"ABC"}`,
		},
		{
			name:     "image_columns_not_in_ddl_appended_alphabetically",
			image:    map[string]any{"id": 1, "sku": "X", "added": "new"},
			ddlOrder: []string{"id", "sku"},
			want:     `{"id":1,"sku":"X","added":"new"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := marshalImageOrdered(tc.image, tc.ddlOrder)
			if got != tc.want {
				t.Errorf("marshalImageOrdered = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestRunPointInTimeInvokesArchiveFetcher exercises the runPointInTime
// → FetchMerged → ArchiveFetcher path with sqlmock, asserting that the
// shim's wiring actually delivers archive rows on virtual-schema
// queries (the issue #255 fix). Uses a stubbed archive_state row so
// FetchMerged calls the injected fetcher.
func TestRunPointInTimeInvokesArchiveFetcher(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// archive_state returns one S3-backed source. The local_path is
	// empty so ResolveArchiveSources falls through to the S3 branch
	// (which doesn't require the directory to exist on disk for the
	// shim host to discover it). The s3_key contains the
	// "bintrail_id=" marker extractBasePath looks for.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("test-id", "", "test-bucket", "bintrail_id=test-id/event_date=2026/events.parquet"))
	// The planner queries information_schema.PARTITIONS. Stub empty
	// so the planner returns no live hours.
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	// Live MySQL fetch may or may not run depending on planner output;
	// stub it permissive (no expected rows) so a call is fine.
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version"}))

	called := false
	fakeFetcher := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
		called = true
		return nil, nil
	}

	h := &Handler{
		indexDB:        db,
		cfg:            Config{AllowGaps: true, IndexDBName: "bintrail_index"},
		logger:         slog.Default(),
		archiveFetcher: fakeFetcher,
	}

	asof := time.Now()
	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "1",
		AsOf:    asof,
	}
	if _, err := h.runPointInTime(q); err != nil {
		// runPointInTime can succeed (empty resultset) or fail with a
		// scan error from sqlmock; both still prove the fetcher was
		// invoked. The assertion that matters is `called`.
		t.Logf("runPointInTime returned %v (acceptable for sqlmock-stubbed DB)", err)
	}
	if !called {
		t.Error("expected archiveFetcher to be invoked when archive_state has rows; was not called")
	}
}

// TestRunFullTableEnforcesCostCap exercises the load-bearing OOM
// guardrail: when FetchMerged returns more rows than the configured
// cap, runFullTable must surface ER_TOO_BIG_SELECT (1104) on the
// wire, not silently truncate (which would hand the customer a
// partial, unverifiable resultset) and not crash.
//
// The cap is configured per-Handler via Config.FullTableRowCap so
// this test can lower it to 3 on a local Handler instance without
// touching a global var — that keeps the test parallel-safe and
// matches the production path (a future per-tenant override would
// flow through the same field).
//
// A regression that drops the +1 sentinel on Limit (e.g. `Limit:
// cap` without the +1) silently turns the cap into "exactly cap
// rows accepted, no error." This test catches that.
func TestRunFullTableEnforcesCostCap(t *testing.T) {
	const testCap = 3

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Planner queries information_schema.PARTITIONS; stub empty so
	// the planner returns nil. AllowGaps=true below disables strict
	// gap enforcement so the planner-empty path doesn't short-circuit
	// with a *GapError before runFullTable gets a chance to evaluate
	// the cap.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))

	// Return cap+1 binlog_events rows. row_after is a JSON image so
	// the scan path produces a non-DELETE non-empty image — ensures
	// extractFullTableImages doesn't filter them out before the cap
	// check sees them.
	rows := sqlmock.NewRows(binlogEventsColumns())
	now := time.Now().UTC()
	for i := 0; i < testCap+1; i++ {
		rows.AddRow(
			int64(i+1), "binlog.000001", int64(100), int64(200), now,
			nil, nil, "myapp", "orders", parser.EventInsert,
			fmt.Sprintf("%d", i+1), nil, nil,
			fmt.Sprintf(`{"id":%d,"sku":"X"}`, i+1), 0,
		)
	}
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

	h := &Handler{
		indexDB: db,
		cfg: Config{AllowGaps: true, IndexDBName: "bintrail_index",
			NoArchive: true, FullTableRowCap: testCap},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
			return nil, nil
		},
	}
	h.UseDB("myapp")

	q := TimeTravelQuery{
		Type:   TypeFlashback,
		Schema: "myapp",
		Table:  "orders",
		AsOf:   now,
		// PKColumn deliberately empty — that's how runPointInTime
		// dispatches into runFullTable.
	}
	_, err = h.runFullTable(q)
	if err == nil {
		t.Fatal("expected ER_TOO_BIG_SELECT for rows > cap; got nil")
	}
	var myErr *gomysql.MyError
	if !errors.As(err, &myErr) {
		t.Fatalf("expected *mysql.MyError, got %T: %v", err, err)
	}
	if myErr.Code != gomysql.ER_TOO_BIG_SELECT {
		t.Errorf("wire code = %d, want %d (ER_TOO_BIG_SELECT, msg=%q)",
			myErr.Code, gomysql.ER_TOO_BIG_SELECT, myErr.Message)
	}
}

// TestNewHandlerDefaultIsStrict pins the library-side counterpart of the
// CLI default-pin in cmd/bintrail/shim_test.go: NewHandler must return a
// Handler configured with AllowGaps=false. The CLI builds Config directly
// via NewHandlerWithConfig, so a regression that restored the legacy
// AllowGaps=true default in NewHandler would not break the production
// path — but library callers (tests, future embedders) would silently
// pick up the permissive behaviour the issue #257 fix turns off.
func TestNewHandlerDefaultIsStrict(t *testing.T) {
	h := NewHandler(nil, nil)
	if h.cfg.AllowGaps {
		t.Error("NewHandler must default AllowGaps=false (strict); got true (see #257)")
	}
}

// TestRunPointInTimeStrictModePropagatesArchiveError pins the issue #257
// fix: when AllowGaps=false (the new production default) and an archive
// source fails, runPointInTime must return an error rather than silently
// swallowing the failure and returning a partial resultset. Without
// propagation, the MySQL client on the wire sees a successful response
// missing rows it should have received — the exact silent failure the
// PR fixes.
func TestRunPointInTimeStrictModePropagatesArchiveError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("test-id", "", "test-bucket", "bintrail_id=test-id/event_date=2026/events.parquet"))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version"}))

	archiveErr := errors.New("synthetic archive failure (e.g. S3 throttling)")
	failingFetcher := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
		return nil, archiveErr
	}

	h := &Handler{
		indexDB:        db,
		cfg:            Config{AllowGaps: false, IndexDBName: "bintrail_index"},
		logger:         slog.Default(),
		archiveFetcher: failingFetcher,
	}

	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "1",
		AsOf:    time.Now(),
	}
	_, err = h.runPointInTime(q)
	if err == nil {
		t.Fatal("expected runPointInTime to propagate archive failure under AllowGaps=false; got nil error")
	}
	// errors.Is over substring match: FetchMerged wraps the synthetic
	// archiveErr with %w, so the sentinel is recoverable. Pinning the
	// exact propagation path survives future error-message rewording —
	// a substring check on "archive" would also pass for an unrelated
	// archive-shaped error (e.g. validate-stage rejection) and that's
	// not the contract this test is here to enforce.
	if !errors.Is(err, archiveErr) {
		t.Errorf("expected wrapped archiveErr sentinel, got %v", err)
	}
}

// TestPlannerScopesPartitionsToIndexDB pins issue #259: the planner
// must scope information_schema.PARTITIONS to the index DB, not the
// user query's schema. A regression that re-passes q.Schema causes
// _flashback/_snapshot to return 0 rows (every hour misclassified as
// a coverage gap) and _diff to abort under strict mode.
func TestPlannerScopesPartitionsToIndexDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WithArgs("bintrail_index").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).
			AddRow("p_2026050415"))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version"}))

	h := &Handler{
		indexDB: db,
		cfg: Config{
			AllowGaps:   false,
			NoArchive:   false,
			IndexDBName: "bintrail_index",
		},
		logger:         slog.Default(),
		archiveFetcher: func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) { return nil, nil },
	}

	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "e2e_source",
		Table:   "orders",
		PKValue: "1",
		AsOf:    time.Date(2026, 5, 4, 15, 17, 52, 0, time.UTC),
	}
	if _, err := h.runPointInTime(q); err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (the planner likely scoped to %q instead of %q): %v",
			q.Schema, h.cfg.IndexDBName, err)
	}
}

// TestRunDiffScopesPartitionsToIndexDB is the runDiff sibling of
// TestPlannerScopesPartitionsToIndexDB. The two call sites do the same
// thing today, but a future refactor that splits Config could re-break
// _diff in isolation while leaving _flashback working — pinning each
// call site independently catches that.
func TestRunDiffScopesPartitionsToIndexDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WithArgs("bintrail_index").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).
			AddRow("p_2026050415"))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version"}))

	h := &Handler{
		indexDB: db,
		cfg: Config{
			AllowGaps:   false,
			NoArchive:   false,
			IndexDBName: "bintrail_index",
		},
		logger:         slog.Default(),
		archiveFetcher: func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) { return nil, nil },
	}

	q := TimeTravelQuery{
		Type:    TypeDiff,
		Schema:  "e2e_source",
		Table:   "orders",
		PKValue: "1",
		Since:   time.Date(2026, 5, 4, 15, 17, 0, 0, time.UTC),
		Until:   time.Date(2026, 5, 4, 15, 18, 0, 0, time.UTC),
	}
	if _, err := h.runDiff(q); err != nil {
		t.Fatalf("runDiff: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (the planner likely scoped to %q instead of %q): %v",
			q.Schema, h.cfg.IndexDBName, err)
	}
}

// TestEndToEndHandshake_AcceptsCorrectPassword boots a real MySQL-protocol
// server with our Handler and asserts that a client connecting with the
// correct username AND password passes the mysql_native_password challenge.
//
// This is the regression guard for issue #254: the handshake exercises
// `compareNativePasswordAuthData(salt, cleartext)` against the value
// `TenantAuth.GetCredential` returns. A regression to the pre-fix
// `("", true, nil)` would only let empty-password clients in — this
// test would fail because the client sends the actual password's
// scrambled response.
func TestEndToEndHandshake_AcceptsCorrectPassword(t *testing.T) {
	if err := runHandshakeTest(t, "alice", "alicepw", "alice", "alicepw"); err != nil {
		t.Fatalf("expected handshake to succeed with matching password: %v", err)
	}
}

// TestEndToEndHandshake_RejectsWrongPassword is the negative half: a
// client sending the wrong password must fail authentication. This
// catches the literal regression of #254 — without it, a pre-fix
// `GetCredential` returning "" would still pass
// TestEndToEndHandshake_AcceptsCorrectPassword if the server happened
// to accept any client response (which it does NOT today, but the
// negative case is what proves real validation is happening).
func TestEndToEndHandshake_RejectsWrongPassword(t *testing.T) {
	err := runHandshakeTest(t, "alice", "alicepw", "alice", "wrongpw")
	if err == nil {
		t.Fatal("expected handshake to fail with wrong password; got nil")
	}
	if !strings.Contains(err.Error(), "Access denied") {
		t.Errorf("expected MySQL 'Access denied' error, got %v", err)
	}
}

// runHandshakeTest spins up one shim listener, configures TenantAuth
// with serverUser/serverPass, and dials with clientUser/clientPass.
// Returns the client's Ping error (nil on success). Used by both the
// positive and negative auth tests above.
func runHandshakeTest(t *testing.T, serverUser, serverPass, clientUser, clientPass string) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	// Server side: accept one connection, perform handshake, then loop
	// HandleCommand until the client disconnects. SetReadDeadline
	// guarantees the loop unblocks even if the client's TCP close does
	// not propagate immediately, so the test can never hang.
	serverErr := make(chan error, 1)
	go func() {
		c, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		h := NewHandler(nil, nil)
		h.UseDB("myapp")
		srv := server.NewDefaultServer()
		auth, _ := NewTenantAuth(map[string]string{serverUser: serverPass})
		mc, err := server.NewCustomizedConn(c, srv, auth, h)
		if err != nil {
			// Auth failure surfaces here as a non-nil error from
			// NewCustomizedConn (handshake fails before the command
			// loop starts). Negative-auth tests rely on this.
			serverErr <- err
			return
		}
		for {
			if err := mc.HandleCommand(); err != nil {
				serverErr <- nil
				return
			}
		}
	}()

	host, port, _ := net.SplitHostPort(addr)
	clientErr := make(chan error, 1)
	go func() {
		clientErr <- driveClient(host+":"+port, clientUser, clientPass)
	}()

	var pingErr error
	select {
	case pingErr = <-clientErr:
	case <-time.After(5 * time.Second):
		t.Fatal("client timed out")
	}

	listener.Close()
	select {
	case <-serverErr:
	case <-time.After(5 * time.Second):
		t.Fatal("server goroutine did not exit")
	}
	return pingErr
}

// driveClient connects to the shim with explicit credentials and
// runs Ping. Returns the Ping error (nil on success).
func driveClient(addr, user, password string) error {
	dsn := user + ":" + password + "@tcp(" + addr + ")/"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}
	return nil
}

// equalMaps compares two map[string]any by length and value identity
// (==). Sufficient for the selectImage tests because they intentionally
// pass the same map literal as both input and expected output, so a
// pointer-equal value comparison detects "did selectImage return the
// expected source map?". Returning a *different* map with equal contents
// would fail this check — which is the correct outcome, since the
// helper's contract is to hand back the input image unchanged, not a
// copy.
func equalMaps(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || va != vb {
			return false
		}
	}
	return true
}

// recordingHandler is a minimal slog.Handler that captures every
// emitted record into an in-memory slice. Used by tests that need
// to assert log levels and messages — without it we'd have to
// either parse a TextHandler's stringly output or skip log-level
// verification entirely (which is what the prior weakened test
// resorted to). Concurrent-safe so it can sit behind a logger
// shared across goroutines if a future test exercises that path.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func newRecordingHandler() *recordingHandler { return &recordingHandler{} }

func (h *recordingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(_ string) slog.Handler      { return h }

// atLevel returns all captured records at exactly the given level.
func (h *recordingHandler) atLevel(level slog.Level) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.records {
		if r.Level == level {
			out = append(out, r)
		}
	}
	return out
}

// Compile-time check: TenantAuth implements the credential provider
// interface.
var _ server.CredentialProvider = TenantAuth{}

// Compile-time check: nil-safe constructor returns a real Handler.
var _ = func() *Handler {
	return NewHandler(nil, nil)
}

// Compile-time check: emptyResult always returns a resultset.
var _ = emptyResult().Resultset

// Suppress unused-import lint: gomysql is referenced only for the
// compile-time assertion below.
var _ = gomysql.Result{}
