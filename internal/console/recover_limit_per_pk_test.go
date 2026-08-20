package console

import (
	"database/sql"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/parser"
)

var lppEventCols = []string{
	"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
	"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
	"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
	"commit_ts_us",
}

// sameSecondRows is the shape that motivated exposing this filter: one row
// INSERTed and then DELETEd inside the SAME second, so no since/until pair can
// separate the two events.
func sameSecondRows() *sqlmock.Rows {
	ts := time.Date(2026, 8, 20, 19, 10, 33, 0, time.UTC)
	after := []byte(`{"id":42,"email":"a@x"}`)
	return sqlmock.NewRows(lppEventCols).
		AddRow(int64(2), "bin.000001", int64(40), int64(80), ts,
			nil, nil, "app", "users", int64(parser.EventDelete), "42",
			nil, after, nil, int64(0), nil, nil, nil).
		AddRow(int64(1), "bin.000001", int64(4), int64(40), ts,
			nil, nil, "app", "users", int64(parser.EventInsert), "42",
			nil, nil, after, int64(0), nil, nil, nil)
}

// sqlCapture records every statement sqlmock is asked to match.
//
// It exists because query applies LimitPerPK in SQL — a ROW_NUMBER() window
// over pk_values — and sqlmock never executes SQL: it replays the rows the
// fixture declares no matter what the statement says. Asserting on the
// generated script would therefore "prove" the filter works whether or not it
// was ever wired, and asserting it does NOT work is equally impossible. What
// this layer actually owns is the wiring: the request field must reach
// query.Options and change the statement. That is what is observable here, so
// that is what is asserted. The trimming semantics themselves are the query
// package's to test (internal/query: LimitPerPK / MergeAndTrimReport).
type sqlCapture struct {
	mu   sync.Mutex
	seen []string
}

func (c *sqlCapture) Match(expected, actual string) error {
	c.mu.Lock()
	c.seen = append(c.seen, actual)
	c.mu.Unlock()
	return sqlmock.QueryMatcherRegexp.Match(expected, actual)
}

func (c *sqlCapture) eventsQuery(t *testing.T) string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.seen {
		if strings.Contains(s, "FROM binlog_events") {
			return s
		}
	}
	t.Fatalf("no binlog_events query was issued; saw %d statement(s): %v", len(c.seen), c.seen)
	return ""
}

func newCapturingMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *sqlCapture, func()) {
	t.Helper()
	cap := &sqlCapture{}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(cap.Match)))
	if err != nil {
		t.Fatal(err)
	}
	return db, mock, cap, func() { db.Close() }
}

// The request field must reach the SQL builder. The window clause is the
// observable difference between "latest N per row" being asked for and not.
func TestRecoverLimitPerPKReachesTheQuery(t *testing.T) {
	const window = "ROW_NUMBER() OVER (PARTITION BY pk_values"

	t.Run("absent by default", func(t *testing.T) {
		db, mock, cap, closeDB := newCapturingMock(t)
		defer closeDB()
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sameSecondRows())

		rec := httptest.NewRecorder()
		newBootServer(db).handleRecover(rec, httptest.NewRequest("POST", "/api/recover",
			strings.NewReader(`{"schema":"app","table":"users","pk":"42"}`)))
		if rec.Code != 200 {
			t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
		}
		if got := cap.eventsQuery(t); strings.Contains(got, window) {
			t.Errorf("a recover with no limit_per_pk emitted the per-PK window anyway:\n%s", got)
		}
	})

	t.Run("present when requested", func(t *testing.T) {
		db, mock, cap, closeDB := newCapturingMock(t)
		defer closeDB()
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sameSecondRows())

		rec := httptest.NewRecorder()
		newBootServer(db).handleRecover(rec, httptest.NewRequest("POST", "/api/recover",
			strings.NewReader(`{"schema":"app","table":"users","pk":"42","limit_per_pk":1}`)))
		if rec.Code != 200 {
			t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
		}
		if got := cap.eventsQuery(t); !strings.Contains(got, window) {
			t.Errorf("limit_per_pk=1 did not reach query.Options — no per-PK window in:\n%s", got)
		}
	})
}

// The PK precondition is enforced by the SERVER, not the form. The API is
// reachable without the UI, and "latest N per row" over an unscoped window
// would quietly keep N events for every PK the other filters happen to touch.
func TestRecoverLimitPerPKRequiresPK(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
	}{
		{"no pk", `{"schema":"app","table":"users","limit_per_pk":1}`, "needs a PK"},
		{"negative", `{"schema":"app","table":"users","pk":"42","limit_per_pk":-1}`, "0 or more"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _, closeDB := newSQLMock(t)
			defer closeDB()
			rec := httptest.NewRecorder()
			newBootServer(db).handleRecover(rec,
				httptest.NewRequest("POST", "/api/recover", strings.NewReader(tc.body)))
			if rec.Code != 400 {
				t.Fatalf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.want) {
				t.Errorf("error body %q does not mention %q", rec.Body.String(), tc.want)
			}
		})
	}
}

// Restore's "Preview rows" promises it shows the events the undo script will
// reverse. That promise only holds if the filter reaches BOTH endpoints —
// wiring it into recover alone would make the preview list events the script
// leaves untouched, which is worse than not offering the control at all.
func TestAppJSPreviewMirrorsLimitPerPK(t *testing.T) {
	data, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(data)

	if !strings.Contains(js, `params.limit_per_pk = String(plpp)`) {
		t.Error("previewRecover no longer forwards limit_per_pk — the preview would list events " +
			"the generated script will not reverse, breaking the mirror it documents")
	}
	if !strings.Contains(js, "body.limit_per_pk = lpp") {
		t.Error("generateUndo no longer sends limit_per_pk — the form field would be inert")
	}
	// Whole-number validation refuses rather than coerces: Number("2.5") is a
	// number, and silently flooring it would reverse a different set of events
	// than the operator typed.
	if !strings.Contains(js, "!Number.isInteger(n) || n < 0") {
		t.Error("parseLatestPerRow no longer rejects non-integer input; a coerced value would " +
			"reverse a different number of events than the operator asked for")
	}
}
