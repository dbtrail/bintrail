package console

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// eventRowCols mirrors the SELECT list Engine.Fetch reads. Kept local to this
// file so a column added upstream fails here loudly rather than silently
// shifting a scan.
var eventRowCols = []string{
	"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
	"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
	"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
	"commit_ts_us",
}

// mockEventRows builds n descending-in-time event rows starting at base.
func mockEventRows(base time.Time, n int) *sqlmock.Rows {
	rows := sqlmock.NewRows(eventRowCols)
	for i := range n {
		rows.AddRow(
			int64(1000-i), "bin.000001", int64(4), int64(40), base.Add(-time.Duration(i)*time.Minute),
			nil, int64(4242), "app", "users", int64(parser.EventUpdate), fmt.Sprint(i),
			[]byte(`["email"]`), []byte(`{"email":"a@x"}`), []byte(`{"email":"b@x"}`), int64(0),
			nil, nil, int64(0),
		)
	}
	return rows
}

func getEvents(t *testing.T, s *Server, url string) (int, eventsResponse, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleEvents(rec, httptest.NewRequest("GET", url, nil))
	var resp eventsResponse
	if rec.Code == 200 {
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v (body %s)", err, rec.Body.String())
		}
	}
	return rec.Code, resp, rec.Body.String()
}

// TestEventsHasMoreProbe: the handler asks the engine for one row PAST the
// page so it can say whether anything is behind it, and that probe row is
// never served. Before #1297 the header could only restate the limit back at
// the reader ("100 event(s) in the newest 100"), which answered nothing about
// whether 101 or ten million events sat behind the cut.
func TestEventsHasMoreProbe(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("more behind the page", func(t *testing.T) {
		db, mock, done := newSQLMock(t)
		defer done()
		// 3 rows come back for a page of 2: the third is the probe.
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(mockEventRows(base, 3))

		code, resp, body := getEvents(t, newBootServer(db), "/api/events?schema=app&table=users&limit=2")
		if code != 200 {
			t.Fatalf("status = %d, body = %s", code, body)
		}
		if !resp.HasMore {
			t.Error("has_more = false with a row past the page; the UI would hide its Next control and event 3 stays unreachable")
		}
		if resp.Count != 2 || len(resp.Events) != 2 {
			t.Errorf("count = %d / %d events, want 2: the probe row must not be served", resp.Count, len(resp.Events))
		}
		if resp.Limit != 2 {
			t.Errorf("limit = %d, want 2 — the page size, never the probe size", resp.Limit)
		}
		if resp.NextBefore == "" {
			t.Error("next_before is empty while has_more is true; the client cannot ask for the next page")
		}
		// The cursor must name the LAST SERVED row, not the probe: naming the
		// probe would skip it on the following page.
		wantID := "|999"
		if !strings.HasSuffix(resp.NextBefore, wantID) {
			t.Errorf("next_before = %q, want it to end at the last SERVED row %q", resp.NextBefore, wantID)
		}
	})

	t.Run("nothing behind the page", func(t *testing.T) {
		db, mock, done := newSQLMock(t)
		defer done()
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(mockEventRows(base, 2))

		code, resp, body := getEvents(t, newBootServer(db), "/api/events?schema=app&table=users&limit=2")
		if code != 200 {
			t.Fatalf("status = %d, body = %s", code, body)
		}
		if resp.HasMore {
			t.Error("has_more = true on an exactly-full page with no probe row: the operator is told to keep paging into nothing")
		}
		if resp.NextBefore != "" {
			t.Errorf("next_before = %q, want empty when there is no next page", resp.NextBefore)
		}
		if resp.Count != 2 {
			t.Errorf("count = %d, want 2", resp.Count)
		}
	})
}

// TestEventsLimitCapUnchanged: paging must not become a way to widen the
// result cap. An over-max request still reports (and is clamped to)
// eventsMaxLimit — the probe adds exactly one row to the FETCH, and never to
// the contract.
func TestEventsLimitCapUnchanged(t *testing.T) {
	db, mock, done := newSQLMock(t)
	defer done()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(mockEventRows(time.Now(), 1))

	code, resp, body := getEvents(t, newBootServer(db), "/api/events?schema=app&table=users&limit=5000")
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if resp.Limit != eventsMaxLimit {
		t.Errorf("limit = %d, want the cap %d", resp.Limit, eventsMaxLimit)
	}
}

// TestEventsBeforeCursorPagesTheIndex: the ?before= cursor reaches the engine
// as a keyset predicate carrying BOTH components. event_id is not decoration —
// event_timestamp has one-second resolution, so a timestamp-only cut would
// re-serve or skip every event sharing the boundary second.
func TestEventsBeforeCursorPagesTheIndex(t *testing.T) {
	db, mock, done := newSQLMock(t)
	defer done()
	// The regex is over the generated SQL: the keyset predicate must be there,
	// or the "next page" silently re-serves page 1 forever.
	mock.ExpectQuery(regexp.QuoteMeta("(event_timestamp < ? OR (event_timestamp = ? AND event_id < ?))")).
		WillReturnRows(mockEventRows(time.Now(), 1))

	cursor := "2026-01-02T03:04:05Z|999"
	code, resp, body := getEvents(t, newBootServer(db),
		"/api/events?schema=app&table=users&limit=2&before="+cursor)
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if resp.Count != 1 {
		t.Errorf("count = %d, want 1", resp.Count)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the cursor never reached the engine: %v", err)
	}
}

// TestEventsCursorErrorsAre400: a cursor the server cannot honor is a hard
// refusal, never a silent fall back to page 1 — that would make the UI's Next
// button re-serve the same page forever while the operator believed they were
// walking backwards through the index.
func TestEventsCursorErrorsAre400(t *testing.T) {
	cases := []struct {
		name, url, wantMsg string
	}{
		{"malformed cursor", "/api/events?before=nonsense", "invalid before cursor"},
		{"bad timestamp", "/api/events?before=not-a-time|7", "invalid before cursor timestamp"},
		{"bad event id", "/api/events?before=2026-01-02T03:04:05Z|abc", "invalid before cursor event id"},
		{"backward cursor on an ascending listing", "/api/events?order=ASC&before=2026-01-02T03:04:05Z|7", "needs order=DESC"},
		{"forward cursor on a descending listing", "/api/events?after=2026-01-02T03:04:05Z|7", "needs order=ASC"},
		{"both directions at once", "/api/events?after=2026-01-02T03:04:05Z|7&before=2026-01-02T03:04:05Z|7", "not both"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// nil db: a handler that reached the fetch would panic or 500, so a
			// clean 400 also proves the refusal happens BEFORE any DB work.
			rec := httptest.NewRecorder()
			newBootServer(nil).handleEvents(rec, httptest.NewRequest("GET", tc.url, nil))
			if rec.Code != 400 {
				t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantMsg) {
				t.Errorf("body should explain the refusal (%q), got %s", tc.wantMsg, rec.Body.String())
			}
		})
	}
}

// TestEventCursorRoundTrip: a cursor rendered from a served row parses back to
// the same instant and id. The timestamp carries its offset on purpose — a
// bare wall clock has to be re-attached to some location on the way in, and
// guessing wrong does not fail, it just returns the wrong page.
func TestEventCursorRoundTrip(t *testing.T) {
	offset := time.FixedZone("UTC+5", 5*3600)
	for _, ts := range []time.Time{
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2026, 1, 2, 3, 4, 5, 0, offset),
	} {
		s := formatEventCursor(query.ResultRow{EventTimestamp: ts, EventID: 4242})
		got, err := parseEventCursor("before", s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if !got.Timestamp.Equal(ts) {
			t.Errorf("%s: timestamp = %v, want the same instant as %v", s, got.Timestamp, ts)
		}
		if got.EventID != 4242 {
			t.Errorf("%s: event id = %d, want 4242", s, got.EventID)
		}
	}
}
