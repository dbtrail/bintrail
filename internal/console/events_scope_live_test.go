package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

func scopeLiveEventRows() *sqlmock.Rows {
	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 8, 21, 3, 4, 5, 0, time.UTC)
	return sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(parser.EventUpdate), "7",
		[]byte(`["email"]`), []byte(`{"email":"a@x"}`), []byte(`{"email":"b@x"}`), int64(0),
		nil, nil, nil,
	)
}

// TestEventsScopeLive pins the phase-1 contract (#1414): a scope=live read
// serves the live index, does NOT touch the archives, and SAYS both — the
// scope echo, the pending flag, and the PARTIAL warning.
//
// The archive-untouched half is pinned by a counting stub fetcher, not by
// SQL expectations: sqlmock never reports a SURPLUS query, so a scope=live
// that read the archives anyway is invisible at the SQL layer. The mutation
// this exists to kill (dropping `|| liveOnly` from fetchRestrictedScoped)
// also runs discovery, which the unordered single archive_state expectation
// converts into a second observable: the pending probe then finds its
// expectation consumed and degrades the warning to the discovery-failed text.
func TestEventsScopeLive(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(scopeLiveEventRows())
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("abc-123", nil, "bkt", "bintrail/bintrail_id=abc-123/x.parquet"))

	var fetcherCalls int
	s := newBootServer(db)
	s.cm.boot.noArchive = false
	s.archiveFetcher = func(context.Context, query.Options, string) ([]query.ResultRow, error) {
		fetcherCalls++
		return nil, nil
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/events?schema=app&table=users&scope=live", nil)
	s.handleEvents(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if fetcherCalls != 0 {
		t.Errorf("archive fetcher called %d time(s) under scope=live — the live phase must not "+
			"wait on (or read) a single archive", fetcherCalls)
	}
	var resp eventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Scope != "live" {
		t.Errorf("scope = %q, want live — the response must mark the caller's own scope", resp.Scope)
	}
	if !resp.ArchivesPending {
		t.Error("archives_pending = false with a registered source unread — the client would " +
			"skip phase 2 and present a partial list as complete")
	}
	if resp.Count != 1 {
		t.Errorf("count = %d, want 1", resp.Count)
	}
	joined := strings.Join(resp.Warnings, " | ")
	if !strings.Contains(joined, "PARTIAL") || !strings.Contains(joined, "1 registered") {
		t.Errorf("warnings = %q, want the loud partial marker naming the unread source count — "+
			"the issue's own requirement: louder than the elision note, never quieter", joined)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestEventsScopeLiveNoSources: nothing registered → the response says the
// live page IS the complete answer (pending false, benign note), so the
// client can skip phase 2 instead of issuing a redundant read forever.
func TestEventsScopeLiveNoSources(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(scopeLiveEventRows())
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))

	s := newBootServer(db)
	s.cm.boot.noArchive = false
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/events?schema=app&table=users&scope=live", nil)
	s.handleEvents(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp eventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Scope != "live" || resp.ArchivesPending {
		t.Errorf("scope=%q pending=%v, want live/false — no sources means nothing further to read",
			resp.Scope, resp.ArchivesPending)
	}
	if !strings.Contains(strings.Join(resp.Notes, " "), "complete answer") {
		t.Errorf("notes = %q, want the nothing-to-read note", resp.Notes)
	}
	if len(resp.Warnings) != 0 {
		t.Errorf("warnings = %q, want none — nothing partial happened", resp.Warnings)
	}
}

// TestEventsScopeInvalid: anything but "live" is a 400, never a silent fall
// back to the full read — a client that believes it asked for the fast phase
// must not be handed the slow one with no way to tell.
func TestEventsScopeInvalid(t *testing.T) {
	s := newBootServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/events?schema=app&scope=fast", nil)
	s.handleEvents(rec, req)
	if rec.Code != 400 {
		t.Errorf("scope=fast: code = %d, want 400", rec.Code)
	}
}

// TestEventsSkippedSourceReachesTheResponse (#1414 review pass 2): a FULL
// read whose archive source fails under AllowGaps used to disclose that only
// in the daemon log — which turned the scope=live phase-1 promise ("a full
// read will report it") into a false claim the moment the client swept the
// PARTIAL marker on phase 2's warning-free response. The incompleteness
// inventory must ride the response.
func TestEventsSkippedSourceReachesTheResponse(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("abc-123", nil, "bkt", "bintrail/bintrail_id=abc-123/x.parquet"))
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(scopeLiveEventRows())

	s := newBootServer(db)
	s.cm.boot.noArchive = false
	s.archiveFetcher = func(context.Context, query.Options, string) ([]query.ResultRow, error) {
		return nil, errors.New("S3 outage (intentional)")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/events?schema=app&table=users", nil)
	s.handleEvents(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp eventsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(resp.Warnings, " | ")
	if !strings.Contains(joined, "failed and was skipped") {
		t.Errorf("warnings = %q, want the skipped-source disclosure — a failed archive read must "+
			"not present a partial list as complete", joined)
	}
}

// TestLiveScopeAdvisories pins the severity decision, including WHEN the
// plan speaks (pass 1 caught the first cut passing nil unconditionally,
// silencing coverage on the three shapes that never get a phase 2).
func TestLiveScopeAdvisories(t *testing.T) {
	gapPlan := &query.QueryPlan{GapHours: []time.Time{time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)}}
	tests := []struct {
		name     string
		plan     *query.QueryPlan
		excl     archiveExclusion
		pending  int
		wantWarn string // "" = no warning expected
		wantNote string // "" = no note expected
		banWarn  string // must NOT appear
	}{
		{name: "sources pending", pending: 2, wantWarn: "2 registered archive source(s) were NOT read"},
		{name: "pending suppresses the misattributed gap line", plan: gapPlan, pending: 2,
			wantWarn: "PARTIAL", banWarn: "rotated and not archived"},
		{name: "discovery failed", pending: -1, wantWarn: "archive discovery failed"},
		{name: "nothing to read, clean coverage", pending: 0, wantNote: "complete answer"},
		// With NOTHING registered the plain "rotated and not archived" text is
		// truthful — nothing is archived — so it stays; the note redirects it.
		{name: "nothing to read, gaps present", plan: gapPlan, pending: 0,
			wantWarn: "covers hours with no data", wantNote: "gaps nothing recorded",
			banWarn: "complete answer"},
		{name: "server no-archive with gaps keeps the coverage story", plan: gapPlan,
			excl: archiveExclusion{server: true}, pending: 0, wantWarn: "LIVE INDEX ONLY"},
		{name: "profile exclusion owns the story", excl: archiveExclusion{profile: true}, pending: 0,
			wantWarn: "", wantNote: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			warnings, notes := liveScopeAdvisories(tc.plan, tc.excl, tc.pending)
			jw, jn := strings.Join(warnings, " | "), strings.Join(notes, " | ")
			if tc.wantWarn != "" && !strings.Contains(jw, tc.wantWarn) {
				t.Errorf("warnings = %q, want %q", jw, tc.wantWarn)
			}
			if tc.wantNote != "" && !strings.Contains(jn, tc.wantNote) {
				t.Errorf("notes = %q, want %q", jn, tc.wantNote)
			}
			if tc.banWarn != "" && (strings.Contains(jw, tc.banWarn) || strings.Contains(jn, tc.banWarn)) {
				t.Errorf("advisories %q + %q must not contain %q", jw, jn, tc.banWarn)
			}
			if tc.wantNote == "" && strings.Contains(jn, "complete answer") {
				t.Errorf("notes = %q — 'complete' claimed on a scope that excluded the archives", jn)
			}
			if tc.excl.profile && !strings.Contains(jw, "profile") {
				t.Errorf("warnings = %q, want the profile exclusion still announced", jw)
			}
		})
	}
}
