package console

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// /api/recover reports how long generation took, and reports it as a key that
// is always on the wire.
//
// The assertion is made against the RAW JSON, not a decoded recoverResponse,
// and that is the whole point: if someone adds `omitempty` to GeneratedInMs,
// a struct decode of the now-missing key still yields 0 — indistinguishable
// from a genuinely sub-millisecond recover. The client's renderer relies on
// exactly that distinction (absent = older server, render nothing; 0 = fast,
// render "<0.1s"), so the key's PRESENCE is the contract, not its value.
func TestRecoverReportsGenerationTime(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(
		sqlmock.NewRows(cols).AddRow(
			int64(1), "bin.000001", int64(4), int64(40), ts,
			nil, nil, "app", "users", int64(parser.EventInsert), "42",
			nil, nil, []byte(`{"id":42,"email":"a@x"}`), int64(0),
			nil, nil, nil,
		))

	s := newBootServer(db)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{"schema":"app","table":"users"}`))
	s.handleRecover(rec, req)

	if rec.Code != 200 {
		t.Fatalf("recover status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	v, ok := raw["generated_in_ms"]
	if !ok {
		t.Fatalf("recover response has no generated_in_ms key — the client cannot tell "+
			"\"this server does not report timing\" from \"generation took under a millisecond\" "+
			"once the key can be absent. Do not mark it omitempty. Body: %s", rec.Body.String())
	}
	ms, isNum := v.(float64)
	if !isNum {
		t.Fatalf("generated_in_ms = %#v, want a number", v)
	}
	if ms < 0 {
		t.Errorf("generated_in_ms = %v, want >= 0", ms)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}

	// The key-presence check above only catches `omitempty` while the measured
	// value happens to be zero — true for this sqlmock fixture, but a busy
	// machine could round to 1ms and let the regression through. Assert the tag
	// itself so the guard does not depend on how fast the test ran.
	assertNoOmitEmpty(t, recoverResponse{}, "GeneratedInMs")
	assertNoOmitEmpty(t, recoverCascadeResponse{}, "GeneratedInMs")
}

// assertNoOmitEmpty fails if a field's json tag carries omitempty. Used where
// a zero value is a meaningful answer and its absence means something else.
func assertNoOmitEmpty(t *testing.T, v any, field string) {
	t.Helper()
	f, ok := reflect.TypeOf(v).FieldByName(field)
	if !ok {
		t.Fatalf("%T has no field %s", v, field)
	}
	if strings.Contains(f.Tag.Get("json"), "omitempty") {
		t.Errorf("%T.%s is tagged omitempty (%q) — zero is a meaningful value here "+
			"(generation took under a millisecond) and omitting it makes a fast recover "+
			"indistinguishable from a server that does not report timing at all",
			v, field, f.Tag.Get("json"))
	}
}

// The frontend must actually RENDER the timing, and must keep absence and zero
// apart while doing it. A field that reaches the wire and no surface is the
// same silence with more code.
func TestAppJSRendersGenerationTime(t *testing.T) {
	data, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	// Matched against raw source, unlike the Undo-banner guard: both needles
	// below are code constructs (a call with its argument, a typeof
	// comparison) rather than prose, so neither can be satisfied accidentally
	// by an explanatory comment the way a user-visible sentence can.
	js := string(data)

	// (a) Wired at the call site, not merely defined. Asserted on the exact
	// argument so a helper left dangling after a refactor is caught.
	if !strings.Contains(js, "formatGeneratedIn(data.generated_in_ms)") {
		t.Error("the reversal panel no longer passes data.generated_in_ms to formatGeneratedIn — " +
			"the server reports the timing and nothing displays it")
	}

	// (b) The absent-vs-zero distinction. A falsy guard (`if (!ms)`) would
	// treat a sub-millisecond recover — the fastest, most reassuring result —
	// as if the server had not reported at all.
	if !strings.Contains(js, `typeof ms !== "number"`) {
		t.Error("formatGeneratedIn no longer discriminates on typeof: a falsy check collapses " +
			"0 ms (measured, fast) into the same branch as a missing field (older server), " +
			"and the fast case silently stops rendering")
	}
}
