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
// fetchDelay is large enough to survive scheduler jitter and small enough not
// to slow the suite. The assertion uses half of it, so a loaded machine cannot
// flake the test while a dropped assignment (0ms) still fails decisively.
const fetchDelay = 40 * time.Millisecond

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
	// The fixture DELAYS the fetch. Without it this test cannot see the mutation
	// it exists to prevent: because the field is deliberately not omitempty,
	// dropping the assignment still emits "generated_in_ms":0, which satisfies
	// both key-presence and ms >= 0. Worse than a missing key — the panel would
	// render "generated in <0.1s", a fabricated fast measurement. A measurable
	// floor is what makes the assignment load-bearing.
	mock.ExpectQuery("FROM binlog_events").WillDelayFor(fetchDelay).WillReturnRows(
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
	if floor := float64(fetchDelay.Milliseconds()) / 2; ms < floor {
		t.Errorf("generated_in_ms = %v, want >= %v — the fixture delays the fetch by %s, so a "+
			"value near zero means the field is being reported without being measured",
			ms, floor, fetchDelay)
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

// The cascade endpoint measures too.
//
// A source guard rather than a handler test, deliberately and with the
// tradeoff stated: /api/recover-cascade needs FK-constraint fixtures to reach
// its 200 path, and the reflection check above only proves the TAG is right —
// it would pass with the assignment deleted, reporting a fabricated 0. This
// pins the assignment itself inside the response literal. It is weaker than
// exercising the handler and is not a substitute for one; it is what makes the
// difference between "guarded" and "guarded on one of two sites".
func TestRecoverCascadeMeasuresGenerationTime(t *testing.T) {
	data, err := os.ReadFile("recover_cascade.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	i := strings.Index(src, "recoverCascadeResponse{")
	if i < 0 {
		t.Fatal("no recoverCascadeResponse literal in recover_cascade.go — this guard covers nothing")
	}
	j := strings.Index(src[i:], "})")
	if j < 0 {
		t.Fatal("could not find the end of the recoverCascadeResponse literal")
	}
	// Matched as two independent needles rather than one aligned string: gofmt
	// re-aligns the literal's values whenever a longer field name is added, so
	// pinning "GeneratedInMs:   time.Since(start)" with its three spaces would
	// fail with "no longer measures" on an unrelated field rename.
	lit := src[i : i+j]
	if !strings.Contains(lit, "GeneratedInMs:") || !strings.Contains(lit, "time.Since(start)") {
		t.Error("the recover-cascade response no longer measures its generation time. " +
			"Because the field is not omitempty it would still emit generated_in_ms:0, so an API " +
			"consumer reads a fabricated sub-millisecond measurement rather than an absent field. " +
			"(The shipped console never calls this endpoint — it reaches cascade through " +
			"/api/recover auto-detection — so the reader here is an external client.)")
	}
}

// The clock must start AFTER the bundle resolves.
//
// This is the half of the documented boundary that nothing else pins.
// TestRecoverReportsGenerationTime proves `start` is BEFORE the fetch (the
// delayed fixture has to land inside it); nothing proved it is AFTER
// resolveOr. Moving the declaration one line up keeps every other test green
// while making the docs false — /api/recover would bill a lazily-opened
// registry connection (a 10s TCP dial ceiling) as generation time, so the
// first request after switching servers reports a dial the operator did not
// ask about.
//
// Source-level for the same reason the cascade guard is: reproducing a cold
// connManager open in a unit test means faking the registry, and the ordering
// IS the contract.
func TestTimerStartsAfterBundleResolution(t *testing.T) {
	for _, tc := range []struct{ file, fn string }{
		{"api.go", "func (s *Server) handleRecover("},
		{"recover_cascade.go", "func (s *Server) handleRecoverCascade("},
	} {
		t.Run(tc.file, func(t *testing.T) {
			data, err := os.ReadFile(tc.file)
			if err != nil {
				t.Fatal(err)
			}
			src := string(data)
			i := strings.Index(src, tc.fn)
			if i < 0 {
				t.Fatalf("%s is gone from %s — this guard covers nothing", tc.fn, tc.file)
			}
			body := src[i:]
			if j := strings.Index(body[1:], "\nfunc "); j > 0 {
				body = body[:j+1]
			}
			resolve := strings.Index(body, "s.resolveOr(w, r)")
			start := strings.Index(body, "start := time.Now()")
			if resolve < 0 {
				t.Fatalf("no resolveOr call in %s — the handler shape changed", tc.fn)
			}
			if start < 0 {
				t.Fatalf("no timer in %s", tc.fn)
			}
			if start < resolve {
				t.Errorf("%s starts its timer BEFORE resolveOr. docs/console.md promises "+
					"generated_in_ms excludes selecting and opening the target server's connection; "+
					"above resolveOr it bills a lazy connection open (10s dial ceiling) as generation "+
					"time, and the first request after a server switch reports a dial.", tc.fn)
			}
		})
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
