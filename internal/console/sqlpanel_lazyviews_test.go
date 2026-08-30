package console

// #1526: the panel builds only the views a statement names, and reports the
// whole wait rather than the statement's share of it.
//
// These drive the REAL functions (runSandboxedSQL / openSandboxedSession /
// sqlPanelGate, and the handler through the mux), like the rest of the panel's
// suite.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

// unbuildableInput is a layout where every view EXCEPT state_shop_orders is
// impossible to define: the archive source names a directory with no partition
// files in it, and the second baseline table names a Parquet file that does not
// exist. Defining a view over Parquet binds its columns, so DuckDB opens the
// file (or resolves the glob) at CREATE VIEW and fails there.
//
// That makes "which views did this session build" observable without timing
// anything: a statement that succeeds over this layout is a statement whose
// session built neither of the broken two.
func unbuildableInput(t *testing.T) views.Input {
	t.Helper()
	baselineRoot, orders := writeSQLPanelBaseline(t)
	emptyArchive := filepath.Join(t.TempDir(), "bintrail_id=11111111-2222-3333-4444-555555555555")
	if err := os.MkdirAll(emptyArchive, 0o755); err != nil {
		t.Fatal(err)
	}
	in := panelInput([]string{emptyArchive}, baselineRoot, orders)
	in.Baselines = append(in.Baselines, views.BaselineTable{
		Schema: "shop", Table: "gone",
		Path: filepath.Join(baselineRoot, "2026-04-30T03-00-00Z", "shop", "gone.parquet"),
	})
	return in
}

// TestSQLPanel_buildsOnlyTheViewsTheStatementNames is the #1526 fix, proven
// without a clock: over a layout whose events view and whose state_shop_gone
// view cannot be defined at all, a statement that names neither must still run.
// Before this, every query defined every view first, so all three of these
// failed on a view the statement never mentioned.
func TestSQLPanel_buildsOnlyTheViewsTheStatementNames(t *testing.T) {
	in := unbuildableInput(t)

	for _, tc := range []struct{ name, stmt string }{
		{"a statement that names no view at all", "SELECT 1"},
		{"a generator", "SELECT count(*) FROM range(10)"},
		{"one state view", "SELECT count(*) FROM state_shop_orders"},
		{"the same view twice, aliased", "SELECT a.id FROM state_shop_orders a JOIN state_shop_orders b ON a.id = b.id"},
		{"a CTE over one state view", "WITH q AS (SELECT * FROM state_shop_orders) SELECT count(*) FROM q"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runSandboxedSQL(context.Background(), in, tc.stmt, time.Now()); err != nil {
				t.Fatalf("the session built a view this statement does not name: %v", err)
			}
		})
	}

	// The other direction, so the guard above cannot pass by building nothing
	// ever: a statement that DOES name the broken events view still fails, with
	// the session-setup message.
	_, err := runSandboxedSQL(context.Background(), in, "SELECT count(*) FROM events", time.Now())
	if err == nil {
		t.Fatal("a statement naming the unbuildable events view succeeded; the fixture no longer discriminates")
	}
	if !strings.Contains(err.Error(), "views over the Parquet layout") {
		t.Fatalf("expected the view-setup failure, got: %v", err)
	}
}

// TestSQLPanel_sessionCatalogHoldsOnlyWhatWasAsked reads the catalog of a
// production-built session directly, which is the same fact the test above
// proves through failure, stated positively.
func TestSQLPanel_sessionCatalogHoldsOnlyWhatWasAsked(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput([]string{filepath.Join(archiveRoot, "bintrail_id="+id)}, baselineRoot, baselinePath)

	for _, tc := range []struct {
		name string
		only views.ViewSet
		want []string
	}{
		{"the whole layout", nil, []string{"events", "state_shop_orders"}},
		{"one view", views.ViewSet{"events": true}, []string{"events"}},
		{"no view", views.ViewSet{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, cleanup, err := openSandboxedSession(context.Background(), in, tc.only)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			// Read the catalog on the session directly, the way the sandbox
			// tests execute directly: duckdb_views() is a table function, so the
			// statement gate would (correctly) refuse it from a user.
			rows, err := db.QueryContext(context.Background(),
				"SELECT view_name FROM duckdb_views() WHERE internal = false")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var got []string
			for rows.Next() {
				var n string
				if err := rows.Scan(&n); err != nil {
					t.Fatal(err)
				}
				got = append(got, n)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("session holds views %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSQLPanel_wantedViews pins the mapping from a parsed statement to the
// views its session needs, through the real gate.
func TestSQLPanel_wantedViews(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput([]string{filepath.Join(archiveRoot, "bintrail_id="+id)}, baselineRoot, baselinePath)

	db, err := openParseSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, tc := range []struct {
		name, stmt string
		want       []string // nil entry below means "the whole catalog"
		all        bool
	}{
		{name: "no relation at all", stmt: "SELECT 1"},
		{name: "an allowlisted generator", stmt: "SELECT count(*) FROM range(10)"},
		{name: "a constant subquery", stmt: "SELECT * FROM (SELECT 1 AS a) x"},
		{name: "the events view", stmt: "SELECT * FROM events", want: []string{"events"}},
		{name: "case does not matter", stmt: "SELECT * FROM EVENTS e", want: []string{"events"}},
		{name: "a join across both", stmt: "SELECT * FROM events e JOIN state_shop_orders s ON true",
			want: []string{"events", "state_shop_orders"}},
		{name: "a WITH name is not a view", stmt: "WITH q AS (SELECT * FROM events) SELECT * FROM q",
			want: []string{"events"}},
		{name: "a WITH name that shadows nothing", stmt: "WITH q AS (SELECT 1) SELECT * FROM q"},
		{name: "a recursive WITH", want: nil,
			stmt: "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 3) SELECT * FROM t"},
		// Shapes where the relation is not where a from-clause-only reader would
		// look. Missing one of these would build too FEW views and turn a working
		// query into "table does not exist", which is the failure this walk has to
		// avoid; each of these runs green end to end.
		{name: "schema-qualified", stmt: "SELECT * FROM main.events", want: []string{"events"}},
		{name: "the bare FROM form", stmt: "FROM events SELECT schema_name", want: []string{"events"}},
		{name: "a scalar subquery in the select list", stmt: "SELECT (SELECT count(*) FROM events) AS c",
			want: []string{"events"}},
		{name: "a subquery in ORDER BY", want: []string{"events", "state_shop_orders"},
			stmt: "SELECT * FROM state_shop_orders ORDER BY (SELECT max(event_id) FROM events)"},
		{name: "a positional join", stmt: "SELECT * FROM events POSITIONAL JOIN state_shop_orders",
			want: []string{"events", "state_shop_orders"}},
		{name: "a quoted identifier", stmt: `SELECT * FROM "events"`, want: []string{"events"}},
		// An unknown relation takes the whole catalog: see the suggestion guard.
		{name: "a relation this layout does not define", stmt: "SELECT * FROM stat_shop_orders", all: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refs, err := sqlPanelGate(context.Background(), db, tc.stmt)
			if err != nil {
				t.Fatalf("gate refused a statement it should classify: %v", err)
			}
			got := wantedViews(in, refs)
			if tc.all {
				if got != nil {
					t.Fatalf("wantedViews = %v, want the whole catalog (nil)", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("wantedViews took the whole catalog, want exactly %v", tc.want)
			}
			var names []string
			for n := range got {
				names = append(names, n)
			}
			sort.Strings(names)
			if strings.Join(names, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("wantedViews = %v, want %v", names, tc.want)
			}
		})
	}
}

// TestSQLPanel_unknownRelationKeepsTheEngineSuggestion: a state view's name is
// derived (sanitized, and suffixed when two tables collide), so it is a name
// people mistype. DuckDB answers an unknown relation out of what is in the
// catalog — "Did you mean state_shop_orders?" — and an empty catalog would
// answer with a system table instead. Naming something this layout does not
// define is therefore the one case that still builds everything.
func TestSQLPanel_unknownRelationKeepsTheEngineSuggestion(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput(nil, baselineRoot, baselinePath)

	_, err := runSandboxedSQL(context.Background(), in, "SELECT * FROM stat_shop_orders", time.Now())
	var ue *sqlUserError
	if err == nil {
		t.Fatal("a statement naming a relation that does not exist succeeded")
	}
	if !errors.As(err, &ue) {
		t.Fatalf("expected a statement error, got: %v", err)
	}
	if !strings.Contains(ue.msg, "state_shop_orders") {
		t.Fatalf("the engine could not suggest the view the reader meant, so the catalog it "+
			"answered from was empty: %v", ue.msg)
	}
}

// TestSQLPanel_reportsTheWholeWait is the timing half of #1526, at the handler,
// where the whole span lives (resolving the layout runs there). `SELECT 1` used
// to report 0 ms because the clock started after the session was already built.
func TestSQLPanel_reportsTheWholeWait(t *testing.T) {
	baselineRoot, _ := writeSQLPanelBaseline(t)
	srv := newSQLPanelServer(t, baselineRoot, true)
	rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var res sqlPanelResult
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	// Opening a DuckDB session costs milliseconds and this request opens two
	// (one to parse, one to run), so a whole-request measurement cannot be zero.
	// The statement itself is a constant: it is the zero the panel used to
	// report as the elapsed time.
	if res.ElapsedMS < 1 {
		t.Fatalf("elapsed_ms = %d for a request that opened two DuckDB sessions: the clock is "+
			"still starting after the setup it should be measuring", res.ElapsedMS)
	}
	if res.QueryMS > res.ElapsedMS {
		t.Fatalf("query_ms=%d is larger than elapsed_ms=%d, so one of them is not what it says",
			res.QueryMS, res.ElapsedMS)
	}
	if !strings.Contains(string(body), `"query_ms"`) {
		t.Fatalf("the response carries no query_ms, so the reader cannot tell the setup from the "+
			"statement: %s", body)
	}
}

// TestSQLPanel_elapsedCoversSessionSetup is the same claim with the setup made
// SLOW on purpose: the baseline Parquet is served over HTTP with a fixed delay
// per request, which is the shape an S3 layout has (a listing and a footer read
// per file, over the network). The reported total must cover that; the query's
// own number must not.
func TestSQLPanel_elapsedCoversSessionSetup(t *testing.T) {
	probe, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	loadErr := duckdbutil.LoadHTTPFS(context.Background(), probe)
	probe.Close()
	if loadErr != nil {
		t.Skipf("httpfs unavailable (offline host?): %v", loadErr)
	}
	_, baselinePath := writeSQLPanelBaseline(t)
	const delay = 60 * time.Millisecond
	var reqs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		time.Sleep(delay)
		http.ServeFile(w, r, baselinePath)
	}))
	defer srv.Close()

	in := panelInput(nil, srv.URL, srv.URL+"/orders.parquet")

	start := time.Now()
	res, err := runSandboxedSQL(context.Background(), in, "SELECT count(*) FROM state_shop_orders", start)
	if err != nil {
		t.Fatalf("query the HTTP-backed state view: %v", err)
	}
	wall := time.Since(start)
	t.Logf("wall=%s elapsed_ms=%d query_ms=%d requests=%d", wall, res.ElapsedMS, res.QueryMS, reqs.Load())
	if reqs.Load() < 2 {
		t.Fatalf("only %d HTTP requests: the fixture did not put the setup behind the network", reqs.Load())
	}
	// Defining the view costs at least two of those delays before the statement
	// runs. A reported total that does not cover them is the bug.
	if min := 2 * delay.Milliseconds(); res.ElapsedMS < min {
		t.Fatalf("elapsed_ms=%d, but defining the view alone cost at least %d ms: the setup is "+
			"outside the number the panel reports", res.ElapsedMS, min)
	}
	if res.QueryMS >= res.ElapsedMS {
		t.Fatalf("query_ms=%d vs elapsed_ms=%d: the split reports no setup at all",
			res.QueryMS, res.ElapsedMS)
	}
}

// TestSQLPanel_parseSessionReadsNothing pins the posture of the session the
// gate classifies in: it is sealed from its first statement, so the ordering
// that makes lazy views possible (parse, THEN build) cannot become a window in
// which a file is reachable.
func TestSQLPanel_parseSessionReadsNothing(t *testing.T) {
	db, err := openParseSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	secret := filepath.Join(t.TempDir(), "s.csv")
	if err := os.WriteFile(secret, []byte("x\n99\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		fmt.Sprintf("SELECT * FROM read_csv('%s')", secret),
		"SELECT * FROM read_text('https://example.com/')",
		"SET enable_external_access = true",
		"SET allowed_directories = ['/']",
	} {
		if _, err := db.ExecContext(context.Background(), stmt); err == nil {
			t.Errorf("the parse session allowed %q", stmt)
		}
	}
	// What it must still do is parse.
	var out string
	if err := db.QueryRowContext(context.Background(),
		"SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", "SELECT 1").Scan(&out); err != nil {
		t.Fatalf("the sealed parse session cannot classify a statement: %v", err)
	}
	if !strings.Contains(out, "SELECT_NODE") {
		t.Fatalf("classification returned %q", out)
	}
}
