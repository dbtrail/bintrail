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

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"go.yaml.in/yaml/v2"

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
		// The row that separates the two orderings inside wantedViews. The one
		// above cannot: `q` is not a view, so it is dropped either way and the
		// answer is {events} under both. Here the WITH name IS a view name, and
		// the walk collects names with no scope, so a ctes-first switch drops
		// the only entry there is and builds nothing — see
		// TestSQLPanel_shadowingCTEStillReadsTheView for the same statement run.
		{name: "a WITH name that shadows a view still reads the view",
			stmt: "WITH events AS (SELECT * FROM events) SELECT count(*) FROM events",
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
		// Catalog listings name no relation, so they need the whole catalog.
		{name: "a catalog listing", stmt: "SHOW TABLES", all: true},
		{name: "a catalog listing, all databases", stmt: "SHOW ALL TABLES", all: true},
		{name: "a catalog listing in a subquery", stmt: "SELECT * FROM (SHOW TABLES)", all: true},
		// The same node WITH a query keeps naming its relation.
		{name: "DESCRIBE a view", stmt: "DESCRIBE events", want: []string{"events"}},
		{name: "SUMMARIZE a view", stmt: "SUMMARIZE state_shop_orders", want: []string{"state_shop_orders"}},
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

// TestSQLPanel_shadowingCTEStillReadsTheView is the running half of the
// wantedViews row above, and what makes the defined-before-ctes ordering a
// behaviour rather than a preference: a non-recursive WITH is not in scope
// inside its own definition, so the inner `FROM events` binds to the VIEW while
// the outer one binds to the CTE. The reference walk has no scope and leaves
// ONE entry standing for both, so asking "is this a CTE?" first drops it, the
// session builds no view, and this working statement fails with "Table with
// name events does not exist".
func TestSQLPanel_shadowingCTEStillReadsTheView(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput([]string{filepath.Join(archiveRoot, "bintrail_id="+id)}, baselineRoot, baselinePath)

	// The view read plainly, so the comparison below is against what this
	// layout actually holds rather than a literal.
	base, err := runSandboxedSQL(context.Background(), in, "SELECT count(*) FROM events", time.Now())
	if err != nil {
		t.Fatalf("the plain statement over the events view: %v", err)
	}
	res, err := runSandboxedSQL(context.Background(), in,
		"WITH events AS (SELECT * FROM events) SELECT count(*) FROM events", time.Now())
	if err != nil {
		t.Fatalf("a WITH name that shadows a view: no view was built for it, so a statement "+
			"that runs answered with an error: %v", err)
	}
	if fmt.Sprint(res.Rows) != fmt.Sprint(base.Rows) {
		t.Fatalf("the shadowing statement counted %v against %v for the view itself: the "+
			"reference inside the WITH clause did not resolve to the view", res.Rows, base.Rows)
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

// requireHTTPFSEnv turns the httpfs skip below into a failure. Set on the CI
// unit-test step, asserted by TestCIRequiresHTTPFSForTheSetupBudget.
const requireHTTPFSEnv = "BINTRAIL_REQUIRE_DUCKDB_HTTPFS"

// requireDuckDBHTTPFS makes sure the embedded engine can read an http:// path,
// which is how the tests below put a Parquet footer read behind the network —
// the shape an S3 layout has, and the only way to make the panel's setup slow
// on purpose. It also PROVISIONS it: the panel's own path never calls
// LoadHTTPFS for an http:// layout (NeedsS3 answers for s3:// only), so those
// reads rely on DuckDB autoloading an extension this call has installed.
//
// The tests it guards are the only coverage the setup budget has, so a silent
// skip would let that claim disappear from a run that still exits 0. With the
// variable set — CI sets it, beside the Iceberg extension it already requires
// off the same network — a missing httpfs is a FAILURE; elsewhere it stays a
// named skip, because an offline machine genuinely cannot run them.
func requireDuckDBHTTPFS(t *testing.T) {
	t.Helper()
	probe, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	loadErr := duckdbutil.LoadHTTPFS(context.Background(), probe)
	probe.Close()
	if loadErr == nil {
		return
	}
	if os.Getenv(requireHTTPFSEnv) != "" {
		t.Fatalf("%s is set and the DuckDB httpfs extension will not load (%v): these are the "+
			"only tests that put the panel's setup behind the network, and skipping them here "+
			"leaves the setup budget with no coverage at all", requireHTTPFSEnv, loadErr)
	}
	t.Skipf("httpfs unavailable (offline host?): %v — set %s=1 to make this a failure",
		loadErr, requireHTTPFSEnv)
}

// TestCIRequiresHTTPFSForTheSetupBudget: the variable above only means
// something if CI sets it, and a variable nobody wires in enables nothing. It
// has to be on the step that RUNS the unit tests, not merely somewhere in the
// file.
//
// EVERY such step, not one of them. The first version accumulated with an OR
// ("some matching step sets it"), which is green the moment one does — so a
// second `go test ./...` step added later without the variable would leave half
// the unit surface skipping in silence while this guard still passed. Only one
// step matches today, so that shape does not exist yet; the accumulator is
// inverted anyway, because the version that would go blind and the version that
// would not cost the same to write, and only one of them has to be remembered.
func TestCIRequiresHTTPFSForTheSetupBudget(t *testing.T) {
	const path = "../../.github/workflows/ci.yml"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc struct {
		Jobs map[string]struct {
			Steps []struct {
				Run string            `yaml:"run"`
				Env map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var ran int
	var unguarded []string
	for name, job := range doc.Jobs {
		for _, step := range job.Steps {
			// The step that runs the whole unit suite, found by what it RUNS
			// rather than by its name: a rename must not quietly empty this.
			if !strings.Contains(step.Run, "go test ./...") {
				continue
			}
			ran++
			if step.Env[requireHTTPFSEnv] == "" {
				unguarded = append(unguarded, name)
			}
		}
	}
	if ran == 0 {
		t.Fatalf("no step in %s runs `go test ./...`; this guard covers nothing", path)
	}
	if len(unguarded) > 0 {
		t.Errorf("%d of %d steps running the unit suite in %s do not set %s (jobs: %s), so the "+
			"setup-budget tests can skip in CI and leave that bound with no coverage there",
			len(unguarded), ran, path, requireHTTPFSEnv, strings.Join(unguarded, ", "))
	}
}

// TestSQLPanel_elapsedCoversSessionSetup is the same claim with the setup made
// SLOW on purpose: the baseline Parquet is served over HTTP with a fixed delay
// per request, which is the shape an S3 layout has (a listing and a footer read
// per file, over the network). The reported total must cover that; the query's
// own number must not.
func TestSQLPanel_elapsedCoversSessionSetup(t *testing.T) {
	requireDuckDBHTTPFS(t)
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

// TestSQLPanel_catalogListingListsTheViews is the discovery path, and the one
// place a lazily built catalog could answer with silence instead of an error:
// `SHOW TABLES` names no relation, so a session built from "what this statement
// names" would hold nothing and the listing would come back empty and
// successful. It is also how an operator learns the state_* names, which are
// derived from the table names and suffixed when two of them collide, so this
// is not a corner: it is the first thing to type into an empty query box.
func TestSQLPanel_catalogListingListsTheViews(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput([]string{filepath.Join(archiveRoot, "bintrail_id="+id)}, baselineRoot, baselinePath)

	for _, stmt := range []string{"SHOW TABLES", "SHOW ALL TABLES", "SELECT * FROM (SHOW TABLES)"} {
		t.Run(stmt, func(t *testing.T) {
			res, err := runSandboxedSQL(context.Background(), in, stmt, time.Now())
			if err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
			listed := fmt.Sprint(res.Rows)
			for _, want := range []string{"events", "state_shop_orders"} {
				if !strings.Contains(listed, want) {
					t.Fatalf("%s returned %d rows and did not list %q: a catalog listing answered "+
						"out of a catalog built for someone else's statement, with no error to read. Got %s",
						stmt, res.RowCount, want, listed)
				}
			}
		})
	}
}

// TestSQLPanel_refusalCarriesTheWait is the timing claim on the path that takes
// the LONGEST: a mistyped relation name. That statement cannot be answered
// selectively (the engine's "Did you mean" needs the whole catalog), so it pays
// for every view in the layout and then fails. If the refusal carries no
// elapsed_ms, the panel blanks its status line and the operator is told nothing
// about the longest wait it has.
func TestSQLPanel_refusalCarriesTheWait(t *testing.T) {
	baselineRoot, _ := writeSQLPanelBaseline(t)
	srv := newSQLPanelServer(t, baselineRoot, true)
	rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT * FROM state_shop_order"}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var res struct {
		Error     string `json:"error"`
		ElapsedMS *int64 `json:"elapsed_ms"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	if res.Error == "" {
		t.Fatalf("a refusal with no message: %s", body)
	}
	if res.ElapsedMS == nil {
		t.Fatalf("the refusal carries no elapsed_ms, so the panel can only blank its status "+
			"line after the slowest path it has: %s", body)
	}
}

// TestSQLPanel_notesDescribeOnlyWhatTheSessionBuilt: a warning has to be about
// the session that answered. `SELECT 1` builds no view and reads no baseline
// file, so a note about baseline column types is describing files this query
// never opened.
func TestSQLPanel_notesDescribeOnlyWhatTheSessionBuilt(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput([]string{filepath.Join(archiveRoot, "bintrail_id="+id)}, baselineRoot, baselinePath)
	if in.Baselines[0].SchemaKnown {
		t.Fatal("the fixture's baseline now carries column types, so the decimal note cannot fire at all")
	}

	res, err := runSandboxedSQL(context.Background(), in, "SELECT 1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("`SELECT 1` built no view and read no baseline file, yet the answer carries %v",
			res.Warnings)
	}
	// The same layout, a statement that DOES read the baseline: the note is the
	// point of this fixture, so it must still be there.
	res, err = runSandboxedSQL(context.Background(), in, "SELECT count(*) FROM state_shop_orders", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "no column types") {
		t.Fatalf("a query that reads the untyped baseline lost its note: %v", res.Warnings)
	}
}

// TestSQLPanel_registryNoteIsAboutTheSessionThatAnswered: the note says this
// session was built over half a layout. That is a true and useful thing to say
// on an answer that READ that layout, which is the contract
// TestSQLPanel_registryReadFailure pins. It is not something to say about
// `SELECT 1`, which opened nothing and is complete on its own terms.
func TestSQLPanel_registryNoteIsAboutTheSessionThatAnswered(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput([]string{"/nonexistent"}, baselineRoot, baselinePath)
	in.ArchiveDiscoveryFailed = true

	res, err := runSandboxedSQL(context.Background(), in, "SELECT 1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("`SELECT 1` built no view, yet its answer explains what a catalog it never "+
			"opened is missing: %v", res.Warnings)
	}
	for _, stmt := range []string{"SELECT count(*) FROM state_shop_orders", "SHOW TABLES"} {
		res, err := runSandboxedSQL(context.Background(), in, stmt, time.Now())
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		var found bool
		for _, w := range res.Warnings {
			if strings.Contains(w, "archive registry") {
				found = true
			}
		}
		if !found {
			t.Errorf("%s was served out of half a layout and never said so: %v", stmt, res.Warnings)
		}
	}
}

// TestSQLPanel_setupRunsUnderTheSetupBudget: the view build reads a Parquet
// footer per view, on an S3 layout that is a network read, and it runs under
// the single-flight latch — it used to run under no deadline at all, so one
// hung read would answer every other reader with 429 for as long as it lasted.
//
// The claim is deliberately narrow, because the wider one is not true: a
// cancelled context does NOT interrupt an httpfs read already in flight
// (measured against the pinned engine, twice, watching the request count).
// What the budget does is stop the setup from starting the next read. Spent
// before the first one is the shape that pins that down: with the budget
// already gone, nothing is read at all.
//
// On its own this case proves LESS than it reads: a budget of one nanosecond
// is already gone when the PARSE session runs its first statement, so the run
// ends before the view build is reached and the read count is zero whichever
// context that build receives. TestSQLPanel_setupBudgetExpiresInsideTheViewBuild
// below is the case that reaches the build; this one is kept because "an
// expired budget reads nothing at all" is a separate fact about the setup.
func TestSQLPanel_setupRunsUnderTheSetupBudget(t *testing.T) {
	requireDuckDBHTTPFS(t)
	_, baselinePath := writeSQLPanelBaseline(t)
	var reqs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		http.ServeFile(w, r, baselinePath)
	}))
	defer srv.Close()

	defer func(old time.Duration) { sqlPanelSetupTimeout = old }(sqlPanelSetupTimeout)
	sqlPanelSetupTimeout = time.Nanosecond

	in := panelInput(nil, srv.URL, srv.URL+"/orders.parquet")
	if _, err := runSandboxedSQL(context.Background(), in,
		"SELECT count(*) FROM state_shop_orders", time.Now()); err == nil {
		t.Fatal("the setup ran to completion on a budget that was already spent")
	}
	if n := reqs.Load(); n != 0 {
		t.Fatalf("the setup made %d network reads after its budget was gone: the deadline does "+
			"not reach the view build", n)
	}
}

// TestSQLPanel_setupBudgetExpiresInsideTheViewBuild is the half the case above
// cannot make: here the budget SURVIVES the parse and runs out DURING the view
// build, which is the step it exists to bound. Neither the parse session nor
// the gate touches the network, so this deadline can only be reached by the
// build itself — and the discriminator is the error TEXT, since "set up views
// over the Parquet layout" is the only step that reports it. The read count
// backs that up from both sides: this run reached the network at all (the
// nanosecond case reaches nothing), and it stopped short of a whole build.
//
// What it deliberately does NOT assert is that the abort is FAST. An httpfs
// read already in flight is not interrupted by an expired context (measured:
// the run takes the whole stall), so what the budget buys is that the setup
// does not start the NEXT read. A wall-clock assertion here would be asserting
// something the engine does not do.
func TestSQLPanel_setupBudgetExpiresInsideTheViewBuild(t *testing.T) {
	requireDuckDBHTTPFS(t)
	_, baselinePath := writeSQLPanelBaseline(t)

	// The budget has to outlast the parse (openParseSession plus the gate:
	// ~5-20 ms, no network), and the stall has to outlast the budget, so the
	// deadline can land nowhere but inside the build.
	const budget = 1 * time.Second
	const stallFor = 3 * time.Second

	var reqs atomic.Int64
	var stall atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Counted on ARRIVAL, so the stalled read counts while it is still in
		// flight — that is the read the budget expires underneath.
		reqs.Add(1)
		if stall.CompareAndSwap(true, false) {
			time.Sleep(stallFor)
		}
		http.ServeFile(w, r, baselinePath)
	}))
	defer srv.Close()

	in := panelInput(nil, srv.URL, srv.URL+"/orders.parquet")
	const stmt = "SELECT count(*) FROM state_shop_orders"

	// A control run first, on the real budget with nothing stalled: it measures
	// what a whole build-and-query costs in reads over THIS fixture, so the
	// budgeted run below is compared against a measured number rather than a
	// literal a fixture change would quietly invalidate. It is also the
	// positive control — without it, "the query failed" could mean the layout
	// was never queryable.
	if _, err := runSandboxedSQL(context.Background(), in, stmt, time.Now()); err != nil {
		t.Fatalf("control run over the HTTP-backed layout: %v", err)
	}
	full := reqs.Load()
	if full < 2 {
		t.Fatalf("the control run made %d HTTP reads: the fixture no longer puts the setup "+
			"behind the network", full)
	}

	defer func(old time.Duration) { sqlPanelSetupTimeout = old }(sqlPanelSetupTimeout)
	sqlPanelSetupTimeout = budget
	reqs.Store(0)
	stall.Store(true)

	_, err := runSandboxedSQL(context.Background(), in, stmt, time.Now())
	if err == nil {
		t.Fatal("the view build ran to completion after its budget was spent")
	}
	if !strings.Contains(err.Error(), "views over the Parquet layout") {
		t.Fatalf("the budget was spent somewhere other than the view build: %v", err)
	}
	switch n := reqs.Load(); {
	case n == 0:
		t.Fatalf("the budget was gone before the build made a single read, so this is the "+
			"nanosecond case again and not a new one: the parse did not fit inside %s", budget)
	case n >= full:
		t.Fatalf("the build made %d reads against %d for a whole build-and-query: the deadline "+
			"did not stop it", n, full)
	}
}

// TestSQLPanel_layoutFailureCarriesTheWait: the step that resolves the layout
// is the one that LISTS an S3 baseline root, so a fault reported by it is the
// long wait that reported nothing at all, which is where #1526 starts. Same
// claim as TestSQLPanel_refusalCarriesTheWait, one step earlier in the request.
func TestSQLPanel_layoutFailureCarriesTheWait(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnError(
		&mysql.MySQLError{Number: 1142, Message: "SELECT command denied"})

	// No baseline: the unreadable registry is then the whole layout, which is
	// an upstream fault (502) rather than an empty server (404).
	srv := newSQLPanelServer(t, "", true)
	srv.cm.boot.db = db

	rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code=%d body=%s; want 502", rec.Code, body)
	}
	var res struct {
		Error     string `json:"error"`
		ElapsedMS *int64 `json:"elapsed_ms"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	if res.Error == "" {
		t.Fatalf("a 502 with no message: %s", body)
	}
	if res.ElapsedMS == nil {
		t.Fatalf("the layout failed after listing the layout and the answer carries no "+
			"elapsed_ms: the one wait #1526 is about reports nothing: %s", body)
	}
}
