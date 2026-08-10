package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// aRow is one grouped row of the aggregate SELECT: (schema, table,
// event_type, count). event_type is the numeric TINYINT the column stores.
type aRow struct {
	schema, table string
	etype         uint8
	n             int64
}

// activityResult builds the grouped result set the aggregate SELECT returns.
func activityResult(rows ...aRow) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"schema_name", "table_name", "event_type", "n"})
	for _, row := range rows {
		r.AddRow(row.schema, row.table, row.etype, row.n)
	}
	return r
}

func getActivity(t *testing.T, srv *Server, path string) (int, activityResponse, string) {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", path, "")
	var got activityResponse
	if rec.Code == 200 {
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", body, err)
		}
	}
	return rec.Code, got, string(body)
}

// TestActivityAPICounts pins the whole point of the endpoint (#1300): the tiles'
// numbers describe a STATED period, are exact (not a fetch-window artefact), and
// carry the label the tile prints. It also pins that Tables is the distinct
// table count over the WHOLE window, not the length of the truncated top list.
func TestActivityAPICounts(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).WillReturnRows(activityResult(
		aRow{"shop", "orders", 1, 10}, // INSERT
		aRow{"shop", "orders", 2, 5},  // UPDATE
		aRow{"shop", "orders", 3, 2},  // DELETE
		aRow{"shop", "users", 3, 7},   // DELETE
		aRow{"shop", "audit", 4, 3},   // DDL → Other
	))
	srv := newBootServer(db)

	code, got, body := getActivity(t, srv, "/api/activity")
	if code != 200 {
		t.Fatalf("code = %d, body = %s", code, body)
	}
	if got.Period != "24h" || got.Label != "last 24 h" {
		t.Errorf("period/label = %q/%q, want 24h/last 24 h", got.Period, got.Label)
	}
	if got.Total != 27 || got.Inserts != 10 || got.Updates != 5 || got.Deletes != 9 || got.Other != 3 {
		t.Errorf("counts = total %d ins %d upd %d del %d other %d; want 27/10/5/9/3",
			got.Total, got.Inserts, got.Updates, got.Deletes, got.Other)
	}
	if got.Tables != 3 {
		t.Errorf("tables = %d, want 3 distinct tables in the window", got.Tables)
	}
	if len(got.TopTables) != 3 || got.TopTables[0].Table != "orders" || got.TopTables[0].Total != 17 {
		t.Errorf("top_tables = %+v, want orders first with 17", got.TopTables)
	}
	if got.TopTables[0].Delete != 2 {
		t.Errorf("orders delete = %d, want 2", got.TopTables[0].Delete)
	}
	// The window must be exactly the period wide, so the label is not a claim
	// about a different span than the counts.
	since, err := time.Parse(consoleTSFormat, got.Since)
	if err != nil {
		t.Fatal(err)
	}
	until, err := time.Parse(consoleTSFormat, got.Until)
	if err != nil {
		t.Fatal(err)
	}
	if d := until.Sub(since); d != 24*time.Hour {
		t.Errorf("until-since = %s, want 24h", d)
	}
	if !got.Complete {
		t.Errorf("complete = false with no coverage finding; notes = %v", got.Notes)
	}
}

// TestActivityAPIPeriods pins the allowlist: a supported key sets both the
// window width and the label, an unsupported one is a 400 that names the
// options rather than silently falling back to a period the tile then mislabels.
func TestActivityAPIPeriods(t *testing.T) {
	for _, tc := range []struct {
		key, label string
		width      time.Duration
	}{
		{"1h", "last 1 h", time.Hour},
		{"6h", "last 6 h", 6 * time.Hour},
		{"24h", "last 24 h", 24 * time.Hour},
	} {
		t.Run(tc.key, func(t *testing.T) {
			db, mock, closeDB := newSQLMock(t)
			defer closeDB()
			mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).WillReturnRows(activityResult())
			code, got, body := getActivity(t, newBootServer(db), "/api/activity?period="+tc.key)
			if code != 200 {
				t.Fatalf("code = %d, body = %s", code, body)
			}
			if got.Label != tc.label {
				t.Errorf("label = %q, want %q", got.Label, tc.label)
			}
			since, _ := time.Parse(consoleTSFormat, got.Since)
			until, _ := time.Parse(consoleTSFormat, got.Until)
			if d := until.Sub(since); d != tc.width {
				t.Errorf("window = %s, want %s", d, tc.width)
			}
		})
	}

	t.Run("unsupported", func(t *testing.T) {
		db, _, closeDB := newSQLMock(t)
		defer closeDB()
		code, _, body := getActivity(t, newBootServer(db), "/api/activity?period=30d")
		if code != 400 {
			t.Fatalf("code = %d, want 400; body = %s", code, body)
		}
		if !strings.Contains(body, "1h") || !strings.Contains(body, "24h") {
			t.Errorf("400 body must name the supported periods, got %s", body)
		}
	})
}

// TestActivitySQLPrunesPartitions pins the two properties the aggregate's cost
// and correctness both rest on: the time predicate is BOUNDED ON BOTH SIDES and
// compares the BARE event_timestamp column. Wrapping it (DATE(), TO_SECONDS())
// still returns right answers while scanning every partition in the index, and
// dropping the upper bound sweeps clock-skewed p_future rows into a window that
// says it excludes them.
func TestActivitySQLPrunesPartitions(t *testing.T) {
	since := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	until := since.Add(24 * time.Hour)
	q, args := buildActivitySQL(since, until, nil)

	if !strings.Contains(q, "event_timestamp >= ?") {
		t.Errorf("missing bare-column lower bound in %q", q)
	}
	if !strings.Contains(q, "event_timestamp < ?") {
		t.Errorf("missing bare-column upper bound in %q", q)
	}
	for _, wrapper := range []string{"TO_SECONDS(event_timestamp", "DATE(event_timestamp", "CAST(event_timestamp"} {
		if strings.Contains(q, wrapper) {
			t.Errorf("time predicate is wrapped in %s — partition pruning is lost: %q", wrapper, q)
		}
	}
	if !strings.Contains(q, "GROUP BY schema_name, table_name, event_type") {
		t.Errorf("missing grouping in %q", q)
	}
	if len(args) != 2 || args[0] != any(since) || args[1] != any(until) {
		t.Errorf("args = %v, want [since until]", args)
	}
}

// TestActivitySQLExcludesDeniedTables pins RBAC: a denied table contributes to
// neither the counts nor the table list. A table NAME is exactly what a deny
// profile withholds elsewhere, so leaking it through an aggregate would be a
// redaction bypass with extra steps.
func TestActivitySQLExcludesDeniedTables(t *testing.T) {
	since := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	deny := []query.SchemaTable{{Schema: "shop", Table: "secrets"}, {Schema: "hr", Table: "salaries"}}
	q, args := buildActivitySQL(since, since.Add(time.Hour), deny)

	if n := strings.Count(q, "NOT (schema_name = ? AND table_name = ?)"); n != 2 {
		t.Errorf("deny clauses = %d, want 2 in %q", n, q)
	}
	want := []any{since, since.Add(time.Hour), "shop", "secrets", "hr", "salaries"}
	if len(args) != len(want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("args[%d] = %v, want %v", i, args[i], want[i])
		}
	}
}

// TestActivityDenyTablesReachTheQuery pins the WIRING, not just the builder:
// the startup profile's deny list must arrive in the SQL the handler runs.
// Deleting the DenyTables plumbing in handleActivity leaves the builder test
// above green and only fails here.
func TestActivityDenyTablesReachTheQuery(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`NOT \(schema_name = \? AND table_name = \?\)`).
		WillReturnRows(activityResult())
	srv := newBootServer(db)
	srv.denyTables = []query.SchemaTable{{Schema: "shop", Table: "secrets"}}
	srv.profileActive = true

	if code, _, body := getActivity(t, srv, "/api/activity"); code != 200 {
		t.Fatalf("code = %d, body = %s", code, body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the deny clause never reached the query: %v", err)
	}
}

// TestActivityRequiresQueryPermission pins the TIER, not merely that the route
// is classified (which the completeness tests already do). /api/activity
// aggregates the indexed row data and reports which tables changed and how
// often; the fact that its handler must apply the profile's DenyTables to be
// safe is what disqualifies it from the status:read floor. Without this, a
// future edit can slide it down a tier and nothing notices.
func TestActivityRequiresQueryPermission(t *testing.T) {
	perm, classified := permForRoute("GET", "/api/activity")
	if !classified {
		t.Fatal("/api/activity is not classified in apiRoutePerms")
	}
	if perm != ext.PermQueryExecute {
		t.Errorf("permission = %q, want %q — activity reads indexed row data, it is not a health read",
			perm, ext.PermQueryExecute)
	}
}

// TestActivityAggFold pins the fold: the type split, the Other bucket for
// non-DML event types, distinct-table counting independent of the rendered
// list, and a deterministic order for the panel.
func TestActivityAggFold(t *testing.T) {
	a := newActivityAgg()
	// Two tables with an identical total: the tie must break on the name, or
	// the panel reshuffles between two loads of the same data.
	a.add("db", "bbb", 1, 4)
	a.add("db", "aaa", 1, 4)
	a.add("db", "big", 2, 9)
	a.add("db", "big", 5, 1) // EventGTID → Other, but still this table's activity
	resp := a.response("1h", "last 1 h", time.Now(), time.Now())

	if resp.Total != 18 || resp.Inserts != 8 || resp.Updates != 9 || resp.Deletes != 0 || resp.Other != 1 {
		t.Errorf("fold = total %d ins %d upd %d del %d other %d", resp.Total, resp.Inserts, resp.Updates, resp.Deletes, resp.Other)
	}
	if resp.Tables != 3 {
		t.Errorf("tables = %d, want 3", resp.Tables)
	}
	got := []string{resp.TopTables[0].Table, resp.TopTables[1].Table, resp.TopTables[2].Table}
	want := []string{"big", "aaa", "bbb"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("top_tables order = %v, want %v", got, want)
		}
	}
	// Other-typed events count toward the table's total but into no DML column.
	if resp.TopTables[0].Total != 10 || resp.TopTables[0].Update != 9 {
		t.Errorf("big = %+v, want total 10 / update 9", resp.TopTables[0])
	}
}

// TestActivityAggCapsRenderedTables pins that only the RENDERED list is capped:
// the distinct-table tile must keep reporting the real number, or "tables
// touched" silently becomes "tables shown".
func TestActivityAggCapsRenderedTables(t *testing.T) {
	a := newActivityAgg()
	for i := 0; i < activityTopTables+5; i++ {
		a.add("db", string(rune('a'+i)), 1, int64(i+1))
	}
	resp := a.response("1h", "last 1 h", time.Now(), time.Now())
	if len(resp.TopTables) != activityTopTables {
		t.Errorf("top_tables = %d, want %d", len(resp.TopTables), activityTopTables)
	}
	if resp.Tables != activityTopTables+5 {
		t.Errorf("tables = %d, want %d", resp.Tables, activityTopTables+5)
	}
}

// TestArchivedHoursInWindow pins the completeness rule. The aggregate reads
// LIVE binlog_events only, so an hour that has rotated into Parquet is an hour
// its counts cannot see — that is the number the caveat is built on. Gap hours
// are NOT it: nothing retrievable lives there, and counting them would cry wolf
// on every index younger than the window and on every lapsed future-partition
// horizon.
func TestArchivedHoursInWindow(t *testing.T) {
	h := func(n int) time.Time { return time.Date(2026, 8, 9, n, 0, 0, 0, time.UTC) }
	since, until := h(10), h(13) // window hours: 10, 11, 12, 13

	cases := []struct {
		name string
		plan *query.QueryPlan
		want int
	}{
		{"all live", &query.QueryPlan{MySQLRanges: []query.TimeRange{{Start: h(10), End: h(14)}}}, 0},
		{"two hours archived", &query.QueryPlan{MySQLRanges: []query.TimeRange{{Start: h(12), End: h(14)}}}, 2},
		{"gap hours are not undercounts", &query.QueryPlan{
			MySQLRanges: []query.TimeRange{{Start: h(12), End: h(14)}},
			GapHours:    []time.Time{h(10), h(11)},
		}, 0},
		{"nil plan claims nothing", nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := archivedHoursInWindow(tc.plan, since, until); got != tc.want {
				t.Errorf("archivedHoursInWindow = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestActivityArchivedWindowIsNotComplete pins the WIRING of the rule above
// through the handler: with part of the window rotated into Parquet, the
// response must say so. Without it the tile answers a "last 24 h" label with
// only the hours still live — a wrong number under an explicit period, which is
// strictly worse than the mislabelled one #1300 started from.
func TestActivityArchivedWindowIsNotComplete(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	now := time.Now().UTC()
	prev := now.Add(-time.Hour)

	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).WillReturnRows(activityResult())
	// Only the current hour has a live partition; the previous hour is in
	// archive_state, so it is archived-away, not a gap.
	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(now.Format("p_2006010215")))
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"partition_name", "min_event_ts", "max_event_ts"}).
			AddRow(prev.Format("p_2006010215"), nil, nil))

	srv := newBootServer(db)
	// The planner is dbName-gated; a bundle without one makes Plan a no-op.
	srv.cm.boot.dbName = "binlog_index"

	code, got, body := getActivity(t, srv, "/api/activity?period=1h")
	if code != 200 {
		t.Fatalf("code = %d, body = %s", code, body)
	}
	if got.Complete {
		t.Fatalf("complete = true with an archived hour inside the window; notes = %v", got.Notes)
	}
	if len(got.Notes) == 0 || !strings.Contains(got.Notes[0], "archived") {
		t.Errorf("notes must name the archived hours, got %v", got.Notes)
	}
}

// TestActivityUnopenedIndexIsNotZero: a server whose index connection never
// opened must fail loudly. "0 deletes in the last 24 h" is an assurance, and
// nobody measured it.
func TestActivityUnopenedIndexIsNotZero(t *testing.T) {
	srv := &Server{token: "t", cm: newConnManager(nil, false)}
	srv.cm.boot = &bundle{db: nil, noArchive: true}
	srv.mux = srv.buildHandler()

	code, _, body := getActivity(t, srv, "/api/activity")
	if code == 200 {
		t.Fatalf("code = 200 with no index connection; body = %s", body)
	}
}
