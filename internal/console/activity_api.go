package console

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// GET /api/activity — the Overview's window aggregate (#1300).
//
// Why this exists at all: the Overview used to derive "N deletes" and "N tables
// touched" client-side from a 200-event /api/events fetch. That made the tiles
// describe an arbitrary FETCH window (the newest 200 rows) while the tile beside
// them described the whole index, and it pulled ~700 kB of row images across the
// wire to produce four integers. Both halves are fixed here: the counts come
// from a grouped COUNT(*) over a stated time window, so the tile can name its
// own scope and the page no longer needs the row images.
//
// Deliberately NOT cached. This is the one page whose entire purpose is "what
// changed recently"; a cache would freeze exactly the numbers an operator is
// watching, and would leave the cost unchanged everywhere else (#1300).
//
// Deliberately NOT audited. ext.Record fires from surfaces that serve HISTORICAL
// ROW DATA; this endpoint returns counts and table names only — never a row
// image, never a PK — so it sits with /api/status and /api/coverage on the
// metadata-only side of the line ext/audit.go draws. Emitting query.run here
// would put a page load in the trail next to real data reads and dilute it.
const (
	// activityDefaultPeriod is the window the Overview asks for when it says
	// nothing else: long enough that a quiet production system still shows
	// activity, short enough to keep the scan bounded (see activityPeriods).
	activityDefaultPeriod = "24h"

	// activityTopTables caps the per-table breakdown returned to the browser.
	// The totals and the distinct-table count are computed over EVERY group —
	// only the rendered list is truncated, so "9 tables touched" is never the
	// length of this slice.
	activityTopTables = 12

	// activityMaxGroups bounds the (schema, table, event_type) groups this
	// handler will fold before it gives up on exactness. MySQL materializes the
	// grouping; the console is a shared daemon (under `bintrail-console watch`
	// it also runs capture), so an index with a pathological table count must
	// not be able to turn a page load into an unbounded Go-side map. Reaching
	// the cap sets Truncated, and the response then says the counts are a floor
	// rather than presenting them as the window's total.
	activityMaxGroups = 20000
)

// activityPeriods is the allowlist of windows this endpoint will aggregate.
//
// The ceiling is deliberate, not arbitrary. binlog_events is RANGE-partitioned
// by hour, so a bounded event_timestamp predicate prunes to the hours in the
// window — but no index leads with event_timestamp (idx_row_lookup leads with
// schema_name) and event_type is in no index at all, so the aggregate is a full
// scan of the pruned partitions plus a grouping pass. 24 h caps that at ~25
// hourly partitions. A week-long aggregate is a reporting query, not a dashboard
// tile that loads on every visit to the landing page; it does not belong here.
var activityPeriods = map[string]struct {
	dur   time.Duration
	label string
}{
	"1h":  {time.Hour, "last 1 h"},
	"6h":  {6 * time.Hour, "last 6 h"},
	"24h": {24 * time.Hour, "last 24 h"},
}

// activityTable is one table's breakdown inside the window.
type activityTable struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Insert int64  `json:"insert"`
	Update int64  `json:"update"`
	Delete int64  `json:"delete"`
	Total  int64  `json:"total"`
}

// activityResponse is the GET /api/activity body. Every count in it describes
// [since, until) and nothing else — the frontend prints Label on the tiles so
// the scope travels WITH the number instead of sitting in prose above the card.
type activityResponse struct {
	// Period is the requested window key ("24h"); Label is what a tile prints
	// ("last 24 h"). The server owns the wording so the two can never disagree.
	Period string `json:"period"`
	Label  string `json:"label"`
	Since  string `json:"since"`
	Until  string `json:"until"`

	Total   int64 `json:"total"`
	Inserts int64 `json:"inserts"`
	Updates int64 `json:"updates"`
	Deletes int64 `json:"deletes"`
	// Other is every remaining event_type (DDL, snapshot rows, ...) so
	// inserts+updates+deletes+other == total holds for any index.
	Other int64 `json:"other"`

	// Tables is the DISTINCT (schema, table) count over the whole window, not
	// the length of TopTables.
	Tables    int             `json:"tables"`
	TopTables []activityTable `json:"top_tables"`

	// Complete is false when something in the window is knowably NOT in these
	// numbers; Notes then says what. A tile must never present a narrower
	// number under a wider label — that is the bug this endpoint exists to fix,
	// so an undercount we can detect is reported, never swallowed.
	Complete bool     `json:"complete"`
	Notes    []string `json:"notes,omitempty"`
	// Truncated reports that activityMaxGroups tripped: the counts are a floor.
	Truncated bool `json:"truncated,omitempty"`
}

func (s *Server) handleActivity(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	key := strings.TrimSpace(r.URL.Query().Get("period"))
	if key == "" {
		key = activityDefaultPeriod
	}
	p, ok := activityPeriods[key]
	if !ok {
		writeJSONError(w, http.StatusBadRequest,
			"unknown period "+key+"; supported: "+strings.Join(activityPeriodKeys(), ", "))
		return
	}
	if b.db == nil {
		// An unopened bundle connection must not render as a quiet zero: "0
		// deletes in the last 24 h" reads as an assurance nobody earned.
		writeJSONError(w, http.StatusBadGateway, "the selected server's index connection is not open")
		return
	}

	until := time.Now().UTC().Truncate(time.Second)
	since := until.Add(-p.dur)

	// RBAC: the same deny rules every read on this server carries — startup
	// floor plus the request session's profile. Denied tables are excluded from
	// the SQL, so they contribute to neither the counts nor the table list (a
	// table NAME is exactly what a deny profile withholds elsewhere).
	opts, err := s.applySessionProfile(r.Context(), r, b, query.Options{
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
		ProfileActive: s.profileActive,
	})
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}

	agg, err := collectActivity(r.Context(), b.db, since, until, opts.DenyTables)
	if err != nil {
		writeFetchError(w, err)
		return
	}

	resp := agg.response(key, p.label, since, until)

	// Completeness. The counts above come from LIVE binlog_events only — by
	// design: aggregating the Parquet archives would mean the DuckDB scan this
	// endpoint exists to remove from the page. So ask the planner whether the
	// window has hours whose data lives ONLY in the archives, and say so when
	// it does. Without this, an index with rotation would answer a "last 24 h"
	// tile with the few hours still live — a silently WRONG number under an
	// explicit label, which is strictly worse than the mislabelled one this
	// issue started from.
	//
	// noArchive is false here on purpose even for a server that never READS
	// archives (--no-archive, or a profiled session): archive_state is index
	// metadata, and knowing that part of the window is archived is precisely
	// what makes the caveat honest. Passing the bundle's noArchive would
	// reclassify those hours as gaps and silence the caveat.
	resp.Complete = !resp.Truncated
	plan, perr := query.Plan(r.Context(), b.db, b.dbName, &since, &until, false)
	switch {
	case perr != nil:
		// Never assume completeness we could not check.
		slog.Warn("console: activity coverage not evaluated", "server", serverID(r), "error", perr)
		resp.Complete = false
		resp.Notes = append(resp.Notes, "Coverage for this window could not be checked, so these counts may be missing archived hours.")
	case plan != nil:
		if n := archivedHoursInWindow(plan, since, until); n > 0 {
			resp.Complete = false
			resp.Notes = append(resp.Notes, fmt.Sprintf(
				"%d hour(s) of this window have been archived to Parquet and are NOT counted here — these totals cover the live index only.", n))
		}
	}
	if resp.Truncated {
		resp.Notes = append(resp.Notes, fmt.Sprintf(
			"This index has more than %d table/event-type groups in the window; the counts below are a floor, not the total.", activityMaxGroups))
	}
	writeJSON(w, http.StatusOK, resp)
}

// activityPeriodKeys returns the allowlist in ascending duration order, for the
// 400 message. Map iteration order would make the error text non-deterministic.
func activityPeriodKeys() []string {
	keys := make([]string, 0, len(activityPeriods))
	for k := range activityPeriods {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return activityPeriods[keys[i]].dur < activityPeriods[keys[j]].dur })
	return keys
}

// activityAgg folds the grouped rows. It is separate from the SQL so the fold
// (which owns "tables touched" and the insert/update/delete split) is testable
// without a database.
type activityAgg struct {
	total, inserts, updates, deletes, other int64
	byTable                                 map[string]*activityTable
	truncated                               bool
}

func newActivityAgg() *activityAgg {
	return &activityAgg{byTable: make(map[string]*activityTable)}
}

// add folds one (schema, table, event_type) group. Returns false once
// activityMaxGroups distinct TABLES have been seen and this group would add
// another, which tells the caller to stop scanning; the aggregate is then
// marked truncated. The cap is checked only on a table the map does not yet
// hold, so a wide index is never truncated over groups that cost no memory.
func (a *activityAgg) add(schema, table string, etype uint8, n int64) bool {
	k := schema + "." + table
	if _, seen := a.byTable[k]; !seen && len(a.byTable) >= activityMaxGroups {
		a.truncated = true
		return false
	}
	a.total += n
	switch event.EventType(etype) {
	case event.EventInsert:
		a.inserts += n
	case event.EventUpdate:
		a.updates += n
	case event.EventDelete:
		a.deletes += n
	default:
		a.other += n
	}
	t := a.byTable[k]
	if t == nil {
		t = &activityTable{Schema: schema, Table: table}
		a.byTable[k] = t
	}
	t.Total += n
	switch event.EventType(etype) {
	case event.EventInsert:
		t.Insert += n
	case event.EventUpdate:
		t.Update += n
	case event.EventDelete:
		t.Delete += n
	}
	return true
}

// response renders the aggregate. TopTables is sorted by total descending with
// the table key as the tiebreaker so the panel does not reshuffle between two
// loads of the same data.
func (a *activityAgg) response(period, label string, since, until time.Time) activityResponse {
	tables := make([]activityTable, 0, len(a.byTable))
	for _, t := range a.byTable {
		tables = append(tables, *t)
	}
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Total != tables[j].Total {
			return tables[i].Total > tables[j].Total
		}
		return tables[i].Schema+"."+tables[i].Table < tables[j].Schema+"."+tables[j].Table
	})
	if len(tables) > activityTopTables {
		tables = tables[:activityTopTables]
	}
	return activityResponse{
		Period:    period,
		Label:     label,
		Since:     since.Format(consoleTSFormat),
		Until:     until.Format(consoleTSFormat),
		Total:     a.total,
		Inserts:   a.inserts,
		Updates:   a.updates,
		Deletes:   a.deletes,
		Other:     a.other,
		Tables:    len(a.byTable),
		TopTables: tables,
		Truncated: a.truncated,
	}
}

// buildActivitySQL renders the aggregate query and its arguments.
//
// The time predicate is on the BARE event_timestamp column, both bounds, and
// nothing wraps it: binlog_events is PARTITION BY RANGE (TO_SECONDS(
// event_timestamp)), and MySQL only prunes partitions when the column appears
// unwrapped in the comparison. A DATE()/TO_SECONDS() wrapper here would still
// return the right answer while silently scanning every partition in the index.
// The upper bound is not optional either: a >=-only predicate would sweep in
// clock-skewed future rows sitting in p_future and count them under a label
// that says they are inside the window.
func buildActivitySQL(since, until time.Time, deny []query.SchemaTable) (string, []any) {
	var sb strings.Builder
	sb.WriteString("SELECT schema_name, table_name, event_type, COUNT(*) AS n FROM binlog_events" +
		" WHERE event_timestamp >= ? AND event_timestamp < ?")
	args := []any{since, until}
	for _, dt := range deny {
		sb.WriteString(" AND NOT (schema_name = ? AND table_name = ?)")
		args = append(args, dt.Schema, dt.Table)
	}
	sb.WriteString(" GROUP BY schema_name, table_name, event_type")
	return sb.String(), args
}

// collectActivity runs the aggregate and folds it.
func collectActivity(ctx context.Context, db *sql.DB, since, until time.Time, deny []query.SchemaTable) (*activityAgg, error) {
	q, args := buildActivitySQL(since, until, deny)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	agg := newActivityAgg()
	for rows.Next() {
		var schema, table string
		var etype uint8
		var n int64
		if err := rows.Scan(&schema, &table, &etype, &n); err != nil {
			return nil, err
		}
		if !agg.add(schema, table, etype, n) {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return agg, nil
}

// archivedHoursInWindow counts the hours of [since, until) whose data has been
// rotated out of live MySQL but IS held in the Parquet archives — the hours the
// live aggregate cannot see.
//
// GapHours are deliberately NOT counted. A gap hour holds no retrievable data
// anywhere, so there is nothing the aggregate failed to count; folding them in
// would also cry wolf twice over — every hour older than the index itself is a
// "gap" (#1126), and so is the leading edge of an index whose future-partition
// horizon has lapsed and whose newest events are landing in p_future (where
// this aggregate DOES count them, since it filters by timestamp, not by
// partition). Data actually lost in a gap is the coverage card's continuity
// verdict, which sits directly above these tiles.
func archivedHoursInWindow(plan *query.QueryPlan, since, until time.Time) int {
	if plan == nil {
		return 0
	}
	live := make(map[time.Time]bool)
	for _, rg := range plan.MySQLRanges {
		for h := rg.Start; h.Before(rg.End); h = h.Add(time.Hour) {
			live[h] = true
		}
	}
	gap := make(map[time.Time]bool, len(plan.GapHours))
	for _, h := range plan.GapHours {
		gap[h] = true
	}
	n := 0
	// Same hour-aligned enumeration the planner used to classify (Plan
	// truncates since and rounds until up), so an hour cannot be classified
	// here that the planner never saw.
	for h := since.Truncate(time.Hour); h.Before(until.Truncate(time.Hour).Add(time.Hour)); h = h.Add(time.Hour) {
		if !live[h] && !gap[h] {
			n++
		}
	}
	return n
}
