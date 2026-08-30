package console

// The server-side SQL panel (#1177): free-form DuckDB SQL over the selected
// server's archive/baseline Parquet, executed by the console daemon inside a
// locked-down DuckDB session. The security posture is layered, and every layer
// is enforced here from the first commit — none are follow-ups:
//
//  1. Filesystem sandbox: `SET enable_external_access = false` is the
//     load-bearing ban — it is what denies every file and URL read. Neither
//     allowed_directories NOR lock_configuration restricts anything on its own
//     (verified: with external access at its default, an out-of-root read
//     succeeds even with both set). allowed_directories is only a CARVE-OUT
//     from the ban, scoping the exception to the resolved archive/baseline
//     roots — local paths AND s3:// prefixes (an out-of-root s3:// read is
//     denied before any network request is made; an in-root one reaches S3).
//     DO NOT remove the enable_external_access line in openSandboxedSession:
//     without it the sandbox opens to the entire filesystem and all of S3, and
//     the local in-root tests stay green. lock_configuration then freezes the
//     whole config so no user SET can widen it.
//  2. Read-only: DuckDB's allowed_directories carve-out permits WRITES inside
//     the roots (COPY TO, ATTACH of a writable database), so read-only is
//     enforced by a SELECT-only statement gate — classified by DuckDB's own
//     parser (json_serialize_sql), never by string matching. Since #1526 the
//     gate runs on its OWN session, sealed from its first statement, before the
//     query session exists: the parsed statement is also what names the views
//     that session has to build. Nothing the user typed executes there, and
//     cannot — see openParseSession.
//  3. Resource bounds: the conservative long-lived-daemon DuckDB budget
//     (duckdbutil.DefaultTuning — never ultrafast), a private spill directory,
//     a hard query timeout with interrupt, a result row/byte cap, and a single
//     query in flight per process.
//  4. RBAC: refused outright when a data profile is active — free-form SQL
//     cannot honor per-column redaction (same gate shape as reconstruct).
//  5. Audit: every statement that reaches the engine emits on the audit seam
//     (console/sql.run) with its outcome (ok/refused/error) — including one the
//     gate refuses. Only a request the client aborted mid-flight is silent
//     (there is no one to answer, and it is not a policy event).
//  6. Opt-in: off by default behind BINTRAIL_CONSOLE_SQL_PANEL=1.
//
// Cancellation is the HTTP request's own lifetime: the browser aborts the
// fetch, r.Context() dies, and the DuckDB query is interrupted through
// QueryContext. No cancel endpoint, no query registry.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	// The panel opens its own DuckDB handles; do not rely on a transitive
	// import to have registered the driver.
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/views"
)

const (
	// sqlPanelMaxRows matches the events API's hard cap: the panel is an
	// interactive surface, not an export path (views.sql is the unbounded one).
	sqlPanelMaxRows = eventsMaxLimit
	// sqlPanelMaxBytes bounds the response payload: a SELECT over row images
	// can carry megabytes per cell, and an unbounded encode would hold it all
	// in the daemon's heap.
	sqlPanelMaxBytes = 8 << 20
	// sqlPanelMaxStatementBytes bounds the request body (and therefore the
	// statement recorded on the audit seam).
	sqlPanelMaxStatementBytes = 64 << 10
)

// sqlPanelTimeout is the hard per-query wall-clock budget. A var so tests can
// shrink it to exercise the interrupt without a 60-second test.
var sqlPanelTimeout = 60 * time.Second

// sqlPanelSetupTimeout bounds the pre-query setup, in both places it happens:
// the S3 baseline LIST in buildViewsInput, and the view build in
// runSandboxedSQL, which reads a Parquet footer per view. Both run under the
// single-flight latch, so a hung read must not pin the panel at 429 for every
// other user. The query itself is bounded separately by sqlPanelTimeout.
//
// It stops the NEXT read, not the current one: cancelling a context does not
// interrupt an httpfs read already in flight (measured against the pinned
// engine; DuckDB bounds that one itself, with http_timeout and its retries).
// So the setup gives up at the first statement boundary after the budget runs
// out rather than at the instant it does.
var sqlPanelSetupTimeout = 30 * time.Second

// forensicsEventColumns are the paid-tier forensics columns the console must
// NOT serve as row data: the SAME set eventDTO omits (connection_id is the free
// query_explorer/paid forensics line; query_text/query_hash ride with it). The
// panel drops them from its `events` view so a SELECT cannot reach them — the
// open-core boundary that the views.sql DOWNLOAD sidesteps by never executing
// (it hands the operator a schema over their own files), but which the panel
// re-crosses by executing server-side.
var forensicsEventColumns = []string{"connection_id", "query_text", "query_hash"}

// allowedTableFunctions is the ALLOWLIST of table functions a panel query may
// name in a FROM clause. It is an allowlist, not a denylist, on purpose: DuckDB
// has too many ways to read a file or re-enter the parser to enumerate safely,
// and a denylist fails OPEN on every name it hasn't heard of — a DuckDB bump or
// a loaded extension silently reopens the hole. This fails CLOSED.
//
// The panel's real data path is the pre-built `events`/`state_*` VIEWS, which
// resolve to BASE_TABLE references at bind time (after this gate), NOT table
// functions — so nothing here is needed to read them. This list only adds a few
// pure in-memory generators (no file, no network, no SQL string argument) so
// simple scaffolding like `FROM range(n)` works.
//
// Everything absent is refused, which is the whole point. In particular this
// blocks: the raw file readers (read_parquet/read_csv/… — reading the raw
// archive Parquet would serve the paid forensics columns the events view
// withholds, and is an arbitrary in-root file-read primitive); the dynamic-SQL
// re-entry functions (query/query_table/json_execute_serialized_sql, which take
// a SQL/path STRING that DuckDB re-parses at bind time, AFTER this gate — the
// bypass that makes a denylist futile, #1177 review); and the secrets manager
// (duckdb_secrets/which_secret, which expose an S3 secret's access-key id).
//
// Scalar functions (repeat, make_timestamp, json_extract, …) are unaffected:
// they appear as FUNCTION nodes in expressions, never as a from-clause
// TABLE_FUNCTION, and none in DuckDB reads a file or executes SQL.
var allowedTableFunctions = map[string]bool{
	"range":           true,
	"generate_series": true,
}

// sqlUserError is a problem with the submitted statement (refused by the
// SELECT-only gate, rejected by DuckDB's parser, failed at execution, or timed
// out) — a 422 with the engine's message, never a server fault.
type sqlUserError struct{ msg string }

func (e *sqlUserError) Error() string { return e.msg }

type sqlPanelRequest struct {
	SQL string `json:"sql"`
}

type sqlPanelResult struct {
	Columns  []string `json:"columns"`
	Rows     [][]any  `json:"rows"`
	RowCount int      `json:"row_count"`
	// Truncated is set when the row cap or the response byte budget cut the
	// result short — never silently.
	Truncated bool `json:"truncated"`
	// ElapsedMS is the whole span the caller waited on, and QueryMS is the part
	// of it the statement itself took (#1526).
	//
	// The split is the point. Before it, the panel reported only the statement
	// and called it the elapsed time, so `SELECT 1` over an S3 layout said 0 ms
	// after 16 seconds of session setup — a number that sent the reader looking
	// at their query for a cost that was never in it. Two numbers say which half
	// to look at: a large total with a small query is the layout (listing
	// objects, reading Parquet footers), and a large query is the query.
	ElapsedMS int64 `json:"elapsed_ms"`
	QueryMS   int64 `json:"query_ms"`
	// Warnings carry what the session is missing and why (#1456): a query
	// that succeeded against half a layout must say so next to its rows.
	Warnings []string `json:"warnings,omitempty"`
}

// sqlPanelRegistryNote is the panel's wording for a session built without the
// events view because archive_state could not be read. The error text stays
// in the console log: it names the index host and the DB user.
const sqlPanelRegistryNote = "the archive registry (archive_state) could not be read, so this session has no events view; the console log has the error"

// handleSQLPanel serves POST /api/sql.
func (s *Server) handleSQLPanel(w http.ResponseWriter, r *http.Request) {
	// The clock the operator is on. Everything after this point is time they
	// wait for — resolving the layout (which LISTS an S3 baseline root), opening
	// the session, building the views the statement needs, and the statement —
	// so the number the panel reports is measured from here, not from the moment
	// the engine finally got the statement.
	reqStart := time.Now()
	if !s.sqlPanel {
		writeJSONError(w, http.StatusForbidden,
			"the SQL panel is not enabled; start the console with BINTRAIL_CONSOLE_SQL_PANEL=1")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, sqlPanelMaxStatementBytes)
	var req sqlPanelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSONError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("statement too large (max %d KiB)", sqlPanelMaxStatementBytes/1024))
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.SQL) == "" {
		writeJSONError(w, http.StatusBadRequest, "missing \"sql\"")
		return
	}
	// Before resolving a bundle, like recover-cascade: a profiled session is
	// refused regardless of which server it selected. Free-form SQL reads the
	// unredacted Parquet directly, so it cannot honor per-column redaction.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "sql")
		writeJSONError(w, http.StatusForbidden,
			"the SQL panel is unavailable while an access-control profile is active: "+
				"free-form SQL cannot honor column redaction")
		return
	}
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	if b.noArchive {
		writeJSONError(w, http.StatusNotFound,
			"archive access is disabled for this server, so there is no Parquet layout to query")
		return
	}
	// One query in flight per process. Each panel session is its own DuckDB
	// instance with its own memory budget, and this daemon may also be the
	// stream supervisor — concurrent 4GB sessions would starve capture.
	if !s.sqlPanelBusy.CompareAndSwap(false, true) {
		writeJSONError(w, http.StatusTooManyRequests,
			"another SQL panel query is already running; wait for it to finish or cancel it")
		return
	}
	defer s.sqlPanelBusy.Store(false)

	// Bound the setup: buildViewsInput's S3 baseline LIST, and the region
	// lookup beside it, are the unbounded steps under the latch. r.Context() still propagates (Cancel works); the
	// deadline is what stops a hung listing from wedging the single-flight.
	setupCtx, setupCancel := context.WithTimeout(r.Context(), sqlPanelSetupTimeout)
	in, err := s.buildViewsInput(setupCtx, b, false) // runs here: local-first routing
	setupCancel()
	switch {
	case errors.Is(err, errNoViewSources):
		writeJSONError(w, http.StatusNotFound, errNoViewSources.Error()+"; nothing to query")
		return
	case err != nil:
		// This one carries the wait; the refusals ABOVE it (panel off, profile
		// active, archives disabled, another query already running, a body too
		// large or malformed) are settled before any layout work and keep the
		// plain {error} body, deliberately: a number that is always about zero
		// says nothing. This fault is reported by the step that LISTS an S3
		// baseline root, so it is the long wait that used to report nothing at
		// all, which is where #1526 starts. The 404 above is on this side of
		// the listing too and stays plain on purpose: "nothing to query yet" is
		// a fact about the server that no elapsed time qualifies. The one
		// refusal above that this reasoning does not cover is the bundle
		// resolution (s.resolveOr): config.Connect Pings eagerly, so an
		// unreachable server refuses after a dial rather than about zero, and
		// it stays plain because it is the resolution EVERY endpoint shares,
		// not because it is fast.
		writeSQLPanelError(w, http.StatusBadGateway, err.Error(), time.Since(reqStart))
		return
	}

	// A session built over half the layout (baseline views, no events view,
	// because archive_state could not be read) is still worth serving: a
	// state_* query is fully answerable. What it must not do is stay quiet
	// about it, in either direction: a success carries the note as a warning
	// (composed in sqlPanelSessionNotes, where the set of views this session
	// actually built is known), and a failed statement carries it here, AFTER
	// the engine's message, so "table
	// events does not exist" is not read as a typo in the operator's SQL. After,
	// not ahead: *sqlUserError is the panel's whole user-error class (timeouts,
	// read-policy refusals, scan failures), and a note leading the message
	// would assert a cause for refusals that never touched the events view.
	// The audit record keeps the engine message alone for the same reason.
	res, err := runSandboxedSQL(r.Context(), in, req.SQL, reqStart)
	if err != nil {
		var ue *sqlUserError
		switch {
		case errors.Is(err, context.Canceled):
			// The human hit Cancel (or closed the tab): the fetch was aborted,
			// the query was interrupted, and there is no one to answer. Not a
			// policy event — stays off the audit seam.
			return
		case errors.As(err, &ue):
			recordSQLRun(r, req.SQL, "refused", ue.msg, 0, false)
			msg := ue.msg
			if in.ArchiveDiscoveryFailed {
				msg += ". Note: " + sqlPanelRegistryNote
			}
			writeSQLPanelError(w, http.StatusUnprocessableEntity, msg, time.Since(reqStart))
		default:
			recordSQLRun(r, req.SQL, "error", err.Error(), 0, false)
			writeSQLPanelError(w, http.StatusBadGateway, err.Error(), time.Since(reqStart))
		}
		return
	}

	recordSQLRun(r, req.SQL, "ok", "", res.RowCount, res.Truncated)
	writeJSON(w, http.StatusOK, res)
}

// writeSQLPanelError answers a statement the panel could not serve, with the
// wait attached. A refusal is on the same clock as an answer, and the longest
// wait this panel has is a MISTYPED relation name: that statement cannot be
// answered out of a selective catalog (the engine's "Did you mean" is computed
// from what is in it), so it builds every view in the layout and then fails. A
// body carrying only "error" leaves the page nothing to say about that wait.
func writeSQLPanelError(w http.ResponseWriter, status int, msg string, elapsed time.Duration) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":      msg,
		"elapsed_ms": elapsed.Milliseconds(),
	})
}

// sqlPanelSessionNotes is what a session says about ITSELF: the layout it was
// built over, narrowed to the views this statement asked for. in.OnlyViews must
// already be the set the session was built with.
//
// The narrowing is the point. A note describes what the answer is missing, so a
// note about files the query never opened is noise on an answer that is
// entirely correct — and after #1526 `SELECT 1` opens none of them.
func sqlPanelSessionNotes(in views.Input) []string {
	var out []string
	// Gated on the session having built ANY view. An answer that read a view is
	// an answer served out of half a layout, and the note is what says so; an
	// answer that read none (`SELECT 1`) is complete on its own terms, and
	// telling it what a catalog it never opened is missing is noise. A nil set
	// is every view, so the readers this note matters most to — a catalog
	// listing, and the "table events does not exist" that follows a typo — are
	// both inside it.
	if in.ArchiveDiscoveryFailed && (in.OnlyViews == nil || len(in.OnlyViews) > 0) {
		out = append(out, sqlPanelRegistryNote)
	}
	if note := sqlPanelDecimalNote(in); note != "" {
		out = append(out, note)
	}
	return out
}

// recordSQLRun emits the panel's audit event. Every statement that reaches the
// engine is recorded with its outcome — including one the gate refuses: a
// refused free-form probe (a read_parquet against the raw archive, a
// duckdb_secrets read) is exactly what an auditor needs, and this codebase
// already audits blocked attempts elsewhere (profile.denied). detail carries the
// gate/engine message on a non-ok outcome; it is never present on "ok".
func recordSQLRun(r *http.Request, stmt, outcome, detail string, rows int, truncated bool) {
	fields := map[string]string{
		"statement": stmt,
		"outcome":   outcome,
		"rows":      fmt.Sprintf("%d", rows),
		"truncated": fmt.Sprintf("%t", truncated),
	}
	if detail != "" {
		fields["error"] = detail
	}
	recordConsoleAccess(r, "sql.run", "", "", fields)
}

// sqlPanelAvailable is the capability gate for /api/capabilities: the process
// opted in AND the selected server has a Parquet layout the panel can query —
// the same per-server conditions the handler enforces (viewsAvailable already
// folds in noArchive and the session profile), so the UI never offers a tab
// that only errors.
func (s *Server) sqlPanelAvailable(r *http.Request, b *bundle) bool {
	return s.sqlPanel && s.viewsAvailable(r, b)
}

// runSandboxedSQL classifies stmt, opens a fresh sandboxed DuckDB session over
// exactly the part of the resolved Parquet layout that stmt names, and executes
// it under the hard timeout. Nothing survives between requests: both sessions
// are opened and discarded here, so lock_configuration can be applied
// unconditionally.
//
// started is when the CALLER began the work the operator is waiting on (the
// handler's request start, which is ahead of this call by the layout
// resolution). The result's ElapsedMS is measured from it, so the number on
// screen is the number they waited; QueryMS, measured here, is the statement's
// own share.
//
// The gate runs FIRST, on its own throwaway session, and that ordering is what
// makes the rest possible: the parsed statement names the views this session has
// to build, and a statement that names none (`SELECT 1`) builds none. Nothing
// the user typed executes on that first session — see openParseSession.
func runSandboxedSQL(ctx context.Context, in views.Input, stmt string, started time.Time) (*sqlPanelResult, error) {
	// Bound the SETUP, both halves of it: parsing the statement, and building
	// the views it names. That build reads a Parquet footer per view, which on
	// an S3 layout is a network read, and it runs under the single-flight latch
	// — so one hung read there answers every other reader with 429 for as long
	// as it lasts. The statement gets its own budget below (sqlPanelTimeout);
	// ctx, the request, still cancels both.
	setupCtx, setupCancel := context.WithTimeout(ctx, sqlPanelSetupTimeout)
	defer setupCancel()

	pdb, err := openParseSession(setupCtx)
	if err != nil {
		return nil, err
	}
	// Deferred AND closed by hand below. The explicit Close frees the parse
	// session before the query session opens, so the two DuckDB budgets never
	// overlap; the defer is what keeps a panic in the gate from orphaning an
	// instance in a daemon that may also be capturing. Close is idempotent.
	defer pdb.Close()
	refs, gateErr := sqlPanelGate(setupCtx, pdb, stmt)
	pdb.Close()
	if gateErr != nil {
		return nil, gateErr
	}

	only := wantedViews(in, refs)
	db, cleanup, err := openSandboxedSession(setupCtx, in, only)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	qctx, cancel := context.WithTimeout(ctx, sqlPanelTimeout)
	defer cancel()
	start := time.Now()
	rows, err := db.QueryContext(qctx, stmt)
	if err != nil {
		return nil, sqlPanelExecError(ctx, qctx, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, &sqlUserError{msg: err.Error()}
	}
	res := &sqlPanelResult{Columns: cols, Rows: [][]any{}}
	bytesUsed := 0
	for rows.Next() {
		if res.RowCount >= sqlPanelMaxRows {
			res.Truncated = true
			break
		}
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, &sqlUserError{msg: err.Error()}
		}
		out := make([]any, len(cols))
		rowBytes := 0
		for i, v := range raw {
			cell, cost := sqlPanelCell(v)
			out[i] = cell
			rowBytes += cost
		}
		// Enforce the byte budget BEFORE appending the row that would breach it —
		// checking after the append overshoots by a full row, and a single row
		// image can be multiple MB. Always keep at least one row so an oversized
		// first row surfaces as data, not an empty truncated result.
		if res.RowCount > 0 && bytesUsed+rowBytes > sqlPanelMaxBytes {
			res.Truncated = true
			break
		}
		res.Rows = append(res.Rows, out)
		bytesUsed += rowBytes
		res.RowCount++
	}
	if err := rows.Err(); err != nil {
		return nil, sqlPanelExecError(ctx, qctx, err)
	}
	res.QueryMS = time.Since(start).Milliseconds()
	res.ElapsedMS = time.Since(started).Milliseconds()
	// The session's own view of the layout: what it built, not what the layout
	// holds. Anything this answer warns about has to be about that.
	in.OnlyViews = only
	res.Warnings = sqlPanelSessionNotes(in)
	return res, nil
}

// wantedViews decides which of the layout's views this statement needs, from
// the relations its parsed tree names.
//
// nil means EVERY view, and it is the answer whenever the set is not certain:
// a tree shape this build cannot read, and a statement naming a relation this
// layout does not define. That second case is not a fallback for its own sake.
// DuckDB answers an unknown relation with "Table with name x does not exist!
// Did you mean ...?", computed from what is in the catalog, so a typo'd view
// name answered out of an EMPTY catalog would suggest a system table instead of
// the view the reader meant. Building everything there puts the same catalog
// behind that message as before any of this was lazy, on a layout whose views
// all build; where one does not, the session fails on THAT, which is what every
// statement did before this was lazy. It costs nothing on the paths that
// matter, since it only happens on a statement that is about to fail.
//
// A name the statement binds itself with WITH is not an unknown relation: it is
// resolved inside the statement, so it neither selects a view nor forces the
// whole catalog.
func wantedViews(in views.Input, refs *statementRefs) views.ViewSet {
	if refs == nil || !refs.readable {
		return nil
	}
	// Ask what the session's own renderer will define, not what the layout
	// could: GenerateViews drops the live leg, so an Input carrying one would
	// otherwise name an events view this session is not going to build. in is a
	// value here, so this changes nothing outside.
	in.LiveIndex = nil
	defined := make(map[string]bool, len(in.Baselines)+1)
	for _, n := range in.DefinedViews() {
		defined[strings.ToLower(n)] = true
	}
	// defined BEFORE ctes, deliberately. The walk collects names with no scope,
	// so a WITH whose name shadows a view leaves ONE entry that can stand for
	// both: `WITH events AS (SELECT * FROM events WHERE ...)` reads the view
	// inside the clause that shadows it. Asking "is this a view?" first can only
	// cost a view nobody reads; asking "is this a CTE?" first drops that
	// reference and turns a working query into "table does not exist".
	// Pinned by TestSQLPanel_shadowingCTEStillReadsTheView and its row in
	// TestSQLPanel_wantedViews — and it takes a WITH name that IS a view name:
	// a `WITH q AS (SELECT * FROM events)` yields {events} under either order,
	// so it cannot tell them apart.
	want := views.ViewSet{}
	for name := range refs.tables {
		switch {
		case defined[name]:
			want[name] = true
		case refs.ctes[name]:
			// Bound by the statement's own WITH clause.
		default:
			return nil
		}
	}
	return want
}

// openParseSession opens the throwaway DuckDB the statement gate classifies in.
//
// It is SEALED from its first statement — no allowed_directories carve-out at
// all, external access off, configuration locked — which is strictly tighter
// than the session the query itself runs in, and it can be, because this session
// reads nothing: the only statement it ever runs is json_serialize_sql over the
// user's statement as a BOUND PARAMETER. That is DuckDB's parser, not its
// binder: it turns text into a tree, resolves no name, opens no file, and never
// executes what it parsed. So the user's SQL does not run here, and cannot.
//
// It exists because the gate has to answer BEFORE the real session is built:
// the parsed tree names the views that session needs, and building the rest of
// them is what #1526 is about. Sealing it also means the gate no longer runs on
// a session with the archive roots carved out, which is a small tightening in
// its own right.
//
// The daemon budget applies here too (this process may co-host the stream
// supervisor), and spilling is turned off rather than pointed at a private
// directory: a parse has nothing to spill, and no temp_directory means no
// implicitly allowed path.
func openParseSession(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open DuckDB: %w", err)
	}
	db.SetMaxOpenConns(1)
	t := duckdbutil.DefaultTuning()
	for _, stmt := range []string{
		fmt.Sprintf("SET threads = %d", t.Threads),
		"SET memory_limit = " + sqlQuoteString(t.MemoryLimit),
		"SET temp_directory = ''",
		"SET enable_external_access = false",
		"SET lock_configuration = true",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply parse-session configuration %q: %w", stmt, err)
		}
	}
	return db, nil
}

// openSandboxedSession builds the locked-down DuckDB session: httpfs/AWS setup
// when S3 sources are present, the same view definitions /api/views.sql serves,
// then the sandbox — allowed_directories over exactly the resolved roots, a
// private spill directory, the conservative tuning budget, external access off,
// and the configuration locked. Every sandbox statement is error-checked: a
// sandbox that silently failed to apply must never serve a query.
//
// only names the views to build. A nil set builds the whole layout, which is
// what the escape tests drive and what runSandboxedSQL falls back to whenever
// the statement's references are not certain; a non-nil one builds exactly its
// names, down to none at all. What it does NOT narrow is allowed_directories:
// the carve-out stays the layout's full roots, because it is a ceiling on what
// this session could ever reach and not a per-statement decision, and because
// the FROM-clause allowlist is what stops a statement from reading anything in
// there other than through a view.
func openSandboxedSession(ctx context.Context, in views.Input, only views.ViewSet) (*sql.DB, func(), error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, fmt.Errorf("open DuckDB: %w", err)
	}
	// One connection: DuckDB SET is applied per database here, but a single
	// conn makes the setup→lock→query ordering self-evident.
	db.SetMaxOpenConns(1)

	// A PRIVATE spill directory. DuckDB implicitly allows file access under
	// temp_directory, so pointing it at the shared os.TempDir() would quietly
	// widen the sandbox to everything in /tmp.
	spill, err := os.MkdirTemp("", "bintrail-sqlpanel-")
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("create spill directory: %w", err)
	}
	cleanup := func() {
		db.Close()
		if err := os.RemoveAll(spill); err != nil {
			// The spill dir can hold row-image data at rest; a failure to remove
			// it must not be silent in a file whose posture is every-step-checked.
			slog.Warn("sql panel: could not remove spill directory", "dir", spill, "error", err)
		}
	}
	fail := func(err error) (*sql.DB, func(), error) {
		cleanup()
		return nil, nil, err
	}

	// Withhold the paid forensics columns from the events view STRUCTURALLY —
	// a property of the panel's session, not of the caller's input, so no future
	// caller can forget it. This is the eventDTO boundary; free-form SQL over an
	// unfiltered events view would serve exactly what eventDTO omits.
	in.ExcludeEventColumns = forensicsEventColumns
	// The statement's own views, and nothing else (#1526). in is a value, so
	// this narrows this session's render and nothing else.
	in.OnlyViews = only
	ddl := views.GenerateViews(in)

	// One predicate, shared with the generator: this used to be a private copy
	// that also matched BaselineSource, and a copy of a question whose answer
	// now decides whether a configuration fault is reported at all is the same
	// drift hazard the settings list just stopped being. Narrowing to the
	// generator's own answer is safe here because the FROM-clause allowlist
	// below refuses every raw file reader, so the ONLY way this session can
	// reach S3 is through a generated view — and a view carrying an s3:// path
	// is exactly what NeedsS3 reports. An s3:// BaselineSource that yielded no
	// snapshots emits no view, so there is nothing left to route. It reads
	// OnlyViews for the same reason: a session that builds no s3:// view makes
	// no S3 read, so resolving a credential chain for it is pure latency.
	//
	// S3 credential setup, when the layout needs it, through bintrail's own
	// tolerant helper — the SAME path parquetquery uses (httpfs + aws + a
	// credential_chain secret). Deliberately NOT views.Generate's inline
	// preamble: that `CREATE SECRET` aborts the whole script when no credential
	// resolves, whereas EnableS3CredentialChain warns and continues — a read
	// inside the allowed roots then fails at the S3 read (with a real auth
	// error), not at session setup, and a local-only layout is unaffected.
	if in.NeedsS3() {
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return fail(fmt.Errorf("S3 archive sources are configured but the DuckDB httpfs extension could not be loaded: %w", err))
		}
		if err := duckdbutil.EnableS3CredentialChainRegion(ctx, db, in.ArchiveRegion); err != nil {
			return fail(err)
		}
	}
	// Only the view DDL — the preamble is for the downloadable file. This runs
	// BEFORE the sandbox SETs below, so for an S3 layout its read_parquet glob
	// resolves over the network with the daemon's ambient credentials while the
	// session is still unlocked. That is safe ONLY because every interpolated
	// path is operator-resolved (archive_state / reconstruct.ListBaselines via
	// buildViewsInput), NEVER user input — routing a user-supplied path here
	// would be an unsandboxed arbitrary file/URL read.
	//
	// Empty means the statement needs no view; DuckDB answers an empty script
	// with "empty query", so there is nothing to run rather than nothing to say.
	if ddl != "" {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fail(fmt.Errorf("set up views over the Parquet layout: %w", err))
		}
	}

	sandbox := []string{
		"SET allowed_directories = " + sqlPanelAllowedList(in),
		"SET temp_directory = " + sqlQuoteString(spill),
		// Every panel session reads and renders in UTC, which is the timezone
		// bintrail stores in. DuckDB's default TimeZone is the HOST's, so
		// without this the archives' TIMESTAMP WITH TIME ZONE columns come
		// back shifted by whatever zone the daemon's machine is set to:
		// date_trunc('day', event_timestamp) lands on a local midnight and
		// strftime prints a local hour, both silently. The statement gate
		// refuses a user-typed SET (deliberately), so this is the only place
		// the panel's timezone can be set at all: a reader who would type
		// `SET TimeZone = 'UTC'` in their own DuckDB has no way to say it
		// here. Ahead of enable_external_access/lock_configuration below:
		// the config freezes there, and a timezone is not a sandbox carve-out.
		"SET TimeZone = 'UTC'",
	}
	// The conservative daemon budget (#510) — never ultrafast: this process may
	// co-host the stream supervisor. Applied by hand rather than Tuning.Apply
	// because Apply is best-effort (warn-and-continue) and a resource bound
	// that silently failed to apply is no bound.
	t := duckdbutil.DefaultTuning()
	sandbox = append(sandbox,
		fmt.Sprintf("SET threads = %d", t.Threads),
		"SET memory_limit = "+sqlQuoteString(t.MemoryLimit),
		"SET enable_external_access = false",
		"SET lock_configuration = true",
	)
	for _, stmt := range sandbox {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fail(fmt.Errorf("apply sandbox configuration %q: %w", stmt, err))
		}
	}
	return db, cleanup, nil
}

// statementRefs is what the gate learned about the relations a statement names,
// for the caller that has to decide which views to build.
//
// readable is not a detail: false means the tree held a shape this walker
// cannot read, and the sets are then meaningless. It is separate from an empty
// tables set, which is a real answer — `SELECT 1` names no relation.
type statementRefs struct {
	// tables are the FROM-clause relation names, lowercased.
	tables map[string]bool
	// ctes are the names the statement binds itself, with WITH. They appear in
	// tables too (the parser does not resolve them), and they are not views.
	ctes     map[string]bool
	readable bool
}

// sqlPanelGate enforces SELECT-only, single-statement — the read-only layer —
// and reports the relations the statement names.
// Classification is DuckDB's own parser: json_serialize_sql serializes SELECT
// statements and refuses everything else (COPY, CREATE, SET, ATTACH, INSTALL,
// CREATE SECRET, ...), so the panel never grows a hand-rolled SQL classifier.
// The statement travels as a bound parameter — it is data here, not SQL.
func sqlPanelGate(ctx context.Context, db *sql.DB, stmt string) (*statementRefs, error) {
	// Both casts are load-bearing. The input ?::VARCHAR is required because a
	// bound parameter's type is otherwise unknown to json_serialize_sql (it
	// errors "first argument must be a VARCHAR"). The ::VARCHAR on the RESULT is
	// needed because json_serialize_sql returns DuckDB's JSON type, which the
	// driver decodes to a Go map — casting back to text is what lets us Scan and
	// re-parse it ourselves.
	var out string
	if err := db.QueryRowContext(ctx, "SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", stmt).Scan(&out); err != nil {
		return nil, fmt.Errorf("classify statement: %w", err)
	}
	var parsed struct {
		Error        bool              `json:"error"`
		ErrorMessage string            `json:"error_message"`
		Statements   []json.RawMessage `json:"statements"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return nil, fmt.Errorf("classify statement: %w", err)
	}
	if parsed.Error {
		msg := parsed.ErrorMessage
		if strings.Contains(msg, "Only SELECT statements") {
			msg = "only SELECT statements can run here: the panel is read-only (writes, settings, ATTACH and COPY are refused)"
		}
		return nil, &sqlUserError{msg: msg}
	}
	if len(parsed.Statements) != 1 {
		return nil, &sqlUserError{msg: "one statement at a time"}
	}
	// The statement is a single SELECT; refuse it if the parsed tree reaches a
	// FROM-clause table function outside the allowlist (every file reader and
	// dynamic-SQL re-entry function lands here), or the replacement-scan form
	// (a file path as a table name). Walking the AST — not the raw text — makes
	// this robust to casing, comments, CTEs and subqueries.
	var tree any
	if err := json.Unmarshal([]byte(out), &tree); err != nil {
		// A statement that already passed json_serialize_sql cannot fail to
		// re-parse here; treat an unexpected shape as a violation, not a pass.
		return nil, &sqlUserError{msg: "unparseable statement is not available in the SQL panel; query the events and state_* views instead"}
	}
	if reason, found := walkFromSources(tree); found {
		return nil, &sqlUserError{msg: reason + " is not available in the SQL panel; query the events and state_* views instead"}
	}
	return collectRefs(tree), nil
}

// collectRefs reads the relation names out of a tree the gate has already
// accepted: every BASE_TABLE's name, and separately every name the statement
// binds with WITH.
//
// It reports readable=false rather than a partial answer whenever a node is not
// the shape it expects, because the caller's fallback for "not certain" is to
// build every view — the behaviour before any of this was selective. A guess
// here would silently leave a view out and turn a working query into "table
// does not exist".
func collectRefs(tree any) *statementRefs {
	refs := &statementRefs{tables: map[string]bool{}, ctes: map[string]bool{}, readable: true}
	walkRefs(tree, refs)
	return refs
}

// walkRefs records BASE_TABLE names and WITH keys, and gives up (readable =
// false) on anything it is not sure about.
//
// The from-clause node types the pinned DuckDB can produce were enumerated
// against it, since a shape this walk passes over IN SILENCE is the failure
// that matters (it yields a smaller set, not a wrong one): BASE_TABLE, recorded
// here; JOIN and SUBQUERY, walked through to the BASE_TABLEs inside them;
// TABLE_FUNCTION, which the gate has already refused unless it is `range` or
// `generate_series`, and neither names a relation; EXPRESSION_LIST (`VALUES`)
// and EMPTY (`SELECT 1`), which name none; PIVOT, which carries the BASE_TABLE
// it pivots; and SHOW_REF, split below. A file-literal read (`FROM 'x.parquet'`)
// parses as a BASE_TABLE and never reaches here — the gate refuses it. So every
// shape either names a relation this walk records, or is walked through to one,
// and a query-less SHOW_REF is the only one that depends on the catalog while
// naming nothing.
func walkRefs(node any, refs *statementRefs) {
	switch v := node.(type) {
	case map[string]any:
		if v["type"] == "BASE_TABLE" {
			name, ok := v["table_name"].(string)
			if !ok {
				refs.readable = false
				return
			}
			refs.tables[strings.ToLower(name)] = true
		}
		// A catalog LISTING (`SHOW TABLES`, `SHOW ALL TABLES`) is a SHOW_REF
		// with no nested query. It names no relation because it is asking for
		// the list of them, so there is nothing here to select on, and answering
		// it out of a catalog built for some other statement returns zero rows
		// with NO error — the one failure shape worse than a missing view. It is
		// also how an operator finds the state_* names, which are derived and
		// suffixed on a collision and are not written anywhere else. Unreadable,
		// therefore, in exactly the sense this flag means: build everything and
		// let it list what is really there.
		//
		// `DESCRIBE x`, `SUMMARIZE x` and `SHOW x` are the same node WITH a
		// query, whose BASE_TABLE this walk already finds, so they stay selective.
		if v["type"] == "SHOW_REF" && v["query"] == nil {
			refs.readable = false
			return
		}
		// A WITH clause is serialized as {"cte_map":{"map":[{"key":"q",...}]}}.
		// The key is the name the body of the statement refers to.
		if cm, ok := v["cte_map"].(map[string]any); ok {
			entries, ok := cm["map"].([]any)
			if !ok {
				refs.readable = false
				return
			}
			for _, e := range entries {
				entry, ok := e.(map[string]any)
				if !ok {
					refs.readable = false
					return
				}
				key, ok := entry["key"].(string)
				if !ok {
					refs.readable = false
					return
				}
				refs.ctes[strings.ToLower(key)] = true
			}
		}
		for _, child := range v {
			walkRefs(child, refs)
		}
	case []any:
		for _, child := range v {
			walkRefs(child, refs)
		}
	}
}

// walkFromSources walks a json_serialize_sql AST and reports the first
// disallowed FROM-clause source: a TABLE_FUNCTION whose name is not in
// allowedTableFunctions, or a BASE_TABLE whose name is a file path (a
// `FROM '<path>'` replacement scan). DuckDB records both — including inside
// CTEs, subqueries and joins — under from-clause nodes typed TABLE_FUNCTION /
// BASE_TABLE, so a single recursive scan for those node types covers them.
func walkFromSources(node any) (string, bool) {
	switch v := node.(type) {
	case map[string]any:
		switch v["type"] {
		case "TABLE_FUNCTION":
			// Fail closed: an unrecognizable name, or any name not explicitly
			// allowed, is refused. This is where read_parquet, query(),
			// query_table(), json_execute_serialized_sql() and every future
			// reader land.
			name := tableFunctionName(v)
			if !allowedTableFunctions[strings.ToLower(name)] {
				label := "a table function"
				if name != "" {
					label = "the function " + strings.ToLower(name)
				}
				return label, true
			}
		case "BASE_TABLE":
			// A replacement scan (`FROM '/path/x.parquet'`) is a BASE_TABLE
			// whose table_name is the file path. A real view/table identifier
			// never contains a path separator or a URL scheme.
			if name, ok := v["table_name"].(string); ok && looksLikeFilePath(name) {
				return "reading a file path directly", true
			}
		}
		for _, child := range v {
			if r, found := walkFromSources(child); found {
				return r, true
			}
		}
	case []any:
		for _, child := range v {
			if r, found := walkFromSources(child); found {
				return r, true
			}
		}
	}
	return "", false
}

// tableFunctionName pulls the function name out of a TABLE_FUNCTION from-clause
// node (its nested "function" object carries function_name). Returns "" when the
// shape is unexpected, which walkFromSources treats as disallowed.
func tableFunctionName(node map[string]any) string {
	fn, ok := node["function"].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := fn["function_name"].(string)
	return name
}

// looksLikeFilePath reports whether a FROM-clause table name is actually a file
// path or URL (a replacement scan) rather than a view/table identifier. Bintrail
// identifiers never contain a path separator or a scheme, so any of those marks
// a file read. A bare relative name (no separator) resolves against the working
// directory, which is outside allowed_directories, so the sandbox denies it —
// this check only needs to catch the forms that could reach an allowed root.
func looksLikeFilePath(name string) bool {
	return strings.ContainsAny(name, "/\\") || strings.Contains(name, "://")
}

// sqlPanelExecError classifies a query failure: the panel's own timeout, the
// client canceling, or a statement error (including the sandbox's Permission
// Error for reads outside the allowed roots — surfaced verbatim).
func sqlPanelExecError(ctx, qctx context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return context.Canceled
	case qctx.Err() != nil:
		return &sqlUserError{msg: fmt.Sprintf("query canceled: it exceeded the %s limit", sqlPanelTimeout)}
	default:
		return &sqlUserError{msg: err.Error()}
	}
}

// sqlPanelAllowedList renders the allowed_directories literal for the resolved
// roots. A trailing separator on every entry keeps the match a DIRECTORY
// boundary: an allowed ".../lake" must never also admit ".../lakeevil".
func sqlPanelAllowedList(in views.Input) string {
	var roots []string
	for _, src := range in.ArchiveSources {
		roots = append(roots, strings.TrimRight(src, "/")+"/")
	}
	if in.BaselineSource != "" {
		roots = append(roots, strings.TrimRight(in.BaselineSource, "/")+"/")
	}
	quoted := make([]string, len(roots))
	for i, r := range roots {
		quoted[i] = sqlQuoteString(r)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// sqlQuoteString renders a DuckDB single-quoted string literal.
func sqlQuoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// sqlPanelCell converts one scanned DuckDB value into a JSON-encodable cell and
// reports its approximate response cost. Strings and byte blobs pass through;
// times render as UTC RFC3339; DuckDB's HUGEINT arrives as *big.Int and is
// rendered as a string (JSON numbers lose precision past 2^53); anything the
// driver hands us beyond that (LIST/STRUCT/DECIMAL/...) is rendered via fmt so
// no exotic type can ever fail the encoder mid-response.
func sqlPanelCell(v any) (any, int) {
	switch x := v.(type) {
	case nil:
		return nil, 4
	case []byte:
		s := string(x)
		return s, len(s)
	case string:
		return x, len(x)
	case time.Time:
		s := x.UTC().Format(time.RFC3339Nano)
		return s, len(s)
	case *big.Int:
		s := x.String()
		return s, len(s)
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return x, 16
	default:
		s := fmt.Sprint(x)
		return s, len(s)
	}
}
