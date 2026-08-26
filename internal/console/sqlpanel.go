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
//     parser (json_serialize_sql), never by string matching.
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

// sqlPanelSetupTimeout bounds the pre-query setup — the S3 baseline LIST in
// buildViewsInput is the one unbounded step that runs under the single-flight
// latch, so a hung listing must not pin the panel at 429 for every other user.
// The query itself is bounded separately by sqlPanelTimeout.
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
	Truncated bool  `json:"truncated"`
	ElapsedMS int64 `json:"elapsed_ms"`
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

	// Bound the setup: buildViewsInput's S3 baseline LIST is the one unbounded
	// step under the latch. r.Context() still propagates (Cancel works); the
	// deadline is what stops a hung listing from wedging the single-flight.
	setupCtx, setupCancel := context.WithTimeout(r.Context(), sqlPanelSetupTimeout)
	in, err := s.buildViewsInput(setupCtx, b, false) // runs here: local-first routing
	setupCancel()
	switch {
	case errors.Is(err, errNoViewSources):
		writeJSONError(w, http.StatusNotFound, errNoViewSources.Error()+"; nothing to query")
		return
	case err != nil:
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}

	// A session built over half the layout (baseline views, no events view,
	// because archive_state could not be read) is still worth serving: a
	// state_* query is fully answerable. What it must not do is stay quiet
	// about it, in either direction: a success carries the note as a warning,
	// and a failed statement carries it AFTER the engine's message, so "table
	// events does not exist" is not read as a typo in the operator's SQL. After,
	// not ahead: *sqlUserError is the panel's whole user-error class (timeouts,
	// read-policy refusals, scan failures), and a note leading the message
	// would assert a cause for refusals that never touched the events view.
	// The audit record keeps the engine message alone for the same reason.
	res, err := runSandboxedSQL(r.Context(), in, req.SQL)
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
			writeJSONError(w, http.StatusUnprocessableEntity, msg)
		default:
			recordSQLRun(r, req.SQL, "error", err.Error(), 0, false)
			writeJSONError(w, http.StatusBadGateway, err.Error())
		}
		return
	}

	if in.ArchiveDiscoveryFailed {
		res.Warnings = append(res.Warnings, sqlPanelRegistryNote)
	}
	recordSQLRun(r, req.SQL, "ok", "", res.RowCount, res.Truncated)
	writeJSON(w, http.StatusOK, res)
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

// runSandboxedSQL opens a fresh sandboxed DuckDB session over the resolved
// Parquet layout, enforces the SELECT-only gate, and executes stmt under the
// hard timeout. One session per call: nothing survives between requests, and
// lock_configuration can be applied unconditionally.
func runSandboxedSQL(ctx context.Context, in views.Input, stmt string) (*sqlPanelResult, error) {
	db, cleanup, err := openSandboxedSession(ctx, in)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	if err := sqlPanelGate(ctx, db, stmt); err != nil {
		return nil, err
	}

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
	res.ElapsedMS = time.Since(start).Milliseconds()
	return res, nil
}

// openSandboxedSession builds the locked-down DuckDB session: httpfs/AWS setup
// when S3 sources are present, the same view definitions /api/views.sql serves,
// then the sandbox — allowed_directories over exactly the resolved roots, a
// private spill directory, the conservative tuning budget, external access off,
// and the configuration locked. Every sandbox statement is error-checked: a
// sandbox that silently failed to apply must never serve a query.
func openSandboxedSession(ctx context.Context, in views.Input) (*sql.DB, func(), error) {
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

	// S3 credential setup, when the layout needs it, through bintrail's own
	// tolerant helper — the SAME path parquetquery uses (httpfs + aws + a
	// credential_chain secret). Deliberately NOT views.Generate's inline
	// preamble: that `CREATE SECRET` aborts the whole script when no credential
	// resolves, whereas EnableS3CredentialChain warns and continues — a read
	// inside the allowed roots then fails at the S3 read (with a real auth
	// error), not at session setup, and a local-only layout is unaffected.
	if viewsInputNeedsS3(in) {
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return fail(fmt.Errorf("S3 archive sources are configured but the DuckDB httpfs extension could not be loaded: %w", err))
		}
		duckdbutil.EnableS3CredentialChainRegion(ctx, db, in.ArchiveRegion)
	}
	// Withhold the paid forensics columns from the events view STRUCTURALLY —
	// a property of the panel's session, not of the caller's input, so no future
	// caller can forget it. This is the eventDTO boundary; free-form SQL over an
	// unfiltered events view would serve exactly what eventDTO omits.
	in.ExcludeEventColumns = forensicsEventColumns
	// Only the view DDL — the preamble is for the downloadable file. This runs
	// BEFORE the sandbox SETs below, so for an S3 layout its read_parquet glob
	// resolves over the network with the daemon's ambient credentials while the
	// session is still unlocked. That is safe ONLY because every interpolated
	// path is operator-resolved (archive_state / reconstruct.ListBaselines via
	// buildViewsInput), NEVER user input — routing a user-supplied path here
	// would be an unsandboxed arbitrary file/URL read.
	if _, err := db.ExecContext(ctx, views.GenerateViews(in)); err != nil {
		return fail(fmt.Errorf("set up views over the Parquet layout: %w", err))
	}

	sandbox := []string{
		"SET allowed_directories = " + sqlPanelAllowedList(in),
		"SET temp_directory = " + sqlQuoteString(spill),
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

// sqlPanelGate enforces SELECT-only, single-statement — the read-only layer.
// Classification is DuckDB's own parser: json_serialize_sql serializes SELECT
// statements and refuses everything else (COPY, CREATE, SET, ATTACH, INSTALL,
// CREATE SECRET, ...), so the panel never grows a hand-rolled SQL classifier.
// The statement travels as a bound parameter — it is data here, not SQL.
func sqlPanelGate(ctx context.Context, db *sql.DB, stmt string) error {
	// Both casts are load-bearing. The input ?::VARCHAR is required because a
	// bound parameter's type is otherwise unknown to json_serialize_sql (it
	// errors "first argument must be a VARCHAR"). The ::VARCHAR on the RESULT is
	// needed because json_serialize_sql returns DuckDB's JSON type, which the
	// driver decodes to a Go map — casting back to text is what lets us Scan and
	// re-parse it ourselves.
	var out string
	if err := db.QueryRowContext(ctx, "SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", stmt).Scan(&out); err != nil {
		return fmt.Errorf("classify statement: %w", err)
	}
	var parsed struct {
		Error        bool              `json:"error"`
		ErrorMessage string            `json:"error_message"`
		Statements   []json.RawMessage `json:"statements"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		return fmt.Errorf("classify statement: %w", err)
	}
	if parsed.Error {
		msg := parsed.ErrorMessage
		if strings.Contains(msg, "Only SELECT statements") {
			msg = "only SELECT statements can run here: the panel is read-only (writes, settings, ATTACH and COPY are refused)"
		}
		return &sqlUserError{msg: msg}
	}
	if len(parsed.Statements) != 1 {
		return &sqlUserError{msg: "one statement at a time"}
	}
	// The statement is a single SELECT; refuse it if the parsed tree reaches a
	// FROM-clause table function outside the allowlist (every file reader and
	// dynamic-SQL re-entry function lands here), or the replacement-scan form
	// (a file path as a table name). Walking the AST — not the raw text — makes
	// this robust to casing, comments, CTEs and subqueries.
	if reason, found := astViolatesReadPolicy([]byte(out)); found {
		return &sqlUserError{msg: reason + " is not available in the SQL panel; query the events and state_* views instead"}
	}
	return nil
}

// astViolatesReadPolicy walks a json_serialize_sql AST and reports the first
// disallowed FROM-clause source: a TABLE_FUNCTION whose name is not in
// allowedTableFunctions, or a BASE_TABLE whose name is a file path (a
// `FROM '<path>'` replacement scan). DuckDB records both — including inside
// CTEs, subqueries and joins — under from-clause nodes typed TABLE_FUNCTION /
// BASE_TABLE, so a single recursive scan for those node types covers them.
func astViolatesReadPolicy(ast []byte) (string, bool) {
	var tree any
	if err := json.Unmarshal(ast, &tree); err != nil {
		// A statement that already passed json_serialize_sql cannot fail to
		// re-parse here; treat an unexpected shape as a violation, not a pass.
		return "unparseable statement", true
	}
	return walkFromSources(tree)
}

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

func viewsInputNeedsS3(in views.Input) bool {
	for _, src := range in.ArchiveSources {
		if strings.HasPrefix(src, "s3://") {
			return true
		}
	}
	if strings.HasPrefix(in.BaselineSource, "s3://") {
		return true
	}
	for _, b := range in.Baselines {
		if strings.HasPrefix(b.Path, "s3://") {
			return true
		}
	}
	return false
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
