package console

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/status"
)

// Result caps. Every endpoint applies a default and a hard maximum; the limit
// is never 0/unlimited. A read-only browser must not be able to ask the index
// for an unbounded result set.
const (
	eventsDefaultLimit  = 100
	eventsMaxLimit      = 1000
	recoverDefaultLimit = 1000
	recoverMaxLimit     = 10000

	// recoverMaxScriptBytes caps the estimated row payload (recovery's
	// EstimateScriptBytes: resident row_before/row_after bytes plus the PK,
	// across every matched row) that POST /api/recover — and its cascade
	// auto-detection and POST /api/recover-cascade siblings — may hold before
	// the console refuses to generate a reversal script (#849, follow-up to
	// #654/#652).
	//
	// recoverMaxLimit already bounds ROW COUNT (10,000), not bytes: a wide
	// table with megabyte BLOB/TEXT columns blows past any sane heap budget
	// well under 10,000 rows — 10k rows at a few MB each is tens of GB. Under
	// `bintrail-console watch` the console shares the process with the
	// capture stream, so an OOM-kill here also kills capture (event loss
	// until the supervisor restarts it).
	//
	// recovery.Generator already refuses BEFORE rendering anything
	// (GenerateSQLFromRows calls CheckScriptBudget/EstimateScriptBytes first,
	// so a refusal never touches the output buffer — see internal/recovery's
	// #654 guard) — but its zero-config default, DefaultMaxScriptBytes, is
	// 2 GiB: sized for an operator-run CLI process, not a long-lived daemon
	// that must keep serving the events browser, status, and (in `watch`)
	// capture at the same time. A 2 GiB reversal script is also useless in a
	// browser tab: it will never render in a <textarea> or survive a JSON
	// round-trip through the tab's own JS heap. 32 MiB keeps an ordinary
	// wide-row recovery (KBs to low MBs of SQL) comfortably in budget while
	// failing fast — well before the CLI's headroom — on the pathological
	// BLOB/TEXT-heavy window this issue is about.
	//
	// No console flag or env var exposes this: unlike the CLI's
	// --max-script-bytes (a per-run operator choice on a process that exits
	// when it's done), this is a shared-daemon OOM guardrail. The escape
	// hatch for a genuinely large recovery is the one the refusal error
	// message gives — narrow the filter, or run `bintrail recover` (which
	// already has --max-script-bytes / BINTRAIL_RECOVER_MAX_BYTES) outside
	// the daemon.
	recoverMaxScriptBytes = 32 << 20
)

// filterParams is the source-agnostic set of query filters parsed from either
// a URL query string (events) or a JSON body (recover).
type filterParams struct {
	Schema        string
	Table         string
	PK            string
	EventType     string
	GTID          string
	Since         string
	Until         string
	ChangedColumn string
	Order         string
	Limit         int
}

// recoverRequest is the JSON body accepted by POST /api/recover.
type recoverRequest struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	PK            string `json:"pk"`
	EventType     string `json:"event_type"`
	GTID          string `json:"gtid"`
	Since         string `json:"since"`
	Until         string `json:"until"`
	ChangedColumn string `json:"changed_column"`
	// Order is accepted for request symmetry with /api/events but IGNORED by
	// recover: handleRecover forces newest-first (DESC) input so a LIMIT
	// truncation keeps the newest suffix of the window (#981); rows are
	// re-sorted ASC before generation. A client-supplied value has no effect.
	Order string `json:"order"`
	Limit int    `json:"limit"`
}

type eventsResponse struct {
	Events   []eventDTO `json:"events"`
	Count    int        `json:"count"`
	Limit    int        `json:"limit"`
	Warnings []string   `json:"warnings,omitempty"`
}

type recoverResponse struct {
	SQL            string   `json:"sql"`
	StatementCount int      `json:"statement_count"`
	RowCount       int      `json:"row_count"`
	Warnings       []string `json:"warnings,omitempty"`
	// Cascade fields are set only when the recover target was auto-detected as a
	// foreign-key parent whose DELETE or key UPDATE cascaded below the binlog (the
	// script then also repairs the invisible children). Zero/false/empty for a
	// plain recover, so existing clients are unaffected.
	CascadeDetected bool `json:"cascade_detected,omitempty"`
	VictimCount     int  `json:"victim_count,omitempty"`
	SetNullCount    int  `json:"set_null_count,omitempty"`
	// KeyRestoreCount is the ON UPDATE CASCADE / SET NULL half (#1002): child
	// foreign keys the cascade rewrote and this script puts back.
	KeyRestoreCount int `json:"key_restore_count,omitempty"`
}

type schemasResponse struct {
	Schemas []string `json:"schemas"`
}

type tablesResponse struct {
	Schema string   `json:"schema"`
	Tables []string `json:"tables"`
}

// buildOptions converts shared filter params into a query.Options, validating
// cross-field requirements and clamping the limit. RBAC rules (deny tables /
// redact columns) resolved at startup are always attached so every query the
// console runs is bound by the operator's profile.
func (s *Server) buildOptions(p filterParams, defaultLimit, maxLimit int) (query.Options, error) {
	et, err := cliutil.ParseEventType(p.EventType)
	if err != nil {
		return query.Options{}, err
	}
	since, err := cliutil.ParseTime(p.Since)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid since: %w", err)
	}
	until, err := cliutil.ParseTime(p.Until)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid until: %w", err)
	}

	// A PK or changed-column filter is only meaningful when scoped to one
	// table; mirror the MCP/CLI validation so the index isn't scanned blindly.
	if p.PK != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, errors.New("the PK filter needs both a schema and a table")
	}
	if p.ChangedColumn != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, errors.New("the changed-column filter needs both a schema and a table")
	}

	// Default to newest-first, the natural order for a browsing UI.
	order := strings.ToUpper(strings.TrimSpace(p.Order))
	if order != "ASC" {
		order = "DESC"
	}

	return query.Options{
		Schema:        p.Schema,
		Table:         p.Table,
		PKValues:      p.PK,
		EventType:     et,
		GTID:          p.GTID,
		Since:         since,
		Until:         until,
		ChangedColumn: p.ChangedColumn,
		Limit:         clampLimit(p.Limit, defaultLimit, maxLimit),
		Order:         order,
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
		// ProfileActive forces the redaction pass even for a named profile that
		// resolved to zero rules, so QueryText/QueryHash are withheld under EVERY
		// named profile per the #699 contract (matching the CLI/MCP). Without
		// this a `--profile <typo>` would leave query_text with sensitive
		// literals visible (#838).
		ProfileActive: s.profileActive,
	}, nil
}

// fetch runs the shared cross-source fetch (live MySQL + Parquet archives)
// against the request's selected server bundle.
//
// AllowGaps is true for both events and recover, matching the CLI `recover`
// (warn-and-continue — a human reviews the script). Coverage gaps the planner
// detects are returned in the QueryPlan and surfaced to the caller as warnings
// via gapWarnings(plan); the recover UI renders them prominently, so an
// incomplete-coverage undo is never presented as a clean success.
//
// One residual case for these permissive endpoints: when several archive
// sources are configured and only SOME fail to load, FetchMerged logs the
// failure server-side and continues (again, matching the CLI). That is a
// deliberate AllowGaps=true trade-off, not a missing signal — under
// AllowGaps=false (the reconstruct endpoint) any source failure aborts the
// fetch (#377). The trade-off is documented in docs/console.md.
func (s *Server) fetch(ctx context.Context, b *bundle, opts query.Options) ([]query.ResultRow, *query.QueryPlan, error) {
	return query.FetchMerged(ctx, b.db, b.engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         b.dbName,
		NoArchive:      b.noArchive,
		AllowGaps:      true,
		ArchiveFetcher: parquetquery.Fetch,
	})
}

// handleEvents serves GET /api/events — the events browser.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	q := r.URL.Query()
	p := filterParams{
		Schema:        q.Get("schema"),
		Table:         q.Get("table"),
		PK:            q.Get("pk"),
		EventType:     q.Get("event_type"),
		GTID:          q.Get("gtid"),
		Since:         q.Get("since"),
		Until:         q.Get("until"),
		ChangedColumn: q.Get("changed_column"),
		Order:         q.Get("order"),
		Limit:         atoiDefault(q.Get("limit"), 0),
	}
	opts, err := s.buildOptions(p, eventsDefaultLimit, eventsMaxLimit)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	opts, err = s.applySessionProfile(r.Context(), r, b, opts)
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}
	rows, plan, err := s.fetchRestricted(r.Context(), r, b, opts)
	if err != nil {
		writeFetchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, eventsResponse{
		Events:   toEventDTOs(rows),
		Count:    len(rows),
		Limit:    opts.Limit,
		Warnings: gapWarnings(plan),
	})
}

// handleRecover serves POST /api/recover — generates undo SQL. It NEVER
// executes the SQL: rows are fetched (read-only), reversed into a buffer, and
// the script is returned as text for the operator to review and apply.
func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	var body recoverRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	p := filterParams{
		Schema:        body.Schema,
		Table:         body.Table,
		PK:            body.PK,
		EventType:     body.EventType,
		GTID:          body.GTID,
		Since:         body.Since,
		Until:         body.Until,
		ChangedColumn: body.ChangedColumn,
		Order:         body.Order,
		Limit:         body.Limit,
	}
	opts, err := s.buildOptions(p, recoverDefaultLimit, recoverMaxLimit)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	opts, err = s.applySessionProfile(r.Context(), r, b, opts)
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}
	// Refuse to generate an undo script for the entire index; a recovery must
	// be scoped to at least one schema.
	if opts.Schema == "" {
		writeJSONError(w, http.StatusBadRequest, "choose at least a schema to search")
		return
	}

	// When --limit truncates the window it must keep the most RECENT events
	// (#981, mirroring the CLI's #785 fix in internal/cli/recover.go): fetching
	// DESC means a LIMIT truncation keeps the newest suffix, rolling the data
	// back to a consistent intermediate point. The ASC default would instead
	// keep the OLDEST prefix — undoing old events underneath later
	// un-reverted ones maps to no state that ever existed (the reverse
	// UPDATE's row_after WHERE no longer matches, or the reverse DELETE
	// removes a row a later event rewrote). Rows are re-sorted ascending
	// below, before generation: GenerateSQLFromRows expects chronological
	// (ASC) input and reverses internally so the most-recent event is undone
	// first.
	opts.Order = "DESC"

	// Coverage gaps come back in plan.GapHours and are surfaced as warnings
	// below — the recover UI renders them, so an incomplete-coverage undo is
	// flagged to the operator rather than silently presented as complete.
	rows, plan, err := s.fetchRestricted(r.Context(), r, b, opts)
	if err != nil {
		writeFetchError(w, err)
		return
	}
	warnings := gapWarnings(plan)

	// The fetch above ran Order=DESC so the limit kept the newest suffix of
	// the window (#981). Detect truncation on the FETCHED row count — before
	// generation, so the warning fires even when generation later refuses —
	// then restore ascending order for GenerateSQLFromRows and the
	// cascade-detection logic below, both of which expect chronological input.
	if opts.Limit > 0 && len(rows) >= opts.Limit {
		warnings = append(warnings,
			fmt.Sprintf("Matched events were truncated at the limit (%d); only the most recent events of the window are being reversed.", opts.Limit))
	}
	rows = query.MergeResults(rows, 0, "ASC")

	// Per-bundle dialect (the console is multi-server): MySQLDialect covers MySQL +
	// MariaDB, PostgresDialect a PG-flavored index. Read once and reused below.
	dialect := recovery.DialectForIndex(b.db)

	// Cascade auto-detection. Undoing a DELETE on a foreign-key parent, the plain
	// reversal is a strict SUBSET: it re-inserts the parent but not the child rows
	// InnoDB cascade-deleted below the binlog (MySQL Bug #32506). The same holds
	// for undoing a parent-key UPDATE, whose ON UPDATE cascade rewrote the child
	// FKs just as invisibly (#1002) — reversing the parent alone leaves them
	// dangling on the new value. When the target is such a parent, synthesize
	// those invisible side effects and fold them into ONE script — the operator
	// never has to know their FK topology or visit a separate tab. Gated to
	// MySQL/MariaDB: it is a binlog blind-spot fix, and PostgreSQL logical
	// replication captures cascades as real events (no blind spot to synthesize —
	// firing here would only surface a misleading "0 victims" banner). Otherwise
	// only meaningful when a single table is in scope and the matched rows contain
	// an event that can cascade (an INSERT undo never does).
	if dialect == recovery.MySQLDialect && body.Table != "" && rowsContainCascadeTriggerOn(rows, body.Table, true, true) {
		// The rules are matched to the event types actually being reversed: an
		// ON UPDATE-only parent must not route a DELETE undo through synthesis,
		// and an ON DELETE-only parent must not route an UPDATE undo.
		onDelete, onUpdate, derr := s.cascadeParentDetect(b, body.Schema, body.Table)
		isParent := rowsContainCascadeTriggerOn(rows, body.Table, onDelete, onUpdate)
		switch {
		case derr != nil:
			// Detection is best-effort: a probe failure must never block a plain
			// recover — but it must NOT silently downgrade one either. If this table
			// IS a cascade parent we couldn't tell, so warn that any cascade side
			// effects may be missing (mirrors the RBAC arm below), then fall through
			// to the plain path.
			slog.Warn("console: cascade parent detection failed; recover proceeds without cascade synthesis", "error", derr)
			warnings = append([]string{
				"Could not check whether this table is a foreign-key parent (detection failed: " + derr.Error() + "). If it is, any cascade-deleted child rows or cascade-rewritten child foreign keys are NOT included in the script below — retry, or use recover-cascade to reconstruct them.",
			}, warnings...)
		case isParent && s.rbacActiveFor(r):
			// Synthesis can't honor redaction (it would leak denied/redacted child
			// rows), so it stays disabled under a profile — startup OR per-session
			// (#1075) — but SAY so, so a parent-only script is never silently
			// presented as a full restore.
			warnings = append([]string{
				"This table has ON DELETE / ON UPDATE CASCADE / SET NULL children, but cascade synthesis is disabled while an RBAC redaction profile is active — the script below reverses the parent only; cascade-deleted child rows and cascade-rewritten child foreign keys are NOT included.",
			}, warnings...)
		case isParent:
			cres, cerr := s.cascadeRecover(r.Context(), b, body, opts, rows)
			if cerr != nil {
				// A *recovery.ScriptBudgetError here means synthesis SUCCEEDED —
				// only the combined (parent + synthesized children) script exceeded
				// the console's budget at render time (recoverMaxScriptBytes, #849).
				// That is a distinct condition from a synthesis failure below: say so
				// precisely (a misdiagnosis as "synthesis failed" would send an
				// operator debugging the wrong thing), and give the same actionable
				// console guidance as the plain-path 422 (writeRecoverError) rather
				// than leaking ScriptBudgetError.Error()'s CLI-only "raise/disable the
				// budget (0 = unlimited)" phrasing — a console setting that doesn't
				// exist.
				var be *recovery.ScriptBudgetError
				if errors.As(cerr, &be) {
					slog.Warn("console: cascade recovery over the script-size budget; falling back to plain recover", "error", cerr)
					warnings = append([]string{
						fmt.Sprintf(
							"Cascade recovery synthesized the deleted rows, but the combined script would hold ~%.1f MiB of row data — over the console's %.0f MiB budget for a single recovery. The script below re-creates the parent only; cascade-deleted child rows are NOT included. Narrow the recovery filter (schema/table/pk/time range) to shrink the window, or use `bintrail recover-cascade` from the CLI for large cascades.",
							float64(be.EstimatedBytes)/(1<<20), float64(be.Budget)/(1<<20)),
					}, warnings...)
					break // out of the switch → plain recover below
				}
				// Cascade synthesis is an ENHANCEMENT of the plain recover, not a
				// precondition — the base rows were already fetched. A synthesis
				// failure must not deny the recover the operator can still get;
				// degrade to the plain path with a loud warning rather than 500ing
				// the whole request (which would block even the parent-only undo).
				slog.Warn("console: cascade synthesis failed; falling back to plain recover", "error", cerr)
				warnings = append([]string{
					"Cascade synthesis failed (" + cerr.Error() + "); the script below reverses the parent only — cascade-deleted child rows and cascade-rewritten child foreign keys are NOT included.",
				}, warnings...)
				break // out of the switch → plain recover below
			}
			if cres.VictimCount+cres.SetNullCount+cres.KeyRestoreCount == 0 {
				// Nothing actually cascaded. rowsContainCascadeTriggerOn's UPDATE
				// arm is deliberately coarse (it cannot check whether a referenced
				// key moved without the FK graph), so ANY update undo on a table
				// with an ON UPDATE child lands here; the synthesis then correctly
				// rejects it. Reporting cascade_detected with all counts zero told
				// the operator "CASCADE — no related rows needed repairing" and,
				// worse, handed back an ordinary reversal silently wrapped in
				// SET FOREIGN_KEY_CHECKS=0/1 — FK validation disabled on a script
				// they expected checked. Fall back to the plain script and the
				// plain response, carrying the synthesis's own notes across so a
				// coverage caveat is never dropped on the way out.
				if len(cres.Caveats) > 0 {
					warnings = append(append([]string{
						"Checked whether MySQL changed other rows automatically alongside these: none were found, but that check is provably partial — review the notes below.",
					}, cres.Caveats...), warnings...)
				}
				warnings = append(warnings, cres.Warnings...)
				break // out of the switch → plain recover below
			}
			cw := warnings
			if len(cres.Caveats) > 0 {
				cw = append([]string{
					"Cascade recovery is provably partial — review the caveats below; some cascade-deleted rows or cascade-rewritten foreign keys may be missing.",
				}, cres.Caveats...)
				cw = append(cw, warnings...)
			}
			// cres.Warnings are advisory-only (#618, e.g. a Phase-2 baseline that
			// fell back to an older snapshot) — appended unconditionally, same
			// treatment the reconstruct tab gives an identical stale-baseline
			// signal (appendStaleWarning in reconstruct.go): visible in the
			// response's Warnings list, but never framed as "provably partial"
			// and never gating CascadeDetected/complete-ness above.
			cw = append(cw, cres.Warnings...)
			writeJSON(w, http.StatusOK, recoverResponse{
				SQL:             cres.SQL,
				StatementCount:  cres.StatementCount,
				RowCount:        len(rows),
				Warnings:        cw,
				CascadeDetected: true,
				VictimCount:     cres.VictimCount,
				SetNullCount:    cres.SetNullCount,
				KeyRestoreCount: cres.KeyRestoreCount,
			})
			return
		}
	}

	var buf bytes.Buffer
	// Per-bundle dialect (read above): a PG-flavored index → PostgreSQL reversal SQL.
	// DialectForIndex defaults to MySQL on any read failure (#533/#573).
	gen := recovery.NewForDialect(b.db, b.resolver, dialect)
	// #849: tighten the CLI-sized 2 GiB zero-config default
	// (recovery.DefaultMaxScriptBytes) down to recoverMaxScriptBytes. The
	// generator's CheckScriptBudget runs BEFORE any byte is written to buf, so
	// a refusal never materializes the oversized script in the shared daemon
	// heap — see the recoverMaxScriptBytes doc comment above.
	gen.SetMaxScriptBytes(recoverMaxScriptBytes)
	n, err := gen.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		writeRecoverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, recoverResponse{
		SQL:            buf.String(),
		StatementCount: n,
		RowCount:       len(rows),
		Warnings:       warnings,
	})
}

// handleStatus serves GET /api/status — index health, partitions, coverage,
// stream lag, archives. Reuses status.CollectStatus + WriteJSON verbatim;
// that surface exposes only aggregate server metadata, never per-event actor
// attribution, so it stays inside the free query_explorer boundary.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	data, err := status.CollectStatus(r.Context(), b.db, b.dbName)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Encode into a buffer first: status data is already in memory, so this is
	// free and avoids committing a 200 then emitting a truncated body if the
	// encode fails partway (mirrors handleRecover).
	var buf bytes.Buffer
	if err := data.WriteJSON(&buf); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(buf.Bytes()); err != nil {
		slog.Error("console: status write failed", "error", err)
	}
}

// handleSchemas serves GET /api/schemas. Without a ?schema= param it returns
// the distinct schemas present in the index; with one it returns that schema's
// tables (snapshot-authoritative, falling back to distinct observed tables).
func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	schema := r.URL.Query().Get("schema")
	if schema == "" {
		names, err := b.distinctSchemas(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, schemasResponse{Schemas: names})
		return
	}
	tables, err := b.tablesForSchema(r.Context(), schema)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tablesResponse{Schema: schema, Tables: tables})
}

// handleHealthz serves GET /api/healthz — an unauthenticated liveness probe.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// distinctSchemas lists the schemas this server can be queried for: those
// observed in the live binlog_events UNION those in the latest schema snapshot.
//
// The snapshot half is load-bearing, not a nicety: once rotate archives the
// partitions to Parquet/S3, binlog_events is empty while /api/events and
// /api/recover still answer from the archives via query.FetchMerged. Listing
// only the live table left the schema dropdown empty — and it is a <select>
// with no free-text fallback, so the recover page became unusable against
// archive-only data (#1065). schema_snapshots is never partitioned and rotate
// never touches it, so it outlives the events it describes.
//
// UNION rather than tablesForSchema's prefer-then-fallback: a schema dropped
// from the source is absent from the latest snapshot, yet its archived events
// are still recoverable and must stay listed.
//
// The snapshot half is gated on archives being reachable (see below), so this
// is strictly additive: a server that cannot read archives keeps the exact
// pre-#1065 listing.
//
// Two residual gaps, both out of scope and both the same shape — this endpoint
// answers "which schemas does this index know of", NOT "which schemas have
// retrievable data in a given window":
//   - a fresh index pointed at foreign archives with no local snapshot still
//     lists nothing; enumerating schemas from the Parquet itself would mean
//     scanning every archive file on each dropdown load.
//   - `rotate --retain` WITHOUT `--archive-dir` drops partitions and writes no
//     Parquet, so a listed schema may have no data anywhere. The designed
//     signal for that is status's continuity verdict and its EVENTS
//     PERMANENTLY LOST banner (#649) — an empty dropdown was only ever an
//     accidental proxy for it, and an empty dropdown on a healthy archived
//     index is the very bug being fixed here.
//
// The resolver is loaded once when the bundle opens (manager.go, server.go), so
// a snapshot taken after the console started is not picked up until restart.
func (b *bundle) distinctSchemas(ctx context.Context) ([]string, error) {
	rows, err := b.db.QueryContext(ctx, "SELECT DISTINCT schema_name FROM binlog_events ORDER BY schema_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	live, err := scanStrings(rows)
	if err != nil {
		return nil, err
	}
	if b.noArchive {
		// Archives are never consulted for this server (--no-archive, or ANY
		// active RBAC profile — see newBundleDerived), so a schema that survives
		// only in the snapshot is unreachable BY CONSTRUCTION: the union's whole
		// justification is that the archives still answer. Advertising it would
		// offer the operator a target this server provably cannot return a row
		// for. Live-only here is byte-identical to the pre-#1065 behaviour.
		return live, nil
	}
	return mergeSchemaNames(live, b.resolver), nil
}

// mergeSchemaNames folds the snapshot's schemas into the observed ones,
// deduplicated and sorted. A nil resolver (no snapshot loaded) returns the
// observed names unchanged, so the pre-snapshot behaviour is preserved.
func mergeSchemaNames(live []string, r *metadata.Resolver) []string {
	if r == nil {
		return live
	}
	seen := make(map[string]bool, len(live))
	out := make([]string, 0, len(live))
	for _, s := range live {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, t := range r.AllTables() {
		if !seen[t.Schema] {
			seen[t.Schema] = true
			out = append(out, t.Schema)
		}
	}
	sort.Strings(out)
	return out
}

// tablesForSchema lists the tables of one schema. It prefers the latest schema
// snapshot (authoritative, includes tables with no recent events) and falls
// back to the distinct tables observed in binlog_events when no snapshot covers
// the schema.
func (b *bundle) tablesForSchema(ctx context.Context, schema string) ([]string, error) {
	if b.resolver != nil {
		if metas := b.resolver.Tables(schema); len(metas) > 0 {
			out := make([]string, len(metas))
			for i, m := range metas {
				out[i] = m.Table
			}
			return out, nil
		}
	}
	rows, err := b.db.QueryContext(ctx,
		"SELECT DISTINCT table_name FROM binlog_events WHERE schema_name = ? ORDER BY table_name", schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStrings(rows)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// scanStrings collects a single-column string result set. The returned slice
// is non-nil so it JSON-encodes as [] rather than null.
func scanStrings(rows *sql.Rows) ([]string, error) {
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// clampLimit enforces the default/maximum result caps: a non-positive request
// becomes the default; an oversized request is capped at the maximum.
func clampLimit(n, def, maxLimit int) int {
	if n <= 0 {
		return def
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

// atoiDefault parses s as an int, returning def when s is empty or invalid.
func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

// writeFetchError maps a cross-source fetch failure onto the right HTTP
// response. The interesting case: a registry index that predates one of the
// post-initial-schema binlog_events columns (connection_id, or #699's
// query_text/query_hash) fails the events SELECT with MySQL error 1054. The
// console deliberately never migrates registry servers (EnsureSchema — an
// ALTER — is confined to the command-line DSN), so instead of a cryptic 500 we
// return an actionable 422 telling the operator how to migrate.
func writeFetchError(w http.ResponseWriter, err error) {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && myErr.Number == 1054 &&
		(strings.Contains(myErr.Message, "connection_id") ||
			strings.Contains(myErr.Message, "query_text") ||
			strings.Contains(myErr.Message, "query_hash")) {
		col := "connection_id"
		for _, c := range []string{"query_text", "query_hash"} {
			if strings.Contains(myErr.Message, c) {
				col = c
			}
		}
		writeJSONError(w, http.StatusUnprocessableEntity,
			"this index predates the "+col+" column, and the console never migrates servers added in the UI; "+
				"run a writer command against it once (bintrail index / stream / agent), or start a console with --index-dsn pointing at it")
		return
	}
	writeJSONError(w, http.StatusInternalServerError, err.Error())
}

// writeRecoverError maps a reversal-script generation failure onto the right
// HTTP response. A *recovery.ScriptBudgetError — the pre-render refusal
// GenerateSQLFromRows/CheckScriptBudget return when the estimated row payload
// exceeds the configured budget (#654), tightened for the console by
// recoverMaxScriptBytes (#849) — gets an actionable 422 telling the operator
// how to get an answer instead of a bare 500: narrow the filter, or reach for
// the CLI, which runs outside the console's shared process and can raise or
// disable the budget entirely.
//
// This builds its own message from the typed error's fields rather than
// reusing ScriptBudgetError.Error() verbatim: that message ends with "raise/
// disable the budget (0 = unlimited)", which is CLI-flag advice
// (--max-script-bytes) that does not apply here — the console exposes no such
// knob (see recoverMaxScriptBytes's doc comment for why), and repeating it
// would send the operator looking for a setting that does not exist.
func writeRecoverError(w http.ResponseWriter, err error) {
	var be *recovery.ScriptBudgetError
	if errors.As(err, &be) {
		writeJSONError(w, http.StatusUnprocessableEntity, fmt.Sprintf(
			"refusing to generate the reversal script — the matched events hold ~%.1f MiB of row data, "+
				"over the console's %.0f MiB budget for a single recovery. Narrow the recovery filter "+
				"(schema/table/pk/time range) to shrink the window, or use `bintrail recover` from the CLI "+
				"for large recoveries — it runs outside the console's shared process and supports "+
				"--max-script-bytes to raise or disable this budget.",
			float64(be.EstimatedBytes)/(1<<20), float64(be.Budget)/(1<<20)))
		return
	}
	writeJSONError(w, http.StatusInternalServerError, err.Error())
}

// gapWarnings renders coverage-gap hours from a query plan into a warning list
// for the API response, or nil when there are none.
func gapWarnings(plan *query.QueryPlan) []string {
	if plan == nil || len(plan.GapHours) == 0 {
		return nil
	}
	return []string{query.FormatGapWarning(plan.GapHours)}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("console: JSON encode failed", "error", err)
	}
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
