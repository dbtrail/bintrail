package console

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// Source labels for capabilitiesResponse.Source. The console reads only the
// index, so the source family is whatever stream_state.flavor records — never a
// live probe of the source database.
const (
	sourceMySQL    = "mysql"
	sourcePostgres = "postgresql"
)

// sourceForDialect maps the index's recovery dialect to a source-family label.
// PostgresDialect is set by DialectForFlavor("postgres"); everything else
// (mysql, mariadb, or an unreadable/legacy index) collapses to "mysql".
func sourceForDialect(d recovery.Dialect) string {
	if d == recovery.PostgresDialect {
		return sourcePostgres
	}
	return sourceMySQL
}

// reconstructMaxEvents caps the binlog events applied to a single row in the
// [baseline, at] window. Reconstruct is scoped to one PK, so this is generous;
// exceeding it means the window is too busy to reconstruct safely, and we refuse
// rather than fold from a truncated event prefix — which would be wrong state,
// not merely incomplete. A var (not const) so tests can lower it to exercise the
// refusal without seeding tens of thousands of rows.
var reconstructMaxEvents = 10000

type capabilitiesResponse struct {
	Reconstruct bool `json:"reconstruct"`
	// Monitor: this process can start/stop monitoring (a control-plane
	// supervisor — `bintrail-console watch`). Process-global, not per-server:
	// it is about what the PROCESS is, not about the selected connection.
	Monitor bool `json:"monitor"`
	// RecoverCascade: the recover-cascade surface is available. Free tier (like
	// recover), so available unless an RBAC redaction profile is active — under a
	// profile cascade victim synthesis cannot honor redaction, so the endpoint
	// refuses (see handleRecoverCascade). Process-global, like Monitor.
	RecoverCascade bool `json:"recover_cascade"`
	// RecoverCascadeBaseline: cascade recovery's Phase-2 (recover children
	// untouched within the window) is active for this server. Per-server, and gated
	// EXACTLY like the handler builds its provider — a baseline source AND a schema
	// snapshot (resolver). baselineConfigured can be true with resolver==nil (a
	// baseline dir set but `bintrail snapshot` never run), where the handler
	// degrades to Phase-1; advertising true there would over-promise.
	RecoverCascadeBaseline bool `json:"recover_cascade_baseline"`
	// BaselineTrigger: this process can create baseline snapshots in-process from
	// the console (the watch daemon opted in with BINTRAIL_CONSOLE_BASELINE_TRIGGER=1).
	// Process-global, like Monitor — the endpoint does the per-server validation
	// (source + baseline destination configured).
	BaselineTrigger bool `json:"baseline_trigger"`
	// VerifyTrigger: this process can run bintrail verify in-process from the
	// console (the watch daemon opted in with BINTRAIL_CONSOLE_VERIFY_TRIGGER=1).
	// Process-global, like BaselineTrigger — the endpoint does the per-server/
	// per-mode validation.
	VerifyTrigger bool `json:"verify_trigger"`
	// Verify: baseline-anchored verify is usable for the SELECTED server — a
	// baseline destination is configured, mirroring Reconstruct's gate (both
	// read baseline state with no RBAC redaction, so an active profile also
	// forces this false — see rbacActive() below). Per-server.
	Verify bool `json:"verify"`
	// VerifyLiveSource: live-source verify is additionally usable — the
	// selected server also has a source DSN configured. Per-server.
	VerifyLiveSource bool `json:"verify_live_source"`
	// ExtensionViews lists console views contributed by an installed
	// extension-view provider (an embedding distribution — a build that wraps the
	// OSS core). Omitted in the stock binary (no provider) and whenever any named
	// profile is active — even a zero-rule one (the provider's data routes are
	// refused then via rbacViewGuard/profileActive, so the SPA must not advertise a
	// nav item that would 403). The SPA reveals one nav item + route ("ext-<id>")
	// per entry. Generic by construction — the core names no specific view.
	ExtensionViews []extensionViewDTO `json:"extension_views,omitempty"`
	Auth           authCapsInfo       `json:"auth"`
	// MCP: the /mcp endpoint is usable — a static console token or a
	// UI-managed MCP token is configured (the endpoint's only accepted
	// credentials; see mcp.go). Process-global, like Monitor. The frontend's
	// "Connect AI client" card keys its ready-vs-explain state on this
	// instead of ever probing /mcp itself.
	MCP bool `json:"mcp"`
	// Version is the running build's version string ("0.36.0"; "dev" or empty
	// on unversioned builds). Presentation-only: the Connect AI client card
	// derives the release-asset download link for the RUNNING version from it.
	Version string `json:"version,omitempty"`
	// Source names the selected server's source database family — "postgresql"
	// or "mysql" — derived per-server from stream_state.flavor (the same field
	// DialectForIndex reads). It drives source-aware PRESENTATION only: the
	// frontend relabels stream vocabulary (LSN vs binlog file/pos/GTID) and shows
	// a connection-id availability note for PostgreSQL, whose pgoutput stream carries no
	// backend connection id. NOT a capability gate — it never hides a surface,
	// only renames or annotates what one shows. Defaults to "mysql" when the
	// bundle can't be resolved, so a degraded console reads as the common case.
	Source string `json:"source"`
}

// authCapsInfo tells the authenticated SPA how it got in and whether a
// password exists: AuthKind gates the logout affordance (only sessions are
// revocable) and PasswordSet picks "Set" vs "Change console password" in the
// command palette. Server-derived on purpose — client-side bookkeeping of
// "how did I log in" goes stale across reloads.
type authCapsInfo struct {
	PasswordSet bool   `json:"password_set"`
	AuthKind    string `json:"auth_kind"` // "token" | "session"
}

// extensionViewDTO is the wire view of one console view contributed by an
// installed ext.ConsoleViewProvider. id keys the nav item, the SPA route
// ("ext-"+id), and the "/api/ext/<id>/" data mount; script is the ES module the
// SPA import()s and calls render(mount, {apiBase, api}) on.
type extensionViewDTO struct {
	ID     string `json:"id"`
	Label  string `json:"label"`
	Script string `json:"script"`
}

// handleCapabilities reports which optional console surfaces are enabled.
// Monitor and Auth are PROCESS-level and must always be reported, even when
// the selected server's index is unreachable — otherwise a broken selection
// (e.g. a monitored source whose per-source index isn't provisioned yet)
// would 502 here and make the frontend's gateCapabilities degrade to {},
// hiding the entire control plane (the Start button, the "+ Add server"
// monitor copy). Reconstruct is per-server (baseline-gated) so it needs the
// bundle; a failed resolve just leaves it false rather than failing the whole
// response. The dead-entry-fails-on-select feedback still comes from the data
// queries (events/status), which resolveOr 502s properly.
func (s *Server) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	kind := "token"
	if authKindFrom(r.Context()) == authKindSession {
		kind = "session"
	}
	resp := capabilitiesResponse{
		Monitor:         s.monitorCtrl != nil,
		BaselineTrigger: s.baselineCtrl != nil,
		VerifyTrigger:   s.verifyCtrl != nil,
		// recover-cascade is the free tier (like recover) and process-global, gated
		// only by the RBAC profile (which would make synthesis leak redacted data).
		RecoverCascade: !s.rbacActive(),
		Auth:           authCapsInfo{PasswordSet: s.passwordLoginEnabled(), AuthKind: kind},
		// The MCP endpoint accepts the static token or the UI-managed one
		// (mcp.go refuses with 403 when neither is configured), so token
		// presence IS the capability.
		MCP:     s.token != "" || s.managedTok.configured(),
		Version: s.version,
		// Default until the bundle resolves: a degraded console renders MySQL
		// vocabulary (the common case), never a blank source.
		Source: sourceMySQL,
	}
	if b, err := s.resolve(r); err == nil {
		resp.Reconstruct = b.baselineConfigured
		// Match the recover-cascade handler's Phase-2 gate exactly so the advertised
		// capability can't over-promise (handler builds the provider only when both
		// a baseline source and a resolver are present).
		resp.RecoverCascadeBaseline = b.baselineConfigured && b.resolver != nil
		// Verify's engine (baseline-anchored and live-source alike) carries no RBAC
		// redaction — see verify_trigger.go — so this reuses baselineConfigured
		// verbatim (not just b.baselineSrc != ""): that field already folds in
		// !noArchive, and verify's own query.FetchMerged calls respect NoArchive
		// too, so a no-archive server would otherwise advertise verify:true and
		// then reliably fail with a coverage-gap error on any window touching a
		// rotated-out hour — worse than just hiding it, like Reconstruct does.
		if s.verifyCtrl != nil && !s.rbacActive() {
			resp.Verify = b.baselineConfigured
			if e, ok := s.selectedEntry(r); ok {
				resp.VerifyLiveSource = e.SourceDSN != ""
			}
		}
		// Per-server source family, read from this index's stream_state.flavor.
		// DialectForIndex is nil-safe and legacy-tolerant (any read error → MySQL),
		// so a pre-flavor index simply presents as MySQL.
		resp.Source = sourceForDialect(recovery.DialectForIndex(b.db))
	} else if !errors.Is(err, ErrUnknownServer) && !errors.Is(err, errNoServers) {
		// Expected for a bad X-Bintrail-Server header or a fresh install;
		// anything else (e.g. a genuine connection failure) would silently
		// strip Time-travel from the UI, so leave an operator trace. Mirrors
		// the Debug/Warn split in connManager.Resolve.
		slog.Warn("console: capabilities resolve failed; reporting reconstruct=false", "error", err)
	}
	// Extension views (ext seam): advertise an installed provider's view so the
	// SPA can reveal its nav item + route. Process-global, like Monitor. Suppressed
	// under an active profile — keyed on s.profileActive (any named profile, even a
	// zero-rule one) to match rbacViewGuard, which refuses the data routes there
	// (advertising a nav item that only 403s would be a lie) — and for an invalid
	// id, which buildHandler declined to mount (advertising it would 404 the data
	// route).
	if p := ext.ConsoleView(); p != nil && !s.profileActive && ext.ValidConsoleViewID(p.ID()) {
		resp.ExtensionViews = []extensionViewDTO{{ID: p.ID(), Label: p.Label(), Script: p.Script()}}
	}
	writeJSON(w, http.StatusOK, resp)
}

// stateEntryDTO is the wire view of a reconstruct.StateEntry (that struct has no
// JSON tags; this keeps the API snake_case and exposes a clear Deleted flag).
type stateEntryDTO struct {
	Time    string         `json:"time"`
	Source  string         `json:"source"` // "baseline" | INSERT | UPDATE | DELETE
	EventID uint64         `json:"event_id"`
	GTID    string         `json:"gtid,omitempty"`
	Deleted bool           `json:"deleted"` // true when this transition deleted the row
	State   map[string]any `json:"state"`   // null when deleted
}

// reconstructResponse distinguishes three outcomes: a row with state, a row
// deleted/absent as of `at` (Found=true, Deleted=true), and a row that never
// existed in the window (Found=false). Deleted and State are point-in-time
// fields only — in history mode they are left zero; read per-entry Deleted from
// History instead.
type reconstructResponse struct {
	Schema       string          `json:"schema"`
	Table        string          `json:"table"`
	PK           string          `json:"pk"`
	At           string          `json:"at"`
	BaselineTime string          `json:"baseline_time"`
	Found        bool            `json:"found"`
	Deleted      bool            `json:"deleted"`
	State        map[string]any  `json:"state"`
	History      []stateEntryDTO `json:"history,omitempty"`
	EventCount   int             `json:"event_count"`
	Warnings     []string        `json:"warnings,omitempty"`
}

// handleReconstruct serves GET /api/reconstruct?schema=&table=&pk=&at=&history=&allow_gaps=
// — a single row's full state "as of T" (baseline + binlog deltas), or its
// history. Read-only: it computes state, it never writes.
func (s *Server) handleReconstruct(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	// The endpoint is the real boundary (the UI merely hides the tab): refuse
	// when reconstruct is not configured FOR THE SELECTED SERVER — no baseline,
	// an RBAC profile is active, or no-archive is set (all collapse into the
	// bundle's baselineConfigured; see newBundleDerived for why archive access
	// is required). Per-server enforcement matters: baseline reads bypass RBAC
	// redaction, so the gate must not leak from one server's config to another.
	if !b.baselineConfigured {
		writeJSONError(w, http.StatusNotFound,
			"time-travel isn't available for this server (no baseline is set up, an access-control profile is active, or archive access is disabled)")
		return
	}

	q := r.URL.Query()
	schema, table, pk := q.Get("schema"), q.Get("table"), q.Get("pk")
	if schema == "" || table == "" || pk == "" {
		writeJSONError(w, http.StatusBadRequest, "schema, table, and pk are all required")
		return
	}

	at, err := cliutil.ParseTime(q.Get("at"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid at: "+err.Error())
		return
	}
	atTime := time.Now().UTC()
	if at != nil {
		atTime = *at
	}
	history := isTrue(q.Get("history"))
	allowGaps := isTrue(q.Get("allow_gaps"))

	// Primary-key column names come from the schema snapshot (ordinal order),
	// so the caller only supplies pipe-delimited values, matching the CLI.
	pkCols, err := b.pkColumns(schema, table)
	if err != nil {
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	pkFilter, err := buildPKFilter(pkCols, pk)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()

	// 1. Locate the baseline at-or-before `at` and read the row's initial state.
	path, snapshotTime, stale, err := b.findBaseline(ctx, schema, table, atTime)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			writeJSONError(w, http.StatusNotFound,
				fmt.Sprintf("no baseline found for %s.%s at or before the target time", schema, table))
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "find baseline: "+err.Error())
		return
	}
	baselineRow, err := reconstruct.ReadBaselineRow(ctx, path, pkFilter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "read baseline: "+err.Error())
		return
	}

	// 2. Fetch this PK's binlog deltas in [baseline, at], oldest-first.
	//    AllowGaps defaults FALSE — the opposite of events/recover: a coverage
	//    gap here means a silently-wrong reconstruction, not a few missing deltas
	//    in a script a human reviews. The window is bounded both ends.
	//    We fetch even when baselineRow == nil: a row created AFTER the baseline
	//    has no baseline entry yet still exists as of `at`, and ApplyAt(nil,
	//    deltas, at) reconstructs it correctly. Reporting found=false before
	//    fetching would mislabel that common case as "never existed".
	opts := query.Options{
		Schema:   schema,
		Table:    table,
		PKValues: pk,
		Since:    &snapshotTime,
		Until:    &atTime,
		Order:    "", // ASC: ApplyAt/BuildHistory require chronological input.
		Limit:    reconstructMaxEvents + 1,
	}
	fmOpts := query.FetchMergedOptions{
		Opts:           opts,
		DBName:         b.dbName,
		NoArchive:      b.noArchive,
		AllowGaps:      allowGaps,
		ArchiveFetcher: parquetquery.Fetch,
	}
	rows, plan, err := query.FetchMerged(ctx, b.db, b.engine, fmOpts)
	if err != nil {
		var gapErr *query.GapError
		if errors.As(err, &gapErr) {
			writeJSONError(w, http.StatusUnprocessableEntity,
				"can't reconstruct across a gap in the captured history — "+err.Error()+" (check \"Continue even if some history is missing\" to proceed anyway)")
			return
		}
		writeFetchError(w, err)
		return
	}
	if len(rows) > reconstructMaxEvents {
		writeJSONError(w, http.StatusUnprocessableEntity,
			fmt.Sprintf("too many events (>%d) for this row between the baseline and the target time to reconstruct safely; narrow the time or use the offline `bintrail reconstruct`", reconstructMaxEvents))
		return
	}
	// Trim a trailing PARTIAL transaction AFTER the overflow check above, not
	// before: trimming reduces len(rows), and running it first would let a
	// window that's genuinely over the cap slip through the >reconstructMaxEvents
	// check (#783).
	rows, err = reconstruct.TrimPartialTailTransaction(ctx, b.db, b.engine, fmOpts, rows, atTime)
	if err != nil {
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	// ENUM/SET ordinals → labels (#476), each delta decoded with the
	// snapshot in effect at its event time (#475); baseline values are
	// already labels and pass through. Must run before the fold below so
	// both State and History carry labels.
	reconstruct.MapEventEnumLabels(b.db, b.resolver, schema, table, rows)
	// BLOB/TEXT columns are stored base64-encoded; decode them on the deltas
	// before the fold so State/History carry the real value, not base64 text
	// (#666). Baseline values are read raw from Parquet and pass through.
	reconstruct.DecodeEventBinaries(b.db, schema, table, rows)

	resp := reconstructResponse{
		Schema: schema, Table: table, PK: pk,
		At:           atTime.Format(consoleTSFormat),
		BaselineTime: snapshotTime.Format(consoleTSFormat),
		EventCount:   len(rows),
		// Surface a stale-baseline fallback (#466) alongside coverage-gap
		// warnings: the table is absent from a newer snapshot, so Time-travel is
		// reconstructing from an older one. The server already logged it; this
		// puts it in front of the operator.
		Warnings: appendStaleWarning(gapWarnings(plan), stale),
	}

	// 3. Fold baseline + deltas. baselineRow may be nil. "existed" = the row was
	//    present at some point in the window (baseline row, or any delta). Three
	//    outcomes: present-with-state / existed-then-deleted-as-of-`at` / never.
	existed := baselineRow != nil || len(rows) > 0
	if history {
		entries, err := reconstruct.BuildHistory(baselineRow, snapshotTime, rows, atTime)
		if err != nil {
			// Residual unchanged-TOAST marker (#592): the stored images can't
			// yield a correct reconstruction. 422 like the coverage-gap refusal —
			// the request is well-formed, the captured history is not usable.
			writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		resp.Found = existed
		resp.History = toStateEntryDTOs(entries)
	} else {
		state, err := reconstruct.ApplyAt(baselineRow, rows, atTime)
		if err != nil {
			writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		switch {
		case state != nil:
			resp.Found, resp.State = true, state
		case existed:
			resp.Found, resp.Deleted = true, true // existed, then deleted as of `at`
		default:
			resp.Found = false // never present in [baseline, at]
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// pkColumns returns the primary-key column names for schema.table from the
// selected server's loaded snapshot, in ordinal order.
func (b *bundle) pkColumns(schema, table string) ([]string, error) {
	if b.resolver == nil {
		return nil, errors.New("no schema snapshot available to determine primary-key columns; run `bintrail snapshot`")
	}
	tm, err := b.resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("no schema snapshot for %s.%s: %w", schema, table, err)
	}
	if len(tm.PKColumns) == 0 {
		return nil, fmt.Errorf("table %s.%s has no primary key; reconstruct requires one", schema, table)
	}
	return tm.PKColumns, nil
}

// buildPKFilter zips ordinal PK column names with the pipe-delimited values.
// Values are used verbatim (no trimming): the binlog-delta fetch matches the raw
// pk against the stored pk_values, which parser.BuildPKValues writes without
// padding, so the baseline lookup must use the identical values or the two
// sources could disagree (matching the baseline but missing the deltas).
func buildPKFilter(cols []string, pk string) (map[string]string, error) {
	vals := strings.Split(pk, "|")
	if len(vals) != len(cols) {
		return nil, fmt.Errorf("pk has %d pipe-delimited value(s) but the primary key has %d column(s): %s",
			len(vals), len(cols), strings.Join(cols, ", "))
	}
	filter := make(map[string]string, len(cols))
	for i, c := range cols {
		filter[c] = vals[i]
	}
	return filter, nil
}

func toStateEntryDTOs(entries []reconstruct.StateEntry) []stateEntryDTO {
	out := make([]stateEntryDTO, len(entries))
	for i, e := range entries {
		out[i] = stateEntryDTO{
			Time:    e.Time.Format(consoleTSFormat),
			Source:  e.Source,
			EventID: e.EventID,
			GTID:    e.GTID,
			// A nil baseline entry means "row absent at baseline" (created
			// later), NOT deleted — only a real DELETE transition is "deleted".
			Deleted: e.State == nil && e.Source != "baseline",
			State:   e.State,
		}
	}
	return out
}

// appendStaleWarning adds a "stale_baseline: …" entry to the warnings list when
// the baseline was an older-snapshot fallback (#466). A no-op for a non-stale
// result, so callers can wire it unconditionally.
func appendStaleWarning(warnings []string, stale reconstruct.StaleWarning) []string {
	if !stale.Stale() {
		return warnings
	}
	return append(warnings, "stale_baseline: "+stale.Message)
}

// isTrue reports whether a query-param flag is set to a truthy value.
func isTrue(v string) bool { return v == "true" || v == "1" }
