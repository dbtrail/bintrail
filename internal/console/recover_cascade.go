package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// recoverCascadeRequest is the JSON body accepted by POST /api/recover-cascade.
// Schema/Table identify the PARENT table whose delete cascaded; the handler
// synthesizes the invisible child victims (ON DELETE CASCADE / SET NULL that
// InnoDB ran below the binlog) and returns reversal SQL — never executing it.
type recoverCascadeRequest struct {
	Schema   string   `json:"schema"`
	Table    string   `json:"table"`
	PK       string   `json:"pk"`
	PKs      []string `json:"pks"`
	Since    string   `json:"since"`
	Until    string   `json:"until"`
	Lookback string   `json:"lookback"`
	MaxDepth int      `json:"max_depth"`
	// AllowIncomplete is reserved (for CLI request symmetry / forward-compat) and
	// currently has NO effect — there is no exit code to gate over a wire, so the
	// response ALWAYS returns 200 carrying `complete` + `incomplete` for the client
	// to decide. It does not buy a fail-closed gate.
	AllowIncomplete bool `json:"allow_incomplete"`
}

// recoverCascadeResponse is text + structured coverage only — never event
// rows, so it stays outside the events-API boundary entirely (there is no
// connection_id/query_text/query_hash field to gate here, exactly like
// recoverResponse). This holds independent of the #701 D1 boundary move —
// connection_id is no longer gated on the events API either, see dto.go.
type recoverCascadeResponse struct {
	SQL            string `json:"sql"`
	StatementCount int    `json:"statement_count"`
	VictimCount    int    `json:"victim_count"`
	SetNullCount   int    `json:"set_null_count"`
	// Complete is a convenience for the client: it is exactly Incomplete being
	// empty (an operational synthesis error is folded into Incomplete too), so the
	// two are always set together — never independently.
	Complete   bool     `json:"complete"`
	Incomplete []string `json:"incomplete,omitempty"`
}

// rbacActive reports whether the console is running under an RBAC profile with
// deny/redact rules. recover-cascade is refused in that mode (see
// handleRecoverCascade) because cascade victim synthesis cannot yet honor column
// redaction / table deny on its internal child fetches.
func (s *Server) rbacActive() bool {
	return len(s.denyTables) > 0 || len(s.redactCols) > 0
}

// handleRecoverCascade serves POST /api/recover-cascade — generates reversal SQL
// for rows hit by a foreign-key ON DELETE CASCADE / SET NULL that InnoDB ran
// below the binlog (MySQL Bug #32506). Like /api/recover it NEVER executes the
// SQL: it synthesizes the invisible victims from the index, builds the script in
// a buffer, and returns it as text for the operator to review.
func (s *Server) handleRecoverCascade(w http.ResponseWriter, r *http.Request) {
	// RBAC guard FIRST — it is process-global and needs no bundle, so refuse before
	// resolveOr even opens the per-server connection or loads a schema snapshot.
	// cascade.SynthesizeVictims' internal child fetches do NOT carry the bundle's
	// DenyTables/RedactColumns, so a redacted column / denied table could surface
	// in a cascade victim's reversal SQL — a leak the normal recover endpoint does
	// not have (its fetched rows are redacted). Until RBAC is threaded through
	// synthesis (#585), refuse cascade recovery whenever a profile is active. The
	// capability gate is intended to hide the tab once the frontend lands (#580);
	// this guard is the actual enforcement / defense-in-depth backstop.
	if s.rbacActive() {
		writeJSONError(w, http.StatusForbidden,
			"recover-cascade is unavailable while an RBAC redaction profile is active "+
				"(cascade victim synthesis cannot yet honor column redaction / table deny)")
		return
	}

	b := s.resolveOr(w, r)
	if b == nil {
		return
	}

	var body recoverCascadeRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	if body.Schema == "" || body.Table == "" {
		writeJSONError(w, http.StatusBadRequest, "recover-cascade requires schema and table (the parent table whose delete cascaded)")
		return
	}
	if body.PK != "" && len(body.PKs) > 0 {
		writeJSONError(w, http.StatusBadRequest, "pk and pks are mutually exclusive; use one or the other")
		return
	}
	maxDepth := body.MaxDepth
	if maxDepth == 0 {
		maxDepth = 5
	}
	if maxDepth < 1 {
		writeJSONError(w, http.StatusBadRequest, "max_depth must be >= 1")
		return
	}
	lookbackStr := body.Lookback
	if lookbackStr == "" {
		lookbackStr = "30d"
	}
	lookback, err := cliutil.ParseRetain(lookbackStr)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid lookback: "+err.Error())
		return
	}
	since, err := cliutil.ParseTime(body.Since)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid since: "+err.Error())
		return
	}
	until, err := cliutil.ParseTime(body.Until)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid until: "+err.Error())
		return
	}

	synth, err := s.synthesizeCascade(r.Context(), b, cascadeSynthParams{
		Schema:   body.Schema,
		Table:    body.Table,
		PK:       body.PK,
		PKs:      body.PKs,
		Since:    since,
		Until:    until,
		Lookback: lookback,
		MaxDepth: maxDepth,
	})
	if err != nil {
		// writeFetchError, not a bare 500: a registry index that predates a
		// post-initial-schema binlog_events column (query_text/query_hash,
		// #699) fails the parent fetch with MySQL 1054, and the operator
		// should get the same actionable 422 the sibling tabs show.
		writeFetchError(w, err)
		return
	}

	// The explicit endpoint emits over its OWN live-only parent fetch (cascade
	// recovery never reads archives), so the parents appear here; the synthesized
	// victims are children-only, so the parent is re-inserted exactly once.
	rows := append(append([]query.ResultRow{}, synth.ParentDeletes...), synth.Victims...)

	var buf bytes.Buffer
	// Per-bundle dialect (matches handleRecover). For a MySQL/MariaDB index — the
	// only flavor cascade recovery supports — this is byte-identical to the CLI's
	// recovery.New(MySQL).
	gen := recovery.NewForDialect(b.db, b.resolver, recovery.DialectForIndex(b.db))
	n, err := cascaderecover.EmitSQL(&buf, gen, rows, synth.SetNullRows, b.resolver, cascaderecover.Header{
		Schema:         body.Schema,
		Table:          body.Table,
		Parents:        len(synth.ParentDeletes),
		Children:       len(synth.Victims),
		Caveats:        synth.Caveats,
		BaselineActive: synth.BaselineActive,
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, recoverCascadeResponse{
		SQL:            buf.String(),
		StatementCount: n,
		VictimCount:    len(synth.Victims),
		SetNullCount:   len(synth.SetNullRows),
		Complete:       len(synth.Caveats) == 0 && synth.SynthErr == nil,
		Incomplete:     synth.Caveats,
	})
}

// cascadeSynthParams are the already-parsed, validated inputs to a cascade victim
// synthesis. Both the explicit POST /api/recover-cascade endpoint and the
// auto-detected cascade branch of POST /api/recover build one and call
// synthesizeCascade. A zero Lookback/MaxDepth falls back to the cascade engine's
// defaults (30d / depth 5) — what the auto-detect path passes (the friction this
// feature removes is making the operator know their FK graph, not tune knobs).
type cascadeSynthParams struct {
	Schema, Table string
	PK            string
	PKs           []string
	Since, Until  *time.Time
	Lookback      time.Duration
	MaxDepth      int
}

// cascadeSynthResult carries the synthesized invisible extras plus coverage
// caveats, WITHOUT any emitted SQL: each caller composes the final script over
// its own base rows (the explicit endpoint over its live-only parent fetch; the
// recover branch over the merged base rows) so neither double-inserts the parent.
type cascadeSynthResult struct {
	ParentDeletes  []query.ResultRow
	Victims        []query.ResultRow
	SetNullRows    []cascade.SetNullRestore
	Caveats        []string
	SynthErr       error // operational synthesis failure; its text is also folded into Caveats
	BaselineActive bool
}

// synthesizeCascade fetches the parent DELETE events (LIVE index only — cascade
// recovery never searches archives), probes archive coverage, sets up the
// optional Phase-2 baseline fallback, and reconstructs the invisible cascade
// victims / SET NULL restorations. It is the shared engine behind both cascade
// surfaces; it emits NO SQL so each caller composes the script over its own base
// rows. A returned error is an operational fetch/FK-load failure (caller 500s);
// a PARTIAL synthesis is reported via SynthErr + Caveats, never an error.
func (s *Server) synthesizeCascade(ctx context.Context, b *bundle, p cascadeSynthParams) (cascadeSynthResult, error) {
	limit := clampLimit(0, recoverDefaultLimit, recoverMaxLimit)
	del := event.EventDelete

	// DenyTables/RedactColumns are attached for consistency but are empty here (an
	// RBAC profile is refused upstream of every caller of synthesizeCascade).
	parentDeletes, err := b.engine.Fetch(ctx, query.Options{
		Schema:        p.Schema,
		Table:         p.Table,
		PKValues:      p.PK,
		PKValuesIn:    p.PKs,
		EventType:     &del,
		Since:         p.Since,
		Until:         p.Until,
		Order:         "ASC",
		Limit:         limit,
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
	})
	if err != nil {
		return cascadeSynthResult{}, fmt.Errorf("fetch parent deletes: %w", err)
	}

	var caveats []string

	// Live-only trap (mirrors the CLI): probe archives UNCONDITIONALLY — the #569
	// over-recovery guard is about whether archived partitions physically EXIST
	// (so the live index has gaps in the Phase-2 window), orthogonal to whether
	// this console reads them. Never gate the probe on no-archive.
	//   - probe failure → coverage unknown (hard caveat)
	//   - archives exist AND nothing matched live → the parent may itself be
	//     archived (hard caveat: the dangerous "nothing found" case)
	//   - archives exist AND parents found → a child whose events were archived
	//     could be missed → a server log only, NOT a caveat (else every archived
	//     deployment trips INCOMPLETE on every run).
	archivesExist := false
	if archives, aerr := query.ResolveArchiveSources(ctx, b.db); aerr != nil {
		caveats = append(caveats, "could not determine whether archived partitions exist (probe failed: "+aerr.Error()+"); coverage is unknown")
	} else if len(archives) > 0 {
		archivesExist = true
		if len(parentDeletes) == 0 {
			caveats = append(caveats, "no parent DELETE matched in the live index, but the index has archived partitions (cascade recovery does NOT search them); the deleted parent may be archived")
		} else {
			slog.Warn("console: index has archived partitions, which cascade recovery does NOT search (live index only); a child whose events were archived may be missed")
		}
	}

	if len(parentDeletes) >= limit {
		caveats = append(caveats, fmt.Sprintf("parent DELETE events were capped at the limit (%d); narrow pk/since/until", limit))
	}

	// Phase-2 baseline fallback — enabled only when the bundle has a baseline
	// configured (baselineConfigured already folds in --no-archive/profile) AND a
	// resolver is available to encode each baseline row's PK to match pk_values.
	var baselineProvider cascade.BaselineProvider
	if b.baselineConfigured && b.resolver != nil {
		baselineProvider = &cascadeBaselineProvider{source: b.baselineSrc, resolver: b.resolver}
	} else if b.baselineConfigured {
		// Baseline source set but no schema snapshot — degrade to Phase-1 rather than
		// silently, mirroring the CLI. The capability already reports this as false.
		slog.Warn("console: baseline configured but no schema snapshot is available; cascade Phase-2 fallback disabled (run `bintrail snapshot`)")
	}

	var res cascade.Result
	var synthErr error
	if len(parentDeletes) > 0 {
		fks, lerr := cascade.LoadCascadeFKs(ctx, b.db, []string{p.Schema})
		if lerr != nil {
			return cascadeSynthResult{}, fmt.Errorf("load FK graph: %w", lerr)
		}
		res, synthErr = cascade.SynthesizeVictims(ctx, b.engine, fks, parentDeletes, cascade.Options{
			Lookback:        p.Lookback,
			MaxDepth:        p.MaxDepth,
			Baseline:        baselineProvider,
			ArchivesPresent: archivesExist,
		})
	}
	caveats = append(caveats, res.Incomplete...)
	if synthErr != nil {
		caveats = append(caveats, "an index query failed mid-synthesis; the result is partial: "+synthErr.Error())
	}

	return cascadeSynthResult{
		ParentDeletes:  parentDeletes,
		Victims:        res.Victims,
		SetNullRows:    res.SetNullRows,
		Caveats:        caveats,
		SynthErr:       synthErr,
		BaselineActive: baselineProvider != nil,
	}, nil
}

// cascadeParentDetect reports whether schema.table is the referenced (parent)
// side of an ON DELETE CASCADE / SET NULL edge in the latest FK snapshot — the
// cheap, one-index-query signal that a DELETE on it may have cascaded below the
// binlog. Scoped to the parent's own schema, matching the synthesis (cascade only
// reconstructs same-schema children). A detection error is returned so the caller
// can log it and fall back to a plain recover; it must never abort one.
func (s *Server) cascadeParentDetect(b *bundle, schema, table string) (bool, error) {
	edges, err := metadata.CascadeConstraintsInIndex(b.db, []string{schema})
	if err != nil {
		return false, err
	}
	for _, e := range edges {
		if e.ReferencedTable == table && (e.DeleteRule == "CASCADE" || e.DeleteRule == "SET NULL") {
			return true, nil
		}
	}
	return false, nil
}

// rowsContainDeleteOn reports whether any row is a DELETE on the given table — the
// precondition (besides cascade-parent-ness) for auto-routing a recover through
// cascade synthesis. Undoing an INSERT/UPDATE never triggers an InnoDB cascade.
func rowsContainDeleteOn(rows []query.ResultRow, table string) bool {
	for _, r := range rows {
		if r.EventType == event.EventDelete && r.TableName == table {
			return true
		}
	}
	return false
}

// cascadeRecoverResult is the combined cascade-aware reversal cascadeRecover
// produces for an auto-detected parent DELETE.
type cascadeRecoverResult struct {
	SQL            string
	StatementCount int
	VictimCount    int
	SetNullCount   int
	Caveats        []string
}

// cascadeRecover composes ONE cascade-aware reversal script for a recover whose
// target handleRecover auto-detected as a foreign-key parent: the base undo of
// baseRows (already fetched — merged live+archive, RBAC-redacted, every event
// type) PLUS the synthesized invisible children and SET NULL restorations, all
// wrapped in SET FOREIGN_KEY_CHECKS=0/1. baseRows already contains the parent
// DELETE(s); the synthesized victims are children-only (cascade never returns a
// parent row), so the parent is re-inserted exactly once.
func (s *Server) cascadeRecover(ctx context.Context, b *bundle, body recoverRequest, opts query.Options, baseRows []query.ResultRow) (cascadeRecoverResult, error) {
	synth, err := s.synthesizeCascade(ctx, b, cascadeSynthParams{
		Schema: body.Schema,
		Table:  body.Table,
		PK:     body.PK,
		Since:  opts.Since,
		Until:  opts.Until,
		// Lookback/MaxDepth left zero → cascade engine defaults (30d / depth 5).
	})
	if err != nil {
		return cascadeRecoverResult{}, err
	}

	caveats := synth.Caveats
	setNull := synth.SetNullRows
	// SET NULL restorations need the schema snapshot for their PK WHERE clause
	// (EmitSQL errors without a resolver). Rather than fail a recover that can
	// still re-create the CASCADE-deleted rows, drop the restorations and flag
	// them — never silently.
	if b.resolver == nil && len(setNull) > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d ON DELETE SET NULL restoration(s) were skipped: a schema snapshot is required for the restore (run `bintrail snapshot`)", len(setNull)))
		setNull = nil
	}

	rows := append(append([]query.ResultRow{}, baseRows...), synth.Victims...)
	var buf bytes.Buffer
	gen := recovery.NewForDialect(b.db, b.resolver, recovery.DialectForIndex(b.db))
	n, err := cascaderecover.EmitSQL(&buf, gen, rows, setNull, b.resolver, cascaderecover.Header{
		Schema:         body.Schema,
		Table:          body.Table,
		Children:       len(synth.Victims),
		Caveats:        caveats,
		BaselineActive: synth.BaselineActive,
		Combined:       true,
	})
	if err != nil {
		return cascadeRecoverResult{}, err
	}
	return cascadeRecoverResult{
		SQL:            buf.String(),
		StatementCount: n,
		VictimCount:    len(synth.Victims),
		SetNullCount:   len(setNull),
		Caveats:        caveats,
	}, nil
}

// cascadeBaselineProvider implements cascade.BaselineProvider over
// internal/reconstruct: it finds the child table's baseline snapshot, scans it
// for rows referencing the deleted parent, and encodes each row's PK to match
// binlog_events.pk_values so the cascade engine can dedup against Phase-1.
//
// Ported from internal/cli/recover_cascade.go (internal/cli is not importable
// from the console binary). Unifying the two copies — and threading RBAC through
// SynthesizeVictims so the profile guard above can be lifted — is tracked as a
// follow-up.
type cascadeBaselineProvider struct {
	source   string             // local dir or s3:// prefix
	resolver *metadata.Resolver // for child PK columns
}

func (p *cascadeBaselineProvider) BaselineChildren(ctx context.Context, schema, table, fkCol, parentPK string, at time.Time, limit int) (cascade.BaselineLookup, bool, error) {
	path, snap, _, err := reconstruct.FindBaseline(ctx, p.source, schema, table, at)
	if err != nil {
		if errors.Is(err, reconstruct.ErrNoBaseline) {
			return cascade.BaselineLookup{}, false, nil // table not covered → Phase-1 only
		}
		return cascade.BaselineLookup{}, false, err
	}

	tm, err := p.resolver.Resolve(schema, table)
	if err != nil {
		return cascade.BaselineLookup{}, false, fmt.Errorf("resolve %s.%s for baseline: %w", schema, table, err)
	}
	// The FK filter binds parentPK as a STRING against the baseline column.
	// DuckDB coerces it exactly for integer/string FK columns, but for
	// DATETIME/DECIMAL/DATE the string form may not match the stored value and
	// would silently zero-match. Refuse those (flagged as a coverage gap) rather
	// than under-recover silently.
	if !fkFilterSafe(columnDataType(tm, fkCol)) {
		return cascade.BaselineLookup{}, false, fmt.Errorf(
			"baseline scan of %s.%s by FK column %q (type %q) is unsupported (string match may not coerce); baseline augmentation skipped",
			schema, table, fkCol, columnDataType(tm, fkCol))
	}

	// Fetch one more than the cap so truncation is observable.
	fetch := 0
	if limit > 0 {
		fetch = limit + 1
	}
	rows, err := reconstruct.ReadBaselineRows(ctx, path, map[string]string{fkCol: parentPK}, fetch)
	if err != nil {
		return cascade.BaselineLookup{}, false, err
	}
	trunc := false
	if limit > 0 && len(rows) > limit {
		trunc = true
		rows = rows[:limit]
	}

	pkCols := tm.PKColumnMetas()
	out := make([]cascade.BaselineRow, 0, len(rows))
	for _, rrow := range rows {
		// Canonicalize PK values the same way the indexer encoded pk_values, so
		// the dedup key matches a Phase-1 victim's PKValues exactly.
		canon, cerr := reconstruct.CanonicalizePKMap(rrow, pkCols)
		if cerr != nil {
			return cascade.BaselineLookup{}, false, fmt.Errorf("canonicalize baseline PK for %s.%s: %w", schema, table, cerr)
		}
		out = append(out, cascade.BaselineRow{
			PKValues: event.BuildPKValues(pkCols, canon),
			Row:      rrow,
		})
	}
	return cascade.BaselineLookup{SnapshotTime: snap, Rows: out, Truncated: trunc}, true, nil
}

func columnDataType(tm *metadata.TableMeta, name string) string {
	for _, c := range tm.Columns {
		if c.Name == name {
			return c.DataType
		}
	}
	return ""
}

// fkFilterSafe reports whether a string-bound equality filter on a column of
// this DATA_TYPE coerces exactly in DuckDB (integer + string families). Types
// where the string form may diverge from the stored value (datetime, decimal,
// date, …) are excluded so the baseline FK scan never silently zero-matches.
func fkFilterSafe(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "int", "integer", "smallint", "tinyint", "mediumint", "bigint",
		"char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set":
		return true
	default:
		return false
	}
}
