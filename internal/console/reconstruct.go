package console

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/query"
	"github.com/dbtrail/bintrail/internal/reconstruct"
)

// reconstructMaxEvents caps the binlog events applied to a single row in the
// [baseline, at] window. Reconstruct is scoped to one PK, so this is generous;
// exceeding it means the window is too busy to reconstruct safely, and we refuse
// rather than fold from a truncated event prefix — which would be wrong state,
// not merely incomplete.
const reconstructMaxEvents = 10000

type capabilitiesResponse struct {
	Reconstruct bool `json:"reconstruct"`
}

// handleCapabilities reports which optional console surfaces are enabled so the
// frontend can show/hide them. Today that is just reconstruct (baseline-gated).
func (s *Server) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, capabilitiesResponse{Reconstruct: s.baselineConfigured})
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
// deleted as of `at` (Deleted=true), and no baseline row for the PK (Found=false).
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
	// The endpoint is the real boundary (the UI merely hides the tab): refuse
	// when reconstruct is not configured — no baseline, or an RBAC profile is
	// active (baseline reads bypass redaction; see Server.baselineConfigured).
	if !s.baselineConfigured {
		writeJSONError(w, http.StatusNotFound,
			"reconstruct is not available (no baseline configured, or an RBAC profile is active)")
		return
	}

	q := r.URL.Query()
	schema, table, pk := q.Get("schema"), q.Get("table"), q.Get("pk")
	if schema == "" || table == "" || pk == "" {
		writeJSONError(w, http.StatusBadRequest, "reconstruct requires schema, table, and pk")
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
	pkCols, err := s.pkColumns(schema, table)
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
	path, snapshotTime, err := reconstruct.FindBaseline(ctx, s.baselineSrc, schema, table, atTime)
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

	resp := reconstructResponse{
		Schema: schema, Table: table, PK: pk,
		At:           atTime.Format(consoleTSFormat),
		BaselineTime: snapshotTime.Format(consoleTSFormat),
	}
	if baselineRow == nil {
		// No baseline row for this PK: a clean "not found", not an error.
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.Found = true

	// 2. Fetch this PK's binlog events in (baseline, at], oldest-first.
	//    AllowGaps defaults FALSE — the opposite of events/recover: a coverage
	//    gap here means a silently-wrong reconstruction, not a few missing
	//    deltas in a script a human reviews. The window is bounded both ends.
	opts := query.Options{
		Schema:   schema,
		Table:    table,
		PKValues: pk,
		Since:    &snapshotTime,
		Until:    &atTime,
		Order:    "", // ASC: ApplyAt/BuildHistory require chronological input.
		Limit:    reconstructMaxEvents + 1,
	}
	rows, plan, err := query.FetchMerged(ctx, s.db, s.engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         s.dbName,
		NoArchive:      s.noArchive,
		AllowGaps:      allowGaps,
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err != nil {
		var gapErr *query.GapError
		if errors.As(err, &gapErr) {
			writeJSONError(w, http.StatusUnprocessableEntity,
				"refusing to reconstruct over a coverage gap — "+err.Error()+" (pass allow_gaps=true to override)")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(rows) > reconstructMaxEvents {
		writeJSONError(w, http.StatusUnprocessableEntity,
			fmt.Sprintf("too many events (>%d) for this row between the baseline and the target time to reconstruct safely; narrow the time or use the offline `bintrail reconstruct`", reconstructMaxEvents))
		return
	}
	resp.EventCount = len(rows)
	resp.Warnings = gapWarnings(plan)

	// 3. Fold to point-in-time state, or trace the full history.
	if history {
		resp.History = toStateEntryDTOs(reconstruct.BuildHistory(baselineRow, snapshotTime, rows, atTime))
	} else if state := reconstruct.ApplyAt(baselineRow, rows, atTime); state == nil {
		resp.Deleted = true // deleted as of `at` — distinct from Found=false
	} else {
		resp.State = state
	}
	writeJSON(w, http.StatusOK, resp)
}

// pkColumns returns the primary-key column names for schema.table from the
// loaded snapshot, in ordinal order.
func (s *Server) pkColumns(schema, table string) ([]string, error) {
	if s.resolver == nil {
		return nil, errors.New("no schema snapshot available to determine primary-key columns; run `bintrail snapshot`")
	}
	tm, err := s.resolver.Resolve(schema, table)
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
			Deleted: e.State == nil,
			State:   e.State,
		}
	}
	return out
}

// isTrue reports whether a query-param flag is set to a truthy value.
func isTrue(v string) bool { return v == "true" || v == "1" }
