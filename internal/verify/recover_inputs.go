package verify

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// ─── Recover-input verification (#1001) ───────────────────────────────────────
//
// The baseline-anchored and live-source modes both compare FULL-TABLE CONTENT
// reconstructed from the LATEST event per PK (`row_after`, LimitPerPK=1). That
// comparison structurally cannot touch the data `bintrail recover` actually
// consumes to build reversal SQL (see internal/recovery):
//
//   - DELETE → INSERT   reads row_before  (buildInsert; nil row_before refuses)
//   - UPDATE → UPDATE   reads row_before for SET **and** row_after for the WHERE
//                       (buildUpdate; either nil refuses)
//   - INSERT → DELETE   reads row_after   (buildDelete; nil row_after refuses)
//
// So a corrupt `row_before`, a corrupt DELETE pre-image, or a corrupt event that
// a NEWER event on the same PK superseded all pass a content match cleanly while
// `recover` would emit wrong reverse SQL from those same rows. This mode closes
// that hole by walking each PK's event chain in time order and asserting the
// images are internally consistent — a check buildable entirely from data
// already in binlog_events, with no baseline and no live source.

// DefaultRecoverInputsMaxEvents bounds how many events one table's chain walk
// loads (see RecoverInputsConfig.MaxEvents).
const DefaultRecoverInputsMaxEvents = 200_000

// RecoverInputsConfig wires the single data source this mode needs: the index.
// Unlike Config/BaselineConfig there is no baseline and no live source — the
// whole point is that recover's inputs are verifiable from binlog_events alone.
type RecoverInputsConfig struct {
	IndexDB     *sql.DB
	Resolver    *metadata.Resolver
	IndexDBName string
	NoArchive   bool
	// ArchiveFetcher reads archived (Parquet) partitions, so a chain that
	// extends back past the live retention window is still walked whole
	// instead of reading as a truncated one. Required unless NoArchive.
	ArchiveFetcher query.ArchiveFetcher
	// Since/Until bound the window. Since is the operator's --lookback
	// horizon; a chain whose first event in the window is not an INSERT has
	// no predecessor state and is reported INCONCLUSIVE, never as a mismatch.
	Since time.Time
	Until time.Time
	// MaxEvents caps the events loaded per table. 0 → DefaultRecoverInputsMaxEvents.
	MaxEvents int
}

// VerifyRecoverInputs walks the per-PK event chains for one table and asserts
// that the before/after images recover consumes are internally consistent.
//
// It fetches the window in ASCENDING time order with NO LimitPerPK — every
// superseded intermediate event is visited, which is precisely the class the
// content-comparison modes skip. Coverage gaps abort as INCONCLUSIVE (a chain
// with an interior hole cannot be asserted against), never as a mismatch.
func VerifyRecoverInputs(ctx context.Context, cfg RecoverInputsConfig, schema, table string) (TableResult, error) {
	res := TableResult{Schema: schema, Table: table}

	tm, err := cfg.Resolver.Resolve(schema, table)
	if err != nil {
		return res, fmt.Errorf("resolve %s.%s: %w", schema, table, err)
	}
	// Chains are keyed by pk_values, and a PK-changing UPDATE can only be
	// recognized with the PK column list. Without a PK, recover itself falls
	// back to an all-columns WHERE and there is no stable chain identity to
	// walk — report it rather than pretend to have checked.
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return inconclusive(res, "table has no primary key; recover falls back to an all-columns WHERE and the event chain has no stable identity to walk"), nil
	}
	// Gap detection is the guard that keeps a hole in the middle of a chain
	// from reading as a corrupt before-image, and the planner cannot run
	// without the index database name.
	if cfg.IndexDBName == "" {
		return inconclusive(res, "index database name unavailable; coverage-gap detection cannot run, and an undetected gap would read as a false mismatch"), nil
	}

	maxEvents := cfg.MaxEvents
	if maxEvents <= 0 {
		maxEvents = DefaultRecoverInputsMaxEvents
	}

	engine := query.New(cfg.IndexDB)
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, query.FetchMergedOptions{
		Opts:           recoverInputsFetchOptions(schema, table, cfg.Since, cfg.Until, maxEvents),
		DBName:         cfg.IndexDBName,
		NoArchive:      cfg.NoArchive,
		ArchiveFetcher: cfg.ArchiveFetcher,
	})
	if err != nil {
		var gap *query.GapError
		if errors.As(err, &gap) {
			return inconclusive(res, "coverage gap in the window: "+gap.Error()+"; a chain with an interior hole cannot be asserted against"), nil
		}
		return res, fmt.Errorf("fetch events %s.%s: %w", schema, table, err)
	}

	truncated := len(rows) > maxEvents
	if truncated {
		rows = rows[:maxEvents]
	}

	// Baseline (SNAPSHOT) rows live in binlog_events too, but they are
	// read-only state, not deltas — recovery rejects them outright
	// ("baseline rows are read-only"). Folding one into a chain as if it were
	// a change would manufacture a mismatch out of nothing.
	rows = dropSnapshotRows(rows)

	// Normalize the event images exactly as the other reconstruction surfaces
	// do BEFORE comparing them. Both sides of every comparison here are event
	// images, but they can come from DIFFERENT schema epochs: event N-1's
	// row_after may carry a raw ENUM ordinal while event N's row_before
	// carries the label. Same data, different bytes — a false mismatch unless
	// both are mapped through the same epoch-aware pass first.
	reconstruct.MapEventEnumLabels(cfg.IndexDB, cfg.Resolver, schema, table, rows)
	binariesTyped := reconstruct.DecodeEventBinaries(cfg.IndexDB, schema, table, rows)

	colByName := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		colByName[c.Name] = c
	}

	out := checkRecoverChains(recoverChainInput{
		Schema:        schema,
		Table:         table,
		Events:        rows,
		PKCols:        pkCols,
		ColByName:     colByName,
		BinariesTyped: binariesTyped,
		Truncated:     truncated,
	})

	res.Status = out.Status
	res.Detail = out.Detail
	res.EventsChecked = out.Events
	res.ChainsChecked = out.Chains
	res.ChainsInconclusive = out.ChainsNoPredecessor
	return res, nil
}

// recoverInputsFetchOptions builds the event fetch for one table's chain walk.
//
// Two properties are load-bearing and are pinned by
// TestRecoverInputsFetchOptions_VisitsSupersededEvents:
//
//   - LimitPerPK is UNSET. The content-comparison modes fetch LimitPerPK=1
//     (latest event per PK), which is exactly why they cannot see a corrupt
//     event that a newer event on the same key superseded. Setting it here
//     would silently reduce this mode to the check it exists to complement.
//   - Order is ASC. The walk reconstructs state forward in time, and the
//     Limit below must therefore keep the OLDEST events. A prefix of the
//     globally time-ordered stream is simultaneously a prefix of EVERY PK's
//     chain, so each chain walked has no interior hole — which is what keeps a
//     truncated window's findings conclusive.
//
// Limit is maxEvents+1 purely so truncation is DETECTABLE: getting the extra
// row proves the window did not fit, and the walk then reports inconclusive
// instead of silently blessing a partial check.
func recoverInputsFetchOptions(schema, table string, since, until time.Time, maxEvents int) query.Options {
	return query.Options{
		Schema: schema,
		Table:  table,
		Since:  &since,
		Until:  &until,
		Order:  "ASC",
		Limit:  maxEvents + 1,
	}
}

// dropSnapshotRows filters out EventSnapshot rows in place-ish (a fresh slice
// is only allocated when at least one is present, the overwhelmingly common
// case being none).
func dropSnapshotRows(rows []query.ResultRow) []query.ResultRow {
	keep := rows[:0]
	dropped := false
	for _, r := range rows {
		if r.EventType == event.EventSnapshot {
			dropped = true
			continue
		}
		keep = append(keep, r)
	}
	if !dropped {
		return rows
	}
	return keep
}

// ─── The pure chain walk ──────────────────────────────────────────────────────

// recoverChainInput is everything the walk needs. Deliberately free of *sql.DB
// and context so the walk is a pure function — the same shape as classify and
// NewReport, and testable without a database.
type recoverChainInput struct {
	Schema, Table string
	// Events must be in ascending (event_timestamp, event_id) order and must
	// contain no SNAPSHOT rows.
	Events        []query.ResultRow
	PKCols        []metadata.ColumnMeta
	ColByName     map[string]metadata.ColumnMeta
	BinariesTyped bool
	// Truncated reports that the window did not fit the event budget, so the
	// events here are a PREFIX of it.
	Truncated bool
}

// recoverChainOutcome is the walk's verdict plus the counts that make an
// operator able to tell "checked a lot and found nothing" from "checked
// nothing".
type recoverChainOutcome struct {
	Status Status
	Detail string
	// Events is the number of events walked; Chains the distinct PKs.
	Events, Chains int
	// ChainsNoPredecessor counts DISTINCT primary keys that held at least one
	// event with no predecessor state to assert against — the window opened
	// mid-history, or a PK-changing UPDATE moved the row out from under the
	// key. Legitimately unverifiable, never a mismatch. Counted per key, not
	// per event, so it can never exceed Chains.
	ChainsNoPredecessor int
	// Assertions is how many before-image comparisons were actually made:
	// the real measure of what this run proved.
	Assertions int
}

// chainState is one PK's reconstructed state between events.
//
// The three states are distinct on purpose. "unknown" (known=false) means the
// walk cannot say what the row held — the window started mid-history, or a
// PK-changing UPDATE moved the row out from under this key; either way the next
// event is a chain restart and asserts nothing. "absent" (known, !present)
// means the chain positively established that the row does not exist (a
// DELETE). "present" carries the row image itself.
type chainState struct {
	known   bool
	present bool
	row     map[string]any
	// schemaVersion of the event that established this state, so a column-set
	// difference across a DDL boundary is recognized as drift rather than
	// reported as corruption.
	schemaVersion uint32
}

// checkRecoverChains walks every PK's event chain and asserts the images
// recover consumes are internally consistent. Pure: no IO, no globals.
//
// Per event, in ascending time order, it asserts what internal/recovery
// actually requires:
//
//  1. The images recovery dereferences are non-nil (DELETE→row_before,
//     UPDATE→both, INSERT→row_after). recovery refuses on a nil one, so a nil
//     here is a recovery that cannot run: a conclusive mismatch.
//  2. No residual unchanged-TOAST marker in either image — the same up-front
//     scan GenerateSQLFromRows runs before it will render anything.
//  3. UPDATE/DELETE row_before equals the state the chain established just
//     before this event. This is the check the content-comparison modes have
//     no way to make, and the reason a superseded intermediate event matters.
//
// Verdict precedence is deliberately fail-safe: a mismatch is always
// conclusive and wins; otherwise anything that leaves part of the window
// unproven (truncation, an unresolvable value representation, or nothing
// asserted at all) degrades to inconclusive rather than reporting a match this
// run did not earn.
func checkRecoverChains(in recoverChainInput) recoverChainOutcome {
	out := recoverChainOutcome{Events: len(in.Events)}
	states := make(map[string]*chainState)

	var mismatches []string
	var unresolved int
	var firstUnresolved string
	noPredecessor := make(map[string]struct{})

	for i := range in.Events {
		ev := &in.Events[i]

		st, seen := states[ev.PKValues]
		if !seen {
			st = &chainState{}
			states[ev.PKValues] = st
			out.Chains++
		}

		// (1) The nil-image contract, mirrored from internal/recovery's
		// buildInsert/buildUpdate/buildDelete. An INSERT with a nil
		// row_before is NORMAL (there is no prior row) and is not flagged.
		if detail, bad := checkImagesPresent(ev); bad {
			mismatches = append(mismatches, detail)
			// The chain's state after an event whose images are broken is
			// not knowable; restart rather than cascade one defect into
			// every later event on this PK.
			*st = chainState{}
			continue
		}

		// (2) The same unchanged-TOAST refusal recovery performs up front.
		if err := event.CheckUnresolvedToast(ev.SchemaName, ev.TableName, ev.PKValues, ev.RowBefore, ev.RowAfter); err != nil {
			mismatches = append(mismatches, fmt.Sprintf("event %d (pk=%s): %v", ev.EventID, ev.PKValues, err))
			*st = chainState{}
			continue
		}

		// (3) The before-image chaining assertion.
		switch ev.EventType {
		case event.EventInsert:
			// Nothing to assert: recovery reverses an INSERT using only
			// row_after's PK, consuming no prior state. Asserting "the row
			// did not exist" here would be pure false-positive surface
			// (REPLACE INTO, PK reuse after a delete) for no coverage.
			*st = chainState{known: true, present: true, row: ev.RowAfter, schemaVersion: ev.SchemaVersion}

		case event.EventUpdate, event.EventDelete:
			verdict, detail := assertBeforeImage(ev, st, in)
			switch verdict {
			case chainMismatch:
				mismatches = append(mismatches, detail)
			case chainUnresolved:
				unresolved++
				if firstUnresolved == "" {
					firstUnresolved = detail
				}
			case chainNoPredecessor:
				noPredecessor[ev.PKValues] = struct{}{}
			case chainAsserted:
				out.Assertions++
			}

			if ev.EventType == event.EventDelete {
				// The chain now positively knows the row is gone.
				*st = chainState{known: true, present: false, schemaVersion: ev.SchemaVersion}
				continue
			}
			// UPDATE. A PK-CHANGING update is stored under the BEFORE-image
			// PK (internal/parser: "PK from before-image"), so the assertion
			// above was valid — but the row has now MOVED to a different key.
			// Carrying row_after forward under this key would false-mismatch
			// the very next event on it (the real `UPDATE pk 1→2; INSERT
			// pk=1` sequence), so the state becomes unknown instead.
			if pkChangedInEvent(ev, in.PKCols) {
				*st = chainState{}
				continue
			}
			*st = chainState{known: true, present: true, row: ev.RowAfter, schemaVersion: ev.SchemaVersion}

		default:
			// A type recovery has no reversal for (it errors with "unknown
			// event type"). Not silently skipped — an unwalkable event means
			// the chain past it is not knowable.
			mismatches = append(mismatches, fmt.Sprintf("event %d (pk=%s): unknown event type %d; recover cannot reverse it",
				ev.EventID, ev.PKValues, ev.EventType))
			*st = chainState{}
		}
	}

	out.ChainsNoPredecessor = len(noPredecessor)
	out.Status, out.Detail = recoverChainVerdict(out, mismatches, unresolved, firstUnresolved, in.Truncated)
	return out
}

// recoverChainVerdict collapses the walk's findings into a status + detail. Kept
// separate (and pure) so the precedence between "found something wrong" and
// "could not check everything" is stated in exactly one place.
func recoverChainVerdict(out recoverChainOutcome, mismatches []string, unresolved int, firstUnresolved string, truncated bool) (Status, string) {
	scope := fmt.Sprintf("%d event(s), %d chain(s), %d before-image assertion(s)", out.Events, out.Chains, out.Assertions)

	// A mismatch is conclusive regardless of what else could not be checked:
	// the events that WERE walked are real, and recover would consume them.
	if len(mismatches) > 0 {
		detail := fmt.Sprintf("%d recover-input inconsistency(ies) in %s: %s", len(mismatches), scope, mismatches[0])
		if len(mismatches) > 1 {
			detail += fmt.Sprintf(" (and %d more)", len(mismatches)-1)
		}
		return StatusMismatch, detail
	}
	if truncated {
		return StatusInconclusive, fmt.Sprintf("checked %s with no inconsistency, but the window exceeded the event budget and only its oldest events were walked; narrow --since/--lookback or raise --max-events to verify it whole", scope)
	}
	if unresolved > 0 {
		return StatusInconclusive, fmt.Sprintf("checked %s; %d before-image comparison(s) were not conclusive because a value's representation could not be normalized: %s", scope, unresolved, firstUnresolved)
	}
	if out.Assertions == 0 {
		return StatusInconclusive, fmt.Sprintf("walked %s: no chain had a verifiable predecessor state in this window (every chain begins mid-history), so nothing was proven", scope)
	}
	detail := fmt.Sprintf("checked %s", scope)
	if out.ChainsNoPredecessor > 0 {
		detail += fmt.Sprintf("; %d chain(s) began mid-history and were not asserted", out.ChainsNoPredecessor)
	}
	return StatusMatch, detail
}

// chainVerdict is one event's before-image assertion outcome.
type chainVerdict int

const (
	chainAsserted chainVerdict = iota
	chainMismatch
	chainUnresolved
	chainNoPredecessor
)

// assertBeforeImage checks one UPDATE/DELETE's row_before against the state the
// chain established just before it — the core of this mode.
//
// A chain that starts mid-window (state unknown) has no predecessor to compare
// against and is reported as such, NEVER as a mismatch: a false MISMATCH on a
// truncated retention window would make the whole mode untrustworthy, and a
// window starting mid-history is the normal case, not an anomaly.
func assertBeforeImage(ev *query.ResultRow, st *chainState, in recoverChainInput) (chainVerdict, string) {
	if !st.known {
		return chainNoPredecessor, ""
	}
	if !st.present {
		// The chain positively established the row was DELETEd, yet this
		// event carries a before-image for it. That is the chaining
		// assertion failing in its starkest form: recover would re-INSERT or
		// reverse-UPDATE a row the chain says did not exist.
		return chainMismatch, fmt.Sprintf("event %d (pk=%s, %s): row_before is present but the chain established this row was deleted by an earlier event; recover would reverse it against state that does not exist",
			ev.EventID, ev.PKValues, eventTypeLabel(ev.EventType))
	}
	equal, unresolvedCol, diffCol := compareImages(st.row, ev.RowBefore, st.schemaVersion, ev.SchemaVersion, in)
	switch {
	case equal:
		return chainAsserted, ""
	case unresolvedCol != "":
		return chainUnresolved, fmt.Sprintf("event %d (pk=%s) column %q holds an ENUM/SET, JSON, binary, BIT or spatial value whose representation could not be normalized",
			ev.EventID, ev.PKValues, unresolvedCol)
	default:
		return chainMismatch, fmt.Sprintf("event %d (pk=%s, %s): row_before does not match the state the previous event on this primary key left (column %s); recover would generate reversal SQL from a stale or corrupt before-image",
			ev.EventID, ev.PKValues, eventTypeLabel(ev.EventType), diffCol)
	}
}

// compareImages reports whether two event images hold the same data.
//
// It renders both sides through renderCellNormalized — this package's existing
// canonicalizer, which already closes the JSON key-order, zero-date-vs-NULL,
// TIME-fraction and FLOAT/DOUBLE exponent gaps that produced false MISMATCHes
// in the other modes. Reusing it is deliberate: a fresh reflect.DeepEqual over
// the decoded maps would reintroduce every one of those bugs.
//
// Returns (equal, unresolvedColumn, differingColumn). A non-empty
// unresolvedColumn means the difference is NOT conclusive — a deferred-type
// value whose event representation provably cannot be made byte-faithful, the
// same gate deferredReprUnresolved applies in the content modes.
func compareImages(prev, cur map[string]any, prevVer, curVer uint32, in recoverChainInput) (equal bool, unresolvedCol, diffCol string) {
	cols := unionKeys(prev, cur)
	for _, name := range cols {
		_, inPrev := prev[name]
		_, inCur := cur[name]
		if inPrev != inCur {
			// The two images disagree on which columns exist. Across a schema
			// version boundary that is DDL, not corruption; within one
			// version it is a genuine structural divergence.
			if prevVer != curVer {
				return false, name, ""
			}
			return false, "", fmt.Sprintf("%q is present in one image and absent from the other", name)
		}
	}

	col := func(name string) metadata.ColumnMeta {
		if c, ok := in.ColByName[name]; ok {
			return c
		}
		// A column the current schema snapshot no longer carries. Both sides
		// still render through the SAME (empty) metadata, so the comparison
		// stays symmetric — the one property that matters here.
		return metadata.ColumnMeta{Name: name}
	}

	for _, name := range cols {
		c := col(name)
		pv, cv := prev[name], cur[name]
		if bytes.Equal(renderCellNormalized(pv, c), renderCellNormalized(cv, c)) {
			continue
		}
		// The values differ. Before calling it a divergence, ask whether
		// either side is a value class whose event representation this
		// version cannot render faithfully — if so the difference is not
		// conclusive.
		if isDeferredType(c.DataType) &&
			(valueUnresolved(pv, c, in.BinariesTyped) || valueUnresolved(cv, c, in.BinariesTyped)) {
			return false, name, ""
		}
		return false, "", fmt.Sprintf("%q differs", name)
	}
	return true, "", ""
}

// valueUnresolved is deferredValueUnresolved with the nil case pinned to
// "resolved": a nil renders as SQL NULL on both sides unambiguously, so a
// NULL-vs-value difference is a real divergence, not a representation gap.
func valueUnresolved(v any, c metadata.ColumnMeta, binariesTyped bool) bool {
	if v == nil {
		return false
	}
	return deferredValueUnresolved(v, c, binariesTyped)
}

// unionKeys returns the sorted union of two row images' column names, so the
// comparison order (and therefore which column a mismatch names) is stable.
func unionKeys(a, b map[string]any) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// checkImagesPresent mirrors internal/recovery's nil-image guards exactly:
// buildInsert (reversing a DELETE) dereferences row_before, buildUpdate
// dereferences both, buildDelete (reversing an INSERT) dereferences row_after.
// Each of those refuses with an error, and #784 makes one refusal abort the
// WHOLE script — so a nil image here is not a curiosity, it is a recovery that
// cannot run at all for the window containing it.
func checkImagesPresent(ev *query.ResultRow) (string, bool) {
	switch ev.EventType {
	case event.EventDelete:
		if ev.RowBefore == nil {
			return fmt.Sprintf("event %d (pk=%s): DELETE has a nil row_before; recover cannot re-INSERT the deleted row and refuses the whole script",
				ev.EventID, ev.PKValues), true
		}
	case event.EventUpdate:
		if ev.RowBefore == nil {
			return fmt.Sprintf("event %d (pk=%s): UPDATE has a nil row_before; recover cannot build the reverse SET clause and refuses the whole script",
				ev.EventID, ev.PKValues), true
		}
		if ev.RowAfter == nil {
			return fmt.Sprintf("event %d (pk=%s): UPDATE has a nil row_after; recover cannot build the reverse WHERE clause and refuses the whole script",
				ev.EventID, ev.PKValues), true
		}
	case event.EventInsert:
		if ev.RowAfter == nil {
			return fmt.Sprintf("event %d (pk=%s): INSERT has a nil row_after; recover cannot build the reverse DELETE and refuses the whole script",
				ev.EventID, ev.PKValues), true
		}
	}
	return "", false
}

// pkChangedInEvent reports whether an UPDATE moved the row to a different
// primary key. Both keys are recomputed from the SAME event's images (never
// compared against the stored pk_values), so a numeric-representation
// difference between the parser-native stored key and the JSON-decoded images
// cannot produce a false positive — the same construction internal/reconstruct
// uses for its own PK-change detection (#782).
func pkChangedInEvent(ev *query.ResultRow, pkCols []metadata.ColumnMeta) bool {
	if ev.EventType != event.EventUpdate || ev.RowBefore == nil || ev.RowAfter == nil {
		return false
	}
	return event.BuildPKValues(pkCols, ev.RowBefore) != event.BuildPKValues(pkCols, ev.RowAfter)
}

func eventTypeLabel(t event.EventType) string {
	switch t {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
		return "DELETE"
	case event.EventSnapshot:
		return "SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}
