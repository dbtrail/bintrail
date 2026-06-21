// Package cascade reconstructs child rows that an InnoDB foreign-key
// ON DELETE CASCADE removed but never wrote to the binary log.
//
// On MySQL ≤ 8.x (and all MariaDB) InnoDB enforces FK cascades inside the
// storage engine, below the SQL layer that writes the binlog — only the parent
// DELETE is logged, the cascaded child deletes are invisible (MySQL Bug
// #32506, manual §17.19; fixed only in MySQL 9.6 via WL#11249, reversible with
// innodb_native_foreign_keys). So bintrail's binlog index has no before-image
// for the cascaded children and the normal delta-only `recover` has nothing to
// reverse.
//
// The reconstruction insight (from the dbtrail SaaS, issue nethalo/dbtrail#1291):
// the cascade is deterministic, so "what did the cascade delete?" has the same
// answer as "what child rows pointed at the parent in their latest known
// row_after immediately before the parent DELETE?" — and that last row_after IS
// indexed (the child INSERTs/UPDATEs were logged; only the cascade delete was
// not). We synthesize a DELETE event whose RowBefore is that last row_after, so
// the existing recovery generator turns it into a restoring INSERT with no new
// SQL path.
//
// Scope: Phase-1, binlog-history only — no baseline fallback yet, so a child
// untouched within the lookback window is not reconstructed (Phase-2 baseline
// fallback is deferred, #548). Design: drafts/cascade-recovery-port-2026-06-21.md.
package cascade

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// CascadeFK is one foreign-key edge plus its referential delete action.
//
// It is loaded from the index's fk_constraints table (LoadCascadeFKs), which
// since the cascade-recovery Slice A migration carries delete_rule/update_rule.
// `recover` never connects to the source, so the rule must come from the index.
type CascadeFK struct {
	Schema           string // child (dependent) schema
	Table            string // child table
	ConstraintName   string // FK constraint name (one CascadeFK per column; composite FKs share it)
	Column           string // child FK column
	ReferencedSchema string // parent schema
	ReferencedTable  string // parent table
	ReferencedColumn string // parent referenced column (usually its PK)
	DeleteRule       string // CASCADE, RESTRICT, SET NULL, NO ACTION
	UpdateRule       string // ON UPDATE rule
}

// Options tunes the synthesis. Zero values fall back to the dbtrail SaaS
// constants via withDefaults.
type Options struct {
	Lookback       time.Duration // how far before the parent delete to look for child state
	MaxDepth       int           // multi-level cascade recursion cap
	CandidateLimit int           // max child candidate rows per (parent event, FK)
}

func (o Options) withDefaults() Options {
	if o.Lookback == 0 {
		o.Lookback = 30 * 24 * time.Hour // CASCADE_LOOKBACK_DAYS=30
	}
	if o.MaxDepth == 0 {
		o.MaxDepth = 5 // FK_MAX_DEPTH=5
	}
	if o.CandidateLimit == 0 {
		o.CandidateLimit = 1000 // CASCADE_VICTIM_CANDIDATE_LIMIT=1000
	}
	return o
}

// Result is the outcome of cascade synthesis. Victims are the synthetic DELETE
// rows to feed recovery.GenerateSQLFromRows. Incomplete lists every reason the
// reconstruction may be partial (composite FKs skipped, depth/candidate caps
// hit, index anomalies, DDL-skew). For a recovery tool the caller MUST treat a
// non-empty Incomplete as "this recovery is provably partial" and surface it —
// never apply it as if complete. A non-nil error from SynthesizeVictims is an
// operational failure (e.g. an index query failed) that ALSO leaves Victims
// partial; it is reported in Incomplete too, so checking either suffices.
type Result struct {
	Victims    []query.ResultRow
	Incomplete []string
}

// Complete reports whether the reconstruction covered everything it could find.
func (r Result) Complete() bool { return len(r.Incomplete) == 0 }

// SynthesizeVictims reconstructs the cascade-deleted children for a set of
// parent DELETE events. It returns synthetic DELETE rows (RowBefore = each
// child's last known state) ready to feed recovery.GenerateSQLFromRows.
//
// It recurses: synthetic victims become the next layer's parents, so a
// parent→child→grandchild cascade is fully reconstructed, bounded by MaxDepth
// and a per-(table,pk) visited guard that also breaks self-referencing-FK
// cycles. Multi-path cascades (diamonds, multiple FKs to the same parent) emit
// each victim once via an emitted set, so recovery never double-INSERTs a PK.
//
// Coverage is best-effort: every gap is recorded in Result.Incomplete, and an
// operational query failure additionally returns a non-nil error. The caller
// must not present a partial Result as a complete recovery.
func SynthesizeVictims(
	ctx context.Context,
	eng *query.Engine,
	fks []CascadeFK,
	parentDeletes []query.ResultRow,
	opts Options,
) (Result, error) {
	opts = opts.withDefaults()

	var (
		victims    []query.ResultRow
		incomplete []string
		errs       []error
	)
	// addIncomplete records a coverage caveat once per distinct key, so a wide
	// cascade (many parent×FK iterations) cannot flood the list with near-
	// identical strings and bury the important ones.
	reported := map[string]bool{}
	addIncomplete := func(key, msg string) {
		if reported[key] {
			return
		}
		reported[key] = true
		incomplete = append(incomplete, msg)
	}

	// Count columns per constraint so composite (multi-column) FKs can be
	// detected: the single-column victim match below would mis-reconstruct them
	// (matching on one column of a multi-column key), so we skip+flag rather
	// than silently corrupt — the cardinal sin for a recovery tool.
	colsPerConstraint := map[string]int{}
	for _, fk := range fks {
		colsPerConstraint[fk.Schema+"."+fk.Table+"."+fk.ConstraintName]++
	}

	// Index CASCADE edges by the parent (referenced) table they protect.
	// Gate on DeleteRule == "CASCADE" ONLY: dbtrail conflates delete_rule/
	// update_rule (data_plane_router.py:2793), which runs DELETE synthesis on
	// ON UPDATE CASCADE edges — a bug we deliberately do not port.
	byParent := map[string][]CascadeFK{}
	for _, fk := range fks {
		if fk.DeleteRule != "CASCADE" {
			continue
		}
		ckey := fk.Schema + "." + fk.Table + "." + fk.ConstraintName
		if colsPerConstraint[ckey] > 1 {
			addIncomplete("composite:"+ckey, fmt.Sprintf(
				"composite FK %q on %s.%s not supported; its cascade-deleted rows were NOT reconstructed",
				fk.ConstraintName, fk.Schema, fk.Table))
			continue
		}
		byParent[fk.ReferencedSchema+"."+fk.ReferencedTable] = append(
			byParent[fk.ReferencedSchema+"."+fk.ReferencedTable], fk)
	}

	visited := map[string]bool{} // "schema.table|pkvalues" → processed as a parent
	emitted := map[string]bool{} // "schema.table|pkvalues" → already emitted as a victim

	// The cascade fires atomically at the ROOT delete's timestamp T, so every
	// descendant existed at T and the lookback window must end at T for EVERY
	// level — not at each victim's own last-modified time, which is ≤ T and
	// would shrink the window deeper in the chain (missing a grandchild updated
	// after its parent's last event, and skewing the re-parented check). Each
	// layer item therefore carries the originating root T down the recursion;
	// distinct root deletes keep their own T.
	type layerItem struct {
		ev     query.ResultRow
		rootTS time.Time
	}
	layer := make([]layerItem, 0, len(parentDeletes))
	for _, pd := range parentDeletes {
		layer = append(layer, layerItem{ev: pd, rootTS: pd.EventTimestamp})
	}

	for depth := 0; depth < opts.MaxDepth && len(layer) > 0; depth++ {
		var next []layerItem
		for _, item := range layer {
			pev := item.ev
			pkey := pev.SchemaName + "." + pev.TableName + "|" + pev.PKValues
			if visited[pkey] {
				continue
			}
			visited[pkey] = true

			// The deleted parent's image. Synthetic victims carry their last
			// row_after here too, which is what makes the recursion work.
			parentRow := pev.RowBefore
			if parentRow == nil {
				// A DELETE always has a before-image under binlog_row_image=FULL,
				// so a nil here is an index/parser anomaly — the whole subtree
				// under this parent goes unreconstructed. Never silent.
				addIncomplete("noparent:"+pev.SchemaName+"."+pev.TableName, fmt.Sprintf(
					"parent %s.%s pk=%s has no before-image; its cascade subtree was NOT reconstructed",
					pev.SchemaName, pev.TableName, pev.PKValues))
				continue
			}

			since := item.rootTS.Add(-opts.Lookback)
			until := item.rootTS

			for _, fk := range byParent[pev.SchemaName+"."+pev.TableName] {
				refVal, ok := parentRow[fk.ReferencedColumn]
				if !ok {
					// The FK graph (latest snapshot) names a referenced column
					// absent from this parent row — the DDL-skew limitation made
					// real (a renamed/dropped parent key). Drop, but never silent.
					addIncomplete("noref:"+fk.Schema+"."+fk.Table+"."+fk.ConstraintName, fmt.Sprintf(
						"FK %q references column %q absent from %s.%s rows (schema changed since snapshot); its cascade-deleted rows were NOT reconstructed",
						fk.ConstraintName, fk.ReferencedColumn, fk.ReferencedSchema, fk.ReferencedTable))
					continue
				}
				parentPK := valToString(refVal)

				// Latest event per child PK that referenced this parent within the
				// lookback window. LimitPerPK=1 keeps the timestamp-latest event
				// per pk_values. Fetch one MORE than CandidateLimit so an overflow
				// is observable — with LimitPerPK=1 a plain LIMIT=CandidateLimit
				// caps at exactly the limit and hides truncation.
				cands, qerr := eng.Fetch(ctx, query.Options{
					Schema:     fk.Schema,
					Table:      fk.Table,
					ColumnEq:   []query.ColumnEq{{Column: fk.Column, Value: parentPK}},
					Since:      &since,
					Until:      &until,
					Order:      "DESC",
					LimitPerPK: 1,
					Limit:      opts.CandidateLimit + 1,
				})
				if qerr != nil {
					// Operational failure: we never learned whether victims
					// existed, so the result is provably partial. Record it AND
					// return a non-nil error so the caller cannot apply a partial
					// recovery as if complete. Accumulate, don't abort the batch.
					errs = append(errs, fmt.Errorf("victim query failed for %s.%s via %s=%s: %w",
						fk.Schema, fk.Table, fk.Column, parentPK, qerr))
					addIncomplete("queryfail:"+fk.Schema+"."+fk.Table+"."+fk.Column, fmt.Sprintf(
						"victim query for %s.%s failed (recovery is partial): %v", fk.Schema, fk.Table, qerr))
					continue
				}
				if len(cands) > opts.CandidateLimit {
					addIncomplete("truncate:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
						"%s.%s has more than %d cascade victims for one parent; the excess (and their descendants) were NOT reconstructed",
						fk.Schema, fk.Table, opts.CandidateLimit))
					cands = cands[:opts.CandidateLimit]
				}

				for _, cev := range cands {
					switch {
					case cev.EventType == event.EventDelete:
						// Already gone before the cascade fired.
						continue
					case cev.RowAfter == nil:
						// Non-DELETE with no post-image: an anomaly under FULL —
						// flag rather than drop a recoverable row silently.
						addIncomplete("noafter:"+fk.Schema+"."+fk.Table, fmt.Sprintf(
							"%s.%s has events with no post-image (index anomaly); some victims may not be reconstructed",
							fk.Schema, fk.Table))
						continue
					case valToString(cev.RowAfter[fk.Column]) != parentPK:
						// Re-parented before the delete → it survived.
						continue
					}
					vkey := fk.Schema + "." + fk.Table + "|" + cev.PKValues
					if emitted[vkey] {
						// Reached via another cascade path (diamond / multiple FKs
						// to the same parent); emit once so recovery never
						// double-INSERTs the PK.
						continue
					}
					emitted[vkey] = true
					victim := query.ResultRow{
						EventTimestamp: cev.EventTimestamp,
						SchemaName:     fk.Schema,
						TableName:      fk.Table,
						EventType:      event.EventDelete,
						PKValues:       cev.PKValues,
						RowBefore:      cev.RowAfter, // last known state → INSERT target
					}
					victims = append(victims, victim)
					// May itself be a parent (grandchildren); keep the SAME root T.
					next = append(next, layerItem{ev: victim, rootTS: item.rootTS})
				}
			}
		}
		if depth == opts.MaxDepth-1 && len(next) > 0 {
			addIncomplete("depth", fmt.Sprintf(
				"recursion hit MaxDepth=%d; deeper cascade victims were NOT reconstructed", opts.MaxDepth))
		}
		layer = next
	}
	return Result{Victims: victims, Incomplete: incomplete}, errors.Join(errs...)
}

// LoadCascadeFKs reads the FK graph WITH referential rules from the INDEX's
// fk_constraints table (the latest snapshot that recorded FKs), optionally
// scoped to schemas. It returns every FK edge — SynthesizeVictims gates on
// DeleteRule itself — so it is also the loader a future SET NULL path uses.
//
// Source-less by design: `recover` never connects to the source, so the rules
// come from the index (populated at snapshot time since cascade-recovery Slice
// A). Pre-Slice-A snapshots whose delete_rule/update_rule are empty load as
// non-cascade and are simply skipped by the synthesis.
//
// LIMITATION: the FK graph is taken from the LATEST snapshot that recorded FKs,
// not the one in effect at the delete being recovered. If DDL changed the FK
// topology between the delete and the latest snapshot, synthesis uses the newer
// graph. Matching the FK graph to event time is deferred (see #548); acceptable
// because cascade DDL churn mid-recovery-window is rare and the FK-checks-off
// apply tolerates over-/under-inclusion that the operator reviews.
func LoadCascadeFKs(ctx context.Context, indexDB *sql.DB, schemas []string) ([]CascadeFK, error) {
	q := `SELECT schema_name, table_name, constraint_name, column_name,
	       referenced_schema_name, referenced_table_name, referenced_column_name,
	       delete_rule, update_rule
	FROM fk_constraints
	WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM fk_constraints)`
	var args []any
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		q += " AND schema_name IN (" + placeholders + ")"
		for _, s := range schemas {
			args = append(args, s)
		}
	}
	q += " ORDER BY schema_name, table_name, constraint_name, ordinal_position"

	rows, err := indexDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("load cascade FKs from index: %w", err)
	}
	defer rows.Close()
	var out []CascadeFK
	for rows.Next() {
		var fk CascadeFK
		if err := rows.Scan(&fk.Schema, &fk.Table, &fk.ConstraintName, &fk.Column,
			&fk.ReferencedSchema, &fk.ReferencedTable, &fk.ReferencedColumn,
			&fk.DeleteRule, &fk.UpdateRule); err != nil {
			return nil, fmt.Errorf("scan cascade FK row: %w", err)
		}
		out = append(out, fk)
	}
	return out, rows.Err()
}

// valToString renders a JSON-decoded value the way MySQL's
// JSON_UNQUOTE(JSON_EXTRACT(...)) does, so comparisons line up with the
// ColumnEq SQL. The index read path decodes JSON numbers as json.Number
// (UseNumber, #496, to preserve BIGINTs > 2^53), so that is the production type
// for values from RowBefore/RowAfter; float64 is handled too for callers that
// build maps with plain numeric literals.
func valToString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		// Integral values must print without a decimal point (1, not 1.0) to
		// match an INT FK column rendered by JSON_UNQUOTE.
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		// JSON_UNQUOTE(JSON_EXTRACT(...)) of a JSON boolean yields "true"/"false".
		if x {
			return "true"
		}
		return "false"
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
