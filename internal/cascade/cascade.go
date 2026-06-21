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
// This is a spike (drafts/cascade-recovery-port-2026-06-21.md): Phase-1,
// binlog-history only, no baseline fallback yet.
package cascade

import (
	"context"
	"database/sql"
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

// SynthesizeVictims reconstructs the cascade-deleted children for a set of
// parent DELETE events. It returns synthetic DELETE rows (RowBefore = each
// child's last known state) ready to feed recovery.GenerateSQLFromRows, plus
// best-effort warnings.
//
// It recurses: synthetic victims become the next layer's parents, so a
// parent→child→grandchild cascade is fully reconstructed, bounded by MaxDepth
// and a per-(table,pk) visited guard that also breaks self-referencing-FK cycles.
func SynthesizeVictims(
	ctx context.Context,
	eng *query.Engine,
	fks []CascadeFK,
	parentDeletes []query.ResultRow,
	opts Options,
) (victims []query.ResultRow, warnings []string, err error) {
	opts = opts.withDefaults()

	// Count columns per constraint so composite (multi-column) FKs can be
	// detected: the single-column victim match below would mis-reconstruct them
	// (matching on one column of a multi-column key), so we skip+warn rather
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
	warnedComposite := map[string]bool{}
	for _, fk := range fks {
		if fk.DeleteRule != "CASCADE" {
			continue
		}
		ckey := fk.Schema + "." + fk.Table + "." + fk.ConstraintName
		if colsPerConstraint[ckey] > 1 {
			if !warnedComposite[ckey] {
				warnedComposite[ckey] = true
				warnings = append(warnings, fmt.Sprintf(
					"composite FK %q on %s.%s not supported; its cascade-deleted rows were NOT reconstructed",
					fk.ConstraintName, fk.Schema, fk.Table))
			}
			continue
		}
		byParent[fk.ReferencedSchema+"."+fk.ReferencedTable] = append(
			byParent[fk.ReferencedSchema+"."+fk.ReferencedTable], fk)
	}

	visited := map[string]bool{} // "schema.table|pkvalues" → processed

	// The cascade fires atomically at the ROOT delete's timestamp T, so every
	// descendant existed at T and the lookback window must end at T for EVERY
	// level — not at each victim's own last-modified time, which is ≤ T and
	// would shrink the window deeper in the chain (missing a grandchild updated
	// after its parent's last event, and skewing the re-parented check). Each
	// layer item therefore carries the originating root T down the recursion.
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
				continue
			}

			since := item.rootTS.Add(-opts.Lookback)
			until := item.rootTS

			for _, fk := range byParent[pev.SchemaName+"."+pev.TableName] {
				refVal, ok := parentRow[fk.ReferencedColumn]
				if !ok {
					continue
				}
				parentPK := valToString(refVal)

				// Latest event per child PK that referenced this parent in the
				// lookback window. LimitPerPK=1 picks the newest per pk_values
				// (event_timestamp DESC, event_id DESC).
				cands, qerr := eng.Fetch(ctx, query.Options{
					Schema:     fk.Schema,
					Table:      fk.Table,
					ColumnEq:   []query.ColumnEq{{Column: fk.Column, Value: parentPK}},
					Since:      &since,
					Until:      &until,
					Order:      "DESC",
					LimitPerPK: 1,
					Limit:      opts.CandidateLimit,
				})
				if qerr != nil {
					warnings = append(warnings, fmt.Sprintf(
						"victim query failed for %s.%s via %s=%s: %v",
						fk.Schema, fk.Table, fk.Column, parentPK, qerr))
					continue
				}
				if len(cands) >= opts.CandidateLimit {
					warnings = append(warnings, fmt.Sprintf(
						"victim candidates hit the %d-row cap for %s.%s; some victims may be missed",
						opts.CandidateLimit, fk.Schema, fk.Table))
				}

				for _, cev := range cands {
					switch {
					case cev.EventType == event.EventDelete:
						// Already gone before the cascade fired.
						continue
					case cev.RowAfter == nil:
						// No post-image to restore (index/parser anomaly).
						continue
					case valToString(cev.RowAfter[fk.Column]) != parentPK:
						// Re-parented before the delete → it survived.
						continue
					}
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
			warnings = append(warnings, fmt.Sprintf(
				"recursion hit MaxDepth=%d; deeper victims (if any) not reconstructed", opts.MaxDepth))
		}
		layer = next
	}
	return victims, warnings, nil
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
	query := `SELECT schema_name, table_name, constraint_name, column_name,
	       referenced_schema_name, referenced_table_name, referenced_column_name,
	       delete_rule, update_rule
	FROM fk_constraints
	WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM fk_constraints)`
	var args []any
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query += " AND schema_name IN (" + placeholders + ")"
		for _, s := range schemas {
			args = append(args, s)
		}
	}
	query += " ORDER BY schema_name, table_name, constraint_name, ordinal_position"

	rows, err := indexDB.QueryContext(ctx, query, args...)
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
// ColumnEq SQL. JSON numbers arrive as float64; integral ones must print
// without a decimal point (1, not 1.0) to match an INT FK column.
func valToString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "1"
		}
		return "0"
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
