package icebergexport

import (
	"fmt"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// netOp is the net effect of a window on one primary key: the row that
// exists at the cut (Row non-nil), or nothing (Row nil). PK holds the key
// columns as the row image spelled them, which is what the equality-delete
// file carries.
type netOp struct {
	PK  map[string]any
	Row map[string]any
}

// fold reduces an ascending event window to one netOp per primary key, in
// first-touched order. Last write wins, page after page, because pages arrive
// in (event_timestamp, event_id) order: the same rule the full-table
// reconstruct applies, so the exported table equals `reconstruct` at the same
// cut.
//
// It is this package's own fold rather than reconstruct's because the delete
// file needs the TYPED key columns of a DELETE, which live in its before-image,
// and the shared fold trims before-images away (retainEvent). Per-event guards
// run here on the untrimmed event for the same reason.
type fold struct {
	schema, table string
	pkCols        []metadata.ColumnMeta
	ops           map[string]*netOp
	order         []string
	events        int64
}

func newFold(schema, table string, pkCols []metadata.ColumnMeta) *fold {
	return &fold{schema: schema, table: table, pkCols: pkCols, ops: map[string]*netOp{}}
}

// addPage folds one page. The guards match the full-table reconstruct's:
// an unresolved TOAST marker (#592) and a PK-changing UPDATE (#782) refuse the
// table, because the alternative is a table that reads cleanly and is wrong.
func (f *fold) addPage(page []query.ResultRow) error {
	for i := range page {
		ev := &page[i]
		if err := event.CheckUnresolvedToast(ev.SchemaName, ev.TableName, ev.PKValues, ev.RowBefore, ev.RowAfter); err != nil {
			return err
		}
		if before, after, ok := reconstruct.PKChangedInEvent(ev, f.pkCols); ok {
			return reconstruct.PKChangingUpdateError(f.schema, f.table, before, after)
		}
		var op *netOp
		switch ev.EventType {
		case event.EventInsert, event.EventUpdate:
			if ev.RowAfter == nil {
				return fmt.Errorf("event %d (type %d) for %s.%s pk %q carries no after-image", ev.EventID, ev.EventType, f.schema, f.table, ev.PKValues)
			}
			row := copyMap(ev.RowAfter)
			op = &netOp{PK: pkImage(row, f.pkCols), Row: row}
		case event.EventDelete:
			if ev.RowBefore == nil {
				return fmt.Errorf("DELETE event %d for %s.%s pk %q carries no before-image", ev.EventID, f.schema, f.table, ev.PKValues)
			}
			op = &netOp{PK: pkImage(ev.RowBefore, f.pkCols)}
		default:
			// EventType 0 is the drift shape (#318): a NULL event_type column.
			// Not skippable: a skipped event is a silently wrong table.
			return fmt.Errorf("event %d for %s.%s pk %q has event type %d, which the export cannot apply", ev.EventID, f.schema, f.table, ev.PKValues, ev.EventType)
		}
		if _, seen := f.ops[ev.PKValues]; !seen {
			f.order = append(f.order, ev.PKValues)
		}
		f.ops[ev.PKValues] = op
		f.events++
	}
	return nil
}

// touched returns the net ops in first-touched order.
func (f *fold) touched() []*netOp {
	out := make([]*netOp, 0, len(f.order))
	for _, k := range f.order {
		out = append(out, f.ops[k])
	}
	return out
}

func (f *fold) len() int { return len(f.ops) }

// pkImage extracts the primary key columns of a row image.
func pkImage(row map[string]any, pkCols []metadata.ColumnMeta) map[string]any {
	out := make(map[string]any, len(pkCols))
	for _, c := range pkCols {
		if v, ok := row[c.Name]; ok {
			out[c.Name] = v
			continue
		}
		out[c.Name] = lookupFold(row, c.Name)
	}
	return out
}

func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
