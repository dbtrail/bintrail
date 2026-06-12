package reconstruct

import (
	"database/sql"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// MapEventEnumLabels rewrites ENUM/SET ordinals in the events' row
// images back to their string labels in place (#472/#476), decoding
// each event with the snapshot in effect at its timestamp (#475).
// Reconstruction surfaces (console Time-travel, `bintrail reconstruct`,
// full-table merge) call this on fetched deltas BEFORE folding them
// onto a baseline, so the merged state carries labels exactly like the
// baseline rows do (mydumper dumps labels as strings; strings pass
// through the mapper untouched).
//
// The raw event-record surfaces — `bintrail query`, the MCP query tool,
// and the console events view — deliberately stay unmapped: the stored
// ordinal is the forensic ground truth, recoverable regardless of later
// enum reshapes.
//
// latest is the caller's already-loaded resolver, used as the fallback
// definition when the epoch lookup is unavailable (nil is fine: the
// remaining degradation is pass-through — raw ordinals, never a guessed
// label).
func MapEventEnumLabels(db *sql.DB, latest *metadata.Resolver, schema, table string, events []query.ResultRow) {
	if len(events) == 0 || db == nil {
		return
	}
	epochs, err := metadata.LoadSnapshotEpochs(db)
	if err != nil {
		slog.Debug("snapshot epoch lookup failed; decoding ENUM/SET with the latest snapshot",
			"schema", schema, "table", table, "err", err)
		epochs = nil
	}
	src := metadata.EnumMapperSource{
		Epochs: epochs,
		ResolverFor: func(id int) (*metadata.Resolver, error) {
			return metadata.NewResolver(db, id)
		},
		Fallback: latest,
	}
	for i := range events {
		m := src.MapperAt(schema, table, events[i].EventTimestamp)
		m.MapImage(events[i].RowBefore)
		m.MapImage(events[i].RowAfter)
	}
}
