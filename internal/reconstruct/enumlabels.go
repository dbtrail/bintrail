package reconstruct

import (
	"database/sql"
	"log/slog"
	"time"

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

// DecodeEventBinaries reverses the storage-side base64 of BLOB/TEXT columns in
// the events' row images in place — the sibling of MapEventEnumLabels for the
// other class of values go-mysql delivers as []byte (#666, same root as recover
// #662 / shim #661). go-mysql delivers BLOB/TEXT as []byte, which marshalRow
// base64-encodes into the stored event JSON; the single-row reconstruction
// surfaces (`bintrail reconstruct`, console Time-travel) fold these events onto
// the baseline via ApplyAt/BuildHistory, which do NOT decode, so without this
// pass the client gets the base64 text instead of the real value.
//
// Call this on fetched deltas BEFORE folding them onto a baseline: event images
// only, never a baseline row (baseline rows are scanned raw from Parquet and
// must not be decoded — a baseline TEXT value that happens to be valid base64
// would be corrupted). The full-table merge path has its own decode
// (decodeChangeBinaries) and must NOT call this — doing so would double-decode.
//
// Each column is typed at the snapshot in effect at its event's timestamp
// (#475-style epoch awareness, matching MapEventEnumLabels): whether a value is
// base64-stored depends on whether the column was delivered as []byte (BLOB/TEXT)
// or string (VARCHAR/CHAR) when the event was captured, so a VARCHAR→TEXT
// widening must not make a wrong-epoch lookup wrongly decode an old plain-string
// value that happens to be valid base64.
//
// Unlike MapEventEnumLabels there is deliberately NO latest-snapshot fallback:
// relabeling an ENUM by the latest definition is harmless (strings pass through),
// but base64-decoding a value the wrong schema calls BLOB/TEXT is DESTRUCTIVE (a
// plain VARCHAR value that happens to be valid base64 would be corrupted to
// garbage bytes). So when the epoch
// typing is unavailable — no snapshots, or a per-epoch resolver that fails to
// load — the value is left as the base64 it was stored as, never decoded by a
// guess.
func DecodeEventBinaries(db *sql.DB, schema, table string, events []query.ResultRow) {
	if len(events) == 0 || db == nil {
		return
	}
	epochs, err := metadata.LoadSnapshotEpochs(db)
	if err != nil {
		slog.Debug("snapshot epoch lookup failed; leaving BLOB/TEXT as stored base64",
			"schema", schema, "table", table, "err", err)
		epochs = nil
	}
	// The per-epoch BLOB/TEXT column map is memoized; the check sits before
	// NewResolver so a snapshot whose resolver fails to load is probed at most
	// once, not once per row.
	memo := make(map[int]map[string]bool)
	binColsAt := func(t time.Time) map[string]bool {
		id, ok := metadata.EpochAt(epochs, t)
		if !ok {
			return nil // no snapshots → no safe typing → leave values as base64
		}
		if m, seen := memo[id]; seen {
			return m
		}
		var m map[string]bool
		if r, nerr := metadata.NewResolver(db, id); nerr == nil && r != nil {
			if tm, rerr := r.Resolve(schema, table); rerr == nil {
				m = binaryColsFromTableMeta(tm)
			}
		}
		memo[id] = m
		return m
	}
	for i := range events {
		binCols := binColsAt(events[i].EventTimestamp)
		decodeImageBinaries(events[i].RowBefore, binCols)
		decodeImageBinaries(events[i].RowAfter, binCols)
	}
}

// decodeImageBinaries decodes the storage-side base64 of every BLOB/TEXT column
// in one event image, in place. No-op when binCols is empty or image is nil.
// The per-image sibling of decodeChangeBinaries (which decodes a change map's
// RowAfter); kept separate so the single-row decode never touches the full-table
// merge path.
func decodeImageBinaries(image map[string]any, binCols map[string]bool) {
	if len(binCols) == 0 || image == nil {
		return
	}
	for col, binary := range binCols {
		if v, ok := image[col]; ok {
			image[col] = decodeStoredBase64(v, binary)
		}
	}
}
