package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// This file holds the streamed event-window fold shared by both full-table
// reconstruct paths (#1097).
//
// The full-table merge is a hash join: the change map is the build side, the
// baseline Parquet scan is the probe side. The build side must be COMPLETE
// before the probe starts, because the probe visits each baseline row once and
// needs that PK's final image right then. What does NOT have to be complete
// first is the raw event window that the map is folded from — and that is what
// used to be materialized whole (one query.FetchMerged call for the entire
// window, every event carrying both decoded JSON row images).
//
// So the fetch is paged and folded incrementally: peak memory moves from
// "every event in the window" to "one page + one entry per distinct touched
// PK". The map itself is still unbounded in the number of touched PKs — see
// #1107, which bounds that half by hash-partitioning the join.

// retainEvent returns a heap copy of ev trimmed to what the merge downstream
// actually reads, for storing in the change map.
//
// The copy is the load-bearing part. Storing &page[i] would pin the ENTIRE
// page's backing array — including every row_before in it — for as long as the
// map lives, so a paged fetch would bound nothing: the map would keep every
// page alive anyway. Copying the struct out lets each page be collected as soon
// as the next one is fetched.
//
// Fields dropped, and why it is safe to drop them:
//
//   - RowBefore: mergeBaselineImages emits row_after only, and both guards that
//     read the before-image (#592 unresolved-TOAST, #782 PK-changing UPDATE)
//     have already run on the FULL event in foldEventWindow, per event, before
//     this call. Moving those checks upstream of the trim is precisely what
//     makes dropping it safe — they must never be re-expressed against the map,
//     where they would read a nil before-image and silently pass.
//   - QueryText/QueryHash: forensic capture (#699), capped at 16 KiB per event.
//     No merge stage reads them, and retaining them would let a
//     statement-logging source dominate the map.
//
// Anything a future merge stage needs must be added back here deliberately —
// a field read from a map entry that this function blanks reads as empty, not
// as missing.
func retainEvent(ev *query.ResultRow) *query.ResultRow {
	kept := *ev
	kept.RowBefore = nil
	kept.QueryText = nil
	kept.QueryHash = nil
	return &kept
}

// foldPage folds one fetched page into changes, running the per-event
// correctness guards on each untrimmed event first.
//
// It is the single place where an event is inspected before being trimmed, and
// therefore the only place a before-image guard can live once the window is
// paged. Split out from foldEventWindow so those guards are unit-testable
// without a MySQL index connection — including across a page BOUNDARY, which is
// the case a whole-slice scan could never get wrong and a paged fold could.
//
// Guards, per event:
//
//   - #592: a residual unchanged-TOAST marker would be written into the
//     reconstructed dump as the marker's own JSON — silent corruption.
//   - #782: the change map is keyed by the BEFORE-image PK, so folding an
//     UPDATE whose PK changed would duplicate, resurrect, or silently drop rows.
//     Checking per event rather than over the finished map catches the
//     permutation a map scan structurally cannot see (`UPDATE pk 1→2` followed
//     by `INSERT pk=1`, where the INSERT overwrites the UPDATE under key "1") —
//     and it catches it no matter which page each event landed on.
//
// Both refuse the whole run. That is deliberate and matches the pre-existing
// stance for these two conditions: the alternative is a dump that loads cleanly
// and is wrong.
func foldPage(
	page []query.ResultRow,
	schema, table string,
	pkCols []metadata.ColumnMeta,
	changes map[string]*query.ResultRow,
) error {
	for i := range page {
		ev := &page[i]
		if err := checkEventToast(*ev); err != nil {
			return err
		}
		if before, after, ok := pkChangedInEvent(ev, pkCols); ok {
			return pkChangingUpdateErr(schema, table, before, after)
		}
		changes[ev.PKValues] = retainEvent(ev)
	}
	return nil
}

// foldConfig drives foldEventWindow.
type foldConfig struct {
	DB       *sql.DB
	Engine   *query.Engine
	DBName   string
	Resolver *metadata.Resolver

	Schema string
	Table  string
	PKCols []metadata.ColumnMeta

	// Opts is the event filter for the window. Limit/LimitPerPK/Order/AfterEvent
	// must be unset — the stream owns paging and rejects a caller that presets
	// them.
	Opts query.Options

	AllowGaps      bool
	ArchiveFetcher query.ArchiveFetcher

	// BatchSize is the fetch page size; 0 → query.DefaultStreamBatchSize.
	BatchSize int

	// WarnEventThreshold / Parallelism drive the #654/#842 volume warning.
	WarnEventThreshold int64
	Parallelism        int
}

// foldResult is the completed build side of the merge.
type foldResult struct {
	// Changes maps pk_values → the LAST event for that PK in the window,
	// trimmed by retainEvent. Last-write-wins survives paging because pages
	// arrive in ascending (event_timestamp, event_id) order and each page is
	// folded in order, so a PK touched in page 1 and again in page 3 ends up
	// holding page 3's image — the same result a single-slice fold produced.
	Changes map[string]*query.ResultRow

	// Total is the number of events folded across every page (the
	// archive-inclusive, deduplicated count — the same number the old
	// len(events) reported).
	Total int64

	// First is a copy of the very first event of the window, or nil when the
	// window was empty. Kept because the baseline-vs-first-event gap warning
	// (#781) needs it and the page it came from is long gone by then.
	First *query.ResultRow
}

// foldEventWindow streams the event window described by fc and folds it into a
// change map, running the per-event correctness guards on the way.
//
// Per page, in order:
//
//  1. ENUM/SET ordinals → labels and BLOB/TEXT base64 → real values, both
//     resolved against the schema snapshot in effect at each event's OWN
//     timestamp (#475/#476/#668). A page is a valid unit for this because the
//     decoding is per-event, not window-relative.
//  2. The #592 unresolved-TOAST and #782 PK-changing-UPDATE guards, per event,
//     on the untrimmed event.
//  3. The fold, via retainEvent.
//
// Running the guards per event over the raw stream is strictly stronger than
// the map-level checks it replaces: the map only ever held the surviving last
// event per PK, so a PK-changing UPDATE whose old key a later event reused was
// invisible to it. That is why fulltable.go's baseline path no longer calls
// pkChangingUpdate/checkChangesToast on the map at all — a nil-before-image map
// would make those calls unconditionally pass while still reading as guards.
func foldEventWindow(ctx context.Context, fc foldConfig) (*foldResult, error) {
	res := &foldResult{Changes: make(map[string]*query.ResultRow)}
	warned := false

	_, err := query.FetchMergedStream(ctx, fc.DB, fc.Engine, query.FetchMergedOptions{
		Opts:      fc.Opts,
		DBName:    fc.DBName,
		NoArchive: false,
		AllowGaps: fc.AllowGaps,
		// nil ArchiveFetcher → the container-safe parquetquery.Fetch, resolved
		// by the caller at the point of use (#510).
		ArchiveFetcher: fc.ArchiveFetcher,
	}, fc.BatchSize, func(page []query.ResultRow) error {
		if len(page) == 0 {
			return nil
		}
		MapEventEnumLabels(fc.DB, fc.Resolver, fc.Schema, fc.Table, page)
		DecodeEventBinaries(fc.DB, fc.Schema, fc.Table, page)

		if res.First == nil {
			first := page[0]
			res.First = &first
		}

		if err := foldPage(page, fc.Schema, fc.Table, fc.PKCols, res.Changes); err != nil {
			return err
		}

		res.Total += int64(len(page))
		// Warn as soon as the running total crosses, not after the last page:
		// a run large enough to trip the threshold is exactly the run that may
		// not survive to reach the end, and a warning the operator never sees
		// is no warning. Latched so it fires once per table, not once per page.
		if !warned && shouldWarnEvents(res.Total, scaledEventThreshold(fc.WarnEventThreshold, fc.Parallelism)) {
			maybeWarnEventVolume(fc.Schema, fc.Table, res.Total, fc.WarnEventThreshold, fc.Parallelism)
			warned = true
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fetch events: %w", err)
	}

	slog.Debug("event window folded",
		"schema", fc.Schema, "table", fc.Table,
		"events", res.Total, "touched_pks", len(res.Changes))
	return res, nil
}
