package query

import (
	"cmp"
	"slices"
)

// MergeResults deduplicates rows by event_id, sorts by (event_timestamp, event_id),
// and applies the limit. MySQL rows should be passed first so in the rare case of a
// duplicate event_id the index version is kept.
func MergeResults(rows []ResultRow, limit int) []ResultRow {
	seen := make(map[uint64]struct{}, len(rows))
	unique := rows[:0]
	for _, r := range rows {
		if _, dup := seen[r.EventID]; !dup {
			seen[r.EventID] = struct{}{}
			unique = append(unique, r)
		}
	}
	slices.SortFunc(unique, func(a, b ResultRow) int {
		if c := a.EventTimestamp.Compare(b.EventTimestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.EventID, b.EventID)
	})
	if limit > 0 && len(unique) > limit {
		unique = unique[:limit]
	}
	return unique
}

// MergeAndTrim runs the full post-fetch pipeline: dedup+sort via MergeResults,
// then the per-PK cap, then the global cap. The order is load-bearing —
// applying the global cap before the per-PK cap can truncate ASC-sorted early
// events for one PK and starve others before the per-PK trim runs.
//
// Used by FetchMerged and by the CLI merge path. Exposing this as a helper
// lets unit tests pin the ordering: a future refactor that swaps the sequence
// or drops the per-PK re-trim will fail TestMergeAndTrim_perPKBeforeGlobal.
func MergeAndTrim(rows []ResultRow, limit, limitPerPK int) []ResultRow {
	rows = MergeResults(rows, 0)
	if limitPerPK > 0 {
		rows = LimitPerPK(rows, limitPerPK)
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

// LimitPerPK trims rows to keep at most n per pk_values, preserving the input
// ordering. Implementation: walk in reverse and keep the last n positional
// occurrences per PK, so the helper only returns the timestamp-latest events
// when the caller has already sorted the input ascending by
// (event_timestamp, event_id) — the shape produced by MergeResults.
//
// Precondition: input must be sorted ascending by (event_timestamp, event_id).
// When violated, the function still returns at most n rows per PK, but the
// kept rows are the last n *positionally*, not the timestamp-latest. The
// helper does not validate the sort order: the happy-path callers in this
// repo all feed it post-MergeResults output where the sort is guaranteed,
// and adding a sort check would hide a caller bug under a silent re-sort.
//
// n <= 0 returns rows unchanged.
//
// Used after MergeResults when each source has applied its own per-PK cap
// independently: the union can still exceed n per PK across sources, so a
// final post-merge trim enforces the contract.
func LimitPerPK(rows []ResultRow, n int) []ResultRow {
	if n <= 0 || len(rows) == 0 {
		return rows
	}
	counts := make(map[string]int, len(rows))
	keep := make([]bool, len(rows))
	// Walk in reverse so the latest events per PK are seen first and kept.
	for i := len(rows) - 1; i >= 0; i-- {
		pk := rows[i].PKValues
		if counts[pk] < n {
			counts[pk]++
			keep[i] = true
		}
	}
	out := rows[:0]
	for i, r := range rows {
		if keep[i] {
			out = append(out, r)
		}
	}
	return out
}
