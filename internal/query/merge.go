package query

import (
	"cmp"
	"fmt"
	"slices"
)

// MergeResults deduplicates rows by event_id, sorts by (event_timestamp, event_id)
// in the requested direction, and applies the limit. MySQL rows should be
// passed first so in the rare case of a duplicate event_id the index version
// is kept.
//
// order is normalised via OrderDirection — empty / "ASC" → ascending,
// "DESC" (case-insensitive) → descending. The same direction is applied to
// both sort keys so the ordering is total and deterministic.
func MergeResults(rows []ResultRow, limit int, order string) []ResultRow {
	seen := make(map[uint64]struct{}, len(rows))
	unique := rows[:0]
	for _, r := range rows {
		if _, dup := seen[r.EventID]; !dup {
			seen[r.EventID] = struct{}{}
			unique = append(unique, r)
		}
	}
	descending := OrderDirection(order) == "DESC"
	slices.SortFunc(unique, func(a, b ResultRow) int {
		c := a.EventTimestamp.Compare(b.EventTimestamp)
		if c == 0 {
			c = cmp.Compare(a.EventID, b.EventID)
		}
		if descending {
			return -c
		}
		return c
	})
	if limit > 0 && len(unique) > limit {
		unique = unique[:limit]
	}
	return unique
}

// MergeAndTrim runs the full post-fetch pipeline: dedup+sort, then the per-PK
// cap (which means "latest N per PK"), then the global cap. The sequence is
// load-bearing — applying the global cap before the per-PK cap can truncate
// early events for one PK and starve others.
//
// Internally we always sort ASC for the LimitPerPK pass (its reverse-walk
// "latest N per PK" semantics depend on ascending input — see LimitPerPK's
// precondition). After the per-PK trim, the rows are re-sorted in the
// caller's requested direction before the global cap is applied so the
// returned slice reflects the requested page (oldest N if ASC, newest N if
// DESC), not the wrong end of the ASC list.
//
// Used by FetchMerged and by the CLI merge path. Exposing this as a helper
// lets unit tests pin the ordering: a future refactor that swaps the sequence
// or drops the per-PK re-trim will fail TestMergeAndTrim_perPKBeforeGlobal.
//
// order is passed through to MergeResults (see its doc); pre-#1511 callers
// that pass "" get the historical ascending behavior.
func MergeAndTrim(rows []ResultRow, limit, limitPerPK int, order string) []ResultRow {
	if limitPerPK > 0 {
		// LimitPerPK requires ASC-sorted input to pick the timestamp-latest
		// events per PK.
		rows = MergeResults(rows, 0, "ASC")
		rows = LimitPerPK(rows, limitPerPK)
		// Re-sort in the caller's direction before slicing — otherwise
		// "limit N + Order=DESC" would slice from the wrong end of the
		// ASC-ordered list.
		if OrderDirection(order) == "DESC" {
			rows = MergeResults(rows, 0, "DESC")
		}
	} else {
		rows = MergeResults(rows, 0, order)
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
		// Drift rows (PKValues == "" from defensive scan of NULL
		// pk_values, dbtrail/bintrail#318) would otherwise all collapse
		// onto bucket "" and silently drop beyond the cap, AND collide
		// with any legitimate empty-PK rows. Give each its own per-row
		// bucket so they pass through unconditionally — the \x00
		// prefix can never appear in a real user-supplied PK string.
		if pk == "" {
			pk = fmt.Sprintf("\x00drift:%d", rows[i].EventID)
		}
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
