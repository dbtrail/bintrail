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

// LimitPerPK trims rows to keep at most n events per pk_values value, keeping
// the latest n (by event_timestamp, event_id). Input is expected to be sorted
// ascending by (event_timestamp, event_id) — the shape produced by
// MergeResults — and the output preserves that ordering. n <= 0 returns rows
// unchanged.
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
