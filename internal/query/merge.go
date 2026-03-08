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
