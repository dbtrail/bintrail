package query

import (
	"slices"
	"strings"
)

// ArchiveScope names the set of Parquet archives a read will actually OPEN
// (#1232), so the planner can restrict archive_state coverage to rows that
// describe them: coverage recorded by an archive the fetch will not read is
// not coverage.
//
// It replaces the bare `[]string` of bintrail_ids whose nil-vs-empty
// distinction was got wrong twice (#1327). "Discovery succeeded and found
// zero sources" naturally comes back as a nil slice (`var x []string` +
// append), and nil was the spelling for "every archive in the index" — so
// "I resolved nothing" silently read as "I resolved everything", the exact
// false OK the scope exists to remove. The two states are now distinct
// constructors that cannot be produced by accident:
//
//   - AllArchives(): every archive registered in the index — the only honest
//     answer for a caller that reads them all, or that cannot enumerate the
//     set (a failed discovery).
//   - OnlyArchives(ids...): exactly these archives, by bintrail_id.
//     OnlyArchives() with no ids is a read that opens NO archives, so every
//     rotated hour is a gap.
//   - ScopeFromPaths(paths): OnlyArchives over the ids embedded in resolved
//     archive source paths. It never returns AllArchives.
//
// The zero value equals OnlyArchives() — opens none. That is the safe
// default: a forgotten scope reports rotated hours as gaps instead of
// crediting coverage the fetch will never open.
//
// Note what a scope deliberately does NOT select by: which source PRODUCED
// the events. binlog_events carries no source discriminator and
// ArchivePartition archives the whole shared partition, so one source's
// archive of hour H holds every source's events for H.
// archive_state.bintrail_id records who archived a partition, not whose rows
// are in it — scoping by data ownership would report gaps over data that is
// present.
type ArchiveScope struct {
	// all is a separate bit rather than a sentinel ids value, so no ids
	// slice a caller can build — nil, empty, or otherwise — accidentally
	// means "every archive".
	all bool
	ids []string
}

// AllArchives returns the scope of a read that opens every archive registered
// in the index — or that cannot enumerate its set and must not invent one
// (an empty guess would report every rotated hour as a gap).
func AllArchives() ArchiveScope {
	return ArchiveScope{all: true}
}

// OnlyArchives returns the scope of a read that opens exactly the archives
// named by these bintrail_ids. With no ids it is the scope of a read that
// opens NO archives — the state that must never be confused with
// AllArchives, because for it every rotated hour is a gap.
func OnlyArchives(ids ...string) ArchiveScope {
	if len(ids) == 0 {
		// Normalise to the zero value so all "opens none" spellings compare
		// equal regardless of how the (absent) ids were passed.
		return ArchiveScope{}
	}
	return ArchiveScope{ids: slices.Clone(ids)}
}

// ScopeFromPaths derives the scope from resolved archive source paths — the
// bintrail_ids embedded in each base's Hive marker `bintrail_id=<id>`
// (extractBasePath is what puts it there, for both local dirs and
// `s3://bucket/prefix` sources), so the caller that resolved the sources
// already holds the scope without a second database read.
//
// It NEVER returns AllArchives: resolving no sources — a nil or empty input —
// is a read that opens no archives, and a path with no marker is DROPPED
// rather than widening the scope. Such a path can only come from a caller
// that bypassed extractBasePath, and counting an unidentifiable archive as
// "every archive" is the same false OK this type exists to prevent. Callers
// for whom unscoped is the honest answer (a failed discovery, an index-wide
// gauge) say so with AllArchives() themselves.
func ScopeFromPaths(paths []string) ArchiveScope {
	seen := make(map[string]bool, len(paths))
	var ids []string
	for _, p := range paths {
		id := sourceIDFromBase(p)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ArchiveScope{ids: ids}
}

// opensNone reports a scope under which no archive_state row can describe
// coverage the read will see. Coverage loaders return early on it — both
// because the answer is known and because the SQL rendering would be
// `IN ()`, a syntax error rather than an empty set.
func (s ArchiveScope) opensNone() bool {
	return !s.all && len(s.ids) == 0
}

// clause renders the optional `WHERE bintrail_id IN (...)` that restricts an
// archive_state read to the scope. AllArchives yields no clause at all —
// every registered archive counts. The opens-none case never reaches here:
// callers guard with opensNone first.
func (s ArchiveScope) clause() (string, []any) {
	if s.all || len(s.ids) == 0 {
		return "", nil
	}
	args := make([]any, len(s.ids))
	for i, id := range s.ids {
		args[i] = id
	}
	return " WHERE bintrail_id IN (?" + strings.Repeat(", ?", len(s.ids)-1) + ")", args
}

// sourceIDFromBase pulls the id out of a base path ending in
// `bintrail_id=<id>`. It matches on the marker rather than on the id's shape:
// rotate's --bintrail-id takes an arbitrary string verbatim, and a reader
// stricter than the writer would silently skip every archive under a
// human-named id (the #392 review lesson, in the discovery direction).
func sourceIDFromBase(base string) string {
	const marker = "bintrail_id="
	i := strings.LastIndex(base, marker)
	if i < 0 {
		return ""
	}
	id := strings.TrimSuffix(base[i+len(marker):], "/")
	if strings.Contains(id, "/") {
		return ""
	}
	return id
}
