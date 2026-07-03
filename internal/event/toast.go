package event

import (
	"fmt"
	"sort"
	"strings"
)

// UnchangedToastKey is the single key of the structurally-distinct marker the
// PostgreSQL decoder (internal/pgcapture) emits for an unchanged-TOAST value
// ('u') that could NOT be resolved from the before-image. It is a one-key
// map[string]any, deliberately NOT a plain string, so it can never collide with
// a real text column that legitimately holds the literal
// "<unchanged-toast>": the indexer serializes a map as a JSON object, while
// every real PG-path text value is a Go string, so the two are structurally
// distinct on disk and any consumer can detect the marker by a type switch on
// the reserved key.
//
// Under REPLICA IDENTITY FULL — the mode bintrail requires (#531) — the before-
// image always carries the real unchanged value, so the decoder resolves 'u'
// from it (RowAfter[col] = RowBefore[col]) and this marker is never persisted.
// The marker is only reachable under a weaker replica identity, where it keeps
// the column visible rather than silently dropped (the never-drop floor).
//
// The constant lives HERE, in the source-neutral event package, rather than in
// internal/pgcapture where the marker is produced: the read side (recovery,
// reconstruct, shim) must detect a residual marker and fail loud (#592), and
// the #528 depguard bans those packages from linking pgcapture (which pulls
// pglogrepl/pgx). pgcapture aliases this constant so the decoder keeps reading
// naturally.
//
// Forward constraint: the collision-freedom argument ("every real value is a Go
// string, the marker is a map") holds only while #530 stores all PG values as
// text. When #533 introduces type-faithful rendering (e.g. parsing jsonb into a
// map), non-string values appear and this structural distinctness must be
// re-validated. IsUnchangedToastMarker already matches STRICTLY (one-key map,
// key present, value true) so a real jsonb object only collides when it is
// byte-for-byte the reserved marker object itself.
const UnchangedToastKey = "__bintrail_unchanged_toast__"

// IsUnchangedToastMarker reports whether v is the residual unchanged-TOAST
// marker: exactly the one-key map the decoder emits
// (map[string]any{UnchangedToastKey: true}), as it round-trips through the
// index's JSON columns and back out of query.UnmarshalRowImage. The match is
// strict — a map with extra keys, a missing key, or a non-true value is NOT the
// marker — so a MySQL JSON column carrying user data can only trip it by
// storing the reserved marker object itself, which is indistinguishable from a
// real leak by construction.
func IsUnchangedToastMarker(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	flag, ok := m[UnchangedToastKey].(bool)
	return ok && flag
}

// UnresolvedToastColumns returns, sorted, the columns of a row image whose
// value is the unchanged-TOAST marker. A nil or marker-free image returns nil.
func UnresolvedToastColumns(image map[string]any) []string {
	var cols []string
	for k, v := range image {
		if IsUnchangedToastMarker(v) {
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)
	return cols
}

// UnresolvedToastError builds the canonical fail-loud error for a residual
// unchanged-TOAST marker on the read side (#592). schema, table, and pkValues
// are best-effort locator context — empty values are elided so callers without
// row context (e.g. the shim's pure resultset builders) still produce a clean
// message.
func UnresolvedToastError(schema, table, pkValues string, cols []string) error {
	var loc strings.Builder
	if schema != "" || table != "" {
		fmt.Fprintf(&loc, " in %s.%s", schema, table)
		if pkValues != "" {
			fmt.Fprintf(&loc, " (pk=%s)", pkValues)
		}
	}
	colList := ""
	if len(cols) > 0 {
		colList = ", column(s) " + strings.Join(cols, ", ")
	}
	return fmt.Errorf("unresolved unchanged-TOAST marker%s%s — capture invariant violated: "+
		"under REPLICA IDENTITY FULL the PostgreSQL decoder resolves every unchanged TOAST value at decode "+
		"time, so a persisted marker means these events were captured without it (or a capture bug); the "+
		"stored image does not contain the real value, and output built from it would silently replace the "+
		"column with the marker. Set REPLICA IDENTITY FULL on the source table and re-capture, or restore "+
		"the column from a baseline", loc.String(), colList)
}

// CheckUnresolvedToast returns the canonical fail-loud error (#592) if any
// value in any of the given row images is the unchanged-TOAST marker; nil
// images are skipped. This is the shared choke-point helper for the read side:
// recovery scans both images of every row before rendering a reversal script,
// reconstruct scans every event it folds.
func CheckUnresolvedToast(schema, table, pkValues string, images ...map[string]any) error {
	for _, img := range images {
		if cols := UnresolvedToastColumns(img); len(cols) > 0 {
			return UnresolvedToastError(schema, table, pkValues, cols)
		}
	}
	return nil
}
