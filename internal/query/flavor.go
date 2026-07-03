package query

import "database/sql"

// SourceFlavor returns the source flavor recorded in stream_state ("mysql",
// "mariadb", "postgres"), the authoritative single-source signal for an index
// database. Best-effort: a nil db, or any read failure (no stream_state row on
// a file-indexed DB, very old schema), returns "" — callers treat empty as
// MySQL-family semantics, the established default (see
// recovery.DialectForFlavor, which owns the canonical "postgres" literal).
// The nil guard lets a caller pass an as-yet-unopened handle directly.
func SourceFlavor(db *sql.DB) string {
	if db == nil {
		return ""
	}
	var flavor string
	if err := db.QueryRow("SELECT flavor FROM stream_state WHERE id = 1").Scan(&flavor); err != nil {
		return ""
	}
	return flavor
}
