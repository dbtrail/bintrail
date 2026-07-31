package query

import (
	"database/sql"
	"errors"
	"log/slog"
)

// SourceFlavor returns the source flavor recorded in stream_state ("mysql",
// "mariadb", "postgres"), the authoritative single-source signal for an index
// database. Best-effort: a nil db, or any read failure (no stream_state row on
// a file-indexed DB, very old schema), returns "" — callers treat empty as
// MySQL-family semantics, the established default (see
// recovery.DialectForFlavor, which owns the canonical "postgres" literal).
// The nil guard lets a caller pass an as-yet-unopened handle directly.
//
// Failures are logged, not swallowed: a missing row (sql.ErrNoRows) is the
// expected shape of a file-indexed / pre-stream index and logs at Debug, but
// any other error (connection blip, old schema without the flavor column) is
// indistinguishable from a legitimate MySQL index once flattened to "" — and
// on the recover path "" selects the MySQL dialect, so a silent flatten is a
// latent wrong-SQL vector. Those log at Warn.
func SourceFlavor(db *sql.DB) string {
	if db == nil {
		return ""
	}
	var flavor string
	if err := db.QueryRow("SELECT flavor FROM stream_state WHERE id = 1").Scan(&flavor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Debug("source flavor unavailable — no stream_state row (file-indexed or pre-stream index); defaulting to MySQL-family semantics")
		} else {
			slog.Warn("source flavor read failed — defaulting to MySQL-family semantics; recovery SQL dialect and gap-check semantics may be wrong for a non-MySQL source",
				"error", err)
		}
		return ""
	}
	return flavor
}

// StreamGTIDSet returns the index's checkpointed GTID coverage
// (stream_state.gtid_set) — the accumulated executed set the streaming daemon
// durably reached. Best-effort, mirroring SourceFlavor: a nil db, a missing
// row (file-indexed index), a NULL column (position-mode stream), or any read
// failure returns "". Unlike SourceFlavor, failures log at Debug only: an
// empty result merely degrades the baseline↔first-event gap check to its
// conservative position heuristic (reconstruct.DecideBaselineGap), it never
// selects a SQL dialect.
func StreamGTIDSet(db *sql.DB) string {
	if db == nil {
		return ""
	}
	var gtidSet sql.NullString
	if err := db.QueryRow("SELECT gtid_set FROM stream_state WHERE id = 1").Scan(&gtidSet); err != nil {
		slog.Debug("indexed GTID coverage unavailable — gap checks fall back to position comparison", "error", err)
		return ""
	}
	return gtidSet.String
}
