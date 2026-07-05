package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrDestructiveDDL is wrapped into the error CheckDestructiveDDL returns
// when it finds a TRUNCATE/DROP/RENAME on the target table inside the
// reconstruction window (#764).
var ErrDestructiveDDL = errors.New("destructive DDL in reconstruction window")

// CheckDestructiveDDL queries schema_changes for a TRUNCATE TABLE, DROP
// TABLE, or RENAME TABLE detected on schema.table in (since, until] — the
// exact window a baseline+delta merge replays (since is the baseline's
// snapshot time, until is the requested --at / AsOf instant).
//
// TRUNCATE and DROP+re-CREATE emit no row-level binlog events — the parser
// only records them as an audit entry in schema_changes (#700-adjacent DDL
// tracking) — so ReconstructTable's and the shim's _snapshot full-table merge
// have no delta to apply and would silently pass every baseline row straight
// through, resurrecting rows the DDL actually deleted as if they still
// existed at --at (#764). Refusing with a clear, actionable error is the
// chosen fix over auto-truncating-and-replaying-from-the-DDL-point, which
// would be materially more complex and itself error prone (Occam's razor).
//
// RENAME TABLE is included because it moves the table's row-event stream to
// a new name; the baseline for the old name can no longer be trusted to
// represent schema.table's state either.
//
// A missing schema_changes table (a pre-DDL-tracking index, or a caller that
// hasn't run indexer.EnsureSchema) is treated as "nothing to check" rather
// than a hard failure: this is an additive safety net on top of the existing
// reconstruct contract, not a new hard dependency.
func CheckDestructiveDDL(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time) error {
	// schema_name = '' is matched too: parseDDL (internal/parser/parser.go)
	// derives schema/table from the DDL statement text alone via a regex, and
	// an unqualified statement — "TRUNCATE TABLE orders" after a session
	// "USE mydb" — has no schema in the text, so schema_changes.schema_name is
	// recorded empty for it even though the table itself is unambiguous. Since
	// go-mysql's QUERY_EVENT does carry the session's default database
	// (QueryEvent.Schema) but parseDDL doesn't consult it, matching schema_name
	// = '' as a fallback is the safe direction here: it can only widen a match
	// (favoring an over-cautious refusal) never narrow one, so a genuine
	// TRUNCATE/DROP/RENAME can never be missed because of this gap. A future
	// fix to plumb the session database through parseDDL would make this
	// fallback redundant, not wrong.
	const q = `SELECT ddl_type, detected_at FROM schema_changes
		WHERE (schema_name = ? OR schema_name = '') AND table_name = ?
		AND ddl_type IN ('TRUNCATE TABLE', 'DROP TABLE', 'RENAME TABLE')
		AND detected_at > ? AND detected_at <= ?
		ORDER BY detected_at ASC LIMIT 1`

	var ddlType string
	var detectedAt time.Time
	err := db.QueryRowContext(ctx, q, schema, table, since, until).Scan(&ddlType, &detectedAt)
	switch {
	case err == nil:
		return fmt.Errorf(
			"%w: %s on %s.%s detected at %s (between the baseline snapshot and the requested point-in-time) "+
				"emits no row-level binlog events to replay — reconstructing to this point-in-time would silently "+
				"resurrect pre-%s rows as if they still existed; re-baseline the table after this DDL and "+
				"reconstruct from the new baseline instead",
			ErrDestructiveDDL, ddlType, schema, table, detectedAt.UTC().Format(time.RFC3339), strings.ToLower(ddlType))
	case errors.Is(err, sql.ErrNoRows):
		return nil
	default:
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "1146") {
			return nil
		}
		return fmt.Errorf("check schema_changes for destructive DDL on %s.%s: %w", schema, table, err)
	}
}
