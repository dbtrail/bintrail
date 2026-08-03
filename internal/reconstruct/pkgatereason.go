package reconstruct

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// PKTypeGateReason renders the refusal detail for a primary-key column a
// MySQL-path SupportedPKType gate rejected. Two physically different causes
// reach such a gate, and they must not share a message (#1009, lifted here
// from internal/verify for the reconstruct and shim surfaces — #1198):
//
//   - A real MySQL DATA_TYPE token the baseline canonicalizer does not handle
//     (float, bit, ...): a genuine per-table limitation the operator can
//     reason about, reported as such.
//   - An EMPTY DataType: MySQL's information_schema always records a
//     DATA_TYPE (the column is NOT NULL and every MySQL snapshot writer
//     stores a non-empty token), so an empty token is the PostgreSQL snapshot
//     shape (WritePGSnapshot stores pg_type_oid, never a MySQL type token —
//     the #533 invariant). Reaching a MySQL-path gate with it means the run
//     selected the MySQL path for a PostgreSQL-shaped index — the source
//     flavor recorded in stream_state did not read "postgres" (unreadable
//     stream_state, or the wrong index database). Blaming the PK type there
//     sends the operator chasing a fixable-looking column problem that
//     nothing they do to the table can fix; the honest verdict names the
//     wrong-path cause.
//
// surface names the feature that took the MySQL path ("verify",
// "reconstruct", "full-table _snapshot", ...) and action the verb it cannot
// perform on a PostgreSQL-sourced table ("verify", "reconstruct",
// "materialize", ...). No CLI flag names in the text: console and wire
// surfaces emit it too.
func PKTypeGateReason(c metadata.ColumnMeta, surface, action string) string {
	if strings.TrimSpace(c.DataType) == "" {
		return fmt.Sprintf("schema snapshot records no MySQL type for primary-key column %q — this is the PostgreSQL snapshot shape, but the index's stream_state flavor did not read \"postgres\", so %s took its MySQL path, which cannot %s a PostgreSQL-sourced table; check that the index database is the one the PostgreSQL stream writes", c.Name, surface, action)
	}
	return fmt.Sprintf("primary-key column %q has type %q unsupported by the baseline canonicalizer", c.Name, c.DataType)
}

// fullTablePKTypeRefusal is the error ReconstructTable's PK-type gate returns
// for a column supportedPKType rejected. An empty DataType gets the honest
// wrong-path verdict (see PKTypeGateReason): the gate sits after the recorded
// source flavor was checked and did NOT read "postgres", so a PG-shaped
// snapshot here means the run took the MySQL path for a PostgreSQL-shaped
// index (typically the wrong index database next to a MySQL-shaped baseline).
// A real MySQL type keeps the per-type refusal the operator can act on.
func fullTablePKTypeRefusal(schema, table string, pkCol metadata.ColumnMeta) error {
	if strings.TrimSpace(pkCol.DataType) == "" {
		return fmt.Errorf("full-table reconstruct: %s.%s: %s", schema, table,
			PKTypeGateReason(pkCol, "full-table reconstruct", "reconstruct"))
	}
	return fmt.Errorf(
		"full-table reconstruct: %s.%s PK column %q has type %q which is not in the supported PK type set; "+
			"file a follow-up issue if you need this type",
		schema, table, pkCol.Name, pkCol.DataType)
}
