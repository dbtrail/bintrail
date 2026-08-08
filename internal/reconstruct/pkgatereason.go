package reconstruct

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ErrGeneratedPK is the errors.Is sentinel for refusals caused by a
// generated primary-key member — the MariaDB system-versioning shape
// (#1266/#1273). Machine callers (the cascade engine's caveat classifier)
// use it to tell this PERMANENT table property apart from transient lookup
// failures without string matching.
//
// Who carries it: every ERROR-typed refusal from this shape — the
// cascadebaseline provider's BaselineChildren refusal and
// fullTableGeneratedPKRefusal below, both built via GeneratedPKRefusalError.
// The verify inconclusive detail and the shim wire message are STRINGS by
// construction and cannot carry a sentinel; do not branch on errors.Is for
// those surfaces.
var ErrGeneratedPK = errors.New("primary key contains a generated column")

// generatedPKErr classifies as ErrGeneratedPK via Is WITHOUT injecting the
// sentinel's text into the rendered message — a %w wrap stutters ("primary
// key contains a generated column: primary-key column ... is a generated
// column ..."). Same pattern as MissingPKColumnError below.
type generatedPKErr struct{ msg string }

func (e *generatedPKErr) Error() string { return e.msg }

// Is makes errors.Is(err, ErrGeneratedPK) true for the concrete type.
func (e *generatedPKErr) Is(target error) bool { return target == ErrGeneratedPK }

// GeneratedPKRefusalError builds an error that renders exactly msg and
// matches errors.Is(err, ErrGeneratedPK). Every error-typed generated-PK
// refusal must be built through this, or the cascade engine's permanent-
// caveat classifier files it as a transient lookup failure.
func GeneratedPKRefusalError(msg string) error { return &generatedPKErr{msg: msg} }

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

// GeneratedPKColumn returns the first primary-key member that is a STORED/
// VIRTUAL generated column, in input order, and whether one exists (#1266).
// Callers pass PKColumnMetas(), which preserves ordinal order — the ordering
// guarantee is the input's, not this function's.
//
// The shape this detects in practice is MariaDB system versioning: MariaDB
// silently extends a versioned table's PRIMARY KEY with its ROW END period
// column (`PRIMARY KEY (id, row_end)`, observed on 11.4 for both visible and
// INVISIBLE explicit period columns) and marks that column STORED GENERATED in
// information_schema, which is how it reaches the snapshot's is_generated
// flag. An ordinary STORED generated column inside a PK trips this too, on
// purpose: in both cases the baseline deliberately omits the column's values
// (mydumper never dumps generated columns), so no baseline-side PK join key
// can be built and the merge would die deep in the probe with
// MissingPKColumnError on every row.
//
// Why the gates that call this REFUSE instead of dropping the column from the
// join key (the fix direction #1266 first suggested): the binlog does NOT
// carry only current-state rows for a versioned table. Verified against
// MariaDB 11.4: an UPDATE logs the current-row update PLUS a Write_rows for
// the history row (same remaining key, row_end = now), and a DELETE logs no
// Delete_rows at all — it is an Update_rows tombstone (row_end sentinel →
// now). Under a reduced key the history insert would overwrite the current
// row in the last-write-wins change map, orphan history rows would be emitted
// as duplicate live rows, and tombstoned deletes would resurrect — silent
// corruption where today's refusal is loud. Supporting these tables takes
// versioning-aware fold semantics, not a smaller key.
func GeneratedPKColumn(pkCols []metadata.ColumnMeta) (metadata.ColumnMeta, bool) {
	for _, c := range pkCols {
		if c.IsGenerated {
			return c, true
		}
	}
	return metadata.ColumnMeta{}, false
}

// GeneratedPKGateReason renders the refusal/inconclusive detail for a primary
// key that contains a generated column (see GeneratedPKColumn). surface names
// the feature speaking ("verify", "full-table reconstruct", ...). No CLI flag
// names in the text: console and wire surfaces emit it too.
func GeneratedPKGateReason(c metadata.ColumnMeta, surface string) string {
	return fmt.Sprintf(
		"primary-key column %q is a generated column — most commonly the MariaDB system-versioning shape, which extends "+
			"a versioned table's PK with its ROW END period column — and baselines deliberately omit generated columns, "+
			"so %s cannot build the baseline-side PK join key for this table; dropping the column from the key instead "+
			"would corrupt silently, because a versioned table's binlog carries history rows (as inserts) and versioned "+
			"deletes (as row_end updates) under the same remaining key; query and recover are not gated, and the CLI's "+
			"single-row reconstruct with an explicit PK column list also works", c.Name, surface)
}

// fullTableGeneratedPKRefusal is the error both full-table reconstruct paths
// (baseline merge and binlog-only fallback) return when the PK contains a
// generated column, so the two cannot drift. Classified as ErrGeneratedPK
// (via GeneratedPKRefusalError) so machine callers need no string matching.
func fullTableGeneratedPKRefusal(schema, table string, pkCol metadata.ColumnMeta) error {
	return GeneratedPKRefusalError(fmt.Sprintf("full-table reconstruct: %s.%s: %s", schema, table,
		GeneratedPKGateReason(pkCol, "full-table reconstruct")))
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
