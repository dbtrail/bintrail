package metadata

import (
	"context"
	"database/sql"
	"fmt"
)

// DDLSnapshotExclusions records which base tables a DEGRADED snapshot
// (TakeSnapshotExcludingInvalid, #1051) left out and why. It is the EXPLICIT
// source of truth the cascade FK loaders read to flag
// CascadeFK.ChildExcludedFromSnapshot: inferring exclusion from "the child has
// fk_constraints rows but no schema_snapshots rows" is unsound — a table
// created between TakeSnapshot's columns query and its FK query produces that
// exact shape with nothing excluded — and a false "provably partial" caveat is
// the cry-wolf failure the cascade Result contract forbids. An absent table
// (legacy index, or no degraded snapshot ever taken) simply means "no
// exclusions"; readers must tolerate it.
const DDLSnapshotExclusions = `CREATE TABLE IF NOT EXISTS snapshot_exclusions (
    snapshot_id INT UNSIGNED NOT NULL,
    schema_name VARCHAR(64)  NOT NULL,
    table_name  VARCHAR(64)  NOT NULL,
    reason      VARCHAR(64)  NOT NULL,
    PRIMARY KEY (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB COMMENT='tables a degraded snapshot excluded (no PK / non-InnoDB, #1051); see metadata.DDLSnapshotExclusions'`

// snapshotExclusion is one table takeSnapshot's degrade branch left out of the
// snapshot, carried separately from the "schema.table" display strings so the
// insert never has to re-split a concatenated key.
type snapshotExclusion struct {
	schema, table, reason string
}

// ensureSnapshotExclusionsTable lazily creates snapshot_exclusions on an index
// that predates it — the ensureSnapshotIDSeqTable pattern: indexer.EnsureSchema
// provisions it eagerly at daemon startup, but the writer must self-heal for
// paths that never run EnsureSchema.
func ensureSnapshotExclusionsTable(ctx context.Context, db *sql.DB) error {
	var exists bool
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'",
	).Scan(&exists); err != nil {
		return fmt.Errorf("metadata: check snapshot_exclusions table: %w", err)
	}
	if exists {
		return nil
	}
	if _, err := db.ExecContext(ctx, DDLSnapshotExclusions); err != nil {
		return fmt.Errorf("metadata: create snapshot_exclusions: %w", err)
	}
	return nil
}

// loadSnapshotExclusions returns the "schema.table" → reason map a degraded
// snapshot (#1051) recorded for snapshotID. A missing snapshot_exclusions
// table is NOT an error (see the DDLSnapshotExclusions doc: an index that only
// ever took strict snapshots simply has no exclusions) — nil map, nil error.
func loadSnapshotExclusions(db *sql.DB, snapshotID int) (map[string]string, error) {
	var exists bool
	if err := db.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'",
	).Scan(&exists); err != nil {
		return nil, fmt.Errorf("metadata: check snapshot_exclusions table: %w", err)
	}
	if !exists {
		return nil, nil
	}
	rows, err := db.Query(
		"SELECT schema_name, table_name, reason FROM snapshot_exclusions WHERE snapshot_id = ?",
		snapshotID)
	if err != nil {
		return nil, fmt.Errorf("metadata: query snapshot_exclusions for snapshot %d: %w", snapshotID, err)
	}
	defer rows.Close()

	var excluded map[string]string
	for rows.Next() {
		var schemaName, tableName, reason string
		if err := rows.Scan(&schemaName, &tableName, &reason); err != nil {
			return nil, fmt.Errorf("metadata: scan snapshot_exclusions row: %w", err)
		}
		if excluded == nil {
			excluded = make(map[string]string)
		}
		excluded[schemaName+"."+tableName] = reason
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate snapshot_exclusions: %w", err)
	}
	return excluded, nil
}
