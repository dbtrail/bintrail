package metadata

import (
	"context"
	"database/sql"
	"fmt"
)

// DDLSnapshotIDSeq creates the dedicated AUTO_INCREMENT counter table used to
// allocate schema_snapshots.snapshot_id values (see allocateSnapshotID).
// Exported so internal/indexer (which already depends on this package —
// internal/event imports metadata, and indexer imports event) can provision
// it in CreateIndexTables/EnsureSchema alongside every other index table,
// mirroring the internal/serverid.DDLBintrailServers cross-package DDL
// convention.
//
// Replaces the earlier `SELECT COALESCE(MAX(snapshot_id),0)+1 FROM
// schema_snapshots FOR UPDATE` allocation (#844): that next-key lock
// serializes concurrent writers correctly in principle, but reliably
// deadlocks (MySQL Error 1213) under 3+ concurrent allocate-then-insert
// transactions — exactly the concurrency #844 names (the watch daemon's
// DDL-hook auto-snapshot, a manual `bintrail snapshot`, and the console
// baseline trigger racing on the same index). Neither caller retries on a
// transient deadlock, so the FOR UPDATE design traded a silent
// data-correctness bug for a hard crash of the ingestion daemon under the
// exact scenario it was meant to fix.
//
// A dedicated AUTO_INCREMENT column sidesteps the problem entirely: InnoDB
// allocates AUTO_INCREMENT values under a lightweight, statement-duration
// lock (not a row/gap lock held for the transaction's lifetime), so
// concurrent allocators serialize on it without ever deadlocking. Verified
// empirically with 20 concurrent writers x 10 rounds x 5 repetitions against
// real MySQL: zero deadlocks, zero collisions.
//
// One row is inserted — never deleted — per snapshot ever allocated. A
// single INT UNSIGNED column and the low frequency of snapshot writes
// (per-DDL-change or manual/triggered, never per binlog event) make the
// resulting growth immaterial; this is a deliberate simplicity trade-off
// over a delete-after-read "ticket server" pattern.
const DDLSnapshotIDSeq = `CREATE TABLE IF NOT EXISTS snapshot_id_seq (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
) ENGINE=InnoDB COMMENT='deadlock-free AUTO_INCREMENT counter for schema_snapshots.snapshot_id allocation (#844); see metadata.DDLSnapshotIDSeq'`

// ensureSnapshotIDSeqTable lazily creates snapshot_id_seq on an index that
// predates it. Both CreateIndexTables (fresh installs) and EnsureSchema
// (existing installs, at daemon startup) already provision it eagerly, but
// the standalone `bintrail snapshot` CLI command calls TakeSnapshot directly
// without going through EnsureSchema — so allocateSnapshotID's callers
// self-heal here too, matching TakeSnapshot's existing tolerance for a
// pre-#758 index missing fk_constraints.
func ensureSnapshotIDSeqTable(ctx context.Context, db *sql.DB) error {
	var exists bool
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_id_seq'",
	).Scan(&exists); err != nil {
		return fmt.Errorf("metadata: check snapshot_id_seq table: %w", err)
	}
	if exists {
		return nil
	}
	if _, err := db.ExecContext(ctx, DDLSnapshotIDSeq); err != nil {
		return fmt.Errorf("metadata: create snapshot_id_seq: %w", err)
	}
	return nil
}

// snapshotIDExecer is satisfied by both *sql.DB (plain autocommit) and *sql.Tx,
// so allocateSnapshotID serves WritePGSnapshot (bounded autocommit, no wrapping
// transaction) and TakeSnapshot (inside its own multi-statement tx) alike.
type snapshotIDExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// allocateSnapshotID reserves the next snapshot_id by inserting a row into
// snapshot_id_seq and reading back its AUTO_INCREMENT value, on the caller's
// db or transaction (see snapshotIDExecer). See DDLSnapshotIDSeq for why this
// replaces the earlier FOR UPDATE design.
//
// res.LastInsertId() is read from the INSERT's own OK packet (not a
// follow-up `SELECT LAST_INSERT_ID()`), so it is safe under connection
// pooling regardless of pool size — no dependency on this statement and the
// read landing on the same pooled connection. That is exactly what lets
// WritePGSnapshot allocate under plain autocommit with no wrapping transaction.
func allocateSnapshotID(ctx context.Context, ex snapshotIDExecer) (int, error) {
	res, err := ex.ExecContext(ctx, "INSERT INTO snapshot_id_seq () VALUES ()")
	if err != nil {
		return 0, fmt.Errorf("metadata: allocate snapshot_id: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("metadata: allocate snapshot_id: %w", err)
	}
	return int(id), nil
}
