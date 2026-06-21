package pgcapture

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/event"
)

// validatePublication checks that the pgoutput publication exists AND covers every
// table the capturer is asked to stream — validate-don't-create, mirroring how the
// MySQL path validates binlog_row_image=FULL rather than setting it. The publication
// defines the captured table set, which is an operator privilege/policy decision; a
// publication that exists but omits a requested table would emit ZERO events for it,
// silently and forever, so a coverage gap fails loud.
func validatePublication(ctx context.Context, conn *pgx.Conn, pubname string, filters event.Filters) error {
	var allTables bool
	err := conn.QueryRow(ctx, `SELECT puballtables FROM pg_publication WHERE pubname = $1`, pubname).Scan(&allTables)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("pgcapture: publication %q does not exist — create it (CREATE PUBLICATION) covering the tables to capture", pubname)
	}
	if err != nil {
		return fmt.Errorf("pgcapture: checking publication %q: %w", pubname, err)
	}
	// FOR ALL TABLES covers everything; a nil table filter means "accept whatever the
	// publication streams", so there is no requested set to verify coverage against.
	if allTables || len(filters.Tables) == 0 {
		return nil
	}

	rows, err := conn.Query(ctx, `SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1`, pubname)
	if err != nil {
		return fmt.Errorf("pgcapture: listing tables of publication %q: %w", pubname, err)
	}
	defer rows.Close()
	published := make(map[string]bool)
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return fmt.Errorf("pgcapture: scanning tables of publication %q: %w", pubname, err)
		}
		published[t] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("pgcapture: listing tables of publication %q: %w", pubname, err)
	}

	var missing []string
	for t := range filters.Tables {
		if !published[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("pgcapture: publication %q does not cover requested table(s) [%s] — their changes would be silently lost; add them to the publication",
			pubname, strings.Join(missing, ", "))
	}
	return nil
}

// validateReplicaIdentity is the PostgreSQL analog of metadata.ValidateBinlogRowImage:
// it refuses a source that can't produce complete before-images. PostgreSQL's knob is
// per-table (REPLICA IDENTITY), not a global like MySQL's binlog_row_image, so every
// table the publication will stream must be at FULL.
//
//   - wal_level must be 'logical' (global) — logical replication is impossible
//     otherwise.
//   - Every table in pg_publication_tables (which expands FOR ALL TABLES to the actual
//     set PostgreSQL will stream — NOT the narrower client-side Filters) must have
//     pg_class.relreplident = 'f' (FULL). Under any weaker identity ('d' default,
//     'i' using-index, 'n' nothing) an unchanged out-of-line TOAST value is GONE from
//     the before-image (proven in the spike, Part A), so we fail loud rather than index
//     partial, unrecoverable before-images.
//
// This is the STARTUP gate (existing tables); the decoder's cacheRelation
// re-enforces REPLICA IDENTITY FULL on every RelationMessage, so a table added to a
// FOR ALL TABLES publication mid-stream — which this one-shot check never re-runs for
// — is caught at the live boundary too.
func validateReplicaIdentity(ctx context.Context, conn *pgx.Conn, publication string) error {
	var walLevel string
	if err := conn.QueryRow(ctx, "SELECT current_setting('wal_level')").Scan(&walLevel); err != nil {
		return fmt.Errorf("pgcapture: checking wal_level: %w", err)
	}
	if walLevel != "logical" {
		return fmt.Errorf("pgcapture: wal_level is %q, must be 'logical' for logical replication", walLevel)
	}

	rows, err := conn.Query(ctx, `
		SELECT pt.schemaname, pt.tablename, c.relreplident::text
		FROM pg_publication_tables pt
		JOIN pg_namespace n ON n.nspname = pt.schemaname
		JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
		WHERE pt.pubname = $1`, publication)
	if err != nil {
		return fmt.Errorf("pgcapture: checking replica identity for publication %q: %w", publication, err)
	}
	defer rows.Close()

	var notFull []string
	for rows.Next() {
		var schema, table, relreplident string
		if err := rows.Scan(&schema, &table, &relreplident); err != nil {
			return fmt.Errorf("pgcapture: scanning replica identity for publication %q: %w", publication, err)
		}
		if relreplident != "f" {
			notFull = append(notFull, fmt.Sprintf("%s.%s (relreplident=%s)", schema, table, relreplident))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("pgcapture: checking replica identity for publication %q: %w", publication, err)
	}
	if len(notFull) > 0 {
		sort.Strings(notFull)
		return fmt.Errorf("pgcapture: table(s) not at REPLICA IDENTITY FULL [%s] — before-images would be partial (an unchanged out-of-line TOAST value is lost under a weaker identity, so recovery would be wrong); run ALTER TABLE <t> REPLICA IDENTITY FULL",
			strings.Join(notFull, ", "))
	}
	return nil
}

// ensureSlot returns the LSN to start replication from, creating the slot on first
// run. The first-run-vs-resume decision is gated on slot EXISTENCE in
// pg_replication_slots (NOT on savedLSN==0 — an empty saved checkpoint decodes to
// LSN 0, which would collide with a genuine first run).
//
//   - Absent: create a permanent pgoutput slot and start from its ConsistentPoint
//     (NOT IdentifySystem().XLogPos — that is past any pre-StartReplication DML and
//     yields an empty stream; a footgun hit in the spike).
//   - Present (resume): return savedLSN. PostgreSQL actually resumes from the slot's
//     own confirmed_flush_lsn (it clamps a lower client LSN forward and re-delivers —
//     at-least-once, never skipping), so savedLSN's load-bearing role is seeding
//     lastAcked, not the StartReplication argument.
//
// The slot is permanent (Temporary defaults false) so it survives restarts — required
// for resume. The flip side is source-side WAL retention if the consumer stalls
// (the PG analog of binlog retention); WAL-retention monitoring is #532.
//
// expectExisting is set by the consumer when it is resuming from a saved checkpoint
// (#534). In that case the slot MUST still exist and be valid: if it was dropped, or
// invalidated by max_slot_wal_keep_size (wal_status='lost' — the PG13+ feature the
// version floor is chosen to bound), the WAL since the checkpoint is gone, and
// silently creating a fresh slot from a new ConsistentPoint would SKIP that data.
// ensureSlot fails loud in that case rather than skip; the recovery policy
// (re-baseline) is #532.
func ensureSlot(ctx context.Context, replConn *pgconn.PgConn, queryConn *pgx.Conn, slotName string, savedLSN pglogrepl.LSN, expectExisting bool) (pglogrepl.LSN, error) {
	var walStatus sql.NullString
	found := true
	err := queryConn.QueryRow(ctx, `SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&walStatus)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		found = false
	case err != nil:
		return 0, fmt.Errorf("pgcapture: checking replication slot %q: %w", slotName, err)
	}

	// A 'lost' slot is unusable regardless of mode — its WAL has been removed.
	if found && walStatus.String == "lost" {
		return 0, fmt.Errorf("pgcapture: replication slot %q is invalidated (wal_status=lost; max_slot_wal_keep_size exceeded) — the WAL it needs is gone; re-baseline rather than resume", slotName)
	}

	if expectExisting {
		if !found {
			return 0, fmt.Errorf("pgcapture: resuming from a saved checkpoint but replication slot %q no longer exists — the WAL since the checkpoint is lost; re-baseline (creating a fresh slot would silently skip data)", slotName)
		}
		return savedLSN, nil
	}
	if found {
		// First run but the slot already exists (a prior run created it, or a TOCTOU).
		// Resume from it; PostgreSQL clamps savedLSN forward to the slot's own point.
		return savedLSN, nil
	}

	res, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		// TOCTOU: another capturer or a restart created the slot between the check
		// and now (SQLSTATE 42710 = duplicate_object). Treat as a resume.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.SQLState() == "42710" {
			return savedLSN, nil
		}
		return 0, fmt.Errorf("pgcapture: creating replication slot %q: %w", slotName, err)
	}
	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("pgcapture: parsing consistent point %q for slot %q: %w", res.ConsistentPoint, slotName, err)
	}
	return lsn, nil
}
