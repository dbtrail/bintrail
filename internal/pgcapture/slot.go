package pgcapture

import (
	"context"
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
	if err := checkWALLevel(ctx, conn); err != nil {
		return err
	}
	return checkReplicaIdentityTables(ctx, conn, publication)
}

// ErrWALLevelNotLogical marks the "wal_level is readable but not 'logical'" case (a
// genuine misconfiguration) so a caller — the doctor — can distinguish it from a
// query failure (connection/permission/timeout) and give the right remediation
// (restart-with-config vs retry) without mislabeling a transient blip as a config bug.
var ErrWALLevelNotLogical = errors.New("wal_level is not 'logical'")

// ErrSlotLost and ErrSlotMissingOnResume mark the two fatal resume conditions where
// the WAL behind the saved checkpoint is irrecoverably gone — the slot was invalidated
// (wal_status=lost) or dropped. ensureSlot wraps them (preserving its descriptive
// message); the consumer (pgstreamrun.One) matches them with errors.Is to durably
// record the permanent loss so index-only `status` can show it after the process exits.
// Base strings are bare (no "pgcapture:" prefix) because ensureSlot wraps them with a
// "pgcapture: ...: %w" message; doubling the prefix would stutter, and the wrapped text
// is stored verbatim in stream_state.gap_lost_detail / shown in the status badge. Mirrors
// ErrWALLevelNotLogical's bare base string.
var (
	ErrSlotLost            = errors.New("replication slot invalidated (wal_status=lost)")
	ErrSlotMissingOnResume = errors.New("replication slot missing on resume")
)

// checkWALLevel verifies the (global) wal_level is 'logical' — logical replication
// is impossible otherwise. A value-wrong result wraps ErrWALLevelNotLogical; a query
// failure does not.
func checkWALLevel(ctx context.Context, conn *pgx.Conn) error {
	var walLevel string
	if err := conn.QueryRow(ctx, "SELECT current_setting('wal_level')").Scan(&walLevel); err != nil {
		return fmt.Errorf("pgcapture: checking wal_level: %w", err)
	}
	if walLevel != "logical" {
		return fmt.Errorf("pgcapture: wal_level is %q, must be 'logical' for logical replication: %w", walLevel, ErrWALLevelNotLogical)
	}
	return nil
}

// replicaIdentityNotFull returns the sorted "schema.table (relreplident=x)" of every
// published table NOT at REPLICA IDENTITY FULL. It is the single catalog-query source
// of truth shared by the capture-time validator (which turns a non-empty list into a
// fatal error) and the report-only QueryReplicaIdentityNotFull (which surfaces the list
// for the console health panel). An empty result means every published table is FULL.
func replicaIdentityNotFull(ctx context.Context, conn *pgx.Conn, publication string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT pt.schemaname, pt.tablename, c.relreplident::text
		FROM pg_publication_tables pt
		JOIN pg_namespace n ON n.nspname = pt.schemaname
		JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
		WHERE pt.pubname = $1`, publication)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: checking replica identity for publication %q: %w", publication, err)
	}
	defer rows.Close()

	var notFull []string
	for rows.Next() {
		var schema, table, relreplident string
		if err := rows.Scan(&schema, &table, &relreplident); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning replica identity for publication %q: %w", publication, err)
		}
		if relreplident != "f" {
			notFull = append(notFull, fmt.Sprintf("%s.%s (relreplident=%s)", schema, table, relreplident))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: checking replica identity for publication %q: %w", publication, err)
	}
	sort.Strings(notFull)
	return notFull, nil
}

// checkReplicaIdentityTables verifies every table the publication streams is at
// REPLICA IDENTITY FULL (the per-table loop; wal_level is checked separately).
func checkReplicaIdentityTables(ctx context.Context, conn *pgx.Conn, publication string) error {
	notFull, err := replicaIdentityNotFull(ctx, conn, publication)
	if err != nil {
		return err
	}
	if len(notFull) > 0 {
		return fmt.Errorf("pgcapture: table(s) not at REPLICA IDENTITY FULL [%s] — before-images would be partial (an unchanged out-of-line TOAST value is lost under a weaker identity, so recovery would be wrong); run ALTER TABLE <t> REPLICA IDENTITY FULL",
			strings.Join(notFull, ", "))
	}
	return nil
}

// The exported Check* functions are the report-only form of the capture-time
// validate* guards above, for bintrail-pg doctor. They are pure probes (no CREATE,
// no mutation) returning nil when healthy and a descriptive error otherwise; the
// doctor converts that error into a CheckResult. They live here so the single
// catalog-query source of truth (and its error wording) is shared with capture.

// CheckWALLevel verifies wal_level = 'logical' on the source.
func CheckWALLevel(ctx context.Context, conn *pgx.Conn) error {
	return checkWALLevel(ctx, conn)
}

// CheckPublication verifies the publication exists and covers the requested tables.
func CheckPublication(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) error {
	return validatePublication(ctx, conn, publication, filters)
}

// CheckReplicaIdentity verifies every table the publication streams is at REPLICA
// IDENTITY FULL. It does NOT re-check wal_level (use CheckWALLevel for that), so the
// doctor can report the two distinctly.
func CheckReplicaIdentity(ctx context.Context, conn *pgx.Conn, publication string) error {
	return checkReplicaIdentityTables(ctx, conn, publication)
}

// QueryReplicaIdentityNotFull is the report-only sibling of CheckReplicaIdentity: it
// returns the published tables NOT at REPLICA IDENTITY FULL (empty = all FULL) instead
// of folding them into a fatal error. The streaming daemon polls it for the console
// health panel (#599) — a coverage signal the operator reads, not a startup gate.
func QueryReplicaIdentityNotFull(ctx context.Context, conn *pgx.Conn, publication string) ([]string, error) {
	return replicaIdentityNotFull(ctx, conn, publication)
}

// listUnloggedCaptureTables returns the "schema.table" of every UNLOGGED base table in
// the capture scope. UNLOGGED tables generate NO WAL, so logical decoding never sees
// their changes — they are captured as exactly nothing, silently; an operator may keep
// ephemeral data UNLOGGED on purpose, so this is surfaced as a WARNING (not the fatal
// fail of validateReplicaIdentity): a coverage hole the operator should see now rather
// than discover during a failed recovery. (#555)
//
// CRUCIAL: an UNLOGGED table is NEVER in pg_publication_tables, so it must be found
// against pg_class directly. PostgreSQL refuses to add an UNLOGGED table to a FOR TABLE
// publication ("cannot add relation ... to publication ... unlogged"), and it EXCLUDES
// UNLOGGED tables from a FOR ALL TABLES publication's pg_publication_tables view
// (pg_relation_is_publishable gates on relpersistence). A query over pg_publication_
// tables therefore can never match an UNLOGGED row — it would be dead code.
//
// The dangerous case is FOR ALL TABLES: the operator believes "everything" is captured,
// but the UNLOGGED tables silently are not. We scan user base tables for relpersistence
// = 'u' and report the ones in the client filter scope. For a FOR TABLE publication an
// UNLOGGED table cannot be in the publication at all, and a client --tables naming an
// unpublished table is already a fatal coverage gap in validatePublication — so there
// is nothing this guard could uniquely surface and it returns nil. (FOR TABLES IN
// SCHEMA, PG 15+, is not covered here — a documented limitation, not silent: the doctor
// still lists the check.)
func listUnloggedCaptureTables(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) ([]string, error) {
	var allTables bool
	err := conn.QueryRow(ctx, `SELECT puballtables FROM pg_publication WHERE pubname = $1`, publication).Scan(&allTables)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // publication absent — validatePublication reports that distinctly
	}
	if err != nil {
		return nil, fmt.Errorf("pgcapture: reading publication %q for the UNLOGGED check: %w", publication, err)
	}
	if !allTables {
		return nil, nil // FOR TABLE: UNLOGGED tables cannot be published (see doc above)
	}

	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND c.relpersistence = 'u'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		  AND n.nspname NOT LIKE 'pg_temp%'
		  AND n.nspname NOT LIKE 'pg_toast_temp%'`)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: scanning for UNLOGGED tables: %w", err)
	}
	defer rows.Close()

	var unlogged []string
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning UNLOGGED tables: %w", err)
		}
		// Under FOR ALL TABLES the captured set is every user table narrowed by the
		// client schema/table filter (the zero Filters accepts all), so honor it here.
		if filters.Matches(schema, table) {
			unlogged = append(unlogged, schema+"."+table)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: scanning for UNLOGGED tables: %w", err)
	}
	sort.Strings(unlogged)
	return unlogged, nil
}

// ListUnloggedCaptureTables is the report-only form of the #555 UNLOGGED guard for
// bintrail-pg doctor (a pure probe; no mutation). nil/empty means no UNLOGGED tables
// in the capture scope.
func ListUnloggedCaptureTables(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters) ([]string, error) {
	return listUnloggedCaptureTables(ctx, conn, publication, filters)
}

// CascadeChild is one foreign-key cascade child whose PARENT the publication captures
// but the child itself it does NOT — so a delete on the parent fires an ON DELETE
// CASCADE / SET NULL / SET DEFAULT that rewrites the child, and that change is never
// captured. (#556)
type CascadeChild struct {
	Child  string // "schema.table" of the FK-bearing (child) table
	Parent string // "schema.table" of the referenced (parent) table
	Action string // human action, e.g. "ON DELETE CASCADE"
}

// listUncoveredCascadeChildren returns cascade children whose parent IS in the
// publication but the child is NOT. On PostgreSQL, FK ON DELETE CASCADE/SET NULL/SET
// DEFAULT is performed by real per-row WAL ops on the child (unlike MySQL ≤8.x, which
// does not log them) — so `recover` works directly IF the child is captured. When the
// parent is published but the child is not, a cascade delete on the parent silently
// rewrites rows we never indexed → unrecoverable, and invisible. This surfaces that as
// a WARNING. (#556)
//
// A FOR ALL TABLES publication covers every child by construction, so this finds
// nothing there; the gap is an explicit FOR TABLE publication that lists a parent but
// forgets its cascade child. A PUBLISHED child is already forced to RI FULL by
// checkReplicaIdentityTables, so only the not-published case needs surfacing here.
//
// Residual (documented, not covered here): a row that was deleted-by-cascade but never
// existed in our captured history (it predates capture) is recoverable only from a
// baseline — the PG baseline producer is a separate GA item (#593).
func listUncoveredCascadeChildren(ctx context.Context, conn *pgx.Conn, publication string) ([]CascadeChild, error) {
	rows, err := conn.Query(ctx, `
		SELECT child_ns.nspname, child.relname, parent_ns.nspname, parent.relname, con.confdeltype
		FROM pg_constraint con
		JOIN pg_class child ON child.oid = con.conrelid
		JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
		JOIN pg_class parent ON parent.oid = con.confrelid
		JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
		WHERE con.contype = 'f' AND con.confdeltype IN ('c', 'n', 'd')
		  AND EXISTS (SELECT 1 FROM pg_publication_tables pt
		              WHERE pt.pubname = $1 AND pt.schemaname = parent_ns.nspname AND pt.tablename = parent.relname)
		  AND NOT EXISTS (SELECT 1 FROM pg_publication_tables pt
		              WHERE pt.pubname = $1 AND pt.schemaname = child_ns.nspname AND pt.tablename = child.relname)`,
		publication)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: checking FK cascade-child coverage for publication %q: %w", publication, err)
	}
	defer rows.Close()

	var out []CascadeChild
	for rows.Next() {
		var childSchema, childTable, parentSchema, parentTable, delType string
		if err := rows.Scan(&childSchema, &childTable, &parentSchema, &parentTable, &delType); err != nil {
			return nil, fmt.Errorf("pgcapture: scanning FK cascade-child coverage for publication %q: %w", publication, err)
		}
		out = append(out, CascadeChild{
			Child:  childSchema + "." + childTable,
			Parent: parentSchema + "." + parentTable,
			Action: cascadeAction(delType),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: checking FK cascade-child coverage for publication %q: %w", publication, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Child < out[j].Child })
	return out, nil
}

// cascadeAction renders a pg_constraint.confdeltype code as its human FK action.
func cascadeAction(confdeltype string) string {
	switch confdeltype {
	case "c":
		return "ON DELETE CASCADE"
	case "n":
		return "ON DELETE SET NULL"
	case "d":
		return "ON DELETE SET DEFAULT"
	default:
		return "ON DELETE (" + confdeltype + ")"
	}
}

// ListUncoveredCascadeChildren is the report-only form of the #556 cascade-coverage
// guard for bintrail-pg doctor (a pure probe; no mutation). nil/empty means every
// cascade child of a published parent is itself published.
func ListUncoveredCascadeChildren(ctx context.Context, conn *pgx.Conn, publication string) ([]CascadeChild, error) {
	return listUncoveredCascadeChildren(ctx, conn, publication)
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
	found, walStatus, err := querySlotState(ctx, queryConn, slotName)
	if err != nil {
		return 0, err
	}

	// A 'lost' slot is unusable regardless of mode — its WAL has been removed.
	if found && walStatus == WalStatusLost {
		return 0, fmt.Errorf("pgcapture: replication slot %q is invalidated (wal_status=lost; max_slot_wal_keep_size exceeded) — the WAL it needs is gone; re-baseline rather than resume: %w", slotName, ErrSlotLost)
	}

	if expectExisting {
		if !found {
			return 0, fmt.Errorf("pgcapture: resuming from a saved checkpoint but replication slot %q no longer exists — the WAL since the checkpoint is lost; re-baseline (creating a fresh slot would silently skip data): %w", slotName, ErrSlotMissingOnResume)
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
