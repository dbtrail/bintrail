package pgstreamrun

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// doctorTimeout bounds the whole health report so `bintrail-pg doctor` can't hang for
// the OS TCP timeout against a black-holed host (it is a connectivity-diagnosis tool).
// Mirrors the MySQL doctor.Build 30s budget.
const doctorTimeout = 30 * time.Second

// dependentCheckNames are the checks BuildPGReport skips when wal_level is genuinely
// not 'logical' — logical decoding is impossible, so they cannot meaningfully run.
var dependentCheckNames = []string{"Publication coverage", "REPLICA IDENTITY FULL", "max_slot_wal_keep_size", "Replication slot health"}

// PGDoctorConfig is the subset of stream settings a health check needs: a normal
// (non-replication) connection to the source, the slot to inspect, and the
// publication + filters to validate coverage/replica-identity against.
type PGDoctorConfig struct {
	QueryDSN    string
	SlotName    string
	Publication string
	Schemas     string // comma-separated; restricts the publication-coverage check
	Tables      string // comma-separated; restricts the publication-coverage check
}

// BuildPGReport runs the bintrail-pg preflight + WAL-retention health checks against
// a live PostgreSQL source and returns a doctor.Report.
//
// Health-only: this connects to the live source, but that does NOT cross the
// offline-recovery invariant — recovery is index-only and never touches the source;
// a health/doctor check legitimately does. It REPORTS every check and never mutates
// anything (no slot create/drop, no config change). A connection failure short-
// circuits (nothing else can run); a wal_level failure skips the dependent checks.
//
// It lives in pgstreamrun (not internal/doctor) because the report builder links
// pgx/pgcapture, which internal/doctor must stay free of (cmd/bintrail's pgfree ban).
func BuildPGReport(ctx context.Context, cfg PGDoctorConfig) *doctor.Report {
	ctx, cancel := context.WithTimeout(ctx, doctorTimeout)
	defer cancel()

	r := &doctor.Report{
		ReadyFooter: "All checks passed. Run `bintrail-pg stream` to start (or resume) capture.",
		FixFooter:   "Fix the failures above, then re-run `bintrail-pg doctor` to verify.",
	}

	conn, err := pgx.Connect(ctx, cfg.QueryDSN)
	if err != nil {
		r.Add(doctor.CheckResult{
			Name:        "Source PostgreSQL connection",
			Status:      doctor.StatusFail,
			Detail:      err.Error(),
			Remediation: "Check --query-dsn (or BINTRAIL_PG_QUERY_DSN) is reachable and the credentials are valid.",
		})
		return r // nothing else can run without a connection
	}
	defer conn.Close(context.Background())
	r.Add(doctor.CheckResult{Name: "Source PostgreSQL connection", Status: doctor.StatusPass})

	// wal_level is the prerequisite for everything else. A genuine misconfiguration
	// (readable, but not 'logical') skips the dependent checks — they can't run. A
	// query FAILURE, by contrast, must NOT skip them: the slot-health check runs its
	// own query and would report the dangerous state honestly, so a transient blip must
	// never silently suppress it.
	walRes, skipDependents := walLevelResult(pgcapture.CheckWALLevel(ctx, conn))
	r.Add(walRes)
	if skipDependents {
		for _, name := range dependentCheckNames {
			r.Add(doctor.CheckResult{Name: name, Status: doctor.StatusSkip, Detail: "requires wal_level=logical"})
		}
		return r
	}

	filters := cliutil.BuildIndexFilters(cfg.Schemas, cfg.Tables)

	if err := pgcapture.CheckPublication(ctx, conn, cfg.Publication, filters); err != nil {
		r.Add(doctor.CheckResult{
			Name:        "Publication coverage",
			Status:      doctor.StatusFail,
			Detail:      err.Error(),
			Remediation: "Create or extend the publication so it covers every table you index (CREATE PUBLICATION ... FOR TABLE ...; or FOR ALL TABLES).",
		})
	} else {
		r.Add(doctor.CheckResult{Name: "Publication coverage", Status: doctor.StatusPass})
	}

	if err := pgcapture.CheckReplicaIdentity(ctx, conn, cfg.Publication); err != nil {
		r.Add(doctor.CheckResult{
			Name:        "REPLICA IDENTITY FULL",
			Status:      doctor.StatusFail,
			Detail:      err.Error(),
			Remediation: "Run ALTER TABLE <t> REPLICA IDENTITY FULL on every replicated table, so before-images (and de-TOASTed unchanged values) are complete — otherwise recovery silently loses columns.",
		})
	} else {
		r.Add(doctor.CheckResult{Name: "REPLICA IDENTITY FULL", Status: doctor.StatusPass})
	}

	addKeepSizeCheck(ctx, r, conn)
	addSlotHealthCheck(ctx, r, conn, cfg.SlotName)
	return r
}

// walLevelResult maps the CheckWALLevel outcome to its CheckResult and whether the
// dependent checks must be SKIPped. It is pure (takes the error) so both branches —
// value-wrong (skip) and query-failed (don't skip) — are unit-testable without a live
// non-logical server. A genuine wal_level!=logical config error fails AND skips
// (logical decoding is impossible); a query error fails but does NOT skip, so a
// transient blip never suppresses the load-bearing slot-health check.
func walLevelResult(err error) (doctor.CheckResult, bool) {
	const name = "wal_level = logical"
	switch {
	case err == nil:
		return doctor.CheckResult{Name: name, Status: doctor.StatusPass}, false
	case errors.Is(err, pgcapture.ErrWALLevelNotLogical):
		return doctor.CheckResult{
			Name:        name,
			Status:      doctor.StatusFail,
			Detail:      err.Error(),
			Remediation: "Set wal_level=logical in postgresql.conf and restart the server (this setting is not reloadable).",
		}, true
	default:
		return doctor.CheckResult{
			Name:        name,
			Status:      doctor.StatusFail,
			Detail:      err.Error(),
			Remediation: "Could not read wal_level. Retry once (a transient connection drop or timeout is the common cause); if it persists, check the role's privileges on the source.",
		}, false
	}
}

// addKeepSizeCheck reads max_slot_wal_keep_size and adds its result (a query failure
// is a WARN — keep-size is advisory). The mapping is the pure keepSizeResult.
func addKeepSizeCheck(ctx context.Context, r *doctor.Report, conn *pgx.Conn) {
	var setting string
	if err := conn.QueryRow(ctx, "SELECT current_setting('max_slot_wal_keep_size')").Scan(&setting); err != nil {
		r.Add(doctor.CheckResult{Name: "max_slot_wal_keep_size", Status: doctor.StatusWarn, Detail: "could not read: " + err.Error()})
		return
	}
	r.Add(keepSizeResult(setting))
}

// keepSizeResult maps the max_slot_wal_keep_size setting — the production red line
// (#532). -1 (unlimited) means a stalled slot can pin WAL until the source disk fills,
// with no in-database bound; that is a WARN, not a FAIL (it is a recommendation, and a
// running consumer keeps it safe), so it does not fail the command.
func keepSizeResult(setting string) doctor.CheckResult {
	const name = "max_slot_wal_keep_size"
	if setting == "-1" {
		return doctor.CheckResult{
			Name:   name,
			Status: doctor.StatusWarn,
			Detail: "-1 (unlimited retention)",
			Remediation: "Unlimited WAL retention: if this stream stalls, its replication slot can pin WAL until the source disk fills — an outage. " +
				"Set a bound (e.g. max_slot_wal_keep_size = '10GB') so PostgreSQL invalidates the slot instead; bintrail-pg then fails loud and you re-baseline. " +
				"This is the single GA-gating operational risk (#532).",
		}
	}
	return doctor.CheckResult{Name: name, Status: doctor.StatusPass, Detail: setting}
}

// addSlotHealthCheck queries the slot's live state and adds its CheckResult. A query
// failure is itself a FAIL; otherwise the (pure) slotHealthResult maps the state.
func addSlotHealthCheck(ctx context.Context, r *doctor.Report, conn *pgx.Conn, slotName string) {
	h, err := pgcapture.QuerySlotHealth(ctx, conn, slotName)
	if err != nil {
		r.Add(doctor.CheckResult{Name: "Replication slot health", Status: doctor.StatusFail, Detail: "could not query slot: " + err.Error()})
		return
	}
	r.Add(slotHealthResult(h, slotName))
}

// slotHealthResult maps a SlotHealth into a CheckResult. It is pure (no I/O) so the
// surfacing logic — especially the load-bearing lost→FAIL path — is unit-testable
// with a stubbed SlotHealth, without driving a real slot to invalidation. A lost slot
// is a loud FAIL with the re-baseline recovery path; extended/unreserved is a WARN
// (approaching invalidation); reserved is a PASS; an absent slot is a SKIP (normal
// before the first stream run).
func slotHealthResult(h pgcapture.SlotHealth, slotName string) doctor.CheckResult {
	const name = "Replication slot health"
	if !h.Exists {
		return doctor.CheckResult{
			Name:        name,
			Status:      doctor.StatusSkip,
			Detail:      fmt.Sprintf("slot %q does not exist yet", slotName),
			Remediation: "Normal before the first `bintrail-pg stream` run (the slot is created then). If you were resuming an existing slot, it was dropped or invalidated — re-baseline.",
		}
	}

	detail := fmt.Sprintf("wal_status=%s, active=%t, retained_wal=%s", h.WalStatus, h.Active, formatBytes(h.RetainedBytes))
	if h.SafeWalSize.Valid {
		detail += ", safe_wal=" + formatBytes(h.SafeWalSize.Int64)
	}

	switch h.WalStatus {
	case pgcapture.WalStatusLost:
		return doctor.CheckResult{Name: name, Status: doctor.StatusFail, Detail: detail, Remediation: lostSlotRemediation(slotName)}
	case pgcapture.WalStatusExtended, pgcapture.WalStatusUnreserved:
		return doctor.CheckResult{
			Name:   name,
			Status: doctor.StatusWarn,
			Detail: detail,
			Remediation: "The slot is retaining WAL beyond max_wal_size and is approaching the max_slot_wal_keep_size limit. " +
				"If it crosses, PostgreSQL invalidates the slot (wal_status=lost) and capture cannot resume — you must re-baseline. " +
				"Make sure `bintrail-pg stream` is running and keeping up with the source's write rate.",
		}
	case pgcapture.WalStatusReserved, "":
		// reserved = healthy; "" = a just-created slot that hasn't reserved WAL yet
		// (restart_lsn NULL) — both benign.
		return doctor.CheckResult{Name: name, Status: doctor.StatusPass, Detail: detail}
	default:
		// An unrecognized, non-empty status (e.g. a future PostgreSQL value) must not
		// be silently shown as healthy — WARN so a new dangerous state can't slip through.
		return doctor.CheckResult{
			Name:        name,
			Status:      doctor.StatusWarn,
			Detail:      detail,
			Remediation: fmt.Sprintf("Unrecognized wal_status %q — treating as non-fatal; verify the slot manually against your PostgreSQL version's pg_replication_slots docs.", h.WalStatus),
		}
	}
}

// lostSlotRemediation frames the lost-slot recovery path: capture is broken, but the
// index (and therefore recovery) is not. The manual re-baseline SQL mirrors the
// docs; the `bintrail-pg reset` convenience command is a later slice (#532).
func lostSlotRemediation(slotName string) string {
	return "The replication slot is invalidated — the WAL it needs is gone, so capture cannot resume.\n" +
		"The index is still fully usable for recovery (recovery never needs the slot).\n" +
		"To resume capture you must re-baseline:\n" +
		fmt.Sprintf("  1. on the source:  SELECT pg_drop_replication_slot('%s');\n", slotName) +
		"  2. on the index:   DELETE FROM stream_state WHERE id = 1;\n" +
		"  3. re-seed the baseline, then re-run `bintrail-pg stream`\n" +
		"Prevent recurrence: raise max_slot_wal_keep_size and keep the consumer running."
}

// formatBytes renders a byte count as a human-readable size (operator-facing detail).
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
