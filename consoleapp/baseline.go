package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
)

// baselineSupervisor implements console.BaselineController by running the
// dump→convert→upload pipeline IN-PROCESS (#613): the console image bundles
// mydumper, so a baseline never starts a sibling container and the daemon never
// mounts the docker socket. One job at a time per server, tracked in-memory —
// the durable record is the snapshot itself (listed by /api/baselines).
type baselineSupervisor struct {
	ctx        context.Context // daemon lifecycle; cancels an in-flight dump on shutdown
	stagingDir string          // base dir for temp dump + staged Parquet (S3-destined runs)

	// pointConsistent opts a MySQL/MariaDB dump into mydumper's FTWRL sync mode
	// instead of the NO_LOCK default, trading the least-privilege requirement
	// for a single point-in-time snapshot across ALL tables (#800). Set only via
	// BINTRAIL_CONSOLE_BASELINE_POINT_CONSISTENT=1 — mirrors how
	// BINTRAIL_CONSOLE_BASELINE_TRIGGER gates the feature itself. No effect on
	// PostgreSQL baselines (executePG uses pgoutput's own consistent-point LSN
	// unconditionally).
	pointConsistent bool

	mu   sync.Mutex
	jobs map[string]*console.BaselineStatus
	// refreshes tracks the PERIODIC refresh jobs (#1171), kept apart from jobs
	// so a manual dump cannot erase the evidence that the automatic refresh has
	// been failing. Both share the single-flight (busyLocked).
	refreshes map[string]*console.BaselineStatus
}

// newBaselineSupervisor builds a supervisor bound to the daemon context. The
// staging dir is created lazily per run. pointConsistent selects the MySQL dump's
// lock mode for every run this supervisor executes — see the field doc.
func newBaselineSupervisor(ctx context.Context, stagingDir string, pointConsistent bool) *baselineSupervisor {
	return &baselineSupervisor{
		ctx:             ctx,
		stagingDir:      stagingDir,
		pointConsistent: pointConsistent,
		jobs:            make(map[string]*console.BaselineStatus),
		refreshes:       make(map[string]*console.BaselineStatus),
	}
}

// Trigger starts a baseline in the background; returns console.ErrBaselineRunning
// if one is already in flight for this server.
func (s *baselineSupervisor) Trigger(req console.BaselineRequest) error {
	s.mu.Lock()
	// Shared with the periodic refresh (#1171): a dump writing a new snapshot
	// while a refresh folds the newest one forward would leave the refresh
	// anchored on a snapshot being written underneath it.
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	s.jobs[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp()}
	s.mu.Unlock()

	slog.Info("baseline: starting in-process snapshot", "server", req.ServerName, "id", req.ServerID)
	go s.run(req)
	return nil
}

// Status returns a copy of the latest known job state (idle if never run here).
func (s *baselineSupervisor) Status(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.jobs[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

func (s *baselineSupervisor) run(req console.BaselineRequest) {
	stats, uploaded, err := s.execute(req)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.jobs[req.ServerID]
	if st == nil { // defensive: never overwritten away under lock, but don't panic
		st = &console.BaselineStatus{}
		s.jobs[req.ServerID] = st
	}
	st.FinishedAt = nowStamp()
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		slog.Error("baseline: snapshot failed", "server", req.ServerName, "id", req.ServerID, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	st.Tables = stats.TablesProcessed
	st.Rows = stats.RowsWritten
	st.Uploaded = uploaded
	slog.Info("baseline: snapshot complete", "server", req.ServerName, "id", req.ServerID,
		"tables", stats.TablesProcessed, "rows", stats.RowsWritten, "uploaded", uploaded)
}

// execute runs the full pipeline: mydumper → baseline.Run → (S3) baseline.Upload.
// For a local-dir destination the Parquet is written there persistently and not
// uploaded; for an S3 destination it is staged under a fresh temp dir, uploaded,
// and the staging removed (so a re-run never re-uploads an old snapshot).
func (s *baselineSupervisor) execute(req console.BaselineRequest) (baseline.Stats, int, error) {
	if req.Flavor == console.FlavorPostgres {
		return s.executePG(req)
	}
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("create staging dir: %w", err)
	}

	dumpDir, err := os.MkdirTemp(s.stagingDir, "dump-")
	if err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("create dump dir: %w", err)
	}
	defer os.RemoveAll(dumpDir)

	// Captured immediately before invoking mydumper: since this pipeline runs
	// mydumper and baseline.Run in the same process, we can pass our own UTC
	// wall-clock time straight through as the snapshot anchor instead of
	// letting baseline.Run re-parse mydumper's "Started dump at" metadata
	// line — which is written in the dump host's LOCAL time and would
	// otherwise be misread as UTC verbatim, skewing the replay window by the
	// host's UTC offset (#768).
	dumpStartedAt := time.Now().UTC()
	if err := runMydumper(s.ctx, req.SourceDSN, req.Schemas, dumpDir, s.pointConsistent); err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("dump: %w", err)
	}

	outputDir := req.LocalDir
	if outputDir == "" { // S3-only: stage then upload, discard staging
		outputDir, err = os.MkdirTemp(s.stagingDir, "baseline-")
		if err != nil {
			return baseline.Stats{}, 0, fmt.Errorf("create baseline staging dir: %w", err)
		}
		defer os.RemoveAll(outputDir)
	}

	stats, err := baseline.Run(s.ctx, baseline.Config{
		InputDir:    dumpDir,
		OutputDir:   outputDir,
		Compression: "zstd",
		Timestamp:   dumpStartedAt,
	})
	if err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("convert: %w", err)
	}

	var uploaded int
	if req.S3 != "" {
		// Region/credentials come from the ambient AWS chain (env / ~/.aws / IAM
		// role), like every other S3 read the console does.
		uploaded, err = baseline.Upload(s.ctx, outputDir, req.S3, "", false)
		if err != nil {
			return baseline.Stats{}, 0, fmt.Errorf("upload: %w", err)
		}
	}
	return stats, uploaded, nil
}

// executePG produces a PostgreSQL baseline in-process via internal/pgbaseline —
// COPY straight to Parquet, anchored at the slot's consistent-point LSN. No
// mydumper subprocess and no #768 timestamp skew: pgbaseline self-stamps the
// snapshot time from the database's own now(). Destination handling mirrors
// execute(): a local dir is written persistently; S3-only stages in a temp dir,
// uploads via the same source-agnostic baseline.Upload, and discards the staging.
func (s *baselineSupervisor) executePG(req console.BaselineRequest) (baseline.Stats, int, error) {
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("create staging dir: %w", err)
	}
	outputDir := req.LocalDir
	if outputDir == "" { // S3-only: stage then upload, discard staging
		var err error
		outputDir, err = os.MkdirTemp(s.stagingDir, "pgbaseline-")
		if err != nil {
			return baseline.Stats{}, 0, fmt.Errorf("create baseline staging dir: %w", err)
		}
		defer os.RemoveAll(outputDir)
	}

	cfg, err := pgBaselineConfig(req, outputDir)
	if err != nil {
		return baseline.Stats{}, 0, err
	}
	pgStats, err := pgbaseline.Run(s.ctx, cfg)
	if err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("pg baseline: %w", err)
	}

	var uploaded int
	if req.S3 != "" {
		uploaded, err = baseline.Upload(s.ctx, outputDir, req.S3, "", false)
		if err != nil {
			return baseline.Stats{}, 0, fmt.Errorf("upload: %w", err)
		}
	}
	return baseline.Stats{
		TablesProcessed: pgStats.TablesProcessed,
		RowsWritten:     pgStats.RowsWritten,
		FilesWritten:    pgStats.FilesWritten,
	}, uploaded, nil
}

// pgBaselineConfig builds the pgbaseline.Config for a PG source, mirroring
// cmd/bintrail-pg's pgBaselineConfigFromFlags. The replication DSN is derived
// from the stored query DSN (console.PGReplDSN — the one home for that
// derivation), needed so pgbaseline can CREATE the slot when a user baselines
// BEFORE the first monitor start; harmless if the slot already exists. Pure —
// unit-testable without a live PG. The registry carries only a schema filter.
func pgBaselineConfig(req console.BaselineRequest, outputDir string) (pgbaseline.Config, error) {
	replDSN, err := console.PGReplDSN(req.SourceDSN)
	if err != nil {
		return pgbaseline.Config{}, err
	}
	return pgbaseline.Config{
		QueryDSN:    req.SourceDSN,
		ReplDSN:     replDSN,
		SlotName:    req.Slot,
		Publication: req.Publication,
		Filters:     cliutil.BuildIndexFilters(strings.Join(req.Schemas, ","), ""),
		OutputDir:   outputDir,
		Compression: "zstd",
	}, nil
}

// runMydumper invokes the bundled mydumper binary against the source DSN, writing
// a dump (with binlog coordinates in its metadata, which baseline.Run reads) into
// dumpDir. The image pins the SAME mydumper version the compose baseline-dump
// pipeline uses, so a console-created baseline matches a CLI/compose one exactly.
// pointConsistent selects the lock mode — see buildConsoleMydumperArgs.
func runMydumper(ctx context.Context, sourceDSN string, schemas []string, dumpDir string, pointConsistent bool) error {
	host, port, user, password, err := config.ParseSourceDSN(sourceDSN)
	if err != nil {
		return err
	}

	if pointConsistent {
		// Hard gate, unlike the NO_LOCK warning below: granting BACKUP_ADMIN
		// without RELOAD/FLUSH_TABLES does not fail cleanly in mydumper — it
		// SEGFAULTS (verified against the pinned build, #800). Never skipped.
		if err := checkPointConsistentPrivileges(ctx, sourceDSN); err != nil {
			return err
		}
	} else {
		// Best-effort, advisory only — never blocks or fails the dump. See
		// warnIfMultiTableNoLock.
		warnIfMultiTableNoLock(ctx, sourceDSN, schemas)
	}

	args := buildConsoleMydumperArgs(host, port, user, schemas, dumpDir, pointConsistent)
	cmd := exec.CommandContext(ctx, "mydumper", args...)
	// Deliver the source password out of band via MYSQL_PWD (honored by the
	// MySQL client library mydumper links against) so it never lands on argv,
	// where it would be world-readable in `ps aux` / /proc/<pid>/cmdline. The
	// child's /proc/<pid>/environ is mode 0400 (#811).
	if password != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+password)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		if msg := strings.TrimSpace(string(out)); msg != "" {
			return fmt.Errorf("mydumper failed: %w; output: %s", err, msg)
		}
		return fmt.Errorf("mydumper failed: %w", err)
	}
	return nil
}

// systemSchemaExcludeRegex dumps every USER schema but excludes the MySQL system
// schemas, matching the compose baseline-dump pipeline (#612). A least-privilege
// capture user (REPLICATION + SELECT, no SHOW VIEW) cannot read the sys views, so
// an unfiltered mydumper dies with "SHOW VIEW command denied … sys.host_summary";
// the system schemas are useless as a baseline anyway. mydumper uses PCRE, so the
// negative lookahead drops a system db both bare and as <db>.<table>.
const systemSchemaExcludeRegex = `^(?!(mysql|sys|performance_schema|information_schema)($|\.))`

// buildConsoleMydumperArgs builds the mydumper argument slice for the console's
// in-process dump. It mirrors `bintrail dump` / the compose baseline-dump
// invocation for the shared flags; the lock mode is selected by pointConsistent
// (#800):
//
//   - pointConsistent=false (the default): --sync-thread-lock-mode NO_LOCK
//     --trx-tables. Each mydumper worker opens its own consistent snapshot
//     independently, with NO synchronization barrier between workers/tables —
//     this is a per-table transactional snapshot, NOT a cross-table-consistent
//     one. On a write-heavy source, different tables (and the metadata's
//     recorded binlog coordinates) can be anchored at slightly different
//     instants, and a multi-table reconstruct spanning a parent/child FK pair
//     can be mutually inconsistent. Non-transactional tables (MyISAM) get no
//     consistency guarantee at all — but they ARE still dumped: under NO_LOCK,
//     --trx-tables' non-transactional-table check does not trigger a refusal
//     (only a warning about binlog-coordinate accuracy), unlike under FTWRL
//     below, where the SAME flag hard-refuses on the SAME table type. Verified
//     empirically in the same session (#800): a MyISAM table dumped
//     successfully (warning only) under NO_LOCK, and was hard-refused under
//     FTWRL — the check is evidently gated to an actual "consistent backup
//     attempt" (mydumper's own wording), which NO_LOCK explicitly is not
//     attempting. It is the default because it needs no elevated privilege: a
//     least-privilege replication user (SELECT + REPLICATION CLIENT only) can
//     run it — verified against a real Percona 8.0 source. See
//     docs/dump-and-baseline.md ("Cross-table consistency") for the
//     operator-facing writeup.
//   - pointConsistent=true (opt-in via BINTRAIL_CONSOLE_BASELINE_POINT_CONSISTENT=1):
//     --sync-thread-lock-mode FTWRL --trx-tables (same --trx-tables as the
//     default — NOT --no-trx-tables). FTWRL (FLUSH TABLES WITH READ LOCK) is
//     mydumper's own built-in sync mode: it holds one global read lock just long
//     enough for every worker to open its consistent snapshot at the SAME
//     instant, then releases it. This gives one point-in-time snapshot across
//     every TRANSACTIONAL table — it does NOT cover non-transactional (MyISAM)
//     tables: unlike the default mode above, mydumper itself detects a MyISAM
//     table under --trx-tables HERE and refuses to run at all ("Non
//     transactional table found ... Restart backup using --trx-tables=0"), a
//     clean failure rather than a silent gap — the check is gated to a
//     "consistent backup attempt," which FTWRL is and NO_LOCK isn't. Requires
//     RELOAD or the FLUSH_TABLES dynamic privilege (for `FLUSH TABLES WITH READ
//     LOCK`) on EVERY flavor, verified against the pinned mydumper build
//     (v1.0.3-1, #800), PLUS BACKUP_ADMIN (for `LOCK INSTANCE FOR BACKUP`,
//     checked first) ONLY on MySQL/Percona 8.0+ — BACKUP_ADMIN is a MySQL 8.0+
//     dynamic privilege that does not exist on MariaDB or MySQL 5.7, and
//     neither issues LOCK INSTANCE FOR BACKUP (see sourceRequiresBackupAdmin).
//     On sources where BOTH are required, they are required together: granting
//     BACKUP_ADMIN WITHOUT RELOAD/FLUSH_TABLES does NOT fail cleanly — the
//     pinned mydumper build SEGFAULTS instead (reproduced on both amd64 and
//     arm64). checkPointConsistentPrivileges below exists specifically to catch
//     that half-privileged state and turn it into a clean Go error before
//     mydumper ever runs, since this code deliberately never lets mydumper
//     crash or silently fall back to NO_LOCK.
//
// Schema selection: single → --database; multiple → an anchored --regex; none →
// every user schema with the system schemas excluded. Extracted for unit testing
// without a live mydumper.
//
// The source password is NEVER placed on argv (world-readable via `ps aux` /
// /proc/<pid>/cmdline); runMydumper delivers it via MYSQL_PWD in the child env
// (#811).
func buildConsoleMydumperArgs(host string, port uint16, user string, schemas []string, dumpDir string, pointConsistent bool) []string {
	syncMode := "NO_LOCK"
	if pointConsistent {
		syncMode = "FTWRL"
	}
	args := []string{
		"--host", host,
		"--port", strconv.Itoa(int(port)),
		"--user", user,
		"--threads", "4",
		"--compress-protocol",
		"--complete-insert",
		"--sync-thread-lock-mode", syncMode, "--trx-tables",
	}
	switch {
	case len(schemas) == 1:
		args = append(args, "--database", schemas[0])
	case len(schemas) > 1:
		args = append(args, "--regex", "^("+strings.Join(schemas, "|")+")\\.")
	default:
		args = append(args, "--regex", systemSchemaExcludeRegex)
	}
	// --outputdir last: docker wrapper scripts read the last arg for the mount.
	args = append(args, "--outputdir", dumpDir)
	return args
}

// pointConsistentRequiredPrivileges are the MySQL privileges the pinned
// mydumper build (v1.0.3-1) can need for --sync-thread-lock-mode FTWRL,
// verified empirically against a real MySQL 8.0 source (#800): BACKUP_ADMIN
// for `LOCK INSTANCE FOR BACKUP` (checked first — missing it alone fails
// cleanly with a CRITICAL "Access denied ... BACKUP_ADMIN" and exit 1) and
// RELOAD or the FLUSH_TABLES dynamic privilege for the subsequent `FLUSH
// TABLES WITH READ LOCK` (missing THIS one while BACKUP_ADMIN is present does
// NOT fail cleanly — mydumper segfaults). BACKUP_ADMIN itself is required only
// on MySQL/Percona 8.0+ — see sourceRequiresBackupAdmin. Both classic (RELOAD)
// and dynamic (BACKUP_ADMIN, FLUSH_TABLES) privileges appear side by side in
// information_schema.USER_PRIVILEGES on MySQL 8.0+.
var pointConsistentRequiredPrivileges = []string{"BACKUP_ADMIN", "RELOAD", "FLUSH_TABLES"}

// sourceRequiresBackupAdmin reports whether the connected server needs the
// BACKUP_ADMIN privilege for FTWRL's `LOCK INSTANCE FOR BACKUP` step.
// BACKUP_ADMIN is a MySQL 8.0+ dynamic privilege (also present on Percona
// Server 8.0+, a MySQL 8.0 fork) — it does not exist on MariaDB (any version)
// or MySQL 5.7, and neither issues `LOCK INSTANCE FOR BACKUP`, so FTWRL there
// needs only RELOAD/FLUSH_TABLES (#800 review). Requiring BACKUP_ADMIN
// unconditionally would make point-consistent mode permanently unusable on
// those sources: `GRANT BACKUP_ADMIN` itself errors on MariaDB, since the
// privilege doesn't exist there at all — there would be no way forward.
//
// Detection is a single SELECT VERSION(), mirroring the same MariaDB
// substring check metadata.DetectFlavor uses (self-contained here rather than
// imported, since this also needs the major version number DetectFlavor
// doesn't expose). An unparseable version string defaults to false (does NOT
// require BACKUP_ADMIN) — the safe direction: if the server actually needs it
// and this under-requires, mydumper's own LOCK INSTANCE FOR BACKUP check
// still fails cleanly on its own (missing BACKUP_ADMIN alone is always a
// clean failure, verified — never the segfault); the dangerous direction is
// over-requiring a privilege that doesn't exist on the source's actual
// flavor, which is exactly the bug this function exists to avoid.
func sourceRequiresBackupAdmin(ctx context.Context, db *sql.DB) (bool, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return false, fmt.Errorf("point-consistent baseline: cannot read source VERSION() to determine privilege requirements: %w", err)
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return false, nil
	}
	major, ok := serverMajorVersion(version)
	if !ok {
		return false, nil
	}
	return major >= 8, nil
}

// serverMajorVersion extracts the leading major version number from a
// SELECT VERSION() string (e.g. "8.0.46" → 8, "5.7.44" → 5, "10.11.6-MariaDB"
// → 10). Returns ok=false for anything it cannot parse.
func serverMajorVersion(version string) (major int, ok bool) {
	dot := strings.IndexByte(version, '.')
	if dot <= 0 {
		return 0, false
	}
	n, err := strconv.Atoi(version[:dot])
	if err != nil {
		return 0, false
	}
	return n, true
}

// checkPointConsistentPrivileges queries the source for its required
// point-consistent privileges and fails loudly, before mydumper ever runs,
// unless they are present. This is a hard gate, not an advisory warning:
// unlike the NO_LOCK skew warning below, a query failure here aborts the dump
// rather than being swallowed, because the alternative — letting mydumper
// attempt FTWRL half-privileged — is a segfault, not a clean error (#800).
// Never silently falls back to NO_LOCK.
func checkPointConsistentPrivileges(ctx context.Context, sourceDSN string) error {
	db, err := config.Connect(sourceDSN)
	if err != nil {
		return fmt.Errorf("point-consistent baseline: cannot connect to source to verify privileges: %w", err)
	}
	defer db.Close()
	return checkPointConsistentPrivilegesDB(ctx, db)
}

// checkPointConsistentPrivilegesDB is checkPointConsistentPrivileges' core logic
// against an already-open *sql.DB, split out so it is unit-testable with
// sqlmock without a live MySQL connection. It first determines whether this
// source's flavor/version needs BACKUP_ADMIN at all (sourceRequiresBackupAdmin)
// — RELOAD/FLUSH_TABLES is mandatory on every flavor, BACKUP_ADMIN only on
// MySQL/Percona 8.0+ — then reads the CURRENT_USER()'s own privileges from
// information_schema.USER_PRIVILEGES, which is self-scoped to the connecting
// user without needing any special grant, so it works even for an otherwise
// least-privilege replication user. The GRANTEE reconstruction (split
// CURRENT_USER() on '@', requote both halves) was verified against a real
// MySQL 8.0 server for both a wildcard-host account ('user'@'%') and a
// specific-host-pattern account ('user'@'172.20.%.%') — CURRENT_USER() always
// returns the exact host pattern from the matched mysql.user row, not the
// connecting client's resolved address, so the reconstruction is not a
// wildcard-only coincidence. Known gap, surfaced in the refusal message below
// so an affected operator can self-diagnose: privileges granted only via an
// activated MySQL 8.0 ROLE (not directly to the user) are attributed to the
// role's own GRANTEE in this view and would not be picked up here — not a
// pattern used anywhere else in this codebase's documented grant examples, and
// this fails CLOSED (over-refuses, never under-refuses) for a role-using
// operator, so the segfault path stays unreachable either way.
func checkPointConsistentPrivilegesDB(ctx context.Context, db *sql.DB) error {
	requireBackupAdmin, err := sourceRequiresBackupAdmin(ctx, db)
	if err != nil {
		return err
	}

	const query = `SELECT PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES ` +
		`WHERE GRANTEE = CONCAT("'", SUBSTRING_INDEX(CURRENT_USER(), '@', 1), "'@'", SUBSTRING_INDEX(CURRENT_USER(), '@', -1), "'")`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("point-consistent baseline: cannot verify source privileges: %w", err)
	}
	defer rows.Close()

	have := make(map[string]bool, len(pointConsistentRequiredPrivileges))
	for rows.Next() {
		var priv string
		if err := rows.Scan(&priv); err != nil {
			return fmt.Errorf("point-consistent baseline: cannot read source privileges: %w", err)
		}
		have[priv] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("point-consistent baseline: cannot read source privileges: %w", err)
	}

	hasBackupAdmin := have["BACKUP_ADMIN"]
	hasFlushTables := have["RELOAD"] || have["FLUSH_TABLES"]
	missingFlushTables := !hasFlushTables
	missingBackupAdmin := requireBackupAdmin && !hasBackupAdmin

	if !missingFlushTables && !missingBackupAdmin {
		return nil
	}

	const roleCaveat = " (privileges granted only via an activated MySQL ROLE, rather than directly to the user, are not detected by this check — grant directly to the user, or double-check with SHOW GRANTS if you believe this refusal is wrong)"

	switch {
	case missingFlushTables && missingBackupAdmin:
		return errors.New("point-consistent baseline mode (BINTRAIL_CONSOLE_BASELINE_POINT_CONSISTENT=1) requires the " +
			"source DB user to have BOTH the BACKUP_ADMIN and the RELOAD (or FLUSH_TABLES) privilege (MySQL/Percona " +
			"8.0+); the current user has neither — grant both, e.g. GRANT BACKUP_ADMIN, RELOAD ON *.* TO '<user>'@'%', " +
			"or disable point-consistent mode" + roleCaveat)
	case missingFlushTables && requireBackupAdmin:
		// The dangerous half-privileged combination: BACKUP_ADMIN is present but
		// RELOAD/FLUSH_TABLES is not — this is exactly what segfaults mydumper.
		return errors.New("point-consistent baseline mode requires the RELOAD (or FLUSH_TABLES) privilege in addition " +
			"to BACKUP_ADMIN, which the current user already has — granting BACKUP_ADMIN alone makes mydumper crash " +
			"rather than fail cleanly; grant RELOAD, e.g. GRANT RELOAD ON *.* TO '<user>'@'%', or disable " +
			"point-consistent mode" + roleCaveat)
	case missingFlushTables:
		// requireBackupAdmin is false here (MariaDB, MySQL 5.7, or an
		// undetectable version) — BACKUP_ADMIN is never mentioned since it
		// isn't required, and on MariaDB the privilege doesn't even exist.
		return errors.New("point-consistent baseline mode requires the RELOAD (or FLUSH_TABLES) privilege; the current " +
			"user has neither — grant it, e.g. GRANT RELOAD ON *.* TO '<user>'@'%', or disable point-consistent mode" +
			roleCaveat)
	default:
		// missingBackupAdmin only: requireBackupAdmin is true and hasFlushTables
		// is true.
		return errors.New("point-consistent baseline mode requires the BACKUP_ADMIN privilege (MySQL/Percona 8.0+) in " +
			"addition to RELOAD/FLUSH_TABLES, which the current user already has — grant it, e.g. " +
			"GRANT BACKUP_ADMIN ON *.* TO '<user>'@'%', or disable point-consistent mode" + roleCaveat)
	}
}

// dumpableTableCountQuery builds the information_schema.TABLES COUNT(*) query and
// its args, approximating buildConsoleMydumperArgs' own schema selection (single
// schema, an explicit list, or every non-system schema when none is given). Used
// only by warnIfMultiTableNoLock below — an advisory approximation, not a
// guarantee: a concurrent DDL between this query and mydumper's own table
// discovery could disagree, and that is fine, since the warning is advisory, not
// a correctness gate. Pure and unit-testable without a live database.
func dumpableTableCountQuery(schemas []string) (string, []any) {
	const base = "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND "
	if len(schemas) == 0 {
		return base + "TABLE_SCHEMA NOT IN ('mysql','sys','performance_schema','information_schema')", nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(schemas)), ",")
	args := make([]any, len(schemas))
	for i, s := range schemas {
		args[i] = s
	}
	return base + "TABLE_SCHEMA IN (" + placeholders + ")", args
}

// warnIfMultiTableNoLock logs an advisory warning when a NO_LOCK dump (the
// default) is about to span more than one table, pointing operators at the
// opt-in point-consistent mode and the docs (#800). It is best-effort: opening
// the source or running the count query never fails or delays the dump — an
// error here is logged at Debug and swallowed, since this is a UX nudge, not a
// correctness gate. Only called when pointConsistent is false.
func warnIfMultiTableNoLock(ctx context.Context, sourceDSN string, schemas []string) {
	db, err := config.Connect(sourceDSN)
	if err != nil {
		slog.Debug("baseline: could not open source to check table count for the NO_LOCK skew warning", "error", err)
		return
	}
	defer db.Close()

	count, err := countDumpableTables(ctx, db, schemas)
	if err != nil {
		slog.Debug("baseline: could not count tables for the NO_LOCK skew warning", "error", err)
		return
	}
	if count > 1 {
		slog.Warn("baseline: dumping multiple tables under the default NO_LOCK mode — each table's snapshot is "+
			"anchored at a slightly different instant (no cross-table synchronization barrier), so a multi-table "+
			"reconstruct (e.g. a parent/child FK pair) can be mutually inconsistent; set "+
			"BINTRAIL_CONSOLE_BASELINE_POINT_CONSISTENT=1 for a single point-in-time snapshot across all transactional "+
			"tables (requires the RELOAD or FLUSH_TABLES privilege; MySQL/Percona 8.0+ also requires BACKUP_ADMIN, not "+
			"needed on MariaDB or MySQL 5.7) — see docs/dump-and-baseline.md",
			"tables", count)
	}
}

// countDumpableTables runs dumpableTableCountQuery against db and returns the
// result.
func countDumpableTables(ctx context.Context, db *sql.DB, schemas []string) (int, error) {
	query, args := dumpableTableCountQuery(schemas)
	var count int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// nowStamp is the RFC3339 timestamp used in job status fields.
func nowStamp() string { return time.Now().UTC().Format(time.RFC3339) }
