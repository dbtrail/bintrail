// Package mydumperlock checks whether a source grants the privileges
// mydumper's point-consistent sync mode needs, BEFORE mydumper is launched.
//
// It exists because the failure is not clean: granting BACKUP_ADMIN without
// RELOAD/FLUSH_TABLES makes the pinned mydumper build SEGFAULT rather than
// report a privilege error (#800, reproduced on amd64 and arm64). It lives in
// its own package because BOTH baseline surfaces need it — `bintrail dump` and
// the console's in-process pipeline — and since #1377 made the point-consistent
// mode the DEFAULT, a surface without this check walks into that crash by
// doing nothing unusual.
package mydumperlock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/config"
)

// requiredPrivileges are the MySQL privileges the pinned
// mydumper build (v1.0.3-1) can need for --sync-thread-lock-mode FTWRL,
// verified empirically against a real MySQL 8.0 source (#800): BACKUP_ADMIN
// for `LOCK INSTANCE FOR BACKUP` (checked first — missing it alone fails
// cleanly with a CRITICAL "Access denied ... BACKUP_ADMIN" and exit 1) and
// RELOAD or the FLUSH_TABLES dynamic privilege for the subsequent `FLUSH
// TABLES WITH READ LOCK` (missing THIS one while BACKUP_ADMIN is present does
// NOT fail cleanly — mydumper segfaults). BACKUP_ADMIN itself is required only
// on MySQL/Percona 8.0+ — see RequiresBackupAdmin. Both classic (RELOAD)
// and dynamic (BACKUP_ADMIN, FLUSH_TABLES) privileges appear side by side in
// information_schema.USER_PRIVILEGES on MySQL 8.0+.
var requiredPrivileges = []string{"BACKUP_ADMIN", "RELOAD", "FLUSH_TABLES"}

// requiresBackupAdmin reports whether the connected server needs the
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
func requiresBackupAdmin(ctx context.Context, db *sql.DB) (bool, error) {
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

// CheckPrivileges queries the source for its required
// point-consistent privileges and fails loudly, before mydumper ever runs,
// unless they are present. This is a hard gate, not an advisory warning:
// unlike the no-lock skew warning in consoleapp, a query failure here aborts the dump
// rather than being swallowed, because the alternative — letting mydumper
// attempt FTWRL half-privileged — is a segfault, not a clean error (#800).
// Never silently falls back to NO_LOCK.
// Remedy is how the CALLING surface selects a weaker lock mode. It is a
// parameter because the refusal text is the one actionable sentence an
// operator gets, and naming the other surface's knob is worse than naming
// none: `bintrail dump` does not read the console's environment variable, and
// the console has no flags. Both callers reach this on their DEFAULT path
// since #1377, so this is the first thing an upgrading least-privilege
// deployment sees on either one.
type Remedy string

const (
	// RemedyCLI is `bintrail dump`'s knob.
	RemedyCLI Remedy = "pass --lock-mode safe-no-lock"
	// RemedyConsole is the console daemon's; it has no flag surface.
	RemedyConsole Remedy = "set BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=safe-no-lock"
)

func CheckPrivileges(ctx context.Context, sourceDSN string, remedy Remedy) error {
	db, err := config.Connect(sourceDSN)
	if err != nil {
		return fmt.Errorf("point-consistent baseline: cannot connect to source to verify privileges: %w", err)
	}
	defer db.Close()
	return checkPrivilegesDB(ctx, db, remedy)
}

// checkPrivilegesDB is checkPrivilegesDB is CheckPrivileges' core logic
// against an already-open *sql.DB, split out so it is unit-testable with
// sqlmock without a live MySQL connection. It first determines whether this
// source's flavor/version needs BACKUP_ADMIN at all (requiresBackupAdmin)
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
func checkPrivilegesDB(ctx context.Context, db *sql.DB, remedy Remedy) error {
	requireBackupAdmin, err := requiresBackupAdmin(ctx, db)
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

	have := make(map[string]bool, len(requiredPrivileges))
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

	// The alternatives clause is load-bearing: this gate now fires on the
	// DEFAULT path, so it is the first thing an upgrading least-privilege
	// deployment sees. It must name the knob that actually exists — the
	// pre-#1377 text pointed at an opt-in variable that is no longer read and
	// told the operator to "disable point-consistent mode", which is not a
	// thing they can do any more.
	alternatives := " — or " + string(remedy) + " to dump without these privileges" +
		" (it needs none, but ABORTS rather than write a snapshot stitched from several instants," +
		" so expect it to refuse on a write-active source); the same knob set to no-lock accepts such a snapshot"

	const roleCaveat = " (privileges granted only via an activated MySQL ROLE, rather than directly to the user, are not detected by this check — grant directly to the user, or double-check with SHOW GRANTS if you believe this refusal is wrong)"

	switch {
	case missingFlushTables && missingBackupAdmin:
		return errors.New("point-consistent baseline mode (the default) requires the " +
			"source DB user to have BOTH the BACKUP_ADMIN and the RELOAD (or FLUSH_TABLES) privilege (MySQL/Percona " +
			"8.0+); the current user has neither — grant both, e.g. GRANT BACKUP_ADMIN, RELOAD ON *.* TO '<user>'@'%'" +
			"" + alternatives + roleCaveat)
	case missingFlushTables && requireBackupAdmin:
		// The dangerous half-privileged combination: BACKUP_ADMIN is present but
		// RELOAD/FLUSH_TABLES is not — this is exactly what segfaults mydumper.
		return errors.New("point-consistent baseline mode requires the RELOAD (or FLUSH_TABLES) privilege in addition " +
			"to BACKUP_ADMIN, which the current user already has — granting BACKUP_ADMIN alone makes mydumper crash " +
			"rather than fail cleanly; grant RELOAD, e.g. GRANT RELOAD ON *.* TO '<user>'@'%'" + alternatives + roleCaveat)
	case missingFlushTables:
		// requireBackupAdmin is false here (MariaDB, MySQL 5.7, or an
		// undetectable version) — BACKUP_ADMIN is never mentioned since it
		// isn't required, and on MariaDB the privilege doesn't even exist.
		return errors.New("point-consistent baseline mode requires the RELOAD (or FLUSH_TABLES) privilege; the current " +
			"user has neither — grant it, e.g. GRANT RELOAD ON *.* TO '<user>'@'%'" + alternatives + roleCaveat)
	default:
		// missingBackupAdmin only: requireBackupAdmin is true and hasFlushTables
		// is true.
		return errors.New("point-consistent baseline mode requires the BACKUP_ADMIN privilege (MySQL/Percona 8.0+) in " +
			"addition to RELOAD/FLUSH_TABLES, which the current user already has — grant it, e.g. " +
			"GRANT BACKUP_ADMIN ON *.* TO '<user>'@'%'" + alternatives + roleCaveat)
	}
}
