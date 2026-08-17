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
	"fmt"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
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

// forMode renders this surface's way of selecting a specific mode.
func (r Remedy) forMode(m baseline.LockMode) string {
	if r == RemedyCLI {
		return "pass --lock-mode " + string(m)
	}
	return "set BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=" + string(m)
}

const (
	// RemedyCLI is `bintrail dump`'s knob.
	RemedyCLI Remedy = "cli"
	// RemedyConsole is the console daemon's; it has no flag surface.
	RemedyConsole Remedy = "console"
)

func CheckPrivileges(ctx context.Context, sourceDSN string, mode baseline.LockMode, remedy Remedy) error {
	db, err := config.Connect(sourceDSN)
	if err != nil {
		return fmt.Errorf("point-consistent baseline: cannot connect to source to verify privileges: %w", err)
	}
	defer db.Close()
	return checkPrivilegesDB(ctx, db, mode, remedy)
}

// grantedPrivileges reads the connecting user's GLOBAL privileges.
//
// It parses SHOW GRANTS FOR CURRENT_USER() rather than reading
// information_schema.USER_PRIVILEGES, which was the source until this was
// measured against RDS: that view exposes only the rows the connecting user
// can SEE in mysql.user, and a managed master user cannot see its own. On RDS
// MySQL 8.4 it returned exactly one row — USAGE — for an account whose
// SHOW GRANTS listed RELOAD and FLUSH_TABLES as direct grants. Reading it made
// this check fail CLOSED on every managed source, which since #1377 made the
// point-consistent default unreachable there. SHOW GRANTS needs no privilege
// to run for the current user and expands whatever the session actually has.
//
// Only global grants count: a privilege is what it is on `ON *.*`, and a
// schema-scoped grant does not authorize FLUSH TABLES WITH READ LOCK or
// LOCK INSTANCE FOR BACKUP.
func grantedPrivileges(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return nil, fmt.Errorf("point-consistent baseline: cannot verify source privileges: %w", err)
	}
	defer rows.Close()

	have := map[string]bool{}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("point-consistent baseline: cannot read source privileges: %w", err)
		}
		// "GRANT <privs> ON *.* TO ..." — anything else (schema grants, role
		// memberships, proxy grants) carries no global privilege.
		const on = " ON *.* TO "
		idx := strings.Index(line, on)
		if idx < 0 || !strings.HasPrefix(line, "GRANT ") {
			continue
		}
		for _, p := range strings.Split(line[len("GRANT "):idx], ",") {
			p = strings.ToUpper(strings.TrimSpace(p))
			// A privilege can carry a column list, e.g. SELECT (col). Keep the
			// name; the parenthesised part is never global-only anyway.
			if k := strings.IndexByte(p, '('); k >= 0 {
				p = strings.TrimSpace(p[:k])
			}
			if p != "" {
				have[p] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("point-consistent baseline: cannot read source privileges: %w", err)
	}
	return have, nil
}

// checkPrivilegesDB is CheckPrivileges' core logic against an already-open
// *sql.DB, split out so it is unit-testable with sqlmock. Requirements are
// PER MODE, because they differ in kind:
//
//   - FTWRL needs RELOAD or FLUSH_TABLES on every flavor, plus BACKUP_ADMIN on
//     MySQL/Percona 8.0+ (it issues LOCK INSTANCE FOR BACKUP first). Granting
//     BACKUP_ADMIN WITHOUT RELOAD does not fail cleanly — the pinned build
//     SEGFAULTS — which is why this check exists at all.
//   - LOCK_ALL needs LOCK TABLES and nothing else. This is the mode that works
//     on managed MySQL, where BACKUP_ADMIN is not grantable.
//   - The low-privilege modes never reach here.
//
// ALL PRIVILEGES satisfies any of them.
func checkPrivilegesDB(ctx context.Context, db *sql.DB, mode baseline.LockMode, remedy Remedy) error {
	have, err := grantedPrivileges(ctx, db)
	if err != nil {
		return err
	}
	if have["ALL PRIVILEGES"] {
		return nil
	}

	const roleCaveat = " (if these are granted through a role, activate it for this connection — SHOW GRANTS reflects the session's active roles)"

	if mode == baseline.LockModeLockAll {
		if have["LOCK TABLES"] {
			return nil
		}
		return fmt.Errorf("lock-all baseline mode requires the LOCK TABLES privilege; the current user does not have it"+
			" — grant it, e.g. GRANT LOCK TABLES ON *.* TO '<user>'@'%%'%s", roleCaveat)
	}

	requireBackupAdmin, err := requiresBackupAdmin(ctx, db)
	if err != nil {
		return err
	}
	hasFlush := have["RELOAD"] || have["FLUSH_TABLES"]
	missingBackupAdmin := requireBackupAdmin && !have["BACKUP_ADMIN"]
	if hasFlush && !missingBackupAdmin {
		return nil
	}

	// The alternatives clause is the one actionable sentence here, and this
	// gate fires on the DEFAULT path. lock-all comes FIRST because it is the
	// only alternative that is still point-consistent — and on managed MySQL
	// (RDS grants LOCK TABLES but refuses BACKUP_ADMIN outright) it is the
	// only one that can work at all.
	// The managed-MySQL parenthetical is gated on requireBackupAdmin: BACKUP_ADMIN
	// does not exist on MariaDB or MySQL 5.7, and naming it there would send an
	// operator hunting a privilege their server cannot have.
	lockAllNote := ", which is also point-consistent and needs only LOCK TABLES"
	if requireBackupAdmin {
		lockAllNote += " (the mode that works on managed MySQL, where BACKUP_ADMIN cannot be granted at all)"
	}
	alternatives := " — or " + remedy.forMode(baseline.LockModeLockAll) + lockAllNote +
		"; or " + remedy.forMode(baseline.LockModeSafeNoLock) +
		", which needs no privilege but ABORTS rather than write a snapshot stitched from several instants," +
		" so expect it to refuse on a write-active source; or no-lock to accept such a snapshot"

	switch {
	case !hasFlush && missingBackupAdmin:
		return fmt.Errorf("point-consistent baseline mode (the default) requires the source DB user to have BOTH the"+
			" BACKUP_ADMIN and the RELOAD (or FLUSH_TABLES) privilege (MySQL/Percona 8.0+); the current user has"+
			" neither — grant both, e.g. GRANT BACKUP_ADMIN, RELOAD ON *.* TO '<user>'@'%%'%s%s", alternatives, roleCaveat)
	case !hasFlush:
		return fmt.Errorf("point-consistent baseline mode requires the RELOAD (or FLUSH_TABLES) privilege; the current"+
			" user has neither — grant it, e.g. GRANT RELOAD ON *.* TO '<user>'@'%%'%s%s", alternatives, roleCaveat)
	default:
		return fmt.Errorf("point-consistent baseline mode requires the BACKUP_ADMIN privilege (MySQL/Percona 8.0+) in"+
			" addition to RELOAD/FLUSH_TABLES, which the current user already has — grant it, e.g."+
			" GRANT BACKUP_ADMIN ON *.* TO '<user>'@'%%'. NOTE: on managed MySQL such as RDS this grant is REFUSED"+
			" outright, so ftwrl cannot work there%s%s", alternatives, roleCaveat)
	}
}
