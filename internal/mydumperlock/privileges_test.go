package mydumperlock

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// grantRows builds the SHOW GRANTS FOR CURRENT_USER() result set.
func grantRows(lines ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"Grants for user"})
	for _, l := range lines {
		r = r.AddRow(l)
	}
	return r
}

// TestCheckPrivilegesReadsShowGrants is the regression for the defect that
// broke a live RDS deployment. The check used to read
// information_schema.USER_PRIVILEGES, which only exposes the rows the
// connecting user can SEE in mysql.user — on RDS MySQL 8.4 that returned a
// single USAGE row for an account whose SHOW GRANTS listed RELOAD and
// FLUSH_TABLES as direct grants. The check therefore failed CLOSED on every
// managed source, which made the point-consistent default unreachable there.
//
// The grant lines below are copied from that real RDS master user.
func TestCheckPrivilegesReadsShowGrants(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, LOCK TABLES, REPLICATION CLIENT ON *.* TO `admin`@`%`",
		"GRANT APPLICATION_PASSWORD_ADMIN,FLUSH_TABLES,ROLE_ADMIN ON *.* TO `admin`@`%`",
		"GRANT `rds_superuser_role`@`%` TO `admin`@`%`",
	))
	mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.4.10"))

	// RELOAD is present but BACKUP_ADMIN is not, so ftwrl is still refused —
	// correctly, and that is the platform's limit, not a reading error. What
	// must NOT happen is the pre-fix behaviour: claiming the user has NEITHER.
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole, nil)
	if err == nil {
		t.Fatal("ftwrl was allowed without BACKUP_ADMIN")
	}
	if strings.Contains(err.Error(), "has neither") {
		t.Errorf("the check did not see RELOAD in SHOW GRANTS — it is reading a source that is blind on managed MySQL: %v", err)
	}
	if !strings.Contains(err.Error(), "BACKUP_ADMIN") {
		t.Errorf("refusal = %v, want it to name the one privilege actually missing", err)
	}
}

// TestCheckPrivilegesLockAllOnManagedMySQL: the same RDS grant set must PASS
// for lock-all. This is the whole point of adding the mode — it is the only
// point-consistent option available where BACKUP_ADMIN cannot be granted.
func TestCheckPrivilegesLockAllOnManagedMySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION CLIENT ON *.* TO `admin`@`%`",
	))
	if err := checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyConsole, nil); err != nil {
		t.Fatalf("lock-all was refused for a user holding LOCK TABLES: %v", err)
	}
	// It must not have asked for the version: lock-all's requirement does not
	// depend on flavor, and an extra round trip here would be dead weight.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected queries: %v", err)
	}
}

func TestCheckPrivilegesLockAllWithoutLockTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, REPLICATION CLIENT ON *.* TO `u`@`%`",
	))
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyCLI, nil)
	if err == nil {
		t.Fatal("lock-all was allowed without LOCK TABLES")
	}
	if !strings.Contains(err.Error(), "LOCK TABLES") {
		t.Errorf("refusal = %v, want it to name LOCK TABLES", err)
	}
}

// TestCheckPrivilegesFTWRLIgnoresSchemaScopedGrants: RELOAD, FLUSH_TABLES and
// BACKUP_ADMIN are global-only privileges, and a schema-scoped row must not be
// read as satisfying them. Accepting one would under-refuse — the direction
// that lets mydumper run half-privileged, which is what segfaults it.
func TestCheckPrivilegesFTWRLIgnoresSchemaScopedGrants(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT ON *.* TO `u`@`%`",
		"GRANT ALL PRIVILEGES ON `appdb`.* TO `u`@`%`",
	))
	mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyCLI, nil)
	if err == nil {
		t.Fatal("a schema-scoped ALL PRIVILEGES was accepted as authorizing FLUSH TABLES WITH READ LOCK")
	}
}

// TestCheckPrivilegesLockAllAcceptsSchemaScopedGrant is the regression for a
// false refusal this package shipped once already, in the very mode added to
// cure one: lock-all demanded LOCK TABLES globally.
//
// LOCK_ALL locks the EXPORTED TABLES, not the instance, so a grant covering the
// dumped schema is sufficient — measured, not reasoned: mydumper v1.0.3-1 ran a
// dump to completion against MySQL 8.0 as a user holding exactly
// `GRANT SELECT, LOCK TABLES, SHOW VIEW ON \`appdb\`.*`. Refusing that sends a
// least-privilege operator to widen a grant they did not need to widen, or to
// give up point-consistency — on managed MySQL, where lock-all is the ONLY
// point-consistent mode available, the second is the likelier outcome.
func TestCheckPrivilegesLockAllAcceptsSchemaScopedGrant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT REPLICATION CLIENT ON *.* TO `scoped`@`%`",
		"GRANT SELECT, LOCK TABLES, SHOW VIEW ON `appdb`.* TO `scoped`@`%`",
	))
	if err := checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyCLI, nil); err != nil {
		t.Fatalf("lock-all refused a per-schema LOCK TABLES grant that runs a real dump to completion: %v", err)
	}
}

// TestCheckPrivilegesFlushTablesSubstitutesForReload: on MySQL 8.0+ the dynamic
// FLUSH_TABLES privilege exists precisely so an account can be granted the
// narrow thing instead of all of RELOAD, so a least-privilege 8.0 deployment
// may hold it ALONE. Refusing that is the same class of over-refusal as #1381.
func TestCheckPrivilegesFlushTablesSubstitutesForReload(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, BACKUP_ADMIN, FLUSH_TABLES ON *.* TO `u`@`%`",
	))
	mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))
	if err := checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole, nil); err != nil {
		t.Fatalf("ftwrl refused a user holding FLUSH_TABLES (without the broader RELOAD): %v", err)
	}
}

// TestCheckPrivilegesBackupAdminWithoutReloadIsRefused pins the exact input
// this package exists for: BACKUP_ADMIN granted, RELOAD/FLUSH_TABLES not. The
// pinned mydumper build does not fail cleanly on it — it SEGFAULTS (#800,
// reproduced on amd64 and arm64). If this check ever accepts this grant set,
// the crash is reachable again from the DEFAULT path.
func TestCheckPrivilegesBackupAdminWithoutReloadIsRefused(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, BACKUP_ADMIN ON *.* TO `u`@`%`",
	))
	mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole, nil)
	if err == nil {
		t.Fatal("BACKUP_ADMIN without RELOAD/FLUSH_TABLES was accepted — this is the mydumper segfault input")
	}
	if !strings.Contains(err.Error(), "RELOAD") {
		t.Errorf("refusal = %v, want it to name the missing RELOAD", err)
	}
	// The operator in this state already granted BACKUP_ADMIN and needs to know
	// the remaining half is not just another clean refusal. Asserting the
	// CONSEQUENCE rather than one word of prose: a meaning-preserving reword
	// must not fail, but dropping the warning entirely must.
	if !strings.Contains(err.Error(), "BACKUP_ADMIN alone") {
		t.Errorf("refusal = %v, want it to warn about the half-grant it is refusing", err)
	}
}

// TestGrantSetParsing covers the shapes SHOW GRANTS actually emits. The parser
// must never REPORT a privilege the user lacks: a false positive lets mydumper
// launch half-privileged, which is the crash this package prevents.
func TestGrantSetParsing(t *testing.T) {
	for _, tc := range []struct {
		name       string
		line       string
		wantGlobal []string
		wantScoped bool
	}{
		{"global list", "GRANT SELECT, LOCK TABLES ON *.* TO `u`@`%`", []string{"SELECT", "LOCK TABLES"}, false},
		{"with grant option", "GRANT ALL PRIVILEGES ON *.* TO `r`@`localhost` WITH GRANT OPTION", []string{"ALL PRIVILEGES"}, false},
		// A role membership carries no privilege of its own. MySQL expands an
		// ACTIVE role's privileges into their own ON *.* lines in the same
		// result set, so skipping this loses nothing.
		{"role membership", "GRANT `r`@`%` TO `u`@`%`", nil, false},
		// PROXY's "object" is an account, not a schema, so it lands in scoped.
		// Harmless: nothing ever asks about PROXY, and it cannot manufacture an
		// entry under a name that is asked about. What matters is that it adds
		// nothing GLOBAL — asserted by the exact-length check below.
		{"proxy", "GRANT PROXY ON ``@`` TO `root`@`localhost` WITH GRANT OPTION", nil, true},
		{"schema scoped", "GRANT LOCK TABLES ON `appdb`.* TO `u`@`%`", nil, true},
		{"table scoped", "GRANT SELECT ON `appdb`.`t` TO `u`@`%`", nil, true},
		// Column names must not be parsed as privilege names: splitting on the
		// comma before stripping the parenthesised list would yield "B".
		{"column list", "GRANT SELECT (a, b) ON `appdb`.`t` TO `u`@`%`", nil, true},
		{"multi column list", "GRANT SELECT (a, b), INSERT (col2) ON `appdb`.`t` TO `u`@`%`", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &grantSet{global: map[string]bool{}, scoped: map[string][]string{}}
			g.add(tc.line)
			for _, w := range tc.wantGlobal {
				if !g.global[w] {
					t.Errorf("global %q missing from %q", w, tc.line)
				}
			}
			if len(tc.wantGlobal) != len(g.global) {
				t.Errorf("global = %v, want exactly %v", g.global, tc.wantGlobal)
			}
			if got := len(g.scoped) > 0; got != tc.wantScoped {
				t.Errorf("scoped = %v, want scoped=%v", g.scoped, tc.wantScoped)
			}
			// Check BOTH maps. Reading only `global` made this guard inert: a
			// column list is only legal on a scoped object, so a misparsed
			// column name lands in `scoped` and the global-only assertion
			// stayed green with the paren-stripping deleted.
			for _, bad := range []string{"B", "COL2", "A"} {
				if g.global[bad] || len(g.scoped[bad]) > 0 {
					t.Errorf("%q was parsed as a privilege name from %q", bad, tc.line)
				}
			}
		})
	}
}

func TestCheckPrivilegesAllPrivileges(t *testing.T) {
	for _, mode := range []baseline.LockMode{baseline.LockModeFTWRL, baseline.LockModeLockAll} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
			"GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost` WITH GRANT OPTION",
		))
		if err := checkPrivilegesDB(context.Background(), db, mode, RemedyConsole, nil); err != nil {
			t.Errorf("%s refused for ALL PRIVILEGES: %v", mode, err)
		}
		db.Close()
	}
}

// TestCheckPrivilegesMariaDBNeverMentionsBackupAdmin: BACKUP_ADMIN does not
// exist on MariaDB, and mydumper does not issue LOCK INSTANCE FOR BACKUP
// there. Naming it would send an operator hunting a privilege their server
// cannot have.
func TestCheckPrivilegesMariaDBNeverMentionsBackupAdmin(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT ON *.* TO `u`@`%`",
	))
	mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("10.11.6-MariaDB"))

	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole, nil)
	if err == nil {
		t.Fatal("ftwrl allowed without RELOAD")
	}
	if strings.Contains(err.Error(), "BACKUP_ADMIN") {
		t.Errorf("MariaDB refusal names BACKUP_ADMIN, which does not exist there: %v", err)
	}
}

// TestCheckPrivilegesRefusalOffersLockAllFirst: on a source that cannot do
// ftwrl, the operator's best move is the OTHER point-consistent mode, not a
// weaker one. On managed MySQL it is the only one that can work at all, so it
// must be named before safe-no-lock.
func TestCheckPrivilegesRefusalOffersLockAllFirst(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, RELOAD, LOCK TABLES ON *.* TO `admin`@`%`",
	))
	mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.4.10"))

	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole, nil)
	if err == nil {
		t.Fatal("ftwrl allowed without BACKUP_ADMIN")
	}
	msg := err.Error()
	la, snl := strings.Index(msg, "lock-all"), strings.Index(msg, "safe-no-lock")
	if la < 0 {
		t.Fatalf("refusal never names lock-all, the only point-consistent option left: %v", err)
	}
	if snl >= 0 && la > snl {
		t.Error("the refusal offers safe-no-lock before lock-all; it points the operator away from consistency first")
	}
	if !strings.Contains(msg, "RDS") {
		t.Error("refusal does not warn that this grant is refused outright on managed MySQL, which is where this branch is most often hit")
	}
}

// TestCheckPrivilegesRemedyIsSurfaceSpecific: the remedy is the one actionable
// sentence, and both surfaces reach it on their DEFAULT path. Naming the other
// surface's knob is worse than naming none — `bintrail dump` does not read the
// console's environment variable, and the console has no flags.
func TestCheckPrivilegesRemedyIsSurfaceSpecific(t *testing.T) {
	for _, tc := range []struct {
		remedy         Remedy
		want           string
		mustNotContain string
	}{
		{RemedyCLI, "--lock-mode lock-all", "BINTRAIL_CONSOLE"},
		{RemedyConsole, "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all", "--lock-mode"},
	} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows("GRANT SELECT ON *.* TO `u`@`%`"))
		mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))
		err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, tc.remedy, nil)
		db.Close()
		if err == nil {
			t.Fatalf("%s: a user with no privileges was allowed to run a point-consistent dump", tc.remedy)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: refusal = %q, want it to name %q", tc.remedy, err, tc.want)
		}
		if strings.Contains(err.Error(), tc.mustNotContain) {
			t.Errorf("%s: refusal names the OTHER surface's knob (%q), which this caller does not read", tc.remedy, tc.mustNotContain)
		}
	}
}

// TestServerMajorVersion covers the version-string parsing behind the
// BACKUP_ADMIN requirement.
func TestServerMajorVersion(t *testing.T) {
	for _, tc := range []struct {
		version   string
		wantMajor int
		wantOK    bool
	}{
		{"8.0.36", 8, true},
		{"8.4.10", 8, true},
		{"5.7.44-log", 5, true},
		{"10.11.6-MariaDB", 10, true},
		{"", 0, false},
		{"weird", 0, false},
	} {
		major, ok := serverMajorVersion(tc.version)
		if major != tc.wantMajor || ok != tc.wantOK {
			t.Errorf("serverMajorVersion(%q) = (%d, %v), want (%d, %v)", tc.version, major, ok, tc.wantMajor, tc.wantOK)
		}
	}
}

// TestCheckPrivilegesPartialRevokeIsNotIgnored: MySQL 8.0.16+ renders a partial
// revoke as its OWN line in SHOW GRANTS, and reading only the GRANT lines
// reports a privilege the user does not have. Measured on MySQL 8.0.46 with
// partial_revokes=ON — this exact grant set produced
// "ERROR 1044 Access denied for user 'prtest'@'%' to database 'appdb'" on
// LOCK TABLES in appdb.
//
// This is the FALSE-PASS direction, the one this package exists to prevent:
// the preflight would wave the dump through and mydumper would die partway,
// leaving a partial dump directory, which is precisely what "fails loudly,
// before mydumper ever runs" promises not to happen.
func TestCheckPrivilegesPartialRevokeIsNotIgnored(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, LOCK TABLES, REPLICATION CLIENT ON *.* TO `prtest`@`%`",
		"REVOKE LOCK TABLES ON `appdb`.* FROM `prtest`@`%`",
	))
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyCLI, []string{"appdb"})
	if err == nil {
		t.Fatal("a partial REVOKE of LOCK TABLES on the dumped schema was ignored — mydumper would launch and fail partway")
	}
	if !strings.Contains(err.Error(), "appdb") {
		t.Errorf("refusal = %v, want it to name the schema the revoke applies to", err)
	}
}

// TestCheckPrivilegesPartialRevokeElsewhereIsNotABlocker is the other half, and
// the more important one: refusing a dump a revoke does not touch would be the
// same false refusal #1381 was filed for. The revoke here names a schema that
// is not in the dump.
func TestCheckPrivilegesPartialRevokeElsewhereIsNotABlocker(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, LOCK TABLES ON *.* TO `u`@`%`",
		"REVOKE LOCK TABLES ON `otherdb`.* FROM `u`@`%`",
	))
	if err := checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyCLI, []string{"appdb"}); err != nil {
		t.Fatalf("lock-all refused over a revoke on a schema this dump never touches: %v", err)
	}
}

// TestCheckPrivilegesPartialRevokeWithNoSchemaFilter: an empty schema list means
// "dump every non-system schema", so a revoke anywhere IS inside the dump.
func TestCheckPrivilegesPartialRevokeWithNoSchemaFilter(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT, LOCK TABLES ON *.* TO `u`@`%`",
		"REVOKE LOCK TABLES ON `otherdb`.* FROM `u`@`%`",
	))
	if err := checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyCLI, nil); err == nil {
		t.Fatal("a whole-instance dump ignored a partial revoke; every schema is in scope when no filter is set")
	}
}

// TestCheckPrivilegesConnectFailureOffersNoWeakerMode: an unreachable source is
// not a privilege problem — mydumper cannot dump in ANY mode. Suggesting a
// weaker one there is worse than saying nothing: on the console the knob is a
// DAEMON environment variable, so an operator who flips it to get past a
// transient blip silently degrades every future baseline, with nothing to
// expire it.
func TestCheckPrivilegesConnectFailureOffersNoWeakerMode(t *testing.T) {
	err := CheckPrivileges(context.Background(), "u:p@tcp(127.0.0.1:1)/db",
		baseline.LockModeFTWRL, RemedyConsole, nil)
	if err == nil {
		t.Fatal("an unreachable source passed the preflight")
	}
	if strings.Contains(err.Error(), "no-lock") {
		t.Errorf("a connection failure recommends downgrading consistency: %v", err)
	}
}
