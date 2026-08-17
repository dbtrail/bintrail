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
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole)
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
	if err := checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyConsole); err != nil {
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
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyCLI)
	if err == nil {
		t.Fatal("lock-all was allowed without LOCK TABLES")
	}
	if !strings.Contains(err.Error(), "LOCK TABLES") {
		t.Errorf("refusal = %v, want it to name LOCK TABLES", err)
	}
}

// TestCheckPrivilegesOnlyGlobalGrantsCount: a schema-scoped grant does not
// authorize FLUSH TABLES WITH READ LOCK, so parsing must ignore anything that
// is not ON *.*. Accepting one would under-refuse — the direction that lets
// mydumper run half-privileged, which is what segfaults it.
func TestCheckPrivilegesOnlyGlobalGrantsCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(grantRows(
		"GRANT SELECT ON *.* TO `u`@`%`",
		"GRANT LOCK TABLES ON `appdb`.* TO `u`@`%`",
	))
	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeLockAll, RemedyCLI)
	if err == nil {
		t.Fatal("a schema-scoped LOCK TABLES was accepted as a global privilege")
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
		if err := checkPrivilegesDB(context.Background(), db, mode, RemedyConsole); err != nil {
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

	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole)
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

	err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, RemedyConsole)
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
		err = checkPrivilegesDB(context.Background(), db, baseline.LockModeFTWRL, tc.remedy)
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
