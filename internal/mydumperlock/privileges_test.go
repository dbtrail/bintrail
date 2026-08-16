package mydumperlock

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestCheckPrivileges covers the privilege-combination matrix
// established empirically against the pinned mydumper build (#800): RELOAD or
// FLUSH_TABLES is required on every flavor; BACKUP_ADMIN is additionally
// required ONLY on MySQL/Percona 8.0+ (it doesn't exist on MariaDB or MySQL
// 5.7 — see requiresBackupAdmin). Critically, on a flavor where
// BACKUP_ADMIN IS required, having it without RELOAD/FLUSH_TABLES must still
// be rejected here rather than let mydumper run, because that specific
// half-privileged combination segfaults mydumper instead of failing cleanly.
func TestCheckPrivileges(t *testing.T) {
	newMockWithPrivileges := func(t *testing.T, version string, privileges []string) *sql.DB {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		mock.ExpectQuery("SELECT VERSION\\(\\)").
			WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow(version))
		rows := sqlmock.NewRows([]string{"PRIVILEGE_TYPE"})
		for _, p := range privileges {
			rows.AddRow(p)
		}
		mock.ExpectQuery("SELECT PRIVILEGE_TYPE FROM information_schema.USER_PRIVILEGES").WillReturnRows(rows)
		return db
	}

	tests := []struct {
		name       string
		version    string
		privileges []string
		wantErr    bool
		wantSubstr string
	}{
		{
			name:       "MySQL 8.0: BACKUP_ADMIN + RELOAD: ok",
			version:    "8.0.46",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "BACKUP_ADMIN", "RELOAD"},
			wantErr:    false,
		},
		{
			name:       "MySQL 8.0: BACKUP_ADMIN + FLUSH_TABLES (dynamic priv alternative to RELOAD): ok",
			version:    "8.0.46",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "BACKUP_ADMIN", "FLUSH_TABLES"},
			wantErr:    false,
		},
		{
			name:       "MySQL 8.0: neither: clear error naming both",
			version:    "8.0.46",
			privileges: []string{"SELECT", "REPLICATION CLIENT"},
			wantErr:    true,
			wantSubstr: "BOTH the BACKUP_ADMIN and the RELOAD",
		},
		{
			// The dangerous half-privileged case: mydumper segfaults here rather
			// than failing cleanly, so this MUST be rejected before mydumper runs.
			name:       "MySQL 8.0: BACKUP_ADMIN only, no RELOAD/FLUSH_TABLES: rejected (would segfault mydumper)",
			version:    "8.0.46",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "BACKUP_ADMIN"},
			wantErr:    true,
			wantSubstr: "requires the RELOAD (or FLUSH_TABLES) privilege",
		},
		{
			name:       "MySQL 8.0: RELOAD only, no BACKUP_ADMIN: rejected",
			version:    "8.0.46",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "RELOAD"},
			wantErr:    true,
			wantSubstr: "requires the BACKUP_ADMIN privilege",
		},
		{
			// #800 review item 1: BACKUP_ADMIN does not exist on MariaDB. A
			// MariaDB user with only RELOAD must be accepted — requiring
			// BACKUP_ADMIN here would make the mode permanently unusable, since
			// GRANT BACKUP_ADMIN itself errors on MariaDB.
			name:       "MariaDB: RELOAD only, no BACKUP_ADMIN: ok (BACKUP_ADMIN doesn't exist on MariaDB)",
			version:    "10.11.6-MariaDB",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "RELOAD"},
			wantErr:    false,
		},
		{
			name:       "MariaDB: neither RELOAD nor FLUSH_TABLES: rejected, BACKUP_ADMIN not mentioned",
			version:    "10.11.6-MariaDB",
			privileges: []string{"SELECT", "REPLICATION CLIENT"},
			wantErr:    true,
			wantSubstr: "requires the RELOAD (or FLUSH_TABLES) privilege",
		},
		{
			// #800 review item 1: MySQL 5.7 predates BACKUP_ADMIN too.
			name:       "MySQL 5.7: RELOAD only, no BACKUP_ADMIN: ok",
			version:    "5.7.44",
			privileges: []string{"SELECT", "REPLICATION CLIENT", "RELOAD"},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newMockWithPrivileges(t, tt.version, tt.privileges)
			err := checkPrivilegesDB(context.Background(), db, RemedyConsole)
			if tt.wantErr && err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.wantSubstr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantSubstr)
			}
			// MariaDB/5.7 cases must never mention BACKUP_ADMIN — it doesn't
			// exist there, and GRANT BACKUP_ADMIN itself errors on MariaDB.
			if tt.wantErr && !strings.Contains(tt.version, "8.0") && strings.Contains(err.Error(), "BACKUP_ADMIN") {
				t.Errorf("error for version %q must not mention BACKUP_ADMIN: %q", tt.version, err.Error())
			}
		})
	}
}

// TestServerMajorVersion covers the version-string parsing that decides
// whether BACKUP_ADMIN applies (#800 review item 1).
func TestServerMajorVersion(t *testing.T) {
	tests := []struct {
		version   string
		wantMajor int
		wantOK    bool
	}{
		{"8.0.46", 8, true},
		{"5.7.44", 5, true},
		{"10.11.6-MariaDB", 10, true},
		{"11.4.2-MariaDB-1:11.4.2+maria~ubu2204", 11, true},
		{"garbage", 0, false},
		{"", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			major, ok := serverMajorVersion(tt.version)
			if ok != tt.wantOK || major != tt.wantMajor {
				t.Errorf("serverMajorVersion(%q) = (%d, %v), want (%d, %v)", tt.version, major, ok, tt.wantMajor, tt.wantOK)
			}
		})
	}
}

// TestCheckPrivilegesRemedyIsSurfaceSpecific: the alternatives clause is the
// one actionable sentence in the refusal, and both surfaces reach it on their
// DEFAULT path. Naming the other surface's knob is worse than naming none —
// `bintrail dump` does not read the console's environment variable, and the
// console has no flags.
func TestCheckPrivilegesRemedyIsSurfaceSpecific(t *testing.T) {
	for _, tc := range []struct {
		remedy         Remedy
		want           string
		mustNotContain string
	}{
		{RemedyCLI, "--lock-mode safe-no-lock", "BINTRAIL_CONSOLE"},
		{RemedyConsole, "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=safe-no-lock", "--lock-mode"},
	} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("VERSION").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))
		mock.ExpectQuery("USER_PRIVILEGES").WillReturnRows(sqlmock.NewRows([]string{"p"}))
		err = checkPrivilegesDB(context.Background(), db, tc.remedy)
		db.Close()
		if err == nil {
			t.Fatalf("%s: a user with no privileges was allowed to run a point-consistent dump", tc.remedy)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: refusal = %q, want it to name %q", tc.remedy, err, tc.want)
		}
		if strings.Contains(err.Error(), tc.mustNotContain) {
			t.Errorf("%s: refusal names the OTHER surface's knob (%q), which this caller does not read: %q",
				tc.remedy, tc.mustNotContain, err)
		}
	}
}
