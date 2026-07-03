package doctor

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestExtractGrantUser(t *testing.T) {
	tests := []struct {
		name  string
		grant string
		want  string
	}{
		{
			name:  "simple user@host with single quotes",
			grant: "GRANT USAGE ON *.* TO 'bintrail'@'%'",
			want:  "'bintrail'@'%'",
		},
		{
			name:  "user@host with localhost",
			grant: "GRANT SELECT ON db.* TO 'app'@'localhost'",
			want:  "'app'@'localhost'",
		},
		{
			name:  "trailing IDENTIFIED BY clause (older MySQL)",
			grant: "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'10.0.0.5' IDENTIFIED BY '<secret>'",
			want:  "'repl'@'10.0.0.5'",
		},
		{
			name:  "WITH GRANT OPTION suffix",
			grant: "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION",
			want:  "'admin'@'%'",
		},
		{
			name:  "lowercase TO is uppercased by ToUpper search",
			grant: "grant select on db.* to 'app'@'localhost'",
			want:  "'app'@'localhost'",
		},
		{
			name:  "backtick-quoted identifier",
			grant: "GRANT USAGE ON *.* TO `bintrail`@`%`",
			want:  "`bintrail`@`%`",
		},
		{
			name:  "no TO clause",
			grant: "REVOKE ALL PRIVILEGES",
			want:  "",
		},
		{
			name:  "empty",
			grant: "",
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractGrantUser(tt.grant)
			if got != tt.want {
				t.Errorf("extractGrantUser(%q) = %q, want %q", tt.grant, got, tt.want)
			}
		})
	}
}

func TestDoctorReportAdd(t *testing.T) {
	r := &Report{}
	r.add(CheckResult{Name: "a", Status: StatusPass})
	r.add(CheckResult{Name: "b", Status: StatusFail})
	r.add(CheckResult{Name: "c", Status: StatusWarn})
	r.add(CheckResult{Name: "d", Status: StatusSkip})
	r.add(CheckResult{Name: "e", Status: StatusPass})

	if r.Passed != 2 || r.Failed != 1 || r.Warnings != 1 || r.Skipped != 1 {
		t.Errorf("counts wrong: %+v", r)
	}
	if len(r.Checks) != 5 {
		t.Errorf("expected 5 checks, got %d", len(r.Checks))
	}
}

func TestDoctorReportAddUnknownStatusDoesNotCountButAppends(t *testing.T) {
	r := &Report{}
	r.add(CheckResult{Name: "weird", Status: CheckStatus("UNKNOWN")})
	if len(r.Checks) != 1 {
		t.Errorf("expected unknown check appended for JSON visibility, got %d entries", len(r.Checks))
	}
	if r.Passed+r.Failed+r.Warnings+r.Skipped != 0 {
		t.Errorf("expected no counters incremented for unknown status, got %+v", r)
	}
}

func TestDoctorReportErr(t *testing.T) {
	// No failures → nil error (warnings are tolerated).
	r := &Report{Passed: 3, Warnings: 2}
	if err := r.Err(); err != nil {
		t.Errorf("expected nil error with no failures, got %v", err)
	}

	// One failure → error.
	r2 := &Report{Passed: 3, Failed: 1}
	err := r2.Err()
	if err == nil {
		t.Error("expected error when Failed > 0")
	}
	if err != nil && !strings.Contains(err.Error(), "1 preflight check") {
		t.Errorf("error message does not mention check count: %v", err)
	}
}

func TestDoctorReportWriteJSON(t *testing.T) {
	// Build a report, marshal via Write, unmarshal, assert deep equality.
	// Catches: missing JSON tags, dropped fields, type mismatches.
	in := &Report{
		Checks: []CheckResult{
			{Name: "ok", Status: StatusPass, Detail: "MySQL 8.0.36"},
			{Name: "bad", Status: StatusFail, Detail: "denied", Remediation: "GRANT X ON *.* ..."},
		},
		Passed: 1,
		Failed: 1,
	}
	var buf bytes.Buffer
	if err := in.Write(&buf, "json"); err != nil {
		t.Fatalf("Write(json) error: %v", err)
	}
	var out Report
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, buf.String())
	}
	if out.Passed != 1 || out.Failed != 1 {
		t.Errorf("counters lost in round-trip: %+v", out)
	}
	if len(out.Checks) != 2 {
		t.Fatalf("expected 2 checks, got %d", len(out.Checks))
	}
	if out.Checks[0].Status != StatusPass {
		t.Errorf("status round-trip failed: %q", out.Checks[0].Status)
	}
	if out.Checks[1].Remediation != "GRANT X ON *.* ..." {
		t.Errorf("remediation lost: %q", out.Checks[1].Remediation)
	}
}

// TestEveryFailCheckCarriesRemediation is the structural invariant guarding
// against the SCHEMATA-class regression where a FAIL slips through with no
// next action for the operator. Per-test wantRemediation flags let new bare
// FAILs sneak in because authors can opt out. This test does not opt out:
// every check that can be exercised with sqlmock is forced into FAIL and
// asserted to carry Remediation. Checks that open their own DB connection
// (checkSourceConnection, checkIndexConnection, checkIndexWriteAccess) are
// covered indirectly via the *On variants where they exist.
func TestEveryFailCheckCarriesRemediation(t *testing.T) {
	ctx := t.Context()
	forcedErr := errors.New("forced query failure")

	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		run   func(db sqlDB) CheckResult
	}{
		{
			name:  "checkLogBin/query error",
			setup: func(m sqlmock.Sqlmock) { m.ExpectQuery("SELECT @@log_bin").WillReturnError(forcedErr) },
			run:   func(db sqlDB) CheckResult { return checkLogBin(db) },
		},
		{
			name:  "checkReplicationGrants/SHOW GRANTS error",
			setup: func(m sqlmock.Sqlmock) { m.ExpectQuery("SHOW GRANTS").WillReturnError(forcedErr) },
			run:   func(db sqlDB) CheckResult { return checkReplicationGrants(ctx, db) },
		},
		{
			name: "checkSchemaVisibility/query error",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnError(forcedErr)
			},
			run: func(db sqlDB) CheckResult { return checkSchemaVisibility(ctx, db, nil) },
		},
		{
			name: "checkIndexWriteAccessOn/SCHEMATA error",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("information_schema.SCHEMATA").WillReturnError(forcedErr)
			},
			run: func(db sqlDB) CheckResult { return checkIndexWriteAccessOn(ctx, db, "binlog_index") },
		},
		{
			name: "checkIndexWriteAccessOn/CREATE TABLE denied",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("information_schema.SCHEMATA").WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("binlog_index"))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnError(forcedErr)
			},
			run: func(db sqlDB) CheckResult { return checkIndexWriteAccessOn(ctx, db, "binlog_index") },
		},
		{
			name: "checkIndexWriteAccessOn/DROP denied (upgraded WARN→FAIL)",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("information_schema.SCHEMATA").WillReturnRows(
					sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow("binlog_index"))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("DROP TABLE").WillReturnError(forcedErr)
			},
			run: func(db sqlDB) CheckResult { return checkIndexWriteAccessOn(ctx, db, "binlog_index") },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			c.setup(mock)
			got := c.run(db)
			if got.Status != StatusFail {
				t.Errorf("expected StatusFail, got %q (detail=%q)", got.Status, got.Detail)
			}
			if got.Remediation == "" {
				t.Errorf("FAIL with no Remediation — operator has no next step.\n  Detail: %q", got.Detail)
			}
		})
	}
}

// sqlDB is a tiny type alias to keep the runner-table signatures readable
// without importing the full database/sql path into every cell.
type sqlDB = *sql.DB

func TestCheckLogBin(t *testing.T) {
	// checkLogBin owns its own string-comparison logic (does not delegate to a
	// validator with its own tests), so the "1"/"ON" parsing and the OFF/0
	// rejection branches are the load-bearing assertions here.
	tests := []struct {
		name       string
		returnVal  string
		queryErr   error
		wantStatus CheckStatus
		wantDetail string
	}{
		{name: "ON via 1", returnVal: "1", wantStatus: StatusPass, wantDetail: "ON"},
		{name: "ON via literal", returnVal: "ON", wantStatus: StatusPass, wantDetail: "ON"},
		{name: "ON case-insensitive", returnVal: "on", wantStatus: StatusPass, wantDetail: "ON"},
		{name: "OFF literal", returnVal: "OFF", wantStatus: StatusFail, wantDetail: `log_bin="OFF"`},
		{name: "OFF via 0", returnVal: "0", wantStatus: StatusFail, wantDetail: `log_bin="0"`},
		{name: "empty string", returnVal: "", wantStatus: StatusFail, wantDetail: `log_bin=""`},
		{name: "query error", queryErr: errors.New("denied"), wantStatus: StatusFail, wantDetail: "denied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@log_bin")
			if tt.queryErr != nil {
				exp.WillReturnError(tt.queryErr)
			} else {
				exp.WillReturnRows(sqlmock.NewRows([]string{"@@log_bin"}).AddRow(tt.returnVal))
			}

			got := checkLogBin(db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", got.Status, tt.wantStatus)
			}
			if !strings.Contains(got.Detail, tt.wantDetail) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetail)
			}
			if tt.wantStatus == StatusFail && got.Remediation == "" {
				t.Error("FAIL outcome with no remediation breaks doctor's promise")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestCheckBinlogRetention(t *testing.T) {
	// Two branches: @@binlog_expire_logs_seconds (modern) and @@expire_logs_days
	// (fallback, still present on MySQL 8.0 as deprecated). Plus retry/error
	// paths. Retention failures are WARN, not FAIL — the property test does not
	// apply here; each row asserts wantRemediation explicitly.
	tests := []struct {
		name            string
		modern          mockSQLScalar // first query — @@binlog_expire_logs_seconds
		legacy          mockSQLScalar // second query — @@expire_logs_days (only invoked when modern errors)
		wantStatus      CheckStatus
		wantDetailFrag  string // substring assertion on detail
		wantRemediation bool   // must remediation be present?
	}{
		// 1. MySQL 8.0+ branches.
		{name: "modern: at threshold", modern: row("172800"), wantStatus: StatusPass, wantDetailFrag: "48h"},
		{name: "modern: above threshold", modern: row("259200"), wantStatus: StatusPass, wantDetailFrag: "72h"},
		{name: "modern: below threshold", modern: row("3600"), wantStatus: StatusWarn, wantDetailFrag: "1h", wantRemediation: true},
		{name: "modern: zero (never expire)", modern: row("0"), wantStatus: StatusWarn, wantDetailFrag: "no automatic expiration"},
		{name: "modern: unparseable", modern: row("not-an-int"), wantStatus: StatusWarn, wantDetailFrag: "could not parse"},
		// 2. Legacy fallback when modern errors.
		{name: "legacy: 7 days", modern: errResp("unknown variable"), legacy: row("7"), wantStatus: StatusPass, wantDetailFrag: "7 days"},
		{name: "legacy: 1 day", modern: errResp("unknown variable"), legacy: row("1"), wantStatus: StatusWarn, wantDetailFrag: "expire_logs_days=1", wantRemediation: true},
		{name: "legacy: unparseable", modern: errResp("unknown variable"), legacy: row("garbage"), wantStatus: StatusWarn, wantDetailFrag: "could not parse"},
		// 3. Both error → warn-only (no remediation; doctor proceeds with degraded info).
		{name: "both error", modern: errResp("conn lost"), legacy: errResp("conn lost"), wantStatus: StatusWarn, wantDetailFrag: "could not read"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			expect := mock.ExpectQuery("SELECT @@binlog_expire_logs_seconds")
			tt.modern.apply(expect, "@@binlog_expire_logs_seconds")
			if tt.modern.err != nil {
				lexpect := mock.ExpectQuery("SELECT @@expire_logs_days")
				tt.legacy.apply(lexpect, "@@expire_logs_days")
			}

			got := checkBinlogRetention(db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestCheckStatementCapture pins the #699 advisory check: PASS when the
// source logs statement text, WARN (never FAIL) when it doesn't, MariaDB
// fallback probe when the MySQL variable is absent, SKIP when neither exists.
func TestCheckStatementCapture(t *testing.T) {
	tests := []struct {
		name            string
		mysqlVar        mockSQLScalar  // SELECT @@binlog_rows_query_log_events
		mariaVar        *mockSQLScalar // SELECT @@binlog_annotate_row_events (only probed on a 1193 from mysqlVar)
		wantStatus      CheckStatus
		wantDetailFrag  string
		wantRemediation bool
	}{
		{"mysql ON", row("1"), nil, StatusPass, "binlog_rows_query_log_events=ON", false},
		{"mysql OFF", row("0"), nil, StatusWarn, "binlog_rows_query_log_events=OFF", true},
		{"mariadb ON", mysqlErrResp(1193, "Unknown system variable 'binlog_rows_query_log_events'"), ptr(row("1")), StatusPass, "binlog_annotate_row_events=ON", false},
		{"mariadb OFF", mysqlErrResp(1193, "Unknown system variable 'binlog_rows_query_log_events'"), ptr(row("OFF")), StatusWarn, "binlog_annotate_row_events=OFF", true},
		{"neither variable", mysqlErrResp(1193, "Unknown system variable"), ptr(mysqlErrResp(1193, "Unknown system variable")), StatusSkip, "neither", false},
		// A non-1193 failure is a READ problem, not a flavor fact — the real
		// error must surface instead of a fabricated "not available" claim.
		{"transient read failure", errResp("driver: bad connection"), nil, StatusWarn, "could not read binlog_rows_query_log_events: driver: bad connection", false},
		{"mariadb probe read failure", mysqlErrResp(1193, "Unknown system variable"), ptr(errResp("driver: bad connection")), StatusWarn, "could not read binlog_annotate_row_events: driver: bad connection", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@binlog_rows_query_log_events")
			tt.mysqlVar.apply(exp, "@@binlog_rows_query_log_events")
			if tt.mariaVar != nil {
				mexp := mock.ExpectQuery("SELECT @@binlog_annotate_row_events")
				tt.mariaVar.apply(mexp, "@@binlog_annotate_row_events")
			}

			got := checkStatementCapture(db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			if got.Status == StatusFail {
				t.Error("statement capture is optional — the check must never FAIL")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// TestCheckRowMetadata pins the #700 advisory check: PASS on FULL, WARN
// (never FAIL) on MINIMAL, SKIP when the variable does not exist.
func TestCheckRowMetadata(t *testing.T) {
	tests := []struct {
		name            string
		probe           mockSQLScalar
		wantStatus      CheckStatus
		wantDetailFrag  string
		wantRemediation bool
	}{
		{"FULL", row("FULL"), StatusPass, "binlog_row_metadata=FULL", false},
		{"MINIMAL", row("MINIMAL"), StatusWarn, "binlog_row_metadata=MINIMAL", true},
		{"MariaDB NO_LOG default", row("NO_LOG"), StatusWarn, "binlog_row_metadata=NO_LOG", true},
		{"variable absent", mysqlErrResp(1193, "Unknown system variable 'binlog_row_metadata'"), StatusSkip, "not available", false},
		{"transient read failure", errResp("driver: bad connection"), StatusWarn, "could not read binlog_row_metadata: driver: bad connection", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			exp := mock.ExpectQuery("SELECT @@binlog_row_metadata")
			tt.probe.apply(exp, "@@binlog_row_metadata")

			got := checkRowMetadata(db)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantRemediation && got.Remediation == "" {
				t.Error("expected remediation but got none")
			}
			if got.Status == StatusFail {
				t.Error("binlog_row_metadata is optional — the check must never FAIL")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// mockSQLScalar is a single-value SELECT mock — value xor err.
type mockSQLScalar struct {
	value string
	err   error
}

func row(v string) mockSQLScalar       { return mockSQLScalar{value: v} }
func errResp(msg string) mockSQLScalar { return mockSQLScalar{err: errors.New(msg)} }

// mysqlErrResp mocks a typed MySQL server error (e.g. 1193 unknown system
// variable) so checks that discriminate by error number can be exercised.
func mysqlErrResp(number uint16, msg string) mockSQLScalar {
	return mockSQLScalar{err: &mysql.MySQLError{Number: number, Message: msg}}
}

func (r mockSQLScalar) apply(exp *sqlmock.ExpectedQuery, col string) {
	if r.err != nil {
		exp.WillReturnError(r.err)
		return
	}
	exp.WillReturnRows(sqlmock.NewRows([]string{col}).AddRow(r.value))
}

// TestCheckIndexWriteAccessOnNeverDropsPreexistingDB is the data-loss guard
// for #384's probe cleanup: when the database PRE-EXISTS (SCHEMATA finds it),
// no DROP DATABASE may ever be issued. The trick: register a DROP DATABASE
// expectation and assert ExpectationsWereMet() ERRORS with it unfulfilled —
// a spurious drop would fulfil it and turn this test red. (The production
// dropErr is swallowed into slog.Warn, so an unexpected-call error from
// sqlmock would otherwise be invisible to assertions.)
func TestCheckIndexWriteAccessOnNeverDropsPreexistingDB(t *testing.T) {
	const dbName = "binlog_index"
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow(dbName))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	// Sentinel: must remain UNFULFILLED.
	mock.ExpectExec("DROP DATABASE").WillReturnResult(sqlmock.NewResult(0, 0))

	got := checkIndexWriteAccessOn(t.Context(), db, dbName)
	if got.Status != StatusPass {
		t.Fatalf("Status = %q, want pass (detail=%q)", got.Status, got.Detail)
	}
	err = mock.ExpectationsWereMet()
	if err == nil {
		t.Fatal("DROP DATABASE expectation was fulfilled — a pre-existing database was dropped by the probe")
	}
	if !strings.Contains(err.Error(), "DROP DATABASE") {
		t.Fatalf("expected the unmet expectation to be DROP DATABASE, got: %v", err)
	}
}

// TestIsUnknownDatabaseErr pins the 1049 detection (#384): it must see
// through config.Connect's %w wrapping and must NOT match other MySQL
// errors or plain errors.
func TestIsUnknownDatabaseErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrapped 1049", fmt.Errorf("failed to ping MySQL: %w", &mysql.MySQLError{Number: 1049, Message: "Unknown database 'binlog_index'"}), true},
		{"bare 1049", &mysql.MySQLError{Number: 1049}, true},
		{"other mysql error", &mysql.MySQLError{Number: 1064}, false},
		{"plain error", errors.New("connection refused"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isUnknownDatabaseErr(tc.err); got != tc.want {
				t.Errorf("isUnknownDatabaseErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestCheckIndexWriteAccessOn(t *testing.T) {
	const dbName = "binlog_index"

	// Each subtest sets up the sqlmock expectation chain that mirrors the
	// branch under test. The probe table name is fixed in checkIndexWriteAccessOn
	// as `binlog_index`.`_bintrail_doctor_probe`.
	tests := []struct {
		name           string
		setup          func(mock sqlmock.Sqlmock)
		wantStatus     CheckStatus
		wantDetailFrag string
	}{
		{
			name: "db exists, create+drop OK",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow(dbName))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("DROP TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantStatus:     StatusPass,
			wantDetailFrag: "CREATE/DROP TABLE OK",
		},
		{
			name: "db missing, create database succeeds, then create+drop OK, probe db dropped",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				m.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("DROP TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
				// A diagnostic must not leave server state behind (#384):
				// the probe-created database is dropped via defer.
				m.ExpectExec("DROP DATABASE IF EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantStatus:     StatusPass,
			wantDetailFrag: "CREATE/DROP TABLE OK",
		},
		{
			name: "db missing, created by probe, CREATE TABLE denied — probe db still dropped",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				m.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").
					WillReturnError(errors.New("CREATE command denied"))
				// Cleanup runs on the FAIL path too (#384).
				m.ExpectExec("DROP DATABASE IF EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "cannot CREATE TABLE",
		},
		{
			name: "db missing, create database denied",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				m.ExpectExec("CREATE DATABASE IF NOT EXISTS").
					WillReturnError(errors.New("Access denied for user"))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "cannot CREATE DATABASE",
		},
		{
			name: "db exists, create table denied",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow(dbName))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").
					WillReturnError(errors.New("CREATE command denied"))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "cannot CREATE TABLE",
		},
		{
			name: "create OK but drop denied — must FAIL (catches partition-rotate bites at runtime)",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}).AddRow(dbName))
				m.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec("DROP TABLE").WillReturnError(errors.New("DROP command denied"))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "user has CREATE but not DROP",
		},
		{
			name: "schemata query errors",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA").
					WillReturnError(errors.New("conn lost"))
			},
			wantStatus:     StatusFail,
			wantDetailFrag: "conn lost",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			tt.setup(mock)

			got := checkIndexWriteAccessOn(t.Context(), db, dbName)
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q (detail=%q)", got.Status, tt.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, tt.wantDetailFrag) {
				t.Errorf("Detail = %q, want substring %q", got.Detail, tt.wantDetailFrag)
			}
			if tt.wantStatus == StatusFail && got.Remediation == "" {
				t.Error("FAIL outcome with no remediation breaks doctor's promise")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

func TestDoctorReportWriteText(t *testing.T) {
	// Text formatter emits a specific status glyph per status. Any UI scraper
	// or grep workflow depends on this contract — assert per-glyph.
	r := &Report{}
	r.add(CheckResult{Name: "p", Status: StatusPass, Detail: "ok"})
	r.add(CheckResult{Name: "f", Status: StatusFail, Detail: "bad", Remediation: "fix it"})
	r.add(CheckResult{Name: "w", Status: StatusWarn, Detail: "meh"})
	r.add(CheckResult{Name: "s", Status: StatusSkip, Detail: "n/a"})

	var buf bytes.Buffer
	if err := r.Write(&buf, "text"); err != nil {
		t.Fatalf("Write(text): %v", err)
	}
	out := buf.String()
	for _, want := range []string{"✓ p", "✗ f", "! w", "- s", "    fix it"} {
		if !strings.Contains(out, want) {
			t.Errorf("text output missing %q\n--- output ---\n%s", want, out)
		}
	}
}

// TestDoctorReportFooter pins the parameterized text footer (#532): a non-MySQL
// caller (e.g. bintrail-pg doctor) can override the trailing guidance, and an empty
// footer falls back to the default MySQL-oriented text — so existing callers (and the
// JSON output) are unaffected.
func TestDoctorReportFooter(t *testing.T) {
	render := func(r *Report) string {
		var buf bytes.Buffer
		if err := r.Write(&buf, "text"); err != nil {
			t.Fatalf("Write: %v", err)
		}
		return buf.String()
	}

	// Custom footers: all-passed → ReadyFooter; has-failure → FixFooter.
	passed := &Report{ReadyFooter: "CUSTOM READY", FixFooter: "CUSTOM FIX"}
	passed.Add(CheckResult{Name: "p", Status: StatusPass})
	if out := render(passed); !strings.Contains(out, "CUSTOM READY") || strings.Contains(out, "Ready to stream") {
		t.Errorf("all-passed should render the custom ReadyFooter, not the default:\n%s", out)
	}
	failed := &Report{ReadyFooter: "CUSTOM READY", FixFooter: "CUSTOM FIX"}
	failed.Add(CheckResult{Name: "f", Status: StatusFail})
	if out := render(failed); !strings.Contains(out, "CUSTOM FIX") {
		t.Errorf("has-failure should render the custom FixFooter:\n%s", out)
	}

	// Empty footers → the default MySQL guidance (back-compat for existing callers).
	def := &Report{}
	def.Add(CheckResult{Name: "p", Status: StatusPass})
	if out := render(def); !strings.Contains(out, "Ready to stream. Run `bintrail up") {
		t.Errorf("empty ReadyFooter should fall back to the MySQL default:\n%s", out)
	}
	defFail := &Report{}
	defFail.Add(CheckResult{Name: "f", Status: StatusFail})
	if out := render(defFail); !strings.Contains(out, "re-run `bintrail doctor`") {
		t.Errorf("empty FixFooter should fall back to the MySQL default:\n%s", out)
	}

	// The footer fields must not leak into JSON.
	var jbuf bytes.Buffer
	if err := passed.Write(&jbuf, "json"); err != nil {
		t.Fatalf("Write(json): %v", err)
	}
	if strings.Contains(jbuf.String(), "CUSTOM") {
		t.Errorf("footer fields leaked into JSON:\n%s", jbuf.String())
	}
}

// TestCheckSchemaVisibility_emptyVsInvisible pins the #402 discrimination:
// zero tables because the schema is EMPTY routes the operator to "create a
// table", zero tables because the schema is INVISIBLE routes to grants — the
// wrong remediation sends a 3am operator down the wrong path.
func TestCheckSchemaVisibility_emptyVsInvisible(t *testing.T) {
	ctx := t.Context()
	countRows := func(tables, schemas int) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"COUNT(*)", "COUNT(DISTINCT TABLE_SCHEMA)"}).AddRow(tables, schemas)
	}
	schemataRows := func(n int) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(n)
	}

	cases := []struct {
		name            string
		setup           func(sqlmock.Sqlmock)
		wantStatus      CheckStatus
		wantDetail      string
		wantRemediation string
	}{
		{
			name: "schema exists but is empty → create-a-table",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnRows(countRows(0, 0))
				m.ExpectQuery("SELECT COUNT").WillReturnRows(schemataRows(1))
			},
			wantStatus:      StatusFail,
			wantDetail:      "no tables yet",
			wantRemediation: "Create at least one table",
		},
		{
			name: "schema invisible → grants",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnRows(countRows(0, 0))
				m.ExpectQuery("SELECT COUNT").WillReturnRows(schemataRows(0))
			},
			wantStatus:      StatusFail,
			wantDetail:      "no tables visible",
			wantRemediation: "GRANT SELECT",
		},
		{
			name: "SCHEMATA probe fails → degrade to grants, never crash",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnRows(countRows(0, 0))
				m.ExpectQuery("SELECT COUNT").WillReturnError(errors.New("denied"))
			},
			wantStatus:      StatusFail,
			wantDetail:      "no tables visible",
			wantRemediation: "GRANT SELECT",
		},
		{
			name: "tables visible → pass",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT COUNT").WillReturnRows(countRows(5, 2))
			},
			wantStatus: StatusPass,
			wantDetail: "5 tables across 2 schemas",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			c.setup(mock)
			got := checkSchemaVisibility(ctx, db, []string{"shop"})
			if got.Status != c.wantStatus {
				t.Errorf("status = %q, want %q (detail=%q)", got.Status, c.wantStatus, got.Detail)
			}
			if !strings.Contains(got.Detail, c.wantDetail) {
				t.Errorf("Detail = %q, want it to contain %q", got.Detail, c.wantDetail)
			}
			if c.wantRemediation != "" && !strings.Contains(got.Remediation, c.wantRemediation) {
				t.Errorf("Remediation = %q, want it to contain %q", got.Remediation, c.wantRemediation)
			}
		})
	}
}
