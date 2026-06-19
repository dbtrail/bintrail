package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// TestConnect_locUTCApplied verifies that Connect injects Loc=UTC, overriding
// any loc the user may have specified in their DSN.
func TestConnect_locUTCApplied(t *testing.T) {
	// Simulate the path Connect takes for a DSN with loc=Local.
	cfg, err := mysql.ParseDSN("root:pass@tcp(127.0.0.1:3306)/test?loc=Local")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC // Connect always forces this

	if cfg.Loc != time.UTC {
		t.Errorf("expected Loc=UTC, got %v", cfg.Loc)
	}
}

// binlogStatusColumns matches the layout SHOW BINARY LOG STATUS /
// SHOW MASTER STATUS returns: File, Position, Binlog_Do_DB,
// Binlog_Ignore_DB, Executed_Gtid_Set. CurrentBinlogPosition scans
// the first two into typed targets and the rest into string sinks.
var binlogStatusColumns = []string{
	"File",
	"Position",
	"Binlog_Do_DB",
	"Binlog_Ignore_DB",
	"Executed_Gtid_Set",
}

// TestCurrentBinlogPosition_NewStatementHappyPath exercises the
// MySQL 8.4 path where SHOW BINARY LOG STATUS returns a row.
func TestCurrentBinlogPosition_NewStatementHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnRows(
		sqlmock.NewRows(binlogStatusColumns).
			AddRow("binlog.000042", uint32(12345), "", "", ""))

	file, pos, err := CurrentBinlogPosition(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if file != "binlog.000042" {
		t.Errorf("file = %q, want %q", file, "binlog.000042")
	}
	if pos != 12345 {
		t.Errorf("pos = %d, want 12345", pos)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCurrentBinlogPosition_FallbackHappyPath exercises the pre-8.4
// path: the new statement returns a syntax error (1064), then
// SHOW MASTER STATUS returns a row.
func TestCurrentBinlogPosition_FallbackHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(&mysql.MySQLError{
		Number:  1064,
		Message: "You have an error in your SQL syntax",
	})
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows(binlogStatusColumns).
			AddRow("mysql-bin.000007", uint32(987), "", "", ""))

	file, pos, err := CurrentBinlogPosition(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if file != "mysql-bin.000007" {
		t.Errorf("file = %q, want %q", file, "mysql-bin.000007")
	}
	if pos != 987 {
		t.Errorf("pos = %d, want 987", pos)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCurrentBinlogPosition_MariaDBFourColumns covers a MariaDB source:
// SHOW BINARY LOG STATUS does not exist there (syntax error 1064), and
// SHOW MASTER STATUS returns only FOUR columns — File, Position, Binlog_Do_DB,
// Binlog_Ignore_DB — because MariaDB has no Executed_Gtid_Set column. The scan
// must tolerate the differing column count and still extract File+Position.
// Reproduces the alpha smoke-test failure "expected 4 destination arguments in
// Scan, not 5" against real MariaDB 11.4.
func TestCurrentBinlogPosition_MariaDBFourColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(&mysql.MySQLError{
		Number:  1064,
		Message: "You have an error in your SQL syntax near 'LOG STATUS'",
	})
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mariadb-bin.000002", uint32(1483), "", ""))

	file, pos, err := CurrentBinlogPosition(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if file != "mariadb-bin.000002" {
		t.Errorf("file = %q, want %q", file, "mariadb-bin.000002")
	}
	if pos != 1483 {
		t.Errorf("pos = %d, want 1483", pos)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCurrentBinlogPosition_LogBinOff covers the real-world shape of
// log_bin=OFF on every supported MySQL/Percona line. The crash-loop
// in #325 surfaced as an asymmetric error pair — exactly one branch
// returns ErrNoRows (empty resultset because log_bin=OFF) and the
// other returns 1064 (statement doesn't exist on that version):
//
//   - 5.7 / 8.0 / Percona <8.4: SHOW BINARY LOG STATUS → 1064,
//     SHOW MASTER STATUS → ErrNoRows.
//   - 8.4+: SHOW BINARY LOG STATUS → ErrNoRows, SHOW MASTER STATUS →
//     1064 (removed in 8.4.0).
//
// Plus the degenerate sanity case where sqlmock simulates both empty.
// All three must hit the domain error.
func TestCurrentBinlogPosition_LogBinOff(t *testing.T) {
	cases := []struct {
		name      string
		firstErr  error
		firstRows *sqlmock.Rows
		fbErr     error
		fbRows    *sqlmock.Rows
	}{
		{
			name:     "pre_8.4 with log_bin=OFF: new statement is 1064, old returns empty",
			firstErr: &mysql.MySQLError{Number: 1064, Message: "syntax error near 'BINARY LOG STATUS'"},
			fbRows:   sqlmock.NewRows(binlogStatusColumns),
		},
		{
			name:      "8.4+ with log_bin=OFF: new returns empty, old is 1064",
			firstRows: sqlmock.NewRows(binlogStatusColumns),
			fbErr:     &mysql.MySQLError{Number: 1064, Message: "syntax error near 'MASTER STATUS'"},
		},
		{
			name:      "both empty (sqlmock degenerate sanity)",
			firstRows: sqlmock.NewRows(binlogStatusColumns),
			fbRows:    sqlmock.NewRows(binlogStatusColumns),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			firstExp := mock.ExpectQuery("SHOW BINARY LOG STATUS")
			if tc.firstErr != nil {
				firstExp.WillReturnError(tc.firstErr)
			} else {
				firstExp.WillReturnRows(tc.firstRows)
			}
			fbExp := mock.ExpectQuery("SHOW MASTER STATUS")
			if tc.fbErr != nil {
				fbExp.WillReturnError(tc.fbErr)
			} else {
				fbExp.WillReturnRows(tc.fbRows)
			}

			_, _, err = CurrentBinlogPosition(db)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			msg := err.Error()
			if !strings.Contains(msg, "log_bin") {
				t.Errorf("error message does not mention log_bin: %q", msg)
			}
			if !strings.Contains(msg, "SHOW VARIABLES") {
				t.Errorf("error message does not point at the remediation query: %q", msg)
			}
			// Domain error must not leak the generic fallback wrap or
			// raw ErrNoRows — operators would chase those fruitlessly.
			if strings.Contains(msg, "fallback:") {
				t.Errorf("domain error should not include 'fallback:' wrap: %q", msg)
			}
			if strings.Contains(msg, "no rows in result set") {
				t.Errorf("domain error should not leak raw sql.ErrNoRows: %q", msg)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestCurrentBinlogPosition_BothNonErrNoRowsFail is the scope-creep
// guard: if both statements fail with non-ErrNoRows errors (e.g.
// connection drop, permission denied), the caller should still get
// the existing combined-error diagnostic so they don't lose the
// underlying detail.
func TestCurrentBinlogPosition_BothNonErrNoRowsFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	firstErr := errors.New("connection reset by peer")
	secondErr := errors.New("permission denied")

	mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(firstErr)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnError(secondErr)

	_, _, err = CurrentBinlogPosition(db)
	if err == nil {
		t.Fatal("expected error when both statements fail, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "SHOW BINARY LOG STATUS / SHOW MASTER STATUS") {
		t.Errorf("error message missing combined diagnostic header: %q", msg)
	}
	if !strings.Contains(msg, "connection reset by peer") {
		t.Errorf("error message lost first diagnostic: %q", msg)
	}
	if !strings.Contains(msg, "permission denied") {
		t.Errorf("error message lost fallback diagnostic: %q", msg)
	}
	if strings.Contains(msg, "log_bin") {
		t.Errorf("error message should not mention log_bin for non-ErrNoRows failures: %q", msg)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ─── ParseSourceDSN ───────────────────────────────────────────────────────────

func TestParseSourceDSN_tcp(t *testing.T) {
	dsn := "root:secret@tcp(db.example.com:3306)/mydb"
	host, port, user, pass, err := ParseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "db.example.com" {
		t.Errorf("host: expected db.example.com, got %q", host)
	}
	if port != 3306 {
		t.Errorf("port: expected 3306, got %d", port)
	}
	if user != "root" {
		t.Errorf("user: expected root, got %q", user)
	}
	if pass != "secret" {
		t.Errorf("password: expected secret, got %q", pass)
	}
}

func TestParseSourceDSN_noPassword(t *testing.T) {
	dsn := "repl@tcp(127.0.0.1:13306)/"
	host, port, user, pass, err := ParseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "127.0.0.1" {
		t.Errorf("host: expected 127.0.0.1, got %q", host)
	}
	if port != 13306 {
		t.Errorf("port: expected 13306, got %d", port)
	}
	if user != "repl" {
		t.Errorf("user: expected repl, got %q", user)
	}
	if pass != "" {
		t.Errorf("password: expected empty, got %q", pass)
	}
}

func TestParseSourceDSN_unixSocket(t *testing.T) {
	dsn := "root@unix(/var/run/mysqld/mysqld.sock)/test"
	_, _, _, _, err := ParseSourceDSN(dsn)
	if err == nil {
		t.Error("expected error for unix socket DSN, got nil")
	}
}

func TestParseSourceDSN_invalid(t *testing.T) {
	_, _, _, _, err := ParseSourceDSN("not-a-valid-dsn::::")
	if err == nil {
		t.Error("expected error for invalid DSN, got nil")
	}
}

// TestParseSourceDSN_ipv6 verifies IPv6 addresses are parsed correctly.
func TestParseSourceDSN_ipv6(t *testing.T) {
	dsn := "root:pw@tcp([::1]:3306)/db"
	host, port, _, _, err := ParseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error for IPv6 DSN: %v", err)
	}
	if host != "::1" {
		t.Errorf("host: expected ::1, got %q", host)
	}
	if port != 3306 {
		t.Errorf("port: expected 3306, got %d", port)
	}
}

// TestParseSourceDSN_portOutOfRange verifies that a port above the uint16 max
// (65535) is rejected. go-mysql-driver accepts it syntactically, but
// ParseSourceDSN uses strconv.ParseUint with bitSize=16 to catch it.
func TestParseSourceDSN_portOutOfRange(t *testing.T) {
	dsn := "root@tcp(localhost:65536)/"
	_, _, _, _, err := ParseSourceDSN(dsn)
	if err == nil {
		t.Error("expected error for port 65536 (exceeds uint16 max), got nil")
	}
	if !strings.Contains(err.Error(), "port") {
		t.Errorf("expected 'port' in error message, got: %v", err)
	}
}
