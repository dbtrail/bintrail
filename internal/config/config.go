package config

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// CurrentBinlogPosition returns the current binlog file name and position
// from the source server. It tries SHOW BINARY LOG STATUS first (MySQL 8.4+)
// and falls back to SHOW MASTER STATUS for 5.7 / 8.0 / Percona <8.4 where the
// new statement does not exist. When both fail, the wrapped error includes
// both diagnostics so the operator does not chase the wrong syntax.
//
// When binary logging is disabled on the source (log_bin=OFF), the statement
// that exists on this server version returns an empty resultset (sql.ErrNoRows
// from Scan), while the statement that doesn't exist returns syntax error 1064
// — never both ErrNoRows. The two real-world shapes are:
//
//   - 5.7/8.0/Percona<8.4 + log_bin=OFF: SHOW BINARY LOG STATUS → 1064,
//     SHOW MASTER STATUS → ErrNoRows
//   - 8.4+ + log_bin=OFF: SHOW BINARY LOG STATUS → ErrNoRows,
//     SHOW MASTER STATUS → 1064 (removed in 8.4.0)
//
// So when EITHER branch returns ErrNoRows we know log_bin=OFF and emit a
// domain-specific error. Privileges missing surfaces as 1227 (ER_SPECIFIC_
// ACCESS_DENIED_ERROR), not ErrNoRows, so false-positive risk is essentially
// nil.
func CurrentBinlogPosition(db *sql.DB) (file string, pos uint32, err error) {
	file, pos, err = scanBinlogStatus(db, "SHOW BINARY LOG STATUS")
	if err == nil {
		return file, pos, nil
	}
	firstErr := err
	file, pos, err = scanBinlogStatus(db, "SHOW MASTER STATUS")
	if err != nil {
		if errors.Is(firstErr, sql.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return "", 0, fmt.Errorf("current binlog position empty — log_bin appears to be OFF on the source server (run \"SHOW VARIABLES LIKE 'log_bin'\" to confirm)")
		}
		return "", 0, fmt.Errorf("SHOW BINARY LOG STATUS / SHOW MASTER STATUS: %w (fallback: %w)", firstErr, err)
	}
	return file, pos, nil
}

// scanBinlogStatus runs a SHOW … STATUS statement and extracts the binlog file
// and position from its first two columns. The remaining columns are discarded,
// which makes the read tolerant to the column-count difference across flavors:
// MySQL 5.7/8.0 and 8.4 return five columns (… Executed_Gtid_Set), MariaDB
// returns four (no Executed_Gtid_Set). Only File and Position are needed.
//
// An empty resultset (log_bin=OFF) surfaces as sql.ErrNoRows so the caller's
// OFF-detection keeps working unchanged; statement-not-found surfaces as the
// driver's syntax error (1064).
func scanBinlogStatus(db *sql.DB, stmt string) (file string, pos uint32, err error) {
	rows, err := db.Query(stmt)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	if len(cols) < 2 {
		return "", 0, fmt.Errorf("%s returned %d column(s), expected at least File and Position", stmt, len(cols))
	}
	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return "", 0, rerr
		}
		return "", 0, sql.ErrNoRows
	}

	dest := make([]any, len(cols))
	dest[0] = &file
	dest[1] = &pos
	for i := 2; i < len(cols); i++ {
		dest[i] = new(sql.RawBytes)
	}
	if err := rows.Scan(dest...); err != nil {
		return "", 0, err
	}
	return file, pos, nil
}

// defaultTimeout is the TCP connect timeout applied when the DSN does not
// specify one. Prevents indefinite hangs when MySQL is unreachable.
const defaultTimeout = 10 * time.Second

// Connect opens and verifies a MySQL connection using the given DSN.
// parseTime=true is always injected so DATETIME columns scan into time.Time.
// A 10-second TCP connect timeout is applied when the DSN does not specify one.
// The caller is responsible for closing the returned *sql.DB.
func Connect(dsn string) (*sql.DB, error) {
	normalized, err := buildDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", normalized)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}
	return db, nil
}

// driverDefaultMaxAllowedPacket is the client-side max_allowed_packet the
// go-sql-driver assigns when a DSN omits the parameter. Read from the driver
// itself (via NewConfig) so it tracks any change to that default instead of
// hardcoding 64 MiB.
var driverDefaultMaxAllowedPacket = mysql.NewConfig().MaxAllowedPacket

// buildDSN applies bintrail's connection invariants to a user DSN and returns
// the normalized DSN string. Split out from Connect so the invariants are unit
// testable without a live server. Invariants: parseTime=true (DATETIME scans
// into time.Time), Loc=UTC, a default connect timeout, and aligning the
// client's max_allowed_packet with the server's.
func buildDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid DSN: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	// Align the client's max_allowed_packet with the server's instead of the
	// go-sql-driver's fixed 64 MiB default. binlog_events stores full
	// before/after row images as JSON, and a large BLOB/JSON value
	// (base64-inflated ~1.33×) can exceed 64 MiB. The rejection #652 reproduced
	// is server-side (Error 1105 "… mysql_send_long_data() … longer than
	// 'max_allowed_packet'"), so raising the *server* limit (docker-compose
	// --max-allowed-packet=1G) is the load-bearing change; maxAllowedPacket=0
	// makes the driver fetch @@max_allowed_packet from the server and size to it
	// (bundled index MySQL: 1 GiB; a BYO index: whatever it set) so the client
	// tracks the server rather than imposing its own cap.
	//
	// This is a ceiling raise, not a fix: events larger than the server's
	// max_allowed_packet are still rejected, and that rejection must — but does
	// not yet (#652) — fail loud rather than drop silently.
	//
	// Precedence: an explicit non-default maxAllowedPacket in the DSN is
	// preserved. We compare against the driver's own default rather than
	// string-matching the DSN, so a value the case-sensitive driver ignores
	// (e.g. a mis-cased param) falls through to the safe server-honoring 0
	// instead of being silently left at the 64 MiB cap.
	if cfg.MaxAllowedPacket == driverDefaultMaxAllowedPacket {
		cfg.MaxAllowedPacket = 0
	}
	return cfg.FormatDSN(), nil
}

// ParseSourceDSN decomposes a go-sql-driver DSN into host, port, user, and
// password. It requires a TCP address and rejects unix-socket DSNs — binlog
// replication (BinlogSyncerConfig) and remote dumps both need a host:port.
func ParseSourceDSN(dsn string) (host string, port uint16, user, password string, err error) {
	cfg, parseErr := mysql.ParseDSN(dsn)
	if parseErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid --source-dsn: %w", parseErr)
	}
	if strings.EqualFold(cfg.Net, "unix") {
		return "", 0, "", "", fmt.Errorf("--source-dsn uses a unix socket; binlog replication requires a TCP address")
	}
	h, p, splitErr := net.SplitHostPort(cfg.Addr)
	if splitErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid address in --source-dsn %q: %w", cfg.Addr, splitErr)
	}
	portN, convErr := strconv.ParseUint(p, 10, 16)
	if convErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid port in --source-dsn: %w", convErr)
	}
	return h, uint16(portN), cfg.User, cfg.Passwd, nil
}
