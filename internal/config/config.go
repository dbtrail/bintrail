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
	scanArgs := []any{&file, &pos, new(string), new(string), new(string)}
	if err = db.QueryRow("SHOW BINARY LOG STATUS").Scan(scanArgs...); err == nil {
		return file, pos, nil
	}
	firstErr := err
	if err = db.QueryRow("SHOW MASTER STATUS").Scan(scanArgs...); err != nil {
		if errors.Is(firstErr, sql.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return "", 0, fmt.Errorf("current binlog position empty — log_bin appears to be OFF on the source server (run \"SHOW VARIABLES LIKE 'log_bin'\" to confirm)")
		}
		return "", 0, fmt.Errorf("SHOW BINARY LOG STATUS / SHOW MASTER STATUS: %w (fallback: %w)", firstErr, err)
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
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}
	return db, nil
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
