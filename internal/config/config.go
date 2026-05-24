package config

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

// CurrentBinlogPosition returns the current binlog file name and position
// from the source server. It tries SHOW BINARY LOG STATUS first (MySQL 8.4+)
// and falls back to SHOW MASTER STATUS for 5.7 / 8.0 / Percona <8.4 where the
// new statement does not exist. When both fail, the wrapped error includes
// both diagnostics so the operator does not chase the wrong syntax.
func CurrentBinlogPosition(db *sql.DB) (file string, pos uint32, err error) {
	scanArgs := []any{&file, &pos, new(string), new(string), new(string)}
	if err = db.QueryRow("SHOW BINARY LOG STATUS").Scan(scanArgs...); err == nil {
		return file, pos, nil
	}
	firstErr := err
	if err = db.QueryRow("SHOW MASTER STATUS").Scan(scanArgs...); err != nil {
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
