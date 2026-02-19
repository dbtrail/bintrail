package config

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

// Connect opens and verifies a MySQL connection using the given DSN.
// parseTime=true is always injected so DATETIME columns scan into time.Time.
// The caller is responsible for closing the returned *sql.DB.
func Connect(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}
	cfg.ParseTime = true

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
