package forensics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
)

// ListUsers returns the list of known MySQL user accounts by querying
// mysql.user (authoritative, survives restarts) and performance_schema.accounts
// (includes historically connected users). The two sources are merged and
// deduplicated, preserving first-seen order. It errors only when BOTH sources
// fail — a single inaccessible source degrades gracefully.
//
// Intended for filter dropdowns in the CLI/console/MCP surfaces.
func ListUsers(ctx context.Context, sourceDB *sql.DB) ([]string, error) {
	seen := make(map[string]struct{})
	users := make([]string, 0)

	collectUsers := func(query, source string) error {
		rows, err := sourceDB.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var u string
			if err := rows.Scan(&u); err != nil {
				slog.Warn("forensics: users scan error", "source", source, "error", err)
				continue
			}
			if u == "" {
				continue
			}
			if _, ok := seen[u]; !ok {
				seen[u] = struct{}{}
				users = append(users, u)
			}
		}
		if err := rows.Err(); err != nil {
			slog.Warn("forensics: users iteration error", "source", source, "error", err)
		}
		return nil
	}

	// Primary source: mysql.user (all defined accounts).
	mysqlUserErr := collectUsers(
		"SELECT DISTINCT User FROM mysql.user WHERE User != '' ORDER BY User",
		"mysql.user")
	if mysqlUserErr != nil {
		slog.Warn("forensics: mysql.user query failed (may lack privileges)", "error", mysqlUserErr)
	}

	// Secondary source: performance_schema.accounts (connected since last restart).
	perfSchemaErr := collectUsers(
		"SELECT DISTINCT USER FROM performance_schema.accounts WHERE USER IS NOT NULL AND USER != '' ORDER BY USER",
		"performance_schema.accounts")
	if perfSchemaErr != nil {
		slog.Warn("forensics: performance_schema.accounts query failed", "error", perfSchemaErr)
	}

	if mysqlUserErr != nil && perfSchemaErr != nil {
		return nil, fmt.Errorf("could not query MySQL user accounts (insufficient privileges or connectivity issue): %w",
			errors.Join(mysqlUserErr, perfSchemaErr))
	}

	return users, nil
}
