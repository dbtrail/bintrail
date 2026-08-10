package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// ListProfiles returns every RBAC data-profile name defined in the index's
// profiles table, sorted by name. Names only — never the rules or the flagged
// tables/columns they map to: the caller is a listing surface (a settings
// panel offering a picker instead of a free-text field), and the vocabulary
// is all it needs.
//
// A missing table (MySQL error 1146 — an index created before `bintrail
// init` grew the RBAC tables and never migrated, the archive_state
// precedent) returns an empty list, not an error: "no profiles exist" is the
// truthful answer for a listing.
func ListProfiles(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM profiles ORDER BY name`)
	if err != nil {
		var myErr *mysqldriver.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			return nil, nil
		}
		return nil, fmt.Errorf("list profiles: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("list profiles: %w", err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list profiles: %w", err)
	}
	return names, nil
}
