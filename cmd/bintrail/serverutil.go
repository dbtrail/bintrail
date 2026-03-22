package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbtrail/bintrail/internal/serverid"
)

// resolveServerIdentity queries @@server_uuid from sourceDB, parses the host,
// port, and username from sourceDSN, then resolves or registers the server in
// the index database. Returns the stable bintrail_id.
func resolveServerIdentity(ctx context.Context, sourceDB, indexDB *sql.DB, sourceDSN string) (string, error) {
	var serverUUID string
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&serverUUID); err != nil {
		return "", fmt.Errorf("query server_uuid: %w", err)
	}
	host, port, user, _, err := parseSourceDSN(sourceDSN)
	if err != nil {
		return "", err
	}
	return serverid.ResolveServer(ctx, indexDB, serverUUID, host, port, user)
}
