package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbtrail/bintrail/internal/byos"
	"github.com/dbtrail/bintrail/internal/serverid"
)

// resolveServerIdentity queries @@server_uuid from sourceDB, parses the host,
// port, and username from sourceDSN, then resolves or registers the server in
// the index database. Returns the stable bintrail_id.
func resolveServerIdentity(ctx context.Context, sourceDB, indexDB *sql.DB, sourceDSN string) (string, error) {
	ident, err := loadSourceIdentity(ctx, sourceDB, sourceDSN)
	if err != nil {
		return "", err
	}
	return serverid.ResolveServer(ctx, indexDB, ident.ServerUUID, ident.Host, uint16(ident.Port), ident.User)
}

// loadSourceIdentity captures the source MySQL server's @@server_uuid plus
// host/port/user parsed from sourceDSN. The result is stamped onto every
// BYOS MetadataRecord so the dbtrail SaaS side can resolve a stable
// bintrail_id against its own bintrail_servers table — see
// bintrail-saas-architecture.md §22.11. Safe to call even when no index DB
// is available locally (the agent is running in fully stateless BYOS mode).
func loadSourceIdentity(ctx context.Context, sourceDB *sql.DB, sourceDSN string) (byos.SourceIdentity, error) {
	var serverUUID string
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&serverUUID); err != nil {
		return byos.SourceIdentity{}, fmt.Errorf("query server_uuid: %w", err)
	}
	host, port, user, _, err := parseSourceDSN(sourceDSN)
	if err != nil {
		return byos.SourceIdentity{}, err
	}
	return byos.SourceIdentity{
		ServerUUID: serverUUID,
		Host:       host,
		Port:       int(port),
		User:       user,
	}, nil
}
