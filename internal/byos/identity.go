package byos

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/serverid"
)

// ResolveServerIdentity captures the source server's identity anchor
// (@@server_uuid on MySQL, or a synthesized address-derived anchor on MariaDB —
// see LoadSourceIdentity), parses the host, port, and username from sourceDSN,
// then resolves or registers the server in the index database. Returns the
// stable bintrail_id.
func ResolveServerIdentity(ctx context.Context, sourceDB, indexDB *sql.DB, sourceDSN string) (string, error) {
	ident, err := LoadSourceIdentity(ctx, sourceDB, sourceDSN)
	if err != nil {
		return "", err
	}
	return serverid.ResolveServer(ctx, indexDB, ident.ServerUUID, ident.Host, uint16(ident.Port), ident.User)
}

// LoadSourceIdentity captures the source server's identity anchor plus
// host/port/user parsed from sourceDSN. For MySQL the anchor is @@server_uuid;
// for MariaDB (which has no @@server_uuid) it is a stable UUID synthesized from
// the source address (see serverid.SyntheticServerUUID). The result is stamped
// onto every BYOS MetadataRecord so the dbtrail SaaS side can resolve a stable
// bintrail_id against its own bintrail_servers table — see
// bintrail-saas-architecture.md §22.11. Safe to call even when no index DB
// is available locally (the agent is running in fully stateless BYOS mode).
func LoadSourceIdentity(ctx context.Context, sourceDB *sql.DB, sourceDSN string) (SourceIdentity, error) {
	host, port, user, _, err := config.ParseSourceDSN(sourceDSN)
	if err != nil {
		return SourceIdentity{}, err
	}

	var serverUUID string
	if uuidErr := sourceDB.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&serverUUID); uuidErr != nil {
		// @@server_uuid is unavailable. The expected cause is a MariaDB source —
		// MariaDB has no such system variable. Confirm the flavor via VERSION()
		// before synthesizing an anchor, so a genuine MySQL failure (permissions,
		// timeout, dropped connection) still propagates instead of silently
		// fabricating an identity. A MySQL source therefore never reaches the
		// synthesis path: its @@server_uuid query succeeds, and even on failure
		// DetectFlavor reports "mysql"/"" and the error is returned as before.
		if metadata.DetectFlavor(sourceDB) != "mariadb" {
			return SourceIdentity{}, fmt.Errorf("query server_uuid: %w", uuidErr)
		}
		serverUUID = serverid.SyntheticServerUUID(host, port)
		slog.Warn("MariaDB source has no @@server_uuid; synthesized a stable bintrail_id anchor from the source address — set a distinct address per server to keep their archives separate in S3",
			"host", host, "port", port, "synthetic_server_uuid", serverUUID)
	}

	return SourceIdentity{
		ServerUUID: serverUUID,
		Host:       host,
		Port:       int(port),
		User:       user,
	}, nil
}
