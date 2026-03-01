// Package serverid manages persistent server identities for bintrail.
// Each MySQL server is assigned a unique bintrail_id (UUID) on first contact.
// Identity resolution uses five rules to handle UUID regeneration, host
// migration, and cloned-server conflicts while preserving the bintrail_id.
package serverid

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ErrConflict is returned by ResolveServer when the observed server_uuid matches
// one active record while the observed host+port+username matches a different
// active record — a cloned-server situation that requires manual resolution.
var ErrConflict = errors.New("server identity conflict: server_uuid and host:port:username match different records — resolve manually")

// Server represents an active row in bintrail_servers.
type Server struct {
	BintrailID string
	ServerUUID string
	Host       string
	Port       uint
	Username   string
}

// resolution is the outcome of resolveIdentity.
type resolution int

const (
	resNoChange  resolution = iota // Rule 1: exact match — no update needed
	resMigration                   // Rule 2: UUID matches, host/port/user changed
	resUUIDRegen                   // Rule 3: host+port+user matches, UUID changed
	resNew                         // Rule 4: no match → register new server
	resConflict                    // Rule 5: ambiguous → manual resolution required
)

// resolveIdentity applies the five-rule identity resolution algorithm against
// a set of active servers. It is a pure function with no DB side-effects,
// making it straightforward to unit-test.
//
// Rules:
//  1. UUID + host + port + user all match same record → no change
//  2. UUID matches one record, host/port/user differ → migration (update fields)
//  3. host+port+user match one record, UUID differs → UUID regeneration (update UUID)
//  4. No record matches either criterion → new server
//  5. UUID matches one record AND host+port+user match a DIFFERENT record → conflict
func resolveIdentity(servers []Server, serverUUID, host string, port uint, username string) (*Server, resolution) {
	var uuidMatch *Server
	var hpuMatch *Server
	for i := range servers {
		s := &servers[i]
		if s.ServerUUID == serverUUID {
			uuidMatch = s
		}
		if s.Host == host && s.Port == port && s.Username == username {
			hpuMatch = s
		}
	}

	switch {
	case uuidMatch != nil && hpuMatch != nil && uuidMatch.BintrailID == hpuMatch.BintrailID:
		// Rule 1: exact match on all components.
		return uuidMatch, resNoChange
	case uuidMatch != nil && hpuMatch != nil && uuidMatch.BintrailID != hpuMatch.BintrailID:
		// Rule 5: UUID matches one record, host+port+user match a different one.
		return nil, resConflict
	case uuidMatch != nil:
		// Rule 2: UUID matches, host/port/user changed (migration or reconfiguration).
		return uuidMatch, resMigration
	case hpuMatch != nil:
		// Rule 3: host+port+user match, UUID changed (UUID regeneration).
		return hpuMatch, resUUIDRegen
	default:
		// Rule 4: no match at all → genuinely new server.
		return nil, resNew
	}
}

// loadActiveServers fetches all non-decommissioned server records from the index DB.
func loadActiveServers(ctx context.Context, db *sql.DB) ([]Server, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT bintrail_id, server_uuid, host, port, username
		 FROM bintrail_servers
		 WHERE decommissioned_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var servers []Server
	for rows.Next() {
		var s Server
		if err := rows.Scan(&s.BintrailID, &s.ServerUUID, &s.Host, &s.Port, &s.Username); err != nil {
			return nil, err
		}
		servers = append(servers, s)
	}
	return servers, rows.Err()
}

// logChange inserts a row into bintrail_server_changes to audit an identity component change.
func logChange(ctx context.Context, db *sql.DB, bintrailID, field, oldVal, newVal string) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO bintrail_server_changes (bintrail_id, field_changed, old_value, new_value)
		 VALUES (?, ?, ?, ?)`,
		bintrailID, field, oldVal, newVal)
	return err
}

// ResolveServer looks up or registers a server in bintrail_servers, returning its
// stable bintrail_id. It applies the five-rule identity resolution algorithm,
// updates changed fields in bintrail_servers, and records each change in
// bintrail_server_changes (append-only audit trail).
//
// Returns ErrConflict if server_uuid and host+port+username match two different
// active records (cloned-server situation). The caller should log a warning and
// refuse to operate until the conflict is resolved manually.
func ResolveServer(ctx context.Context, db *sql.DB, serverUUID, host string, port uint, username string) (string, error) {
	servers, err := loadActiveServers(ctx, db)
	if err != nil {
		return "", fmt.Errorf("load active servers: %w", err)
	}

	matched, rule := resolveIdentity(servers, serverUUID, host, port, username)

	switch rule {
	case resNoChange:
		return matched.BintrailID, nil

	case resConflict:
		// Locate both conflicting records to include them in the error message.
		var uuidMatch, hpuMatch *Server
		for i := range servers {
			s := &servers[i]
			if s.ServerUUID == serverUUID {
				uuidMatch = s
			}
			if s.Host == host && s.Port == port && s.Username == username {
				hpuMatch = s
			}
		}
		return "", fmt.Errorf("%w: server_uuid %q belongs to bintrail_id %q but %s:%d/%s belongs to bintrail_id %q",
			ErrConflict, serverUUID, uuidMatch.BintrailID, host, port, username, hpuMatch.BintrailID)

	case resMigration:
		// Rule 2: UUID matched — update whichever of host/port/username changed.
		id := matched.BintrailID
		if matched.Host != host {
			if err := logChange(ctx, db, id, "host", matched.Host, host); err != nil {
				return "", fmt.Errorf("log host change: %w", err)
			}
		}
		if matched.Port != port {
			if err := logChange(ctx, db, id, "port", fmt.Sprint(matched.Port), fmt.Sprint(port)); err != nil {
				return "", fmt.Errorf("log port change: %w", err)
			}
		}
		if matched.Username != username {
			if err := logChange(ctx, db, id, "username", matched.Username, username); err != nil {
				return "", fmt.Errorf("log username change: %w", err)
			}
		}
		if _, err := db.ExecContext(ctx,
			`UPDATE bintrail_servers SET host = ?, port = ?, username = ?, updated_at = UTC_TIMESTAMP()
			 WHERE bintrail_id = ?`,
			host, port, username, id); err != nil {
			return "", fmt.Errorf("update server record: %w", err)
		}
		return id, nil

	case resUUIDRegen:
		// Rule 3: host+port+user matched — update the server_uuid.
		id := matched.BintrailID
		if err := logChange(ctx, db, id, "server_uuid", matched.ServerUUID, serverUUID); err != nil {
			return "", fmt.Errorf("log uuid change: %w", err)
		}
		if _, err := db.ExecContext(ctx,
			`UPDATE bintrail_servers SET server_uuid = ?, updated_at = UTC_TIMESTAMP()
			 WHERE bintrail_id = ?`,
			serverUUID, id); err != nil {
			return "", fmt.Errorf("update server uuid: %w", err)
		}
		return id, nil

	default: // resNew
		// Rule 4: no match — generate a new stable identity.
		id := uuid.NewString()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, port, username)
			 VALUES (?, ?, ?, ?, ?)`,
			id, serverUUID, host, port, username); err != nil {
			return "", fmt.Errorf("register server: %w", err)
		}
		return id, nil
	}
}

// DecommissionServer sets decommissioned_at on the given bintrail_id, excluding
// it from future identity resolution. The record and its archived data are preserved.
func DecommissionServer(ctx context.Context, db *sql.DB, bintrailID string) error {
	res, err := db.ExecContext(ctx,
		`UPDATE bintrail_servers SET decommissioned_at = UTC_TIMESTAMP()
		 WHERE bintrail_id = ? AND decommissioned_at IS NULL`,
		bintrailID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("bintrail_id %q not found or already decommissioned", bintrailID)
	}
	return nil
}
