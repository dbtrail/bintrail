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

// DDLBintrailServers is the canonical CREATE TABLE statement for bintrail_servers.
// Used by `bintrail init` and by testutil.InitIndexTables to ensure a single
// source of truth for the schema.
const DDLBintrailServers = `CREATE TABLE IF NOT EXISTS bintrail_servers (
    bintrail_id       CHAR(36)        NOT NULL,
    server_uuid       CHAR(36)        NOT NULL,
    host              VARCHAR(255)    NOT NULL,
    port              SMALLINT UNSIGNED NOT NULL DEFAULT 3306,
    username          VARCHAR(255)    NOT NULL,
    created_at        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    decommissioned_at TIMESTAMP       NULL DEFAULT NULL,
    PRIMARY KEY (bintrail_id),
    INDEX idx_server_uuid    (server_uuid),
    INDEX idx_host_port_user (host, port, username)
) ENGINE=InnoDB`

// DDLBintrailServerChanges is the canonical CREATE TABLE statement for
// bintrail_server_changes. Append-only audit trail — never update or delete rows.
const DDLBintrailServerChanges = `CREATE TABLE IF NOT EXISTS bintrail_server_changes (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    bintrail_id   CHAR(36)        NOT NULL,
    field_changed ENUM('server_uuid','host','port','username') NOT NULL,
    old_value     VARCHAR(255)    NOT NULL,
    new_value     VARCHAR(255)    NOT NULL,
    detected_at   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_bintrail_id (bintrail_id),
    INDEX idx_detected_at (detected_at)
) ENGINE=InnoDB`

// ErrConflict is returned by ResolveServer when the observed server_uuid matches
// one active record while the observed host+port+username matches a different
// active record — a cloned-server situation that requires manual resolution.
var ErrConflict = errors.New("server identity conflict: server_uuid and host:port:username match different records — resolve manually")

// Server represents an active row in bintrail_servers.
type Server struct {
	BintrailID string
	ServerUUID string
	Host       string
	Port       uint16
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
func resolveIdentity(servers []Server, serverUUID, host string, port uint16, username string) (*Server, resolution) {
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

// loadCandidatesForUpdate fetches active server records that match either the
// server_uuid or the host+port+username, locking them for update to prevent
// concurrent registrations from producing duplicate active entries.
//
// The SELECT ... FOR UPDATE + InnoDB gap locks ensure that no other transaction
// can insert a matching row between the SELECT and the subsequent INSERT/UPDATE
// in this transaction.
func loadCandidatesForUpdate(ctx context.Context, tx *sql.Tx, serverUUID, host string, port uint16, username string) ([]Server, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT bintrail_id, server_uuid, host, port, username
		 FROM bintrail_servers
		 WHERE (server_uuid = ? OR (host = ? AND port = ? AND username = ?))
		   AND decommissioned_at IS NULL
		 FOR UPDATE`,
		serverUUID, host, port, username)
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
func logChange(ctx context.Context, tx *sql.Tx, bintrailID, field, oldVal, newVal string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO bintrail_server_changes (bintrail_id, field_changed, old_value, new_value)
		 VALUES (?, ?, ?, ?)`,
		bintrailID, field, oldVal, newVal)
	return err
}

// ResolveServer looks up or registers a server in bintrail_servers, returning its
// stable bintrail_id. It runs inside a transaction with SELECT ... FOR UPDATE to
// prevent concurrent callers from creating duplicate active entries for the same
// server. Any detected component changes are recorded in bintrail_server_changes.
//
// Returns ErrConflict if server_uuid and host+port+username match two different
// active records (cloned-server situation). The caller should log a warning and
// refuse to operate until the conflict is resolved manually.
func ResolveServer(ctx context.Context, db *sql.DB, serverUUID, host string, port uint16, username string) (string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	id, err := resolveInTx(ctx, tx, serverUUID, host, port, username)
	if err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}
	return id, nil
}

func resolveInTx(ctx context.Context, tx *sql.Tx, serverUUID, host string, port uint16, username string) (string, error) {
	servers, err := loadCandidatesForUpdate(ctx, tx, serverUUID, host, port, username)
	if err != nil {
		return "", fmt.Errorf("load candidates: %w", err)
	}

	matched, rule := resolveIdentity(servers, serverUUID, host, port, username)

	switch rule {
	case resNoChange:
		return matched.BintrailID, nil

	case resConflict:
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
		id := matched.BintrailID
		if matched.Host != host {
			if err := logChange(ctx, tx, id, "host", matched.Host, host); err != nil {
				return "", fmt.Errorf("log host change: %w", err)
			}
		}
		if matched.Port != port {
			if err := logChange(ctx, tx, id, "port", fmt.Sprint(matched.Port), fmt.Sprint(port)); err != nil {
				return "", fmt.Errorf("log port change: %w", err)
			}
		}
		if matched.Username != username {
			if err := logChange(ctx, tx, id, "username", matched.Username, username); err != nil {
				return "", fmt.Errorf("log username change: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE bintrail_servers SET host = ?, port = ?, username = ?, updated_at = UTC_TIMESTAMP()
			 WHERE bintrail_id = ?`,
			host, port, username, id); err != nil {
			return "", fmt.Errorf("update server record: %w", err)
		}
		return id, nil

	case resUUIDRegen:
		id := matched.BintrailID
		if err := logChange(ctx, tx, id, "server_uuid", matched.ServerUUID, serverUUID); err != nil {
			return "", fmt.Errorf("log uuid change: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE bintrail_servers SET server_uuid = ?, updated_at = UTC_TIMESTAMP()
			 WHERE bintrail_id = ?`,
			serverUUID, id); err != nil {
			return "", fmt.Errorf("update server uuid: %w", err)
		}
		return id, nil

	default: // resNew
		id := uuid.NewString()
		if _, err := tx.ExecContext(ctx,
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
