// Package byos implements the Bring Your Own Storage (BYOS) data separation
// layer. In BYOS mode, parsed binlog events are split into two streams:
//
//   - Metadata — lightweight indexing records sent to the dbtrail API
//     (operation details, primary key hashes, and column change tracking,
//     but never actual row values)
//   - Payload — full row data written to the customer's storage backend
//     as Parquet (complete before/after images, primary key values, and
//     event context)
//
// In hosted mode this package is not used and events flow directly to the
// indexer as they do today.
package byos

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
)

// SourceIdentity describes the source MySQL server that produced an event.
// It is captured once at agent startup and stamped onto every MetadataRecord
// so the dbtrail SaaS side can resolve a stable bintrail_id against its own
// local bintrail_servers table — closing the identity-model asymmetry between
// hosted and BYOS modes.  See bintrail-saas-architecture.md §22.11.
type SourceIdentity struct {
	ServerUUID string // @@server_uuid from the source DB
	Host       string // host from --source-dsn
	Port       int    // port from --source-dsn
	User       string // user from --source-dsn
}

// MetadataRecord contains only indexing metadata — no row-level data.
// It is sent to the dbtrail API so the hosted index can locate events
// without ever seeing the actual row contents.
//
// The ServerUUID/SourceHost/SourcePort/SourceUser fields are populated from
// SourceIdentity at SplitEvent time and let the SaaS-side ingest handler
// resolve a stable bintrail_id (architecture §22.11). They are optional on
// the wire so older bintrail clients remain compatible.
type MetadataRecord struct {
	PKHash         string    `json:"pk_hash"`
	SchemaName     string    `json:"schema_name"`
	TableName      string    `json:"table_name"`
	EventType      string    `json:"event_type"` // "INSERT", "UPDATE", "DELETE"
	EventTimestamp time.Time `json:"event_timestamp"`
	ServerID       string    `json:"server_id"`
	GTID           string    `json:"gtid,omitempty"`
	ConnectionID   uint32    `json:"connection_id,omitempty"` // MySQL pseudo_thread_id; 0 = unknown
	RowCount       int       `json:"row_count"`
	ChangedColumns []string  `json:"changed_columns"` // nil for INSERT/DELETE

	// Source identity (architecture §22.11). All four are optional; older
	// bintrail clients omit them entirely and the SaaS side falls back to
	// the legacy NULL-bintrail_id behavior.
	ServerUUID string `json:"server_uuid,omitempty"`
	SourceHost string `json:"source_host,omitempty"`
	SourcePort int    `json:"source_port,omitempty"`
	SourceUser string `json:"source_user,omitempty"`
}

// PayloadRecord contains full row data that stays in the customer's
// infrastructure. It is written to the customer's storage backend as Parquet.
type PayloadRecord struct {
	PKHash         string         `json:"pk_hash"`
	PKValues       string         `json:"pk_values"`
	SchemaName     string         `json:"schema_name"`
	TableName      string         `json:"table_name"`
	EventType      string         `json:"event_type"`
	EventTimestamp time.Time      `json:"event_timestamp"`
	RowBefore      map[string]any `json:"row_before,omitempty"`
	RowAfter       map[string]any `json:"row_after,omitempty"`
	ChangedColumns []string       `json:"changed_columns,omitempty"`
	SchemaVersion  uint32         `json:"schema_version"`
}

// PKHash computes the SHA-256 hex digest of pkValues, matching MySQL's
// SHA2(pk_values, 256) stored generated column in binlog_events.
func PKHash(pkValues string) string {
	h := sha256.Sum256([]byte(pkValues))
	return hex.EncodeToString(h[:])
}

// SplitEvent splits a parsed binlog event into a metadata record (for the
// dbtrail API) and a payload record (for the customer's storage backend).
// serverID identifies the source MySQL server. ident carries the source
// server's @@server_uuid + DSN host/port/user so the SaaS-side ingest can
// resolve a stable bintrail_id (architecture §22.11). A zero-value ident is
// permitted (older callers / tests) and produces a record without source
// identity fields.
//
// Returns an error for unsupported event types (DDL, GTID). Callers should
// filter these before calling SplitEvent.
func SplitEvent(ev parser.Event, serverID string, ident SourceIdentity) (MetadataRecord, PayloadRecord, error) {
	typeName, err := eventTypeName(ev.EventType)
	if err != nil {
		return MetadataRecord{}, PayloadRecord{}, err
	}

	hash := PKHash(ev.PKValues)
	changed := parser.ChangedColumns(ev.RowBefore, ev.RowAfter)

	meta := MetadataRecord{
		PKHash:         hash,
		SchemaName:     ev.Schema,
		TableName:      ev.Table,
		EventType:      typeName,
		EventTimestamp: ev.Timestamp,
		ServerID:       serverID,
		GTID:           ev.GTID,
		ConnectionID:   ev.ConnectionID,
		RowCount:       1,
		ChangedColumns: changed,
		ServerUUID:     ident.ServerUUID,
		SourceHost:     ident.Host,
		SourcePort:     ident.Port,
		SourceUser:     ident.User,
	}

	payload := PayloadRecord{
		PKHash:         hash,
		PKValues:       ev.PKValues,
		SchemaName:     ev.Schema,
		TableName:      ev.Table,
		EventType:      typeName,
		EventTimestamp: ev.Timestamp,
		RowBefore:      maps.Clone(ev.RowBefore),
		RowAfter:       maps.Clone(ev.RowAfter),
		ChangedColumns: slices.Clone(changed),
		SchemaVersion:  ev.SchemaVersion,
	}

	return meta, payload, nil
}

// eventTypeName converts a parser.EventType to the string representation
// used in metadata and payload records. Returns an error for unsupported
// event types (DDL, GTID, etc.).
func eventTypeName(et parser.EventType) (string, error) {
	switch et {
	case parser.EventInsert:
		return "INSERT", nil
	case parser.EventUpdate:
		return "UPDATE", nil
	case parser.EventDelete:
		return "DELETE", nil
	default:
		return "", fmt.Errorf("unsupported event type %d for BYOS split", et)
	}
}
