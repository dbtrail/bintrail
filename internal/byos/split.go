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

// MetadataRecord contains only indexing metadata — no row-level data.
// It is sent to the dbtrail API so the hosted index can locate events
// without ever seeing the actual row contents.
type MetadataRecord struct {
	PKHash         string    `json:"pk_hash"`
	SchemaName     string    `json:"schema_name"`
	TableName      string    `json:"table_name"`
	EventType      string    `json:"event_type"` // "INSERT", "UPDATE", "DELETE"
	EventTimestamp time.Time `json:"event_timestamp"`
	ServerID       string    `json:"server_id"`
	GTID           string    `json:"gtid,omitempty"`
	RowCount       int       `json:"row_count"`
	ChangedColumns []string  `json:"changed_columns"` // nil for INSERT/DELETE
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
// serverID identifies the source MySQL server.
//
// Returns an error for unsupported event types (DDL, GTID). Callers should
// filter these before calling SplitEvent.
func SplitEvent(ev parser.Event, serverID string) (MetadataRecord, PayloadRecord, error) {
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
		RowCount:       1,
		ChangedColumns: changed,
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
