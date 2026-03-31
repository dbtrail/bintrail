// Package byos implements the Bring Your Own Storage (BYOS) data separation
// layer. In BYOS mode, parsed binlog events are split into two streams:
//
//   - Metadata — lightweight indexing records sent to the dbtrail API
//     (pk_hash, table, schema, operation, timestamp, changed column names)
//   - Payload — full row data written to the customer's storage backend
//     as Parquet (pk_values, row_before, row_after)
//
// In hosted mode this package is not used and events flow directly to the
// indexer as they do today.
package byos

import (
	"crypto/sha256"
	"encoding/hex"
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
func SplitEvent(ev parser.Event, serverID string) (MetadataRecord, PayloadRecord) {
	hash := PKHash(ev.PKValues)
	typeName := eventTypeName(ev.EventType)
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
		RowBefore:      ev.RowBefore,
		RowAfter:       ev.RowAfter,
		ChangedColumns: changed,
		SchemaVersion:  ev.SchemaVersion,
	}

	return meta, payload
}

// eventTypeName converts a parser.EventType to the string representation
// used in metadata and payload records.
func eventTypeName(et parser.EventType) string {
	switch et {
	case parser.EventInsert:
		return "INSERT"
	case parser.EventUpdate:
		return "UPDATE"
	case parser.EventDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}
