package console

import (
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// consoleTSFormat is the timestamp layout used in API responses. It matches
// the MySQL datetime format accepted by cliutil.ParseTime, so a timestamp
// rendered here round-trips back into a filter unchanged.
const consoleTSFormat = "2006-01-02 15:04:05"

// eventDTO is the JSON-serialisable view of a query.ResultRow exposed by the
// console's read-only API.
//
// connection_id is INCLUDED: it is captured index data (the transaction's
// originating connection id from the binlog), and the general events surface
// carries it like any other indexed column.
//
// query_text and query_hash remain OMITTED. That is a distinct, still-live
// boundary (#699, "what statement produced this row") — this DTO has simply
// never grown a consumer for statement text. Do not add them here without a
// surface that actually reads them.
type eventDTO struct {
	EventID        uint64         `json:"event_id"`
	BinlogFile     string         `json:"binlog_file"`
	StartPos       uint64         `json:"start_pos"`
	EndPos         uint64         `json:"end_pos"`
	EventTimestamp string         `json:"event_timestamp"`
	GTID           *string        `json:"gtid"`
	ConnectionID   *uint32        `json:"connection_id"`
	SchemaName     string         `json:"schema_name"`
	TableName      string         `json:"table_name"`
	EventType      string         `json:"event_type"`
	PKValues       string         `json:"pk_values"`
	ChangedColumns []string       `json:"changed_columns"`
	RowBefore      map[string]any `json:"row_before"`
	RowAfter       map[string]any `json:"row_after"`
}

// toEventDTO maps a query.ResultRow to the wire view: connection_id now
// passes through (#701 D1); query_text/query_hash are still dropped (#699);
// the event type and timestamp are stringified.
func toEventDTO(r query.ResultRow) eventDTO {
	return eventDTO{
		EventID:        r.EventID,
		BinlogFile:     r.BinlogFile,
		StartPos:       r.StartPos,
		EndPos:         r.EndPos,
		EventTimestamp: r.EventTimestamp.Format(consoleTSFormat),
		GTID:           r.GTID,
		ConnectionID:   r.ConnectionID,
		SchemaName:     r.SchemaName,
		TableName:      r.TableName,
		EventType:      eventTypeName(r.EventType),
		PKValues:       r.PKValues,
		ChangedColumns: r.ChangedColumns,
		RowBefore:      r.RowBefore,
		RowAfter:       r.RowAfter,
	}
}

// toEventDTOs maps a slice of result rows. The returned slice is non-nil even
// when rows is empty, so the JSON encodes as [] rather than null.
func toEventDTOs(rows []query.ResultRow) []eventDTO {
	out := make([]eventDTO, len(rows))
	for i, r := range rows {
		out[i] = toEventDTO(r)
	}
	return out
}

// eventTypeName renders an event.EventType as its canonical SQL keyword,
// matching the strings used elsewhere in the codebase (query, recovery).
func eventTypeName(et event.EventType) string {
	switch et {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
		return "DELETE"
	case event.EventSnapshot:
		return "SNAPSHOT"
	default:
		return "UNKNOWN"
	}
}
