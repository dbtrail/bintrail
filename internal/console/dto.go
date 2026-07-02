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
// It deliberately OMITS connection_id, query_text, and query_hash. Those
// fields — the MySQL pseudo_thread_id of the transaction that produced the
// change, and the originating SQL statement + its STATEMENT_DIGEST (#699) —
// are actor/statement-attribution data and belong to the paid "forensics"
// surface (who-changed / what-statement / audit), not the free
// "query_explorer" surface this console exposes. The omission is the entire
// open-core boundary for the events API: query.ResultRow carries those fields,
// the package's own jsonRow serialises them, but this DTO drops them on the
// way out. Do not add them here without crossing the licensing line.
type eventDTO struct {
	EventID        uint64         `json:"event_id"`
	BinlogFile     string         `json:"binlog_file"`
	StartPos       uint64         `json:"start_pos"`
	EndPos         uint64         `json:"end_pos"`
	EventTimestamp string         `json:"event_timestamp"`
	GTID           *string        `json:"gtid"`
	SchemaName     string         `json:"schema_name"`
	TableName      string         `json:"table_name"`
	EventType      string         `json:"event_type"`
	PKValues       string         `json:"pk_values"`
	ChangedColumns []string       `json:"changed_columns"`
	RowBefore      map[string]any `json:"row_before"`
	RowAfter       map[string]any `json:"row_after"`
}

// toEventDTO maps a query.ResultRow to the redacted wire view, dropping
// connection_id/query_text/query_hash and stringifying the event type and
// timestamp.
func toEventDTO(r query.ResultRow) eventDTO {
	return eventDTO{
		EventID:        r.EventID,
		BinlogFile:     r.BinlogFile,
		StartPos:       r.StartPos,
		EndPos:         r.EndPos,
		EventTimestamp: r.EventTimestamp.Format(consoleTSFormat),
		GTID:           r.GTID,
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
