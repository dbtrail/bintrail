// Package agent implements the outbound communication channel between the
// bintrail agent (running in customer infrastructure) and the dbtrail
// service. The agent opens a WebSocket connection to dbtrail, receives
// commands, and returns results without requiring any inbound ports.
//
// Command vocabulary:
//
//   - resolve_pk              — resolve pk_hash values to pk_values
//   - recover                 — generate reversal SQL for scoped events
//   - forensics_query         — fixed diagnostic queries (recent_queries,
//     lock_waits, table_io); predates the retired attribution surface and
//     keeps its wire name
//
// Any other command type is looked up in the extension registry
// (ext.RegisterAgentCommand) — embedding distributions register their
// commands there; unregistered types fail as unknown. See dispatch.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// ─── Wire messages ───────────────────────────────────────────────────────────

// Command is a message received from dbtrail through the WebSocket channel.
// Type is one of the command vocabulary entries listed in the package doc.
type Command struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// Response is the agent's reply to a Command.
type Response struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

// Heartbeat is sent periodically to keep the connection alive and report
// agent status to dbtrail.
type Heartbeat struct {
	Type       string    `json:"type"` // always "heartbeat"
	Version    string    `json:"version"`
	Uptime     string    `json:"uptime"`
	BintrailID string    `json:"bintrail_id,omitempty"`
	Timestamp  time.Time `json:"timestamp"`

	// Flush pipeline status (BYOS mode only).
	BufferEvents      *int       `json:"buffer_events,omitempty"`
	BufferBytes       *int64     `json:"buffer_bytes,omitempty"`
	SizeEvictions     *int64     `json:"size_evictions,omitempty"`
	MetadataStatus    string     `json:"metadata_status,omitempty"` // "ok" or "degraded"
	PayloadStatus     string     `json:"payload_status,omitempty"`  // "ok" or "degraded"
	LastMetadataFlush *time.Time `json:"last_metadata_flush,omitempty"`
	LastPayloadFlush  *time.Time `json:"last_payload_flush,omitempty"`

	// Cumulative events/batches permanently dropped after retries were
	// exhausted (BYOS mode; no on-disk spool). Monotonic; omitted while zero.
	MetadataLostEvents  int64 `json:"metadata_lost_events,omitempty"`
	MetadataLostBatches int64 `json:"metadata_lost_batches,omitempty"`
	PayloadLostEvents   int64 `json:"payload_lost_events,omitempty"`
	PayloadLostBatches  int64 `json:"payload_lost_batches,omitempty"`
}

// ─── Command payloads ────────────────────────────────────────────────────────

// ResolvePKRequest is the payload for "resolve_pk" commands.
type ResolvePKRequest struct {
	Items []PKItem `json:"items"`
}

// PKItem identifies a single primary-key hash to resolve.
type PKItem struct {
	PKHash string `json:"pk_hash"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// PKResult is one resolved pk_values entry.
type PKResult struct {
	PKHash   string `json:"pk_hash"`
	PKValues string `json:"pk_values"`
	Found    bool   `json:"found"`
}

// RecoverRequest is the payload for "recover" commands.
//
// GTID, when non-empty, scopes recovery to a single transaction.  The agent
// must honour it as the precise filter; the time range becomes optional
// (callers may send zero-value TimeStart/TimeEnd alongside a GTID, expecting
// the gtid to be the sole scope).  Dropping the GTID field silently
// produced reversal SQL for unrelated events — see nethalo/dbtrail#1512.
type RecoverRequest struct {
	PKHashes   []string  `json:"pk_hashes"`
	Schema     string    `json:"schema"`
	Table      string    `json:"table"`
	TimeStart  time.Time `json:"time_start"`
	TimeEnd    time.Time `json:"time_end"`
	EventTypes []string  `json:"event_types,omitempty"`
	GTID       string    `json:"gtid,omitempty"`
}

// ForensicsQueryRequest is the payload for "forensics_query" commands.
// Query is a predefined identifier (e.g. "recent_queries", "lock_waits",
// "table_io"), NOT arbitrary SQL.
type ForensicsQueryRequest struct {
	Query string `json:"query"`
}

// ForensicsResult holds the rows returned by a forensics query.
type ForensicsResult struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
}

// ─── Handler interface ───────────────────────────────────────────────────────

// Handler processes commands received from dbtrail. Each method receives
// a context that is cancelled when the WebSocket connection drops or the
// agent shuts down.
type Handler interface {
	HandleResolvePK(ctx context.Context, req ResolvePKRequest) ([]PKResult, error)
	HandleRecover(ctx context.Context, req RecoverRequest) (string, error)
	HandleForensicsQuery(ctx context.Context, req ForensicsQueryRequest) (*ForensicsResult, error)
}

// dispatch routes a Command to the appropriate Handler method and returns
// the Response to send back. A command type with no builtin case is looked
// up in the extension registry (ext.RegisterAgentCommand) — builtin types
// always win — and only fails as unknown when the registry has no handler
// either. deps carries the connection handles registry handlers receive;
// the zero value is fine when nothing is registered.
func dispatch(ctx context.Context, h Handler, cmd Command, deps ext.AgentDeps) Response {
	resp := Response{ID: cmd.ID, Type: cmd.Type}

	switch cmd.Type {
	case "resolve_pk":
		var req ResolvePKRequest
		if err := json.Unmarshal(cmd.Data, &req); err != nil {
			resp.Error = fmt.Sprintf("invalid resolve_pk payload: %v", err)
			return resp
		}
		results, err := h.HandleResolvePK(ctx, req)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		resp.Data = results

	case "recover":
		var req RecoverRequest
		if err := json.Unmarshal(cmd.Data, &req); err != nil {
			resp.Error = fmt.Sprintf("invalid recover payload: %v", err)
			return resp
		}
		sql, err := h.HandleRecover(ctx, req)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		resp.Data = sql

	case "forensics_query":
		var req ForensicsQueryRequest
		if err := json.Unmarshal(cmd.Data, &req); err != nil {
			resp.Error = fmt.Sprintf("invalid forensics_query payload: %v", err)
			return resp
		}
		result, err := h.HandleForensicsQuery(ctx, req)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		resp.Data = result

	default:
		if handler, ok := ext.LookupAgentCommand(cmd.Type); ok {
			return runExtCommand(ctx, handler, cmd, deps)
		}
		resp.Error = fmt.Sprintf("unknown command type %q", cmd.Type)
	}
	return resp
}

// runExtCommand invokes an extension-registered command handler with two
// containment layers the builtin cases don't need:
//
//   - Panic containment: a registry handler is externally-registered code
//     reachable by a remote command. A panic must degrade to a per-command
//     error response and a normal return — never kill the agent process
//     (the in-memory BYOS buffer dies with it).
//   - Response pre-marshaling: the handler's result is marshalled HERE, so a
//     value json.Marshal rejects (NaN, channel, cyclic structure, ...)
//     becomes a per-command error on the wire instead of failing later in
//     writeJSON — which would tear down the connection and enter a reconnect
//     loop that dies the same way on every redelivery. Response.Data carries
//     the pre-marshalled bytes as json.RawMessage; the envelope marshal
//     splices them verbatim, so writeJSON can no longer fail on handler
//     output.
func runExtCommand(ctx context.Context, handler ext.AgentCommandFunc, cmd Command, deps ext.AgentDeps) (resp Response) {
	resp = Response{ID: cmd.ID, Type: cmd.Type}
	defer func() {
		if p := recover(); p != nil {
			resp.Data = nil
			resp.Error = fmt.Sprintf("command handler for %q panicked: %v", cmd.Type, p)
		}
	}()
	result, err := handler(ctx, deps, cmd.Data)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	if result == nil {
		// (nil, nil) is a valid empty success: the response omits the data
		// field entirely, indistinguishable on the wire from "no data".
		return resp
	}
	data, err := json.Marshal(result)
	if err != nil {
		resp.Error = fmt.Sprintf("marshal response for %q: %v", cmd.Type, err)
		return resp
	}
	resp.Data = json.RawMessage(data)
	return resp
}
