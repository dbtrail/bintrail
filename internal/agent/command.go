// Package agent implements the outbound communication channel between the
// bintrail agent (running in customer infrastructure) and the dbtrail
// service. The agent opens a WebSocket connection to dbtrail, receives
// commands (resolve_pk, recover, forensics_query), and returns results
// without requiring any inbound ports.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ─── Wire messages ───────────────────────────────────────────────────────────

// Command is a message received from dbtrail through the WebSocket channel.
type Command struct {
	ID   string          `json:"id"`
	Type string          `json:"type"` // "resolve_pk", "recover", "forensics_query"
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
	MetadataStatus    string     `json:"metadata_status,omitempty"`    // "ok" or "degraded"
	PayloadStatus     string     `json:"payload_status,omitempty"`     // "ok" or "degraded"
	LastMetadataFlush *time.Time `json:"last_metadata_flush,omitempty"`
	LastPayloadFlush  *time.Time `json:"last_payload_flush,omitempty"`
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
type RecoverRequest struct {
	PKHashes   []string  `json:"pk_hashes"`
	Schema     string    `json:"schema"`
	Table      string    `json:"table"`
	TimeStart  time.Time `json:"time_start"`
	TimeEnd    time.Time `json:"time_end"`
	EventTypes []string  `json:"event_types,omitempty"`
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
// the Response to send back.
func dispatch(ctx context.Context, h Handler, cmd Command) Response {
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
		resp.Error = fmt.Sprintf("unknown command type %q", cmd.Type)
	}
	return resp
}
