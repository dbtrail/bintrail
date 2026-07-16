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
//     lock_waits, table_io)
//   - forensics_capabilities  — detect forensic data sources on the source
//     server (performance_schema, audit plugin, server variant)
//   - forensics_enrich        — live thread/connection attribution for a
//     set of connection IDs
//   - forensics_activity      — user_activity / connection_history
//     queries against performance_schema
//   - forensics_users         — list known MySQL user accounts
//   - forensics_audit_log     — parse the server's on-disk audit log
//
// The forensics_* attribution family (everything except the legacy
// forensics_query) is gated behind forensics.Enabled() — see dispatch.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/forensics"
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

// ─── Forensics attribution payloads ──────────────────────────────────────────
//
// BYOS payload/metadata split: attribution results carry identity (user,
// host, client program) and statement text — row-level-adjacent data. Like
// `recover` results, they travel over this WebSocket channel only as a
// response to an explicit SaaS command; they are NEVER part of the BYOS
// metadata ingest stream (same consideration #699/#712 flag for query_text:
// payload channel, never the metadata record).
//
// JSON field names mirror the SaaS wire contract (models/forensics.py);
// response bodies marshal internal/forensics structs, whose tags already
// match it.

// ForensicsEnrichRequest is the payload for "forensics_enrich" commands.
// The library caps ThreadIDs at 500 per call; larger sets must be chunked
// by the caller.
type ForensicsEnrichRequest struct {
	ThreadIDs []int64 `json:"thread_ids"`
}

// ForensicsActivityRequest is the payload for "forensics_activity" commands.
// QueryType selects the mode ("user_activity" or "connection_history"); the
// remaining fields are mode-specific filters — see forensics.ActivityQuery
// for which apply to which mode.
type ForensicsActivityRequest struct {
	QueryType string `json:"query_type"`
	User      string `json:"user,omitempty"`
	Host      string `json:"host,omitempty"`
	// Since/Until accept MySQL DATETIME or ISO 8601 strings.
	Since string `json:"since,omitempty"`
	Until string `json:"until,omitempty"`
	Limit int    `json:"limit,omitempty"`
	Order string `json:"order,omitempty"` // "ASC" or "DESC" (default)
}

// ForensicsAuditLogRequest is the payload for "forensics_audit_log"
// commands. Zero values disable the corresponding filter — see
// forensics.AuditReadOptions for the exact semantics (limit clamping,
// tail-mode defaults, rotated-file caps).
type ForensicsAuditLogRequest struct {
	Since          time.Time `json:"since,omitzero"`
	Until          time.Time `json:"until,omitzero"`
	User           string    `json:"user,omitempty"`
	EventType      string    `json:"event_type,omitempty"`
	Limit          int       `json:"limit,omitempty"`
	Offset         int       `json:"offset,omitempty"`
	IncludeRotated bool      `json:"include_rotated,omitempty"`
	TailLines      int       `json:"tail_lines,omitempty"`
	// Source selects where the audit log is read from ("" = auto: local file,
	// then the RDS file API when SourceHost looks like an RDS/Aurora endpoint;
	// "local"/"rds"/"cloudwatch" force a source). See forensics.AuditSource.
	Source string `json:"source,omitempty"`
	// SourceHost overrides the host used to detect/reach the RDS/CloudWatch
	// remote audit sources. Empty => the agent's own source host (derived from
	// its --source-dsn), so a BYOS agent on RDS/Aurora works without the caller
	// knowing the endpoint.
	SourceHost string `json:"source_host,omitempty"`
	// CloudWatchLogGroup names the log group when Source="cloudwatch"
	// (e.g. /aws/rds/instance/<id>/audit or /aws/rds/cluster/<id>/audit).
	CloudWatchLogGroup string `json:"cloudwatch_log_group,omitempty"`
}

// ForensicsUsersResult holds the user accounts known to the source server,
// for "forensics_users" commands. Mirrors the SaaS agent's HTTP response
// shape ({"users": [...]}, minus the transport-level success flag — the
// Response envelope's empty Error field carries that here).
type ForensicsUsersResult struct {
	Users []string `json:"users"`
}

// ─── Handler interface ───────────────────────────────────────────────────────

// Handler processes commands received from dbtrail. Each method receives
// a context that is cancelled when the WebSocket connection drops or the
// agent shuts down.
type Handler interface {
	HandleResolvePK(ctx context.Context, req ResolvePKRequest) ([]PKResult, error)
	HandleRecover(ctx context.Context, req RecoverRequest) (string, error)
	HandleForensicsQuery(ctx context.Context, req ForensicsQueryRequest) (*ForensicsResult, error)
	HandleForensicsCapabilities(ctx context.Context) (forensics.Capabilities, error)
	HandleForensicsEnrich(ctx context.Context, req ForensicsEnrichRequest) (forensics.EnrichResult, error)
	HandleForensicsActivity(ctx context.Context, req ForensicsActivityRequest) (forensics.ActivityResult, error)
	HandleForensicsUsers(ctx context.Context) (ForensicsUsersResult, error)
	HandleForensicsAuditLog(ctx context.Context, req ForensicsAuditLogRequest) (forensics.AuditReadResult, error)
}

// forensicsAttributionGate returns a non-empty error message when cmd.Type
// belongs to the forensics attribution family and forensics is disabled in
// this build. The gate lives here — at the WS surface entry point — per the
// #701 D1 entitlement seam: policy at the surface, mechanism-only library.
//
// The legacy "forensics_query" command is intentionally NOT gated: it
// predates the forensics library (three fixed diagnostic aggregates in this
// package, not attribution) and existing SaaS callers rely on it.
func forensicsAttributionGate(cmdType string) string {
	switch cmdType {
	case "forensics_capabilities", "forensics_enrich", "forensics_activity",
		"forensics_users", "forensics_audit_log":
		if !forensics.Enabled() {
			return "forensics disabled in this build"
		}
	}
	return ""
}

// dispatch routes a Command to the appropriate Handler method and returns
// the Response to send back. A command type with no builtin case is looked
// up in the extension registry (ext.RegisterAgentCommand) — builtin types
// always win — and only fails as unknown when the registry has no handler
// either. deps carries the connection handles registry handlers receive;
// the zero value is fine when nothing is registered.
func dispatch(ctx context.Context, h Handler, cmd Command, deps ext.AgentDeps) Response {
	resp := Response{ID: cmd.ID, Type: cmd.Type}

	// Entitlement gate for the forensics attribution family (#701 D1) —
	// checked once, before any payload is unmarshalled or handler invoked.
	if msg := forensicsAttributionGate(cmd.Type); msg != "" {
		resp.Error = msg
		return resp
	}

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

	case "forensics_capabilities":
		// No payload beyond the envelope.
		result, err := h.HandleForensicsCapabilities(ctx)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		resp.Data = result

	case "forensics_enrich":
		var req ForensicsEnrichRequest
		if err := json.Unmarshal(cmd.Data, &req); err != nil {
			resp.Error = fmt.Sprintf("invalid forensics_enrich payload: %v", err)
			return resp
		}
		result, err := h.HandleForensicsEnrich(ctx, req)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		resp.Data = result

	case "forensics_activity":
		var req ForensicsActivityRequest
		if err := json.Unmarshal(cmd.Data, &req); err != nil {
			resp.Error = fmt.Sprintf("invalid forensics_activity payload: %v", err)
			return resp
		}
		result, err := h.HandleForensicsActivity(ctx, req)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		resp.Data = result

	case "forensics_users":
		// No payload beyond the envelope.
		result, err := h.HandleForensicsUsers(ctx)
		if err != nil {
			resp.Error = err.Error()
			return resp
		}
		resp.Data = result

	case "forensics_audit_log":
		var req ForensicsAuditLogRequest
		if err := json.Unmarshal(cmd.Data, &req); err != nil {
			resp.Error = fmt.Sprintf("invalid forensics_audit_log payload: %v", err)
			return resp
		}
		result, err := h.HandleForensicsAuditLog(ctx, req)
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
