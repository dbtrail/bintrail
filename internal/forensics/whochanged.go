package forensics

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Confidence grades how strongly an attribution is supported by its source.
type Confidence string

// Confidence tiers (epic #701): exact means the identity join is positively
// bounded (an audit-log CONNECT..DISCONNECT lifetime containing the
// event); corroborated means the identity matched but its
// session lifetime could not be verified (nearest audit record without
// brackets, the connection_cache, or a live performance_schema session — which
// reflects the current holder of the connection id, and that id may have been
// reused since the event); heuristic means we had to choose between multiple
// plausible identities (e.g. two audit lifetimes abutting within the log's
// one-second granularity).
const (
	ConfidenceExact        Confidence = "exact"
	ConfidenceCorroborated Confidence = "corroborated"
	ConfidenceHeuristic    Confidence = "heuristic"
)

// Attribution sources, from most to least durable.
const (
	AttributionSourceAuditLog   = "audit_log"
	AttributionSourcePerfSchema = "performance_schema"
	AttributionSourceConnCache  = "connection_cache"
)

// Attribution is the resolved "who" for one binlog event.
type Attribution struct {
	User          string `json:"user"`
	Host          string `json:"host,omitempty"`
	ClientProgram string `json:"client_program,omitempty"`
	// AuditSQL is the SQL text of the audit record that matched, when the
	// match came from a nearest-record fallback (the record is then itself
	// evidence worth showing). Bracketed CONNECT matches carry no SQL.
	AuditSQL   string     `json:"audit_sql,omitempty"`
	Source     string     `json:"source"`
	Confidence Confidence `json:"confidence"`
}

// WhoChangedEvent is one binlog change with its attribution (nil when no
// source could resolve the session — binlog-only degradation, never an error).
type WhoChangedEvent struct {
	EventID      uint64    `json:"event_id"`
	Timestamp    time.Time `json:"timestamp"`
	EventType    string    `json:"event_type"`
	Schema       string    `json:"schema"`
	Table        string    `json:"table"`
	PKValues     string    `json:"pk_values"`
	GTID         *string   `json:"gtid,omitempty"`
	ConnectionID *uint32   `json:"connection_id,omitempty"`
	// QueryText is the original SQL statement captured per row event when the
	// source logs ROWS_QUERY/ANNOTATE events (#699/#712) — the highest-fidelity
	// "what" next to the "who", no join needed.
	QueryText   *string      `json:"query_text,omitempty"`
	Attribution *Attribution `json:"attribution,omitempty"`
}

// WhoChangedResult is the outcome of a who-changed correlation.
type WhoChangedResult struct {
	Events     []WhoChangedEvent `json:"events"`
	TotalCount int               `json:"total_count"`
	// AppliedDefaultWindow is true when no time bounds were given and the
	// engine bounded the search to the last 24 hours (partition-pruning perf
	// + honest empty-results UX — an empty result under a silent default
	// window would read as "nothing happened ever").
	AppliedDefaultWindow bool `json:"applied_default_window,omitempty"`
	// Notes carries the structured caveats for this response — each relevant
	// note exactly once, never as log lines.
	Notes           []string        `json:"notes,omitempty"`
	FallbackQueries []FallbackQuery `json:"fallback_queries,omitempty"`
}

// WhoChangedParams filters which binlog events to attribute.
type WhoChangedParams struct {
	Schema string // required
	Table  string // required
	// PK restricts to a single row (pipe-delimited for composite PKs).
	PK    string
	Since *time.Time
	Until *time.Time
	Limit int    // <= 0 defaults to 100
	Order string // "DESC" for newest first; anything else is ascending
}

// WhoChangedDeps carries the engine's data sources. Fetch is a seam rather
// than a *sql.DB so the CLI can wire query.FetchMerged (live index + archive
// auto-discovery) while unit tests drive the cascade with fixtures.
type WhoChangedDeps struct {
	// Fetch returns binlog events matching opts. Required.
	Fetch func(ctx context.Context, opts query.Options) ([]query.ResultRow, error)
	// SourceDB is the watched MySQL server, used for the audit-log and live
	// performance_schema tiers. nil skips those tiers (index-only mode).
	SourceDB *sql.DB
	// SourceHost is the resolved host[:port] of the source server (from the
	// source DSN). It lets the audit-log tier reach the RDS/Aurora audit source
	// (the RDS file API) when the server's audit log is not on the local
	// filesystem — without it, AuditSourceAuto never leaves the local-file path
	// and the RDS/Aurora audit tier is silently never tried. (The who-changed
	// path always runs auto mode, which never reaches CloudWatch; that source is
	// only selected by an explicit Source="cloudwatch" on the agent request.)
	// Empty => local-file audit reads only.
	SourceHost string
	// IndexDB is the bintrail index database, used for the connection_cache
	// tier (#703). nil skips that tier.
	IndexDB *sql.DB
}

const (
	whoChangedDefaultLimit = 100
	// whoChangedDefaultWindow bounds an unbounded who-changed to recent
	// history (see WhoChangedResult.AppliedDefaultWindow).
	whoChangedDefaultWindow = 24 * time.Hour
	// auditWindowPad widens the audit-log read window past the event span so
	// records straddling the first/last event are not lost to timestamp
	// granularity (same ±5s the SaaS used).
	auditWindowPad = 5 * time.Second
)

// The standard caveats (epic #701). They are structured notes on the result —
// filtered by relevance, each at most once — because an operator acting on an
// attribution must see them even when logs are discarded.
const (
	notePooler = "If connections pass through a pooler or proxy (ProxySQL, RDS Proxy), " +
		"many application users share one backend session — the attributed user@host " +
		"is then the pool's backend account, not the end user."
	noteReplica = "If this index was captured from a replica's binlog, connection ids " +
		"belong to the replica's applier, not the original client session — run " +
		"who-changed against the primary's index for client attribution."
	noteSpoof = "pseudo_thread_id can be set by privileged sessions, so a binlog " +
		"connection id is corroborating evidence, not proof of identity."
	noteInvestigate = "Use `bintrail user-activity` / `bintrail connection-history` " +
		"to investigate further."
)

// WhoChanged fetches the binlog events matching params and attributes each to
// a database session through the tier cascade: audit log (lifetime-bounded),
// live performance_schema, connection_cache, and
// finally binlog-only with an explanatory note — degradation is an answer,
// never an error. Only parameter validation and the binlog fetch itself can
// fail; every attribution source degrades to the next tier.
func WhoChanged(ctx context.Context, deps WhoChangedDeps, params WhoChangedParams) (WhoChangedResult, error) {
	if params.Schema == "" || params.Table == "" {
		return WhoChangedResult{}, fmt.Errorf("schema and table are required")
	}
	if deps.Fetch == nil {
		return WhoChangedResult{}, fmt.Errorf("WhoChanged: deps.Fetch is required")
	}
	limit := params.Limit
	if limit <= 0 {
		limit = whoChangedDefaultLimit
	}

	// Default window: 24h when unbounded (#1272 in the SaaS): enables
	// partition pruning on binlog_events and keeps empty results honest.
	since, until := params.Since, params.Until
	appliedDefault := false
	if since == nil && until == nil {
		now := time.Now().UTC()
		s := now.Add(-whoChangedDefaultWindow)
		since, until = &s, &now
		appliedDefault = true
	}

	rows, err := deps.Fetch(ctx, query.Options{
		Schema:   params.Schema,
		Table:    params.Table,
		PKValues: params.PK,
		Since:    since,
		Until:    until,
		Limit:    limit,
		Order:    params.Order,
	})
	if err != nil {
		return WhoChangedResult{}, fmt.Errorf("fetch binlog events: %w", err)
	}

	attr := map[int]Attribution{}
	var tierNotes []string
	if len(rows) > 0 {
		attr, tierNotes = attributeEvents(ctx, deps, rows)
	}
	return assembleResult(rows, attr, appliedDefault, limit, tierNotes), nil
}

// assembleResult builds the response from the fetched events and their
// attributions (keyed by index into rows), including the relevance-filtered
// note set. tierNotes carries source-degradation notes from attributeEvents —
// a tier that FAILED must read differently from a tier that was consulted and
// had no data, because a JSON consumer sees only this payload, never the
// logs. Split from WhoChanged so the note/assembly rules are unit-testable
// without any database.
func assembleResult(rows []query.ResultRow, attr map[int]Attribution, appliedDefault bool, limit int, tierNotes []string) WhoChangedResult {
	res := WhoChangedResult{
		Events:               []WhoChangedEvent{},
		TotalCount:           len(rows),
		AppliedDefaultWindow: appliedDefault,
	}
	if len(rows) == 0 {
		if appliedDefault {
			res.Notes = append(res.Notes,
				"No change events found in the last 24 hours (default window). "+
					"Pass an explicit since bound to search further back.")
		} else {
			res.Notes = append(res.Notes, "No change events found in the specified time range.")
		}
		return res
	}

	// Assemble the response and the relevance-filtered note set.
	attributed, withConnID := 0, 0
	unresolvedIDs := map[int64]struct{}{}
	for i := range rows {
		ev := WhoChangedEvent{
			EventID:      rows[i].EventID,
			Timestamp:    rows[i].EventTimestamp,
			EventType:    eventTypeName(rows[i].EventType),
			Schema:       rows[i].SchemaName,
			Table:        rows[i].TableName,
			PKValues:     rows[i].PKValues,
			GTID:         rows[i].GTID,
			ConnectionID: rows[i].ConnectionID,
			QueryText:    rows[i].QueryText,
		}
		if a, ok := attr[i]; ok {
			ac := a
			ev.Attribution = &ac
			attributed++
		} else if rows[i].ConnectionID != nil {
			unresolvedIDs[int64(*rows[i].ConnectionID)] = struct{}{}
		}
		if rows[i].ConnectionID != nil {
			withConnID++
		}
		res.Events = append(res.Events, ev)
	}

	if appliedDefault {
		res.Notes = append(res.Notes,
			"Results are limited to the last 24 hours (default window). "+
				"Pass an explicit since bound to search further back.")
	}
	if limit > 0 && len(rows) >= limit {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"Results were truncated at the limit of %d events; more matching changes may exist. "+
				"Narrow the time range or raise the limit.", limit))
	}
	res.Notes = append(res.Notes, tierNotes...)
	if attributed > 0 {
		res.Notes = append(res.Notes, notePooler, noteReplica, noteSpoof)
	}
	if unattributed := len(rows) - attributed; unattributed > 0 {
		switch {
		case withConnID == 0:
			res.Notes = append(res.Notes,
				"The unattributed event(s) carry no connection id (indexed before "+
					"connection-id capture, or the source does not log it), so no "+
					"session attribution is possible for them. "+noteInvestigate)
		default:
			res.Notes = append(res.Notes, fmt.Sprintf(
				"%d of %d event(s) could not be attributed to a session: the event carries "+
					"no connection id, or its connection id matched no identity in the "+
					"attribution sources that were consulted (see any source notes above). %s",
				unattributed, len(rows), noteInvestigate))
		}
	}

	if len(unresolvedIDs) > 0 {
		ids := make([]int64, 0, len(unresolvedIDs))
		for id := range unresolvedIDs {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		res.FallbackQueries = generateThreadFallbackQueries(ids)
	}
	return res
}

// attributeEvents runs the tier cascade and returns attributions keyed by
// index into rows, plus source-degradation notes for the response. Keying by
// event — not by connection id — is deliberate: connection ids are reused, so
// two events with the same id can belong to different identities (the
// misattribution the audit tier's lifetime bounding exists to prevent).
//
// A tier that fails is noted, not just logged: without the note, a failed
// tier is indistinguishable from a tier that was consulted and had no data,
// and the response would affirm "no coverage" for sources that were never
// actually checked.
func attributeEvents(ctx context.Context, deps WhoChangedDeps, rows []query.ResultRow) (map[int]Attribution, []string) {
	attr := map[int]Attribution{}
	var notes []string

	// ── Tier 1: audit log — durable identity, works after the fact ──────────
	if deps.SourceDB != nil {
		caps, err := DetectCapabilities(ctx, deps.SourceDB)
		if err != nil {
			slog.Warn("who-changed: source unreachable; skipping audit/performance_schema tiers",
				"error", err)
			notes = append(notes, "The source server was unreachable, so the audit-log "+
				"and performance_schema attribution sources were NOT consulted; only "+
				"index-side sources ran.")
			deps.SourceDB = nil // local copy: disable the source tiers below
		} else if caps.AuditLog.Installed {
			auditRes, aerr := ReadAuditLog(ctx, deps.SourceDB, auditReadOptionsFor(rows, deps.SourceHost))
			switch {
			case aerr != nil:
				// Not configured / file on another host / unknown format —
				// all legitimate; the next tiers take over. Noted so the
				// response doesn't claim the audit log had no coverage.
				slog.Info("who-changed: audit log tier unavailable", "error", aerr)
				notes = append(notes, fmt.Sprintf(
					"An audit plugin is active on the source but its log could not be "+
						"read (%v); the audit-log source was NOT consulted.", aerr))
			default:
				// The read stopping exactly at the cap means an unknown suffix
				// of in-window records was dropped: interval bounds touching
				// the missing region cannot be trusted (a dropped DISCONNECT +
				// re-CONNECT would otherwise misattribute later events as
				// exact — the very bug lifetime bounding exists to prevent).
				truncated := len(auditRes.Events) >= auditMaxLimit
				if truncated {
					notes = append(notes, fmt.Sprintf(
						"The audit log read hit its %d-record cap for this window, so "+
							"session lifetimes may be incomplete; audit matches whose "+
							"lifetime is not fully bracketed were downgraded to corroborated.",
						auditMaxLimit))
				}
				if len(auditRes.Warnings) > 0 {
					for _, w := range auditRes.Warnings {
						slog.Warn("who-changed: audit log read warning", "warning", w)
					}
					notes = append(notes, fmt.Sprintf(
						"The audit log read reported %d warning(s) (first: %s); "+
							"audit-tier attribution may be incomplete.",
						len(auditRes.Warnings), auditRes.Warnings[0]))
				}
				for i, a := range attributeFromAudit(rows, auditRes.Events, truncated) {
					attr[i] = a
				}
			}
		}
	}

	// ── Tiers 2a/2b: live performance_schema, then connection_cache ─────────
	// Both key on connection id; they apply to every still-unresolved event
	// carrying that id. Live wins over cache (fresher and positively current).
	unresolved := unresolvedConnIDs(rows, attr)
	if len(unresolved) > 0 && deps.SourceDB != nil {
		live, lerr := lookupLiveThreads(ctx, deps.SourceDB, unresolved)
		if lerr != nil {
			slog.Warn("who-changed: live performance_schema tier unavailable", "error", lerr)
			notes = append(notes, "performance_schema on the source could not be queried "+
				"(missing SELECT/PROCESS grants?); the live-session source was NOT consulted.")
		}
		applyConnAttributions(rows, attr, live)
		unresolved = unresolvedConnIDs(rows, attr)
	}
	if len(unresolved) > 0 && deps.IndexDB != nil {
		cached, err := LookupCachedThreads(ctx, deps.IndexDB, unresolved)
		if err != nil {
			// Pre-#703 index schemas have no connection_cache table; the
			// cascade degrades to binlog-only rather than failing the call.
			slog.Warn("who-changed: connection_cache tier unavailable", "error", err)
			notes = append(notes, "The connection_cache table could not be queried (index "+
				"created before bintrail's attribution capture?); the cached-identity "+
				"source was NOT consulted.")
		} else {
			byConn := map[int64]Attribution{}
			for id, ct := range cached {
				if ct.User == "" {
					continue
				}
				byConn[id] = Attribution{
					User:          ct.User,
					Host:          ct.Host,
					ClientProgram: ct.ConnAttrs["program_name"],
					Source:        AttributionSourceConnCache,
					Confidence:    ConfidenceCorroborated,
				}
			}
			applyConnAttributions(rows, attr, byConn)
		}
	}
	return attr, notes
}

// auditReadOptionsFor builds the audit-log read for the who-changed tier: the
// window is the events' timestamp span padded by auditWindowPad on both
// sides, rotated files are included (a 24h default window routinely spans a
// rotation), and TailLines forces a full scan — the recent-history tail
// auto-mode would silently read only the last ~2.5MB of each file, dropping
// the CONNECT/DISCONNECT brackets of older events with no signal. The parsers
// push the time filter down and cap memory on matched events only, so a full
// scan costs a linear read, not unbounded memory.
//
// Known limit (inherited from the SaaS): audit timestamps without a zone
// (MariaDB family writes server-local time) are parsed as UTC, so on a server
// whose local time is not UTC the audit window and interval matching skew by
// the zone offset. Percona/Enterprise JSON timestamps carry Z and are exact.
//
// sourceHost is the source server's host[:port]; passing it through as
// AuditReadOptions.SourceHost lets AuditSourceAuto fall back to the RDS file API
// for a managed RDS/Aurora source whose audit log is not on the local
// filesystem. Empty => local-file only. (Auto mode never reaches CloudWatch —
// that source requires an explicit Source="cloudwatch".)
func auditReadOptionsFor(rows []query.ResultRow, sourceHost string) AuditReadOptions {
	minTS, maxTS := rows[0].EventTimestamp, rows[0].EventTimestamp
	for _, r := range rows[1:] {
		if r.EventTimestamp.Before(minTS) {
			minTS = r.EventTimestamp
		}
		if r.EventTimestamp.After(maxTS) {
			maxTS = r.EventTimestamp
		}
	}
	return AuditReadOptions{
		Since:          minTS.Add(-auditWindowPad),
		Until:          maxTS.Add(auditWindowPad),
		Limit:          auditMaxLimit,
		SourceHost:     sourceHost,
		IncludeRotated: true,
		TailLines:      -1,
	}
}

// unresolvedConnIDs returns the distinct connection ids of events not yet
// attributed, sorted for deterministic downstream queries.
func unresolvedConnIDs(rows []query.ResultRow, attr map[int]Attribution) []int64 {
	set := map[int64]struct{}{}
	for i := range rows {
		if _, done := attr[i]; done {
			continue
		}
		if rows[i].ConnectionID != nil {
			set[int64(*rows[i].ConnectionID)] = struct{}{}
		}
	}
	ids := make([]int64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// applyConnAttributions assigns a per-connection-id attribution to every
// still-unresolved event carrying that id.
func applyConnAttributions(rows []query.ResultRow, attr map[int]Attribution, byConn map[int64]Attribution) {
	if len(byConn) == 0 {
		return
	}
	for i := range rows {
		if _, done := attr[i]; done {
			continue
		}
		if rows[i].ConnectionID == nil {
			continue
		}
		if a, ok := byConn[int64(*rows[i].ConnectionID)]; ok {
			attr[i] = a
		}
	}
}

// lookupLiveThreads resolves connection ids against live performance_schema
// via EnrichThreads, chunking to its batch cap. On error it returns whatever
// it resolved so far together with the error — the caller degrades to the
// cache tier and notes the failure.
func lookupLiveThreads(ctx context.Context, sourceDB *sql.DB, ids []int64) (map[int64]Attribution, error) {
	out := map[int64]Attribution{}
	for start := 0; start < len(ids); start += maxEnrichThreadIDs {
		chunk := ids[start:min(start+maxEnrichThreadIDs, len(ids))]
		enriched, err := EnrichThreads(ctx, sourceDB, chunk)
		if err != nil {
			return out, err
		}
		for _, ti := range enriched.Threads {
			if ti == nil || ti.User == "" {
				// session_connect_attrs-only entries carry no identity.
				continue
			}
			out[ti.ConnectionID] = Attribution{
				User:          ti.User,
				Host:          ti.Host,
				ClientProgram: ti.ConnAttrs["program_name"],
				Source:        AttributionSourcePerfSchema,
				// Corroborated, not exact: this is the CURRENT holder of the
				// connection id, whose lifetime is not bounded against the
				// event. A reused id (or COM_CHANGE_USER) can make the live
				// session a different actor than the one that ran the event —
				// the exact tier (audit CONNECT..DISCONNECT) is the one
				// that positively bounds identity to the event.
				Confidence: ConfidenceCorroborated,
			}
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Tier 1: audit-log attribution with CONNECT..DISCONNECT lifetime bounding
// ---------------------------------------------------------------------------

// auditRecordKind classifies an audit record for interval building.
type auditRecordKind int

const (
	kindOther auditRecordKind = iota // QUERY and friends — identity evidence
	kindConnect
	kindDisconnect
)

// classifyAuditEventType maps a vendor event-type string to a record kind.
// Vocabulary across the parser family: MariaDB/RDS/Aurora emit CONNECT /
// DISCONNECT / FAILED_CONNECT / QUERY; Percona CSV and MySQL Enterprise XML
// emit Connect / Quit / Query; the JSON dialects emit class/event composites
// like "connection/connect" and "connection/disconnect". Composites classify
// on the event half so "connection/change_user" is not mistaken for a
// connect. FAILED_CONNECT opens no session and stays kindOther.
func classifyAuditEventType(eventType string) auditRecordKind {
	s := strings.ToLower(eventType)
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	switch {
	case strings.Contains(s, "failed"):
		return kindOther
	case strings.Contains(s, "disconnect") || s == "quit":
		return kindDisconnect
	case strings.Contains(s, "connect"):
		return kindConnect
	default:
		return kindOther
	}
}

// auditIdentityRecord is one parsed audit record relevant to a connection id.
type auditIdentityRecord struct {
	ts      time.Time
	kind    auditRecordKind
	user    string
	host    string
	sqlText string
}

// identityInterval is one CONNECT..DISCONNECT lifetime of a connection id. A
// zero start means the log never showed the CONNECT (truncation — unbounded
// below); a zero end means the session was still connected at the end of the
// log (unbounded above).
type identityInterval struct {
	start, end time.Time
	user, host string
}

// contains reports whether ts falls inside the interval, inclusive on both
// ends (audit and binlog timestamps share one-second granularity).
func (iv identityInterval) contains(ts time.Time) bool {
	if !iv.start.IsZero() && ts.Before(iv.start) {
		return false
	}
	if !iv.end.IsZero() && ts.After(iv.end) {
		return false
	}
	return true
}

// buildIdentityIntervals folds the audit records into per-connection-id
// lifetime intervals plus the remaining identity-bearing records (QUERY et
// al.) used by the nearest-record fallback. Records without a parseable
// timestamp or a connection id are skipped.
func buildIdentityIntervals(auditEvents []AuditEvent) (map[int64][]identityInterval, map[int64][]auditIdentityRecord) {
	recs := map[int64][]auditIdentityRecord{}
	for i := range auditEvents {
		ae := &auditEvents[i]
		if ae.ConnectionID == 0 {
			continue
		}
		ts, err := parseFlexTimestamp(ae.Timestamp)
		if err != nil {
			continue
		}
		recs[ae.ConnectionID] = append(recs[ae.ConnectionID], auditIdentityRecord{
			ts:      ts,
			kind:    classifyAuditEventType(ae.EventType),
			user:    ae.User,
			host:    ae.Host,
			sqlText: ae.SQLText,
		})
	}

	intervals := map[int64][]identityInterval{}
	evidence := map[int64][]auditIdentityRecord{}
	for id, rs := range recs {
		// Stable sort: equal timestamps keep log order, which is the only
		// ordering signal the one-second granularity leaves us.
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].ts.Before(rs[j].ts) })

		var ivs []identityInterval
		var open *identityInterval
		for _, r := range rs {
			switch r.kind {
			case kindConnect:
				if open != nil {
					// A second CONNECT on the same id implies the previous
					// session ended unlogged; close it at the new start.
					open.end = r.ts
					ivs = append(ivs, *open)
				}
				open = &identityInterval{start: r.ts, user: r.user, host: r.host}
			case kindDisconnect:
				if open != nil {
					open.end = r.ts
					if open.user == "" {
						open.user, open.host = r.user, r.host
					}
					ivs = append(ivs, *open)
					open = nil
				} else if r.user != "" {
					// DISCONNECT whose CONNECT predates the log window:
					// identity holds for everything up to the disconnect.
					ivs = append(ivs, identityInterval{end: r.ts, user: r.user, host: r.host})
				}
			default:
				if r.user != "" {
					evidence[id] = append(evidence[id], r)
				}
			}
		}
		if open != nil {
			ivs = append(ivs, *open) // still connected at end of log
		}
		if len(ivs) > 0 {
			intervals[id] = ivs
		}
	}
	return intervals, evidence
}

// attributeFromAudit attributes events to identities from audit-log records,
// keyed by index into events.
//
// Required improvement over the SaaS (forensics.py:271-285): the SaaS did
// first-match-wins on connection_id inside a ±5s window, which misattributes
// when a connection id is reused (pool churn). Here each id's identity is
// bounded by its CONNECT..DISCONNECT lifetime taken from the audit log
// itself, so an event only inherits an identity whose session actually
// contained it:
//
//   - exactly one lifetime contains the event      → that identity, exact;
//   - several contain it (same-second boundary)    → latest-starting one, heuristic;
//   - lifetimes known but none contains the event  → unresolved (next tier) —
//     the audit log positively says none of the known sessions covers it;
//   - no lifetime known for the id (log truncation) → nearest identity-bearing
//     record for the id, corroborated (its lifetime is unverified).
//
// truncated means the audit read stopped at its record cap, so an unknown
// suffix of in-window records is missing. A fully-bounded lifetime is still
// exact (both its endpoints are real records, and sessions on one id are
// serial — nothing dropped can fit between them), but a lifetime with an
// unbounded side may only look unbounded because its closing record was
// dropped, so matches against it are downgraded to corroborated.
func attributeFromAudit(events []query.ResultRow, auditEvents []AuditEvent, truncated bool) map[int]Attribution {
	out := map[int]Attribution{}
	if len(events) == 0 || len(auditEvents) == 0 {
		return out
	}
	intervals, evidence := buildIdentityIntervals(auditEvents)

	for i := range events {
		if events[i].ConnectionID == nil {
			continue
		}
		cid := int64(*events[i].ConnectionID)
		ts := events[i].EventTimestamp

		var bracketed []identityInterval // identity-bearing lifetimes for this id
		for _, iv := range intervals[cid] {
			if iv.user != "" {
				bracketed = append(bracketed, iv)
			}
		}

		if len(bracketed) > 0 {
			var candidates []identityInterval
			for _, iv := range bracketed {
				if iv.contains(ts) {
					candidates = append(candidates, iv)
				}
			}
			switch {
			case len(candidates) == 1:
				confidence := ConfidenceExact
				if truncated && (candidates[0].start.IsZero() || candidates[0].end.IsZero()) {
					confidence = ConfidenceCorroborated
				}
				out[i] = Attribution{
					User: candidates[0].user, Host: candidates[0].host,
					Source: AttributionSourceAuditLog, Confidence: confidence,
				}
			case len(candidates) > 1:
				// Two lifetimes abut inside the log's one-second granularity.
				// The latest-starting one owned the id at that instant in any
				// serial reuse — but it is a guess, so grade it as one.
				best := candidates[0]
				for _, iv := range candidates[1:] {
					if iv.start.After(best.start) {
						best = iv
					}
				}
				out[i] = Attribution{
					User: best.user, Host: best.host,
					Source: AttributionSourceAuditLog, Confidence: ConfidenceHeuristic,
				}
			}
			// candidates == 0: known lifetimes exclude this event — leave it
			// for the next tier rather than guess against the log.
			continue
		}

		// No brackets for this id (log truncation / long-lived session whose
		// CONNECT predates the window): nearest identity-bearing record.
		if evs := evidence[cid]; len(evs) > 0 {
			best := evs[0]
			bestGap := absDuration(ts.Sub(best.ts))
			for _, r := range evs[1:] {
				if gap := absDuration(ts.Sub(r.ts)); gap < bestGap {
					best, bestGap = r, gap
				}
			}
			out[i] = Attribution{
				User: best.user, Host: best.host, AuditSQL: best.sqlText,
				Source: AttributionSourceAuditLog, Confidence: ConfidenceCorroborated,
			}
		}
	}
	return out
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// eventTypeName renders an event type for who-changed output using the same
// INSERT/UPDATE/DELETE names as the query formatters.
func eventTypeName(t event.EventType) string {
	switch t {
	case event.EventInsert:
		return "INSERT"
	case event.EventUpdate:
		return "UPDATE"
	case event.EventDelete:
		return "DELETE"
	case event.EventDDL:
		return "DDL"
	case event.EventSnapshot:
		return "SNAPSHOT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(t))
	}
}
