// Package parser — this file provides SkipCounters, the capture-time tally of
// events the daemon READ from the stream and chose to DROP (#1034): per-reason
// monotonic counters, persisted alongside the stream checkpoint so `bintrail
// status` can render a Capture health verdict that survives daemon restarts.
//
// This is the sibling of the #649 continuity verdict: continuity answers "did
// the stream LOSE events it never received?" (a binlog gap); these counters
// answer "did the stream DISCARD events it did receive?" (e.g. the column-count
// guard rejecting every row against a stale/corrupt snapshot). Both end the
// same way for the user — a restore window they believe exists and doesn't —
// but without these counters the second failure was invisible: the checkpoint
// stayed fresh and continuity honestly reported "no gaps" while 100% of rows
// were skipped.
package parser

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// Capture-skip reason identifiers. Stable strings: they are persisted verbatim
// in stream_state.capture_skips and surfaced by `bintrail status` (text and
// JSON), so renaming one silently splits its history into two counters.
const (
	// SkipColumnCountMismatch — the binlog TABLE_MAP column count diverged
	// from the schema snapshot; indexing would map values to wrong names.
	SkipColumnCountMismatch = "column_count_mismatch"
	// SkipTableNotInSnapshot — the row's table is absent from the schema
	// snapshot (system schemas the snapshot deliberately excludes are NOT
	// counted — e.g. mysql.rds_heartbeat2 is a routine, permanent skip that
	// must never mark capture degraded).
	SkipTableNotInSnapshot = "table_not_in_snapshot"
	// SkipNoResolver — no schema snapshot was available at all.
	SkipNoResolver = "no_resolver"
	// SkipUnhandledRowEvent — a RowsEvent type bintrail does not decode
	// (e.g. PARTIAL_UPDATE_ROWS_EVENT under binlog_row_value_options).
	SkipUnhandledRowEvent = "unhandled_row_event"
	// SkipStatementFormatDML — a STATEMENT/MIXED-format DML whose row image
	// is not in the binlog (#999); the change cannot be captured.
	SkipStatementFormatDML = "statement_format_dml"
)

// SkipEscalationThreshold is the number of CONSECUTIVE skipped events (no
// captured event in between) after which SkipCounters emits a single ERROR
// with remediation guidance — the per-event WARNs blend into noise exactly
// when they matter most (#1034 observed 100% skips for ~2 days). One ERROR
// per degraded episode: the flag re-arms only after an event is captured.
const SkipEscalationThreshold = 100

// SkipStat is one reason's monotonic tally. The JSON field names are the
// persistence format of stream_state.capture_skips — internal/status decodes
// the same shape independently (it deliberately does not import this package).
type SkipStat struct {
	Count  int64     `json:"count"`
	LastAt time.Time `json:"last_at"`
}

// SkipCounters aggregates capture-time skips by reason. Safe for concurrent
// use; every method is nil-receiver-safe so the file-index path (which has its
// own failure mechanism, the #778 gap tracker) can simply pass nil.
type SkipCounters struct {
	mu          sync.Mutex
	logger      *slog.Logger
	byReason    map[string]SkipStat
	consecutive int64
	escalated   bool
}

// NewSkipCounters creates an empty counter set. logger (nil = slog.Default())
// receives the single escalation ERROR when SkipEscalationThreshold
// consecutive events have been skipped.
func NewSkipCounters(logger *slog.Logger) *SkipCounters {
	if logger == nil {
		logger = slog.Default()
	}
	return &SkipCounters{logger: logger, byReason: map[string]SkipStat{}}
}

// Seed replaces the counters with a previously persisted Snapshot document, so
// a daemon restart resumes the monotonic tallies instead of silently zeroing
// the DEGRADED verdict. Empty raw is a no-op (nothing persisted yet); invalid
// JSON is returned as an error and leaves the counters unchanged.
func (c *SkipCounters) Seed(raw string) error {
	if c == nil || raw == "" {
		return nil
	}
	m := map[string]SkipStat{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byReason = m
	return nil
}

// RecordSkip notes one skipped event under reason, stamping the current time.
// When SkipEscalationThreshold consecutive events have been skipped it emits
// ONE ERROR with remediation (re-armed by the next RecordCaptured).
func (c *SkipCounters) RecordSkip(reason string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.byReason[reason]
	st.Count++
	st.LastAt = time.Now().UTC()
	c.byReason[reason] = st
	c.consecutive++
	if c.consecutive >= SkipEscalationThreshold && !c.escalated {
		c.escalated = true
		remediation := "the schema snapshot is likely stale or corrupt — run `bintrail snapshot` against the source, then restart the stream"
		if reason == SkipStatementFormatDML {
			remediation = "set binlog_format=ROW server-wide on the source (a STATEMENT/MIXED format or a session-level override is producing row-less events)"
		}
		c.logger.Error("sustained event skipping — capture is effectively stopped: every recent event was read from the stream and discarded, while the checkpoint keeps advancing past them; the skipped changes are NOT in the index",
			"consecutive_skips", c.consecutive,
			"reason", reason,
			"remediation", remediation)
	}
}

// RecordCaptured notes that an event cleared every guard and was emitted for
// indexing: the consecutive-skip run is broken and the escalation re-arms.
// The per-reason tallies are monotonic and deliberately NOT reset.
func (c *SkipCounters) RecordCaptured() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consecutive = 0
	c.escalated = false
}

// Snapshot returns the JSON document persisted to stream_state.capture_skips:
// {"<reason>":{"count":N,"last_at":"RFC3339"}}. Always valid JSON — "{}" when
// nothing was skipped, which is the affirmative evaluated-and-clean marker
// that lets `status` print "Capture health: OK" (a NULL column means no
// skip-aware daemon ever wrote, so OK must not be asserted).
func (c *SkipCounters) Snapshot() (string, error) {
	if c == nil {
		return "{}", nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := json.Marshal(c.byReason)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Total returns the sum of all per-reason counts (test/display helper).
func (c *SkipCounters) Total() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int64
	for _, st := range c.byReason {
		n += st.Count
	}
	return n
}
