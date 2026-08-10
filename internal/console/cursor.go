package console

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// Keyset paging cursors for GET /api/events (#1297).
//
// The Events view is newest-first and used to render one un-paged window: the
// header read "100 event(s) in the newest 100 events" and event 101 was
// unreachable without inventing a filter. Paging it needs a cursor, and the
// cursor is KEYSET rather than OFFSET — an OFFSET grows the work the engine
// does with how deep you have paged, re-scanning and re-sorting every skipped
// row, and on a merged live+archive read it would re-download the archives for
// every page. A keyset cut costs the same on page 40 as on page 1.
//
// A cursor is (event_timestamp, event_id) — the index's total sort order.
// event_id is not decoration: event_timestamp has one-second resolution and
// collides heavily under load, so a timestamp-only cursor either re-returns or
// skips every event sharing the boundary second.

// eventCursorSep separates the two components of the wire form. It cannot
// appear in either half (an RFC 3339 timestamp and a decimal id), so splitting
// on it is unambiguous.
const eventCursorSep = "|"

// formatEventCursor renders the position of a served row as the opaque token a
// client passes back as ?before= / ?after=.
//
// The timestamp is RFC 3339 with its offset rather than the DTO's bare
// "2006-01-02 15:04:05": a bare wall clock has to be re-attached to some
// location on the way back in, and guessing wrong shifts the cut by the
// connection's offset — which does not fail, it just silently returns the
// wrong page. Carrying the offset pins the instant, so the row this cursor
// names is the same row whatever location the index connection uses.
func formatEventCursor(r query.ResultRow) string {
	return r.EventTimestamp.Format(time.RFC3339Nano) + eventCursorSep + strconv.FormatUint(r.EventID, 10)
}

// parseEventCursor turns the wire form back into an engine cursor.
func parseEventCursor(param, s string) (*query.EventCursor, error) {
	ts, idStr, ok := strings.Cut(s, eventCursorSep)
	if !ok {
		return nil, fmt.Errorf("invalid %s cursor: expected <timestamp>%s<event_id>", param, eventCursorSep)
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return nil, fmt.Errorf("invalid %s cursor timestamp: %v", param, err)
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid %s cursor event id: %v", param, err)
	}
	return &query.EventCursor{Timestamp: t, EventID: id}, nil
}

// applyEventCursors parses the ?after= / ?before= params onto opts, rejecting
// the pairings the engine's keyset predicates cannot serve.
//
// The direction rule is checked HERE, ahead of the fetch, so a mismatch is a
// 400 the client can act on rather than the engine's validateCursor error
// arriving through writeFetchError as a 500. `before` pages a DESCENDING
// (newest-first) listing toward older events; `after` pages an ASCENDING one
// toward newer. Crossing them walks away from the unread remainder — the page
// would come back plausibly full and simply be the wrong half of the window,
// the failure mode worth a hard refusal.
func applyEventCursors(opts *query.Options, after, before string) error {
	if after == "" && before == "" {
		return nil
	}
	if after != "" && before != "" {
		return errors.New("pass either after or before, not both: they page in opposite directions")
	}
	desc := query.OrderDirection(opts.Order) == "DESC"
	if before != "" {
		if !desc {
			return errors.New("the before cursor pages a newest-first listing; it needs order=DESC")
		}
		c, err := parseEventCursor("before", before)
		if err != nil {
			return err
		}
		opts.BeforeEvent = c
		return nil
	}
	if desc {
		return errors.New("the after cursor pages an oldest-first listing; it needs order=ASC")
	}
	c, err := parseEventCursor("after", after)
	if err != nil {
		return err
	}
	opts.AfterEvent = c
	return nil
}
