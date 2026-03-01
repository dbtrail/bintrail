// Package reconstruct implements row state reconstruction by combining baseline
// Parquet snapshots with indexed binlog change events.
//
// The core idea: a baseline Parquet file captures the full row state at a point
// in time (produced by "bintrail baseline"). Binlog events captured after that
// snapshot each carry a full row_after image. Applying them in order up to a
// target timestamp yields the exact row state at that moment.
package reconstruct

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/bintrail/bintrail/internal/parser"
	"github.com/bintrail/bintrail/internal/query"
)

// StateEntry is the row state at a single point in time.
type StateEntry struct {
	Time    time.Time      // when this state was established
	Source  string         // "baseline" | "INSERT" | "UPDATE" | "DELETE"
	EventID uint64         // 0 for the baseline entry
	GTID    string         // empty when not applicable
	State   map[string]any // nil means the row was deleted
}

// ApplyAt applies binlog events to initialState up to and including at,
// returning the final row state. Returns nil if the row was deleted.
// Events must be sorted by (event_timestamp, event_id).
func ApplyAt(initialState map[string]any, events []query.ResultRow, at time.Time) map[string]any {
	state := copyMap(initialState)
	for _, ev := range events {
		if ev.EventTimestamp.After(at) {
			break
		}
		state = applyEvent(state, ev)
	}
	return state
}

// BuildHistory applies events chronologically up to at and returns a StateEntry
// for each state transition, starting with the baseline as the first entry.
// Events must be sorted by (event_timestamp, event_id).
func BuildHistory(initialState map[string]any, baselineTime time.Time, events []query.ResultRow, at time.Time) []StateEntry {
	entries := []StateEntry{{
		Time:   baselineTime,
		Source: "baseline",
		State:  copyMap(initialState),
	}}
	current := copyMap(initialState)
	for _, ev := range events {
		if ev.EventTimestamp.After(at) {
			break
		}
		current = applyEvent(current, ev)
		gtid := ""
		if ev.GTID != nil {
			gtid = *ev.GTID
		}
		entries = append(entries, StateEntry{
			Time:    ev.EventTimestamp,
			Source:  eventTypeName(ev.EventType),
			EventID: ev.EventID,
			GTID:    gtid,
			State:   copyMap(current),
		})
	}
	return entries
}

// ─── Formatters ───────────────────────────────────────────────────────────────

// WriteStateJSON writes a single row state as indented JSON.
func WriteStateJSON(state map[string]any, w io.Writer) error {
	if state == nil {
		_, err := fmt.Fprintln(w, "null")
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(state)
}

// WriteHistoryJSON writes history entries as an indented JSON array.
func WriteHistoryJSON(entries []StateEntry, w io.Writer) error {
	type jsonEntry struct {
		Time    string         `json:"time"`
		Source  string         `json:"source"`
		EventID uint64         `json:"event_id,omitempty"`
		GTID    string         `json:"gtid,omitempty"`
		State   map[string]any `json:"state"`
	}
	out := make([]jsonEntry, len(entries))
	for i, e := range entries {
		out[i] = jsonEntry{
			Time:    e.Time.UTC().Format(time.RFC3339),
			Source:  e.Source,
			EventID: e.EventID,
			GTID:    e.GTID,
			State:   e.State,
		}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// WriteStateTable writes a single row state as aligned key-value pairs.
func WriteStateTable(state map[string]any, w io.Writer) error {
	if state == nil {
		_, err := fmt.Fprintln(w, "(row deleted)")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "COLUMN\tVALUE")
	fmt.Fprintln(tw, "──────\t─────")
	for _, col := range sortedKeys(state) {
		fmt.Fprintf(tw, "%s\t%v\n", col, state[col])
	}
	return nil
}

// WriteHistoryTable writes history entries as a tabwriter table.
// The STATE column renders as compact "key=value" pairs.
func WriteHistoryTable(entries []StateEntry, w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "TIMESTAMP\tSOURCE\tEVENT_ID\tGTID\tSTATE")
	fmt.Fprintln(tw, "─────────────────────\t──────\t────────\t────\t─────")
	for _, e := range entries {
		eid := "-"
		if e.EventID > 0 {
			eid = fmt.Sprintf("%d", e.EventID)
		}
		gtid := "-"
		if e.GTID != "" {
			gtid = e.GTID
		}
		stateStr := "(deleted)"
		if e.State != nil {
			var parts []string
			for _, col := range sortedKeys(e.State) {
				parts = append(parts, fmt.Sprintf("%s=%v", col, e.State[col]))
			}
			stateStr = strings.Join(parts, " ")
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			e.Time.UTC().Format("2006-01-02 15:04:05"),
			e.Source, eid, gtid, stateStr)
	}
	return nil
}

// WriteStateCSV writes a single row state as CSV (header + one data row).
func WriteStateCSV(state map[string]any, w io.Writer) error {
	cw := csv.NewWriter(w)
	if state == nil {
		if err := cw.Write([]string{"(deleted)"}); err != nil {
			return err
		}
		cw.Flush()
		return cw.Error()
	}
	keys := sortedKeys(state)
	if err := cw.Write(keys); err != nil {
		return err
	}
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = fmt.Sprintf("%v", state[k])
	}
	if err := cw.Write(vals); err != nil {
		return err
	}
	cw.Flush()
	return cw.Error()
}

// WriteHistoryCSV writes history entries as CSV. Columns are: time, source,
// event_id, gtid, followed by all state columns (union of all entries, sorted).
func WriteHistoryCSV(entries []StateEntry, w io.Writer) error {
	// Collect all unique state columns across every entry.
	colSet := make(map[string]struct{})
	for _, e := range entries {
		for col := range e.State {
			colSet[col] = struct{}{}
		}
	}
	stateCols := sortedKeysBool(colSet)
	headers := append([]string{"time", "source", "event_id", "gtid"}, stateCols...)

	cw := csv.NewWriter(w)
	if err := cw.Write(headers); err != nil {
		return err
	}
	for _, e := range entries {
		record := []string{
			e.Time.UTC().Format(time.RFC3339),
			e.Source,
			fmt.Sprintf("%d", e.EventID),
			e.GTID,
		}
		for _, col := range stateCols {
			if e.State == nil {
				record = append(record, "")
			} else {
				record = append(record, fmt.Sprintf("%v", e.State[col]))
			}
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// ─── SQL result formatters ────────────────────────────────────────────────────

// WriteSQLResultsJSON writes dynamic DuckDB SQL result rows as an indented JSON array.
func WriteSQLResultsJSON(rows []map[string]any, w io.Writer) error {
	if len(rows) == 0 {
		_, err := fmt.Fprintln(w, "[]")
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

// WriteSQLResultsTable writes dynamic DuckDB SQL results as an aligned table.
func WriteSQLResultsTable(rows []map[string]any, cols []string, w io.Writer) error {
	if len(rows) == 0 {
		_, err := fmt.Fprintln(w, "No results.")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	defer tw.Flush()
	headers := make([]string, len(cols))
	for i, c := range cols {
		headers[i] = strings.ToUpper(c)
	}
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	seps := make([]string, len(cols))
	for i, c := range cols {
		seps[i] = strings.Repeat("─", len(c))
	}
	fmt.Fprintln(tw, strings.Join(seps, "\t"))
	for _, row := range rows {
		vals := make([]string, len(cols))
		for i, col := range cols {
			vals[i] = fmt.Sprintf("%v", row[col])
		}
		fmt.Fprintln(tw, strings.Join(vals, "\t"))
	}
	return nil
}

// WriteSQLResultsCSV writes dynamic DuckDB SQL results as CSV.
func WriteSQLResultsCSV(rows []map[string]any, cols []string, w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(cols); err != nil {
		return err
	}
	for _, row := range rows {
		record := make([]string, len(cols))
		for i, col := range cols {
			record[i] = fmt.Sprintf("%v", row[col])
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// ─── internal helpers ─────────────────────────────────────────────────────────

func applyEvent(state map[string]any, ev query.ResultRow) map[string]any {
	switch ev.EventType {
	case parser.EventInsert:
		return copyMap(ev.RowAfter)
	case parser.EventUpdate:
		return copyMap(ev.RowAfter)
	case parser.EventDelete:
		return nil
	default:
		return state
	}
}

func copyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

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

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func sortedKeysBool(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
