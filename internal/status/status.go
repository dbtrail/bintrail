// Package status provides shared types and display helpers for the binlog index status.
// It is used by both cmd/bintrail/status.go and cmd/bintrail-mcp/main.go.
package status

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// IndexStateRow holds one row from the index_state table.
type IndexStateRow struct {
	BinlogFile    string
	Status        string
	EventsIndexed int64
	FileSize      int64
	LastPosition  int64
	StartedAt     time.Time
	CompletedAt   sql.NullTime
	ErrorMessage  sql.NullString
	BintrailID    sql.NullString
}

// PartitionStat holds one partition row from information_schema.PARTITIONS.
type PartitionStat struct {
	Name        string
	Description string // LESS THAN value (integer TO_SECONDS value) or "MAXVALUE"
	TableRows   int64  // estimate from information_schema
	Ordinal     int
}

// TSFmt is the timestamp format used in status output.
const TSFmt = "2006-01-02 15:04:05"

// LoadIndexState loads all rows from index_state ordered by bintrail_id, then started_at.
func LoadIndexState(ctx context.Context, db *sql.DB) ([]IndexStateRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT binlog_file, status, events_indexed, file_size, last_position,
		       started_at, completed_at, error_message, bintrail_id
		FROM index_state
		ORDER BY bintrail_id, started_at, binlog_file`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []IndexStateRow
	for rows.Next() {
		var r IndexStateRow
		if err := rows.Scan(
			&r.BinlogFile, &r.Status, &r.EventsIndexed, &r.FileSize, &r.LastPosition,
			&r.StartedAt, &r.CompletedAt, &r.ErrorMessage, &r.BintrailID,
		); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// LoadPartitionStats loads partition metadata for binlog_events from information_schema.
func LoadPartitionStats(ctx context.Context, db *sql.DB, dbName string) ([]PartitionStat, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, IFNULL(PARTITION_DESCRIPTION, ''),
		       PARTITION_ORDINAL_POSITION, COALESCE(TABLE_ROWS, 0)
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'
		ORDER BY PARTITION_ORDINAL_POSITION`,
		dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []PartitionStat
	for rows.Next() {
		var p PartitionStat
		if err := rows.Scan(&p.Name, &p.Description, &p.Ordinal, &p.TableRows); err != nil {
			return nil, err
		}
		stats = append(stats, p)
	}
	return stats, rows.Err()
}

// WriteStatus writes a three-section status report (Indexed Files, Partitions, Summary) to w.
func WriteStatus(w io.Writer, files []IndexStateRow, parts []PartitionStat) {
	// ── Section 1: Indexed Files ──────────────────────────────────────────────
	fmt.Fprintln(w, "=== Indexed Files ===")
	if len(files) == 0 {
		fmt.Fprintln(w, "  (no files indexed yet)")
	} else {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "FILE\tSTATUS\tEVENTS\tSTARTED_AT\tCOMPLETED_AT\tBINTRAIL_ID\tERROR")
		fmt.Fprintln(tw, "────\t──────\t──────\t──────────\t────────────\t───────────\t─────")
		for _, f := range files {
			completedAt := "-"
			if f.CompletedAt.Valid {
				completedAt = f.CompletedAt.Time.Format(TSFmt)
			}
			bintrailID := "-"
			if f.BintrailID.Valid && f.BintrailID.String != "" {
				bintrailID = f.BintrailID.String
			}
			errMsg := "-"
			if f.ErrorMessage.Valid && f.ErrorMessage.String != "" {
				errMsg = Truncate(f.ErrorMessage.String, 60)
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
				f.BinlogFile, f.Status, f.EventsIndexed,
				f.StartedAt.Format(TSFmt),
				completedAt, bintrailID, errMsg,
			)
		}
		tw.Flush()
	}

	// ── Section 2: Partitions ─────────────────────────────────────────────────
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Partitions ===")
	if len(parts) == 0 {
		fmt.Fprintln(w, "  (no partitions found — run 'bintrail init' first)")
	} else {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "PARTITION\tLESS_THAN\tROWS (est.)")
		fmt.Fprintln(tw, "─────────\t─────────\t───────────")
		var totalRows int64
		for _, p := range parts {
			fmt.Fprintf(tw, "%s\t%s\t%d\n", p.Name, DescriptionToHuman(p.Description), p.TableRows)
			totalRows += p.TableRows
		}
		tw.Flush()
		fmt.Fprintf(w, "Total events (est.): %d\n", totalRows)
	}

	// ── Section 3: Summary (grouped by server) ────────────────────────────────
	if len(files) > 0 {
		// Group files by bintrail_id; preserve insertion order for display.
		type serverStats struct {
			counts map[string]int
			events int64
		}
		serverOrder := []string{}
		byServer := map[string]*serverStats{}
		for _, f := range files {
			key := "(unknown)"
			if f.BintrailID.Valid && f.BintrailID.String != "" {
				key = f.BintrailID.String
			}
			if _, seen := byServer[key]; !seen {
				serverOrder = append(serverOrder, key)
				byServer[key] = &serverStats{counts: map[string]int{}}
			}
			byServer[key].counts[f.Status]++
			byServer[key].events += f.EventsIndexed
		}

		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== Summary ===")
		for _, id := range serverOrder {
			s := byServer[id]
			fmt.Fprintf(w, "Server %s\n", id)
			fmt.Fprintf(w, "  Files:  %d completed, %d in_progress, %d failed\n",
				s.counts["completed"], s.counts["in_progress"], s.counts["failed"])
			fmt.Fprintf(w, "  Events: %d indexed\n", s.events)
		}
	}
}

// DescriptionToHuman converts a PARTITION_DESCRIPTION value to a readable string.
// RANGE partitions using TO_SECONDS() store the evaluated integer second count; MAXVALUE is literal.
// TO_SECONDS('1970-01-01 00:00:00') = 62167219200, so we convert back via: time.Unix(secs-62167219200, 0).
func DescriptionToHuman(desc string) string {
	if desc == "" || strings.EqualFold(desc, "MAXVALUE") {
		return "MAXVALUE"
	}
	secs, err := strconv.ParseInt(desc, 10, 64)
	if err != nil {
		return desc // not an integer — return raw value
	}
	return time.Unix(secs-62167219200, 0).UTC().Format("2006-01-02 15:00 UTC")
}

// Truncate shortens s to at most n bytes, appending "…" if truncated.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
