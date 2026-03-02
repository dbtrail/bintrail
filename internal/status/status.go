// Package status provides shared types and display helpers for the binlog index status.
// It is used by both cmd/bintrail/status.go and cmd/bintrail-mcp/main.go.
package status

import (
	"context"
	"database/sql"
	"encoding/json"
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

// ArchiveStats holds aggregate statistics from the archive_state table.
// A single archive file may be both local and in S3, so
// LocalFiles + S3Files may exceed TotalFiles.
type ArchiveStats struct {
	TotalFiles     int
	TotalRows      int64
	TotalSizeBytes int64
	LocalFiles     int
	S3Files        int
	S3Buckets      []string // distinct non-empty buckets
}

// SchemaChange holds one row from the schema_changes table.
type SchemaChange struct {
	ID         int
	DetectedAt time.Time
	BinlogFile string
	SchemaName string
	TableName  string
	DDLType    string
	SnapshotID sql.NullInt32
}

// CoverageInfo summarizes the restore coverage of the index.
type CoverageInfo struct {
	EarliestEvent     sql.NullTime
	LatestEvent       sql.NullTime
	TotalEvents       int64
	SchemaChanges     int
	UncoveredDDLs     int // DDLs without a snapshot (file mode, or failed auto-snapshot in stream mode)
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

// LoadArchiveStats loads aggregate archive statistics from the archive_state table.
func LoadArchiveStats(ctx context.Context, db *sql.DB) (*ArchiveStats, error) {
	var a ArchiveStats
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COALESCE(SUM(row_count), 0),
		       COALESCE(SUM(file_size_bytes), 0),
		       COALESCE(SUM(CASE WHEN local_path IS NOT NULL THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN s3_key IS NOT NULL THEN 1 ELSE 0 END), 0)
		FROM archive_state`).Scan(
		&a.TotalFiles, &a.TotalRows, &a.TotalSizeBytes,
		&a.LocalFiles, &a.S3Files,
	)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT s3_bucket FROM archive_state WHERE s3_bucket IS NOT NULL AND s3_bucket != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, err
		}
		a.S3Buckets = append(a.S3Buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &a, nil
}

// LoadCoverage loads restore coverage info from binlog_events and schema_changes.
func LoadCoverage(ctx context.Context, db *sql.DB) (*CoverageInfo, error) {
	var c CoverageInfo
	err := db.QueryRowContext(ctx, `
		SELECT MIN(event_timestamp),
		       MAX(event_timestamp),
		       COUNT(*)
		FROM binlog_events`).Scan(&c.EarliestEvent, &c.LatestEvent, &c.TotalEvents)
	if err != nil {
		return nil, fmt.Errorf("query binlog_events coverage: %w", err)
	}

	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_changes`).Scan(&c.SchemaChanges)
	if err != nil {
		return nil, fmt.Errorf("query schema_changes count: %w", err)
	}

	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_changes WHERE snapshot_id IS NULL`).Scan(&c.UncoveredDDLs)
	if err != nil {
		return nil, fmt.Errorf("query uncovered DDLs: %w", err)
	}

	return &c, nil
}

// LoadSchemaChanges loads all schema changes ordered by detection time.
func LoadSchemaChanges(ctx context.Context, db *sql.DB) ([]SchemaChange, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, detected_at, binlog_file, schema_name, table_name, ddl_type, snapshot_id
		FROM schema_changes
		ORDER BY detected_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []SchemaChange
	for rows.Next() {
		var sc SchemaChange
		if err := rows.Scan(&sc.ID, &sc.DetectedAt, &sc.BinlogFile,
			&sc.SchemaName, &sc.TableName, &sc.DDLType, &sc.SnapshotID); err != nil {
			return nil, err
		}
		changes = append(changes, sc)
	}
	return changes, rows.Err()
}

// WriteStatus writes a multi-section status report (Indexed Files, Partitions, Archives, Coverage, Summary) to w.
func WriteStatus(w io.Writer, files []IndexStateRow, parts []PartitionStat, archives *ArchiveStats, coverage *CoverageInfo) {
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

	// ── Section 3: Archives ──────────────────────────────────────────────────
	if archives != nil && archives.TotalFiles > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== Archives ===")
		fmt.Fprintf(w, "  Total:  %d files (%s, %d rows)\n",
			archives.TotalFiles, formatBytes(archives.TotalSizeBytes), archives.TotalRows)
		fmt.Fprintf(w, "  Local:  %d\n", archives.LocalFiles)
		if archives.S3Files > 0 {
			fmt.Fprintf(w, "  S3:     %d (bucket: %s)\n",
				archives.S3Files, strings.Join(archives.S3Buckets, ", "))
		} else {
			fmt.Fprintf(w, "  S3:     0\n")
		}
	}

	// ── Section 4: Restore Coverage ─────────────────────────────────────────
	if coverage != nil {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== Restore Coverage ===")
		if coverage.EarliestEvent.Valid {
			fmt.Fprintf(w, "  Earliest event: %s\n", coverage.EarliestEvent.Time.Format(TSFmt))
		} else {
			fmt.Fprintln(w, "  Earliest event: (none)")
		}
		if coverage.LatestEvent.Valid {
			fmt.Fprintf(w, "  Latest event:   %s\n", coverage.LatestEvent.Time.Format(TSFmt))
		} else {
			fmt.Fprintln(w, "  Latest event:   (none)")
		}
		fmt.Fprintf(w, "  Total events:   %d\n", coverage.TotalEvents)
		fmt.Fprintf(w, "  Schema changes: %d\n", coverage.SchemaChanges)
		if coverage.UncoveredDDLs > 0 {
			fmt.Fprintf(w, "  Warning: %d DDL(s) detected without auto-snapshot (file mode) — recovery across these DDLs may require manual snapshot\n",
				coverage.UncoveredDDLs)
		}
	}

	// ── Section 5: Summary (grouped by server) ────────────────────────────────
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

// formatBytes converts a byte count to a human-readable string (e.g. "1.2 GB").
func formatBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)
	switch {
	case b >= tb:
		return fmt.Sprintf("%.1f TB", float64(b)/float64(tb))
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// Truncate shortens s to at most n bytes, appending "…" if truncated.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// WriteStatusJSON writes the status data as a JSON object to w.
func WriteStatusJSON(w io.Writer, files []IndexStateRow, parts []PartitionStat, archives *ArchiveStats, coverage *CoverageInfo) error {
	type jsonFile struct {
		BinlogFile    string  `json:"binlog_file"`
		Status        string  `json:"status"`
		EventsIndexed int64   `json:"events_indexed"`
		FileSize      int64   `json:"file_size"`
		LastPosition  int64   `json:"last_position"`
		StartedAt     string  `json:"started_at"`
		CompletedAt   *string `json:"completed_at"`
		BintrailID    *string `json:"bintrail_id"`
		ErrorMessage  *string `json:"error_message"`
	}
	type jsonPartition struct {
		Name      string `json:"name"`
		LessThan  string `json:"less_than"`
		TableRows int64  `json:"table_rows"`
	}
	type jsonArchives struct {
		TotalFiles     int      `json:"total_files"`
		TotalRows      int64    `json:"total_rows"`
		TotalSizeBytes int64    `json:"total_size_bytes"`
		TotalSizeHuman string   `json:"total_size_human"`
		LocalFiles     int      `json:"local_files"`
		S3Files        int      `json:"s3_files"`
		S3Buckets      []string `json:"s3_buckets"`
	}
	type jsonCoverage struct {
		EarliestEvent *string `json:"earliest_event"`
		LatestEvent   *string `json:"latest_event"`
		TotalEvents   int64   `json:"total_events"`
		SchemaChanges int     `json:"schema_changes"`
		UncoveredDDLs int     `json:"uncovered_ddls"`
	}
	type jsonSummary struct {
		Files    []jsonFile      `json:"files"`
		Parts    []jsonPartition `json:"partitions"`
		Total    int64           `json:"total_events_estimate"`
		Archives *jsonArchives   `json:"archives,omitempty"`
		Coverage *jsonCoverage   `json:"coverage,omitempty"`
	}

	jf := make([]jsonFile, len(files))
	for i, f := range files {
		jf[i] = jsonFile{
			BinlogFile:    f.BinlogFile,
			Status:        f.Status,
			EventsIndexed: f.EventsIndexed,
			FileSize:      f.FileSize,
			LastPosition:  f.LastPosition,
			StartedAt:     f.StartedAt.Format(TSFmt),
		}
		if f.CompletedAt.Valid {
			s := f.CompletedAt.Time.Format(TSFmt)
			jf[i].CompletedAt = &s
		}
		if f.BintrailID.Valid && f.BintrailID.String != "" {
			jf[i].BintrailID = &f.BintrailID.String
		}
		if f.ErrorMessage.Valid && f.ErrorMessage.String != "" {
			jf[i].ErrorMessage = &f.ErrorMessage.String
		}
	}

	jp := make([]jsonPartition, len(parts))
	var total int64
	for i, p := range parts {
		jp[i] = jsonPartition{
			Name:      p.Name,
			LessThan:  DescriptionToHuman(p.Description),
			TableRows: p.TableRows,
		}
		total += p.TableRows
	}

	out := jsonSummary{Files: jf, Parts: jp, Total: total}
	if archives != nil && archives.TotalFiles > 0 {
		out.Archives = &jsonArchives{
			TotalFiles:     archives.TotalFiles,
			TotalRows:      archives.TotalRows,
			TotalSizeBytes: archives.TotalSizeBytes,
			TotalSizeHuman: formatBytes(archives.TotalSizeBytes),
			LocalFiles:     archives.LocalFiles,
			S3Files:        archives.S3Files,
			S3Buckets:      archives.S3Buckets,
		}
	}
	if coverage != nil {
		jc := &jsonCoverage{
			TotalEvents:   coverage.TotalEvents,
			SchemaChanges: coverage.SchemaChanges,
			UncoveredDDLs: coverage.UncoveredDDLs,
		}
		if coverage.EarliestEvent.Valid {
			s := coverage.EarliestEvent.Time.Format(TSFmt)
			jc.EarliestEvent = &s
		}
		if coverage.LatestEvent.Valid {
			s := coverage.LatestEvent.Time.Format(TSFmt)
			jc.LatestEvent = &s
		}
		out.Coverage = jc
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
