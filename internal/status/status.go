// Package status provides shared types and display helpers for the binlog index status.
// It is used by both cmd/bintrail/status.go and cmd/bintrail-mcp/main.go.
package status

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-sql-driver/mysql"
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

// ServerInfo holds one active row from the bintrail_servers table.
type ServerInfo struct {
	BintrailID       string
	ServerUUID       string
	Host             string
	Port             uint16
	Username         string
	CreatedAt        time.Time
	DecommissionedAt sql.NullTime
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

// StreamStateInfo holds the single row from the stream_state table.
type StreamStateInfo struct {
	Mode           string // "position" or "gtid"
	BinlogFile     string
	BinlogPosition uint64
	GTIDSet        sql.NullString
	EventsIndexed  int64
	LastEventTime  sql.NullTime
	LastCheckpoint time.Time
	ServerID       uint32
	BintrailID     sql.NullString
	// GapLostAt / GapLostDetail record that the stream permanently lost data it could
	// not recover — an unfillable binlog gap (MySQL) or an invalidated/lost replication
	// slot (PostgreSQL, #532). When set, the index is valid only up to the gap and
	// capture must be re-baselined to resume; status surfaces them loudly. GapLostAt is
	// authoritative (the badge is gated on it alone); GapLostDetail is supplementary
	// human-readable context and may be absent. Both writers set them atomically.
	GapLostAt     sql.NullTime
	GapLostDetail sql.NullString
	// GapColumnsPresent is true when the gap_lost_* columns existed and were read
	// (the normal, migrated index). It is false only on a legacy index whose
	// schema predates those columns, read before any migration — there the gap
	// state was never evaluable, so the continuity verdict is "unknown" (not a
	// false "ok" asserted from absent data), and --fail-on-gap fails closed.
	GapColumnsPresent bool
	// SourceHealth is the latest source-side health snapshot a streaming daemon polled
	// (#599) — for PostgreSQL, a JSON document with the replication slot's wal_status/lag
	// and REPLICA IDENTITY coverage plus an embedded checked_at. Opaque here (raw JSON);
	// the console renders it and computes staleness from checked_at. Invalid on a legacy
	// index without the column or an index no daemon has polled.
	SourceHealth sql.NullString
	// CaptureSkips is the raw capture_skips JSON (#1034): per-reason monotonic
	// counters of events the streaming daemon READ and chose to DROP (e.g. the
	// column-count guard rejecting rows against a stale snapshot), shaped
	// {"<reason>":{"count":N,"last_at":"RFC3339"}}. "{}" is the affirmative
	// evaluated-and-clean marker written by a skip-aware daemon. Invalid on a
	// legacy index without the column or one no such daemon has written — the
	// Capture health verdict is then unknown and the line is omitted rather
	// than asserting OK from absent data (the GapColumnsPresent philosophy).
	CaptureSkips sql.NullString
}

// CaptureSkipStat is one reason's tally decoded from CaptureSkips. The JSON
// field names mirror the persistence format written by the capture daemon
// (parser.SkipStat) — kept as an independent decl so this display package does
// not import the binlog parser.
type CaptureSkipStat struct {
	Count  int64     `json:"count"`
	LastAt time.Time `json:"last_at"`
	// Last-seen attribution (#999), stamped by the capture daemon for the most
	// recent skip of a reason — present only for reasons whose detection site
	// stamps it (see parser.SkipStat, the single source of truth for which do).
	LastFile          string `json:"last_file,omitempty"`
	LastPos           uint64 `json:"last_pos,omitempty"`
	LastStatementType string `json:"last_statement_type,omitempty"`
	LastConnectionID  uint32 `json:"last_connection_id,omitempty"`
}

// CaptureSkipReasonStatementFormatDML mirrors parser.SkipStatementFormatDML —
// the persisted reason key for STATEMENT/MIXED-format DML drops (#999). Kept as
// an independent decl for the same reason CaptureSkipStat is: this display
// package deliberately does not import the binlog parser.
const CaptureSkipReasonStatementFormatDML = "statement_format_dml"

// CaptureSkipReasonUnreadablePreviousLedger mirrors
// parser.SkipUnreadablePreviousLedger — the meta-reason a restarting daemon
// stamps when the previously persisted ledger could not be parsed (#1206).
const CaptureSkipReasonUnreadablePreviousLedger = "unreadable_previous_ledger"

// ParseCaptureSkips decodes the persisted capture_skips document. ok is false
// when the verdict is not evaluable (no column / no skip-aware daemon /
// unparseable payload) — callers must then omit the Capture health verdict
// entirely, never render OK. An empty map with ok=true is the affirmative
// "evaluated, nothing skipped".
func (s *StreamStateInfo) ParseCaptureSkips() (skips map[string]CaptureSkipStat, ok bool) {
	if !s.CaptureSkips.Valid || strings.TrimSpace(s.CaptureSkips.String) == "" {
		return nil, false
	}
	m := map[string]CaptureSkipStat{}
	if err := json.Unmarshal([]byte(s.CaptureSkips.String), &m); err != nil {
		slog.Warn("could not parse capture_skips; capture health shown as unknown", "error", err)
		return nil, false
	}
	return m, true
}

// CoverageInfo summarizes the restore coverage of the index.
type CoverageInfo struct {
	EarliestEvent sql.NullTime
	LatestEvent   sql.NullTime
	TotalEvents   int64
	SchemaChanges int
	// UncoveredDDLs counts schema_changes rows with snapshot_id IS NULL whose
	// DDL type NEEDS a snapshot — i.e. file-mode indexing without --source-dsn,
	// or a failed auto-snapshot (any mode). TRUNCATE TABLE rows are excluded:
	// they record snapshot_id = NULL by design (no structure change, so both
	// the stream DDL hook and the file-mode handler deliberately skip the
	// snapshot), and counting them would permanently inflate the warning.
	UncoveredDDLs int

	// Archive-derived fields (from archive_state partition names and row counts).
	ArchiveEarliestHour sql.NullTime // earliest hour derived from MIN(partition_name)
	ArchiveTotalRows    int64

	// IndexSizeBytes is the on-disk size of the binlog_events table
	// (DATA_LENGTH + INDEX_LENGTH, an InnoDB estimate). Surfaced so an operator
	// sees how much disk the live index occupies alongside its time coverage.
	IndexSizeBytes int64
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

// LoadCoverage loads restore coverage info from binlog_events, schema_changes,
// and archive_state. Archive coverage is derived from partition names stored in
// archive_state (e.g. "p_2026021914" → 2026-02-19 14:00 UTC) without reading
// Parquet files.
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

	// TRUNCATE TABLE is excluded: it does not change table structure, so every
	// capture path records it with snapshot_id = NULL on purpose ("DDL detected
	// (no snapshot needed)") — a NULL there is not a coverage gap.
	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_changes
		 WHERE snapshot_id IS NULL AND ddl_type <> 'TRUNCATE TABLE'`).Scan(&c.UncoveredDDLs)
	if err != nil {
		return nil, fmt.Errorf("query uncovered DDLs: %w", err)
	}

	// Extend coverage with archived partition data.
	var minPartition sql.NullString
	err = db.QueryRowContext(ctx, `
		SELECT MIN(partition_name), COALESCE(SUM(row_count), 0)
		FROM archive_state`).Scan(&minPartition, &c.ArchiveTotalRows)
	if err != nil {
		// archive_state may not exist in older indexes — treat as non-fatal.
		slog.Warn("could not load archive coverage", "error", err)
		return &c, nil
	}
	if minPartition.Valid {
		if t, ok := parsePartitionName(minPartition.String); ok {
			c.ArchiveEarliestHour = sql.NullTime{Time: t, Valid: true}
		}
	}

	return &c, nil
}

// parsePartitionName converts a partition name like "p_2026021914" to the
// corresponding UTC hour. Returns false for "p_future" or malformed names.
func parsePartitionName(name string) (time.Time, bool) {
	if len(name) != 12 || !strings.HasPrefix(name, "p_") {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("p_2006010215", name, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
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

// LoadServers loads all rows from bintrail_servers ordered by created_at.
func LoadServers(ctx context.Context, db *sql.DB) ([]ServerInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT bintrail_id, server_uuid, host, port, username,
		       created_at, decommissioned_at
		FROM bintrail_servers
		ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var servers []ServerInfo
	for rows.Next() {
		var s ServerInfo
		if err := rows.Scan(
			&s.BintrailID, &s.ServerUUID, &s.Host, &s.Port, &s.Username,
			&s.CreatedAt, &s.DecommissionedAt,
		); err != nil {
			return nil, err
		}
		servers = append(servers, s)
	}
	return servers, rows.Err()
}

// LoadStreamState loads the single row from stream_state (if any).
// Returns nil with no error when the table is empty (no active stream).
func LoadStreamState(ctx context.Context, db *sql.DB) (*StreamStateInfo, error) {
	s, err := loadStreamStateCore(ctx, db)
	if err != nil || s == nil {
		return s, err
	}
	// source_health (#599) is loaded by a SEPARATE best-effort query, not folded into the
	// SELECTs above: it is newer than gap_lost_*, so adding it there would drag any index
	// that has gap_lost but not yet source_health down to the base fallback (losing the
	// loss record). The separate query degrades to "no health" on the unknown-column error
	// (a legacy/un-migrated index) or an empty row; any OTHER error surfaces, since the core
	// load already proved the DB reachable; any hard error here is logged and ignored,
	// keeping the already-loaded stream state (including gap_lost) visible rather than
	// discarding it — that would re-hide the very GAP LOST banner #815 exists to surface.
	if err := loadSourceHealth(ctx, db, s); err != nil {
		slog.Warn("could not load source_health; keeping stream state without it", "error", err)
	}
	// capture_skips (#1034) follows the same separate best-effort pattern (and
	// the same rationale) as source_health above: newer than both, so folding
	// it into either SELECT would drag older indexes down a fallback tier.
	if err := loadCaptureSkips(ctx, db, s); err != nil {
		slog.Warn("could not load capture_skips; keeping stream state without it", "error", err)
	}
	return s, nil
}

func loadStreamStateCore(ctx context.Context, db *sql.DB) (*StreamStateInfo, error) {
	var s StreamStateInfo
	err := db.QueryRowContext(ctx, `
		SELECT mode, binlog_file, binlog_position, gtid_set,
		       events_indexed, last_event_time, last_checkpoint,
		       server_id, bintrail_id, gap_lost_at, gap_lost_detail
		FROM stream_state
		WHERE id = 1`).Scan(
		&s.Mode, &s.BinlogFile, &s.BinlogPosition, &s.GTIDSet,
		&s.EventsIndexed, &s.LastEventTime, &s.LastCheckpoint,
		&s.ServerID, &s.BintrailID, &s.GapLostAt, &s.GapLostDetail,
	)
	// A legacy index predating the gap_lost_* columns (added by the cascade-recovery
	// work), read before any migrating command (EnsureSchema) ran — the console never
	// migrates registry DSNs — lacks those columns. Degrade gracefully to the base
	// columns rather than erroring `status`; such an index simply has no loss record.
	if isUnknownColumnErr(err) {
		return loadStreamStateBase(ctx, db)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	s.GapColumnsPresent = true // the gap_lost_* columns were read — gap state is evaluable
	return &s, nil
}

// loadSourceHealth augments an already-loaded StreamStateInfo with the source_health
// column. It tolerates ONLY the unknown-column error (a legacy index missing the column)
// and an empty row — there it leaves SourceHealth invalid (no health to show). Any other
// error is returned, so a genuine fault is not silently hidden behind a blank panel.
func loadSourceHealth(ctx context.Context, db *sql.DB, s *StreamStateInfo) error {
	err := db.QueryRowContext(ctx, `SELECT source_health FROM stream_state WHERE id = 1`).Scan(&s.SourceHealth)
	if isUnknownColumnErr(err) || errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

// loadCaptureSkips augments an already-loaded StreamStateInfo with the
// capture_skips column (#1034) — same tolerance contract as loadSourceHealth:
// only the unknown-column error (legacy index) and an empty row leave
// CaptureSkips invalid (verdict unknown); any other error is returned.
func loadCaptureSkips(ctx context.Context, db *sql.DB, s *StreamStateInfo) error {
	err := db.QueryRowContext(ctx, `SELECT capture_skips FROM stream_state WHERE id = 1`).Scan(&s.CaptureSkips)
	if isUnknownColumnErr(err) || errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

// loadStreamStateBase loads the stream_state columns guaranteed by the original CREATE
// TABLE (no gap_lost_*), for a legacy index that has not been migrated. The gap-loss
// fields stay zero (no badge).
func loadStreamStateBase(ctx context.Context, db *sql.DB) (*StreamStateInfo, error) {
	var s StreamStateInfo
	err := db.QueryRowContext(ctx, `
		SELECT mode, binlog_file, binlog_position, gtid_set,
		       events_indexed, last_event_time, last_checkpoint,
		       server_id, bintrail_id
		FROM stream_state
		WHERE id = 1`).Scan(
		&s.Mode, &s.BinlogFile, &s.BinlogPosition, &s.GTIDSet,
		&s.EventsIndexed, &s.LastEventTime, &s.LastCheckpoint,
		&s.ServerID, &s.BintrailID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// isUnknownColumnErr reports whether err is MySQL error 1054 (ER_BAD_FIELD_ERROR,
// "Unknown column"), i.e. the index schema predates a column this build SELECTs.
func isUnknownColumnErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1054
}

// StatusData holds all data sections loaded by CollectStatus.
type StatusData struct {
	Files     []IndexStateRow
	Parts     []PartitionStat
	Archives  *ArchiveStats
	Coverage  *CoverageInfo
	Servers   []ServerInfo
	Stream    *StreamStateInfo
	Baselines []BaselineInfo
	// BaselinesUnavailable: a baseline location was configured but could not
	// be read, so Baselines is empty for a BAD reason — JSON must report
	// baseline_staleness "unknown", not omit it as if nothing were configured.
	BaselinesUnavailable bool
	// StreamErr records a failure to READ stream_state (transient timeout, revoked
	// permission, an unexpected loadSourceHealth error) — as distinct from an empty
	// table (Stream==nil, StreamErr==nil = no active stream). When set, the continuity
	// verdict could not be evaluated: the output must show it as "unavailable" rather
	// than silently omit the Stream section and its permanent-loss banner (fail visible,
	// not silent). json:"-" — StatusData is rendered via the manual jsonSummary mapping,
	// never marshalled directly; the tag is insurance against a future direct marshal.
	StreamErr error `json:"-"`
}

// BaselineInfo holds metadata about a discovered baseline Parquet file.
// Populated externally (from baseline.DiscoverBaselines) and attached to StatusData.
type BaselineInfo struct {
	SnapshotTime time.Time
	Database     string
	Table        string
	BinlogFile   string
	BinlogPos    int64
	GTIDSet      string
	// Staleness is this snapshot's #1193 verdict against the oldest available
	// delta coverage ("" until AnnotateBaselineStaleness runs).
	Staleness BaselineStalenessVerdict
	Path      string // filesystem path; ignored by display/JSON output
	// Size is the Parquet file size in bytes (0 = unknown). Surfaced so an
	// operator can see per-table baseline size — the signal that tells whether
	// a single-table baseline has grown into the large regime.
	Size int64
}

// CollectStatus loads all status data from the index database.
// IndexState and PartitionStats are required (errors are returned).
// Servers, StreamState, ArchiveStats, and Coverage are best-effort
// (failures are logged as warnings and the field is left nil).
func CollectStatus(ctx context.Context, db *sql.DB, dbName string) (*StatusData, error) {
	files, err := LoadIndexState(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to load index state: %w", err)
	}

	parts, err := LoadPartitionStats(ctx, db, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to load partition info: %w", err)
	}

	d := &StatusData{Files: files, Parts: parts}

	if servers, err := LoadServers(ctx, db); err != nil {
		slog.Warn("could not load servers", "error", err)
	} else {
		d.Servers = servers
	}

	if stream, err := LoadStreamState(ctx, db); err != nil {
		slog.Warn("could not load stream state", "error", err)
		// Record the read failure so the continuity verdict reports "unavailable"
		// instead of the output silently omitting the Stream section (and its
		// permanent-loss banner) — a swallowed error must not read as "no stream".
		d.StreamErr = err
	} else {
		d.Stream = stream
	}

	if archives, err := LoadArchiveStats(ctx, db); err != nil {
		slog.Warn("could not load archive stats", "error", err)
	} else {
		d.Archives = archives
	}

	if coverage, err := LoadCoverage(ctx, db); err != nil {
		slog.Warn("could not load coverage info", "error", err)
	} else {
		d.Coverage = coverage
	}

	// Best-effort: the binlog_events on-disk size, attached to coverage so it
	// surfaces alongside the time-coverage figures (and is reused by the
	// bintrail_index_storage_bytes gauge).
	if size, err := LoadIndexSizeBytes(ctx, db, dbName); err != nil {
		slog.Warn("could not load index size", "error", err)
	} else if d.Coverage != nil {
		d.Coverage.IndexSizeBytes = size
	}

	return d, nil
}

// LoadIndexSizeBytes returns the on-disk size of the binlog_events table
// (DATA_LENGTH + INDEX_LENGTH summed across partitions) — an InnoDB estimate
// from information_schema, the same figure the doctor capacity check uses.
func LoadIndexSizeBytes(ctx context.Context, db *sql.DB, dbName string) (int64, error) {
	var b sql.NullInt64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0)
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'`, dbName).Scan(&b)
	if err != nil {
		return 0, err
	}
	return b.Int64, nil
}

// Write writes the status data as a human-readable report to w.
func (d *StatusData) Write(w io.Writer) {
	WriteStatus(w, d.Files, d.Parts, d.Archives, d.Coverage, d.Servers, d.Stream)
	if d.Stream == nil && d.StreamErr != nil {
		writeStreamUnavailable(w, d.StreamErr)
	}
	writeBaselines(w, d.Baselines)
}

// writeStreamUnavailable renders a visible Stream block when stream_state could not
// be READ (StreamErr set, Stream nil). Distinct from an empty table (no block): here
// the continuity verdict — and any permanent-loss banner — could not be evaluated, so
// the operator sees "unavailable" rather than an absence they'd misread as "no loss".
func writeStreamUnavailable(w io.Writer, err error) {
	fmt.Fprintln(w, "=== Stream ===")
	fmt.Fprintf(w, "  Continuity:      ⚠ unavailable (could not read stream state: %v)\n", err)
	fmt.Fprintln(w, "  The gap/continuity state could not be read from the index; a permanent-loss")
	fmt.Fprintln(w, "  banner can neither be shown nor ruled out. Re-run status to retry.")
	fmt.Fprintln(w)
}

// WriteJSON writes the status data as JSON to w.
func (d *StatusData) WriteJSON(w io.Writer) error {
	return writeStatusJSONFull(w, d.Files, d.Parts, d.Archives, d.Coverage, d.Servers, d.Stream, d.Baselines, d.BaselinesUnavailable, d.StreamErr)
}

// WriteStatus writes a multi-section status report (Servers, Stream, Indexed Files, Partitions, Archives, Coverage, Summary) to w.
func WriteStatus(w io.Writer, files []IndexStateRow, parts []PartitionStat, archives *ArchiveStats, coverage *CoverageInfo, servers []ServerInfo, stream *StreamStateInfo) {
	// ── Section 0: Servers ───────────────────────────────────────────────────
	if len(servers) > 0 {
		fmt.Fprintln(w, "=== Servers ===")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "BINTRAIL_ID\tHOST\tPORT\tSERVER_UUID\tCREATED_AT\tSTATUS")
		fmt.Fprintln(tw, "───────────\t────\t────\t───────────\t──────────\t──────")
		for _, s := range servers {
			st := "active"
			if s.DecommissionedAt.Valid {
				st = "decommissioned"
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n",
				s.BintrailID, s.Host, s.Port, s.ServerUUID,
				s.CreatedAt.Format(TSFmt), st,
			)
		}
		tw.Flush()
		fmt.Fprintln(w)
	}

	// ── Section 0b: Stream ──────────────────────────────────────────────────
	if stream != nil {
		fmt.Fprintln(w, "=== Stream ===")
		bintrailID := "(none)"
		if stream.BintrailID.Valid && stream.BintrailID.String != "" {
			bintrailID = stream.BintrailID.String
		}
		fmt.Fprintf(w, "  Bintrail ID:     %s\n", bintrailID)
		fmt.Fprintf(w, "  Mode:            %s\n", stream.Mode)
		if stream.BinlogFile != "" {
			fmt.Fprintf(w, "  Position:        %s:%d\n", stream.BinlogFile, stream.BinlogPosition)
		}
		if stream.GTIDSet.Valid && stream.GTIDSet.String != "" {
			fmt.Fprintf(w, "  GTID set:        %s\n", stream.GTIDSet.String)
		}
		fmt.Fprintf(w, "  Events indexed:  %d\n", stream.EventsIndexed)
		if stream.LastEventTime.Valid {
			fmt.Fprintf(w, "  Last event:      %s\n", stream.LastEventTime.Time.Format(TSFmt))
		}
		fmt.Fprintf(w, "  Last checkpoint: %s\n", stream.LastCheckpoint.Format(TSFmt))
		fmt.Fprintf(w, "  Server ID:       %d\n", stream.ServerID)
		// Always-present continuity verdict — the cheap "did I lose any events?"
		// answer the gap detector already computes, surfaced so an operator reads
		// it at a glance instead of inferring it from the absence of the loud
		// banner below. It is strictly about gap-CONTIGUITY of the captured range
		// (the cursor is the Position / GTID set above, when one is printed); it is
		// deliberately NOT a liveness/lag check — a contiguous stream may still be
		// stopped or behind. "not evaluated" guards a legacy index that never had
		// the gap_lost_* columns, so a clean verdict is never asserted from
		// un-evaluated data.
		switch {
		case stream.GapLostAt.Valid:
			fmt.Fprintf(w, "  Continuity:      ⚠ GAP LOST at %s\n", stream.GapLostAt.Time.Format(TSFmt))
		case !stream.GapColumnsPresent:
			fmt.Fprintln(w, "  Continuity:      not evaluated (legacy index — migrate the schema to enable gap detection)")
		default:
			fmt.Fprintln(w, "  Continuity:      no gaps in the captured range (not a liveness check)")
		}
		// Capture health (#1034) — the continuity verdict's sibling for
		// IN-STREAM discards: events the daemon read and chose to drop (e.g.
		// the column-count guard rejecting every row against a stale snapshot)
		// while the checkpoint stayed fresh and continuity honestly said "no
		// gaps". Omitted (not asserted OK) when no skip-aware daemon has
		// written the counters — same never-a-false-ok stance as Continuity's
		// "not evaluated".
		if skips, ok := stream.ParseCaptureSkips(); ok {
			if total := totalCaptureSkips(skips); total > 0 {
				fmt.Fprintf(w, "  Capture health:  ⚠ DEGRADED — %s events skipped (%s), last %s\n",
					commaGroup(total), captureSkipReasons(skips), lastCaptureSkip(skips).Format(TSFmt))
				if attr := lastCaptureSkipAttribution(skips); attr != "" {
					fmt.Fprintf(w, "  Last drop:       %s\n", attr)
				}
				fmt.Fprintln(w, "  Skipped events were read from the stream but NOT indexed — a restore window")
				fmt.Fprintln(w, "  over them is incomplete. Most often the schema snapshot is stale or corrupt:")
				fmt.Fprintln(w, "  run `bintrail snapshot` against the source, then check the daemon log.")
			} else {
				fmt.Fprintln(w, "  Capture health:  OK — no events skipped")
			}
		}
		fmt.Fprintln(w)

		// Loud, unmissable banner when the stream permanently lost data (an unfillable
		// binlog gap, or an invalidated/lost PostgreSQL replication slot — #532). This
		// is the only way index-only `status` can show a lost stream after the capture
		// process has exited; the index up to the gap is still valid for recovery.
		if stream.GapLostAt.Valid {
			fmt.Fprintln(w, "=== ⚠ EVENTS PERMANENTLY LOST ===")
			fmt.Fprintf(w, "  Detected:  %s\n", stream.GapLostAt.Time.Format(TSFmt))
			if stream.GapLostDetail.Valid && stream.GapLostDetail.String != "" {
				fmt.Fprintf(w, "  Detail:    %s\n", stream.GapLostDetail.String)
			}
			fmt.Fprintln(w, "  The capture stream lost data it could not recover. The index up to the")
			fmt.Fprintln(w, "  gap is still valid for recovery, but to resume capture you must re-baseline.")
			fmt.Fprintln(w)
		}
	}

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

		// Determine the effective earliest event: archive may extend further back.
		earliest := coverage.EarliestEvent
		hasArchive := coverage.ArchiveEarliestHour.Valid
		if hasArchive && (!earliest.Valid || coverage.ArchiveEarliestHour.Time.Before(earliest.Time)) {
			earliest = coverage.ArchiveEarliestHour
		}
		if earliest.Valid {
			label := earliest.Time.Format(TSFmt)
			if hasArchive {
				label += " (includes archives)"
			}
			fmt.Fprintf(w, "  Earliest event: %s\n", label)
		} else {
			fmt.Fprintln(w, "  Earliest event: (none)")
		}
		if coverage.LatestEvent.Valid {
			fmt.Fprintf(w, "  Latest event:   %s\n", coverage.LatestEvent.Time.Format(TSFmt))
		} else {
			fmt.Fprintln(w, "  Latest event:   (none)")
		}

		totalEvents := coverage.TotalEvents + coverage.ArchiveTotalRows
		if coverage.ArchiveTotalRows > 0 {
			fmt.Fprintf(w, "  Total events:   %d (%d live + %d archived)\n",
				totalEvents, coverage.TotalEvents, coverage.ArchiveTotalRows)
		} else {
			fmt.Fprintf(w, "  Total events:   %d\n", totalEvents)
		}
		if coverage.IndexSizeBytes > 0 {
			fmt.Fprintf(w, "  Index size:     %s (MySQL binlog_events)\n", formatBytes(coverage.IndexSizeBytes))
		}
		fmt.Fprintf(w, "  Schema changes: %d\n", coverage.SchemaChanges)
		if coverage.UncoveredDDLs > 0 {
			fmt.Fprintf(w, "  Warning: %d DDL(s) detected without auto-snapshot (file-mode indexing without --source-dsn, or a failed auto-snapshot) — recovery across these DDLs may require manual snapshot\n",
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

// totalCaptureSkips sums the per-reason capture-skip counts.
func totalCaptureSkips(skips map[string]CaptureSkipStat) int64 {
	var n int64
	for _, st := range skips {
		n += st.Count
	}
	return n
}

// lastCaptureSkip returns the most recent per-reason last_at.
func lastCaptureSkip(skips map[string]CaptureSkipStat) time.Time {
	var last time.Time
	for _, st := range skips {
		if st.LastAt.After(last) {
			last = st.LastAt
		}
	}
	return last
}

// lastCaptureSkipAttribution formats the newest attributed skip (#999) for the
// DEGRADED block: "file:pos" plus, when a statement keyword was stamped, a
// " (STATEMENT_TYPE, connection id N)" segment. An empty file (a drop before
// the first rotate event) renders as "?" rather than a malformed ":pos". ""
// when no stat carries attribution (pre-#999 ledger, or only unattributed
// reasons).
func lastCaptureSkipAttribution(skips map[string]CaptureSkipStat) string {
	var best CaptureSkipStat
	found := false
	for _, st := range skips {
		if st.LastFile == "" && st.LastStatementType == "" {
			continue
		}
		if !found || st.LastAt.After(best.LastAt) {
			best, found = st, true
		}
	}
	if !found {
		return ""
	}
	file := best.LastFile
	if file == "" {
		file = "?"
	}
	s := fmt.Sprintf("%s:%d", file, best.LastPos)
	if best.LastStatementType != "" {
		s += fmt.Sprintf(" (%s, connection id %d)", best.LastStatementType, best.LastConnectionID)
	}
	return s
}

// captureSkipReasons renders the non-zero reasons for the DEGRADED line: the
// bare reason when there is one ("column_count_mismatch", the #1034 wording),
// else "reason: count" pairs sorted by count descending (ties alphabetical).
func captureSkipReasons(skips map[string]CaptureSkipStat) string {
	var reasons []string
	for r, st := range skips {
		if st.Count > 0 {
			reasons = append(reasons, r)
		}
	}
	if len(reasons) == 1 {
		return reasons[0]
	}
	sort.Slice(reasons, func(i, j int) bool {
		if skips[reasons[i]].Count != skips[reasons[j]].Count {
			return skips[reasons[i]].Count > skips[reasons[j]].Count
		}
		return reasons[i] < reasons[j]
	})
	parts := make([]string, len(reasons))
	for i, r := range reasons {
		parts[i] = fmt.Sprintf("%s: %s", r, commaGroup(skips[r].Count))
	}
	return strings.Join(parts, ", ")
}

// commaGroup formats n with thousands separators ("41203" → "41,203").
func commaGroup(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

// Truncate shortens s to at most n bytes, appending "…" if truncated.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// WriteStatusJSON writes the status data as a JSON object to w.
func WriteStatusJSON(w io.Writer, files []IndexStateRow, parts []PartitionStat, archives *ArchiveStats, coverage *CoverageInfo, servers []ServerInfo, stream *StreamStateInfo) error {
	return writeStatusJSONFull(w, files, parts, archives, coverage, servers, stream, nil, false, nil)
}

func writeStatusJSONFull(w io.Writer, files []IndexStateRow, parts []PartitionStat, archives *ArchiveStats, coverage *CoverageInfo, servers []ServerInfo, stream *StreamStateInfo, baselines []BaselineInfo, baselinesUnavailable bool, streamErr error) error {
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
		EarliestEvent        *string `json:"earliest_event"`
		LatestEvent          *string `json:"latest_event"`
		TotalEvents          int64   `json:"total_events"`
		LiveEvents           int64   `json:"live_events"`
		ArchivedEvents       int64   `json:"archived_events"`
		ArchiveEarliestEvent *string `json:"archive_earliest_event,omitempty"`
		IndexSizeBytes       int64   `json:"index_size_bytes,omitempty"`
		IndexSizeHuman       string  `json:"index_size_human,omitempty"`
		SchemaChanges        int     `json:"schema_changes"`
		UncoveredDDLs        int     `json:"uncovered_ddls"`
	}
	type jsonServer struct {
		BintrailID       string  `json:"bintrail_id"`
		ServerUUID       string  `json:"server_uuid"`
		Host             string  `json:"host"`
		Port             uint16  `json:"port"`
		Username         string  `json:"username"`
		CreatedAt        string  `json:"created_at"`
		DecommissionedAt *string `json:"decommissioned_at"`
	}
	type jsonGapLost struct {
		At     string `json:"at"`
		Detail string `json:"detail,omitempty"`
	}
	// jsonContinuity is the always-present, machine-readable continuity verdict —
	// the affirmative counterpart to gap_lost. status is one of:
	//   "ok"       — no gap in the captured range (NOT a liveness/lag assertion;
	//                a contiguous stream may still be stopped or behind)
	//   "gap_lost" — an unfillable gap was stamped (see the gap_lost object)
	//   "unknown"  — a legacy index without the gap_lost_* columns; the gap state
	//                was never evaluable, so "ok" is not asserted from absent data
	// The console green badge keys on "ok"; gap_lost stays for the loud red detail.
	type jsonContinuity struct {
		Status string `json:"status"`
	}
	// jsonCaptureHealth is the machine-readable Capture health verdict (#1034)
	// — the continuity verdict's sibling for in-stream discards. Present only
	// when a skip-aware daemon has persisted the counters ("unknown" is an
	// omitted key, never a false "ok"). status is "ok" or "degraded"; skipped
	// carries the per-reason monotonic tallies.
	type jsonSkipStat struct {
		Count  int64  `json:"count"`
		LastAt string `json:"last_at"`
		// Last-seen attribution (#999); present only for reasons the capture
		// daemon stamps (see parser.SkipStat for which do).
		LastFile          string `json:"last_file,omitempty"`
		LastPos           uint64 `json:"last_pos,omitempty"`
		LastStatementType string `json:"last_statement_type,omitempty"`
		LastConnectionID  uint32 `json:"last_connection_id,omitempty"`
	}
	type jsonCaptureHealth struct {
		Status       string                  `json:"status"`
		TotalSkipped int64                   `json:"total_skipped"`
		LastSkipAt   string                  `json:"last_skip_at,omitempty"`
		Skipped      map[string]jsonSkipStat `json:"skipped,omitempty"`
	}
	type jsonStream struct {
		BintrailID     *string        `json:"bintrail_id"`
		Mode           string         `json:"mode"`
		BinlogFile     string         `json:"binlog_file,omitempty"`
		BinlogPosition uint64         `json:"binlog_position,omitempty"`
		GTIDSet        *string        `json:"gtid_set,omitempty"`
		EventsIndexed  int64          `json:"events_indexed"`
		LastEventTime  *string        `json:"last_event_time"`
		LastCheckpoint string         `json:"last_checkpoint"`
		ServerID       uint32         `json:"server_id"`
		Continuity     jsonContinuity `json:"continuity"`
		GapLost        *jsonGapLost   `json:"gap_lost,omitempty"`
		// SourceHealth is the raw source_health JSON passed through verbatim (#599):
		// the console knows its shape (slot wal_status/lag, replica_identity_not_full,
		// checked_at), this layer does not. Omitted when no daemon has polled.
		SourceHealth  json.RawMessage    `json:"source_health,omitempty"`
		CaptureHealth *jsonCaptureHealth `json:"capture_health,omitempty"`
	}
	type jsonBaseline struct {
		SnapshotTime string  `json:"snapshot_time"`
		Database     string  `json:"database"`
		Table        string  `json:"table"`
		BinlogFile   *string `json:"binlog_file,omitempty"`
		BinlogPos    *int64  `json:"binlog_position,omitempty"`
		GTIDSet      *string `json:"gtid_set,omitempty"`
		Size         int64   `json:"size_bytes,omitempty"`
		SizeHuman    string  `json:"size_human,omitempty"`
		Staleness    string  `json:"staleness,omitempty"`
	}
	// jsonStreamError is emitted (under a distinct key, never a fake `stream` object —
	// jsonStream's non-omitempty events_indexed:0/mode:"" would read as a real empty
	// stream) when stream_state could not be READ. continuity.status is "unavailable":
	// a consumer switching on continuity keeps the gap state OUT of "ok", and one that
	// only checks stream presence still finds this loud sibling instead of silence.
	type jsonStreamError struct {
		Continuity jsonContinuity `json:"continuity"`
		Error      string         `json:"error"`
	}
	type jsonSummary struct {
		Servers     []jsonServer     `json:"servers,omitempty"`
		Stream      *jsonStream      `json:"stream,omitempty"`
		StreamError *jsonStreamError `json:"stream_error,omitempty"`
		Files       []jsonFile       `json:"files"`
		Parts       []jsonPartition  `json:"partitions"`
		Total       int64            `json:"total_events_estimate"`
		Archives    *jsonArchives    `json:"archives,omitempty"`
		Coverage    *jsonCoverage    `json:"coverage,omitempty"`
		Baselines   []jsonBaseline   `json:"baselines,omitempty"`
		// BaselineStaleness is the worst per-table-newest verdict — the same
		// headline the text banner keys on (#1193).
		BaselineStaleness string `json:"baseline_staleness,omitempty"`
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

	var js []jsonServer
	for _, s := range servers {
		srv := jsonServer{
			BintrailID: s.BintrailID,
			ServerUUID: s.ServerUUID,
			Host:       s.Host,
			Port:       s.Port,
			Username:   s.Username,
			CreatedAt:  s.CreatedAt.Format(TSFmt),
		}
		if s.DecommissionedAt.Valid {
			ts := s.DecommissionedAt.Time.Format(TSFmt)
			srv.DecommissionedAt = &ts
		}
		js = append(js, srv)
	}

	out := jsonSummary{Servers: js, Files: jf, Parts: jp, Total: total}
	if stream != nil {
		jstr := &jsonStream{
			Mode:           stream.Mode,
			BinlogFile:     stream.BinlogFile,
			BinlogPosition: stream.BinlogPosition,
			EventsIndexed:  stream.EventsIndexed,
			LastCheckpoint: stream.LastCheckpoint.Format(TSFmt),
			ServerID:       stream.ServerID,
		}
		if stream.BintrailID.Valid && stream.BintrailID.String != "" {
			jstr.BintrailID = &stream.BintrailID.String
		}
		if stream.GTIDSet.Valid && stream.GTIDSet.String != "" {
			jstr.GTIDSet = &stream.GTIDSet.String
		}
		if stream.LastEventTime.Valid {
			s := stream.LastEventTime.Time.Format(TSFmt)
			jstr.LastEventTime = &s
		}
		jstr.Continuity.Status = ContinuityStatus(stream, nil)
		if stream.GapLostAt.Valid {
			jstr.GapLost = &jsonGapLost{At: stream.GapLostAt.Time.Format(TSFmt), Detail: stream.GapLostDetail.String}
		}
		if stream.SourceHealth.Valid && stream.SourceHealth.String != "" {
			jstr.SourceHealth = json.RawMessage(stream.SourceHealth.String)
		}
		if skips, ok := stream.ParseCaptureSkips(); ok {
			ch := &jsonCaptureHealth{Status: "ok"}
			if total := totalCaptureSkips(skips); total > 0 {
				ch.Status = "degraded"
				ch.TotalSkipped = total
				ch.LastSkipAt = lastCaptureSkip(skips).Format(TSFmt)
				ch.Skipped = make(map[string]jsonSkipStat, len(skips))
				for r, st := range skips {
					if st.Count > 0 {
						ch.Skipped[r] = jsonSkipStat{
							Count:             st.Count,
							LastAt:            st.LastAt.Format(TSFmt),
							LastFile:          st.LastFile,
							LastPos:           st.LastPos,
							LastStatementType: st.LastStatementType,
							LastConnectionID:  st.LastConnectionID,
						}
					}
				}
			}
			jstr.CaptureHealth = ch
		}
		out.Stream = jstr
	} else if streamErr != nil {
		// stream_state could not be READ (nil stream + error) — surface it as a distinct
		// "unavailable" verdict so the omitted Stream section is not misread as "no loss".
		out.StreamError = &jsonStreamError{
			Continuity: jsonContinuity{Status: ContinuityStatus(nil, streamErr)},
			Error:      streamErr.Error(),
		}
	}
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
			TotalEvents:    coverage.TotalEvents + coverage.ArchiveTotalRows,
			LiveEvents:     coverage.TotalEvents,
			ArchivedEvents: coverage.ArchiveTotalRows,
			IndexSizeBytes: coverage.IndexSizeBytes,
			SchemaChanges:  coverage.SchemaChanges,
			UncoveredDDLs:  coverage.UncoveredDDLs,
		}
		if coverage.IndexSizeBytes > 0 {
			jc.IndexSizeHuman = formatBytes(coverage.IndexSizeBytes)
		}

		// Effective earliest: archive may extend further back than live data.
		earliest := coverage.EarliestEvent
		if coverage.ArchiveEarliestHour.Valid &&
			(!earliest.Valid || coverage.ArchiveEarliestHour.Time.Before(earliest.Time)) {
			earliest = coverage.ArchiveEarliestHour
		}
		if earliest.Valid {
			s := earliest.Time.Format(TSFmt)
			jc.EarliestEvent = &s
		}
		if coverage.LatestEvent.Valid {
			s := coverage.LatestEvent.Time.Format(TSFmt)
			jc.LatestEvent = &s
		}
		if coverage.ArchiveEarliestHour.Valid {
			s := coverage.ArchiveEarliestHour.Time.Format(TSFmt)
			jc.ArchiveEarliestEvent = &s
		}
		out.Coverage = jc
	}

	for _, b := range baselines {
		jb := jsonBaseline{
			SnapshotTime: b.SnapshotTime.Format(TSFmt),
			Database:     b.Database,
			Table:        b.Table,
			Size:         b.Size,
		}
		if b.Size > 0 {
			jb.SizeHuman = formatBytes(b.Size)
		}
		if b.BinlogFile != "" {
			jb.BinlogFile = &b.BinlogFile
		}
		if b.BinlogPos != 0 {
			jb.BinlogPos = &b.BinlogPos
		}
		if b.GTIDSet != "" {
			jb.GTIDSet = &b.GTIDSet
		}
		jb.Staleness = string(b.Staleness)
		out.Baselines = append(out.Baselines, jb)
	}
	out.BaselineStaleness = string(OverallBaselineStaleness(baselines))
	if out.BaselineStaleness == "" && baselinesUnavailable {
		out.BaselineStaleness = string(BaselineUnknown)
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// writeBaselines writes the baselines section to a text status report.
func writeBaselines(w io.Writer, baselines []BaselineInfo) {
	if len(baselines) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "=== Baselines ===")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SNAPSHOT\tDATABASE\tTABLE\tSIZE\tBINLOG_FILE\tBINLOG_POS\tGTID\tSTALENESS")
	fmt.Fprintln(tw, "────────\t────────\t─────\t────\t───────────\t──────────\t────\t─────────")
	// The ⚠ glyph is reserved for rows the banner keys on — each table's
	// NEWEST snapshot. A superseded snapshot past coverage is routine on a
	// healthy retention cadence (the console's rule too); it still reads
	// "broken" honestly, just not as an alarm.
	newestOf := make(map[string]time.Time, len(baselines))
	for _, b := range baselines {
		k := b.Database + "." + b.Table
		if b.SnapshotTime.After(newestOf[k]) {
			newestOf[k] = b.SnapshotTime
		}
	}
	for _, b := range baselines {
		binlogFile := "-"
		if b.BinlogFile != "" {
			binlogFile = b.BinlogFile
		}
		binlogPos := "-"
		if b.BinlogPos != 0 {
			binlogPos = strconv.FormatInt(b.BinlogPos, 10)
		}
		gtid := "-"
		if b.GTIDSet != "" {
			gtid = Truncate(b.GTIDSet, 40)
		}
		size := "-"
		if b.Size > 0 {
			size = formatBytes(b.Size)
		}
		staleness := "-"
		if b.Staleness != "" {
			staleness = string(b.Staleness)
			if b.Staleness == BaselineBroken && newestOf[b.Database+"."+b.Table].Equal(b.SnapshotTime) {
				staleness = "⚠ broken"
			}
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			b.SnapshotTime.Format(TSFmt),
			b.Database, b.Table, size,
			binlogFile, binlogPos, gtid, staleness)
	}
	tw.Flush()

	// Continuity-banner-style loud line: a table whose NEWEST baseline
	// predates delta coverage cannot be fully restored through that hole, and
	// waiting for restore time to find out is the failure mode #1193 exists
	// to remove.
	// The check being DISARMED is itself a finding: a bare "unknown" in the
	// column reads as a cosmetic gap, while it actually means a broken
	// restore window would not be detected here (#1219 makes this the routine
	// verdict for below-floor snapshots on multi-source indexes; an
	// unreadable floor lands here too).
	if OverallBaselineStaleness(baselines) == BaselineUnknown {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== ⚠ BASELINE STALENESS NOT EVALUABLE ===")
		fmt.Fprintln(w, "The delta-coverage floor could not be established for at least one table:")
		fmt.Fprintln(w, "an index serving more than one source cannot attribute archived coverage to")
		fmt.Fprintln(w, "the source that owns a baseline, and an unreadable index yields the same.")
		fmt.Fprintln(w, "A broken restore window would NOT be detected here — see")
		fmt.Fprintln(w, "docs/rotation-and-status.md (Baseline staleness).")
	}
	if OverallBaselineStaleness(baselines) == BaselineBroken {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "=== ⚠ BASELINE STALE — FULL-TABLE RESTORE BROKEN ===")
		fmt.Fprintln(w, "The newest baseline for at least one table predates the oldest available")
		fmt.Fprintln(w, "delta coverage: reconstructing those tables through the missing window is")
		fmt.Fprintln(w, "impossible. Take a fresh baseline (bintrail dump + bintrail baseline).")
	}
}
