package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/storage"
)

var restoreIndexCmd = &cobra.Command{
	Use:   "restore-index",
	Short: "Rebuild a lost index database from the Parquet archive tier",
	Long: `Turns the archive tier back into a working index — the recovery story
for the one stateful component that had none ("who backs up the backup"):

  1. Refuses an index that already holds events (restore-index is for a
     FRESH index — mixing a partial restore into a live one creates states
     nothing can reason about).
  2. Creates the index schema (same DDL as 'bintrail init') and
     re-partitions binlog_events to cover exactly the archived hours plus
     a forward horizon (one ALTER on the empty table — instant).
  3. Scans the Hive archive layout (--archive-dir or --archive-s3), bulk-
     loads every archived partition back into binlog_events, and rebuilds
     archive_state from the scan (it is a rebuildable cache by design).
  4. Restores schema_snapshots and server identity from the index-meta
     sidecar that rotation persists alongside the archives — when present.
  5. Reports what was and was NOT recovered, and the next steps.

Deliberately NOT recovered (and never persisted): stream_state and
index_state — a replication position that survived an index loss is stale,
and resuming from it would fake continuity. Restart the stream cleanly; the
continuity verdict then reports the seam honestly instead of pretending.

Example:
  bintrail restore-index \
    --index-dsn "root:pw@tcp(127.0.0.1:3306)/bintrail_index" \
    --archive-s3 s3://backups/bintrail --region us-east-1`,
	RunE: runRestoreIndex,
}

var (
	riIndexDSN   string
	riArchiveDir string
	riArchiveS3  string
	riRegion     string
	riBatch      int
	riPartitions int
	riFormat     string
)

func init() {
	f := restoreIndexCmd.Flags()
	f.StringVar(&riIndexDSN, "index-dsn", "", "DSN of the FRESH index MySQL database to rebuild into (required; refused if it already holds events)")
	f.StringVar(&riArchiveDir, "archive-dir", "", "Local root of the Hive archive layout")
	f.StringVar(&riArchiveS3, "archive-s3", "", "S3 URL of the archive layout (s3://bucket/prefix)")
	f.StringVar(&riRegion, "region", "", "AWS region for --archive-s3")
	f.IntVar(&riBatch, "batch-size", 5000, "Rows per INSERT batch while loading")
	f.IntVar(&riPartitions, "partitions", 48, "Forward partition horizon to create beyond the archived hours (same default as init)")
	f.StringVar(&riFormat, "format", "text", "Output format: text or json")
	_ = restoreIndexCmd.MarkFlagRequired("index-dsn")
	BindCommandEnv(restoreIndexCmd)
}

// restoreIndexReport is the honest inventory of a rebuild: what came back,
// what could not, and what the operator must do next.
type restoreIndexReport struct {
	EventsLoaded      int64    `json:"events_loaded"`
	FilesLoaded       int      `json:"files_loaded"`
	FailedFiles       []string `json:"failed_files,omitempty"`
	PartitionsCreated int      `json:"partitions_created"`
	ArchiveStateRows  int      `json:"archive_state_rows"`
	SnapshotsRestored int64    `json:"snapshots_restored"`
	ServersRestored   int64    `json:"servers_restored"`
	SidecarFound      bool     `json:"sidecar_found"`
	// NotRecovered lists state this rebuild cannot bring back — absence of
	// an entry is a recovery claim, so unknowns are listed, never omitted.
	NotRecovered []string `json:"not_recovered"`
	NextSteps    []string `json:"next_steps"`
}

// ExitError is the single exit decision for both output formats.
func (r *restoreIndexReport) ExitError() error {
	if len(r.FailedFiles) == 0 {
		return nil
	}
	return fmt.Errorf("restore-index: %d archive file(s) failed to load: %s", len(r.FailedFiles), strings.Join(r.FailedFiles, ", "))
}

func runRestoreIndex(cmd *cobra.Command, args []string) error {
	if riFormat != "text" && riFormat != "json" {
		return fmt.Errorf("invalid --format %q; must be text or json", riFormat)
	}
	if (riArchiveDir == "") == (riArchiveS3 == "") {
		return fmt.Errorf("exactly one of --archive-dir or --archive-s3 is required")
	}
	cfg, err := drivermysql.ParseDSN(riIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name")
	}
	ctx := cmd.Context()
	db, err := config.Connect(riIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer db.Close()

	if err := restoreIndexTargetEmpty(ctx, db, dbName); err != nil {
		return err
	}
	if err := indexer.CreateIndexTables(ctx, db, riPartitions, false, nil); err != nil {
		return err
	}

	// ── Scan the archive layout ────────────────────────────────────────────
	var files []archive.ScannedFile
	if riArchiveDir != "" {
		files, err = scanLocalArchive(riArchiveDir)
	} else {
		files, err = scanS3Archive(ctx, riArchiveS3, riRegion, false)
	}
	if err != nil {
		return fmt.Errorf("scan archives: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no archive files found under the given location — nothing to restore")
	}

	// ── Re-partition the empty table to cover the archived hours ──────────
	hours := map[time.Time]bool{}
	ids := map[string]bool{}
	for _, f := range files {
		ids[f.BintrailID] = true
		if d, ok := indexer.PartitionDate(f.PartitionName); ok {
			hours[d] = true
		}
	}
	partSQL, partCount := buildRestorePartitionSQL(dbName, hours, time.Now().UTC(), riPartitions)
	if _, err := db.ExecContext(ctx, partSQL); err != nil {
		return fmt.Errorf("re-partition binlog_events: %w", err)
	}

	report := &restoreIndexReport{PartitionsCreated: partCount}

	// ── Load every archived partition, rebuilding archive_state as we go ──
	var s3c *s3.Client
	if riArchiveS3 != "" {
		if s3c, err = storage.NewS3Client(ctx, riRegion); err != nil {
			return fmt.Errorf("init S3 client: %w", err)
		}
	}
	for _, f := range files {
		path := f.LocalPath
		if f.Backend == archive.BackendS3 {
			path, err = downloadS3ToTemp(ctx, s3c, f.S3Bucket, f.S3Key)
			if err != nil {
				report.FailedFiles = append(report.FailedFiles, "s3://"+f.S3Bucket+"/"+f.S3Key+": "+err.Error())
				continue
			}
		}
		n, lerr := archive.RestorePartition(ctx, db, path, riBatch)
		if f.Backend == archive.BackendS3 {
			os.Remove(path)
		}
		if lerr != nil {
			report.FailedFiles = append(report.FailedFiles, f.PartitionName+": "+lerr.Error())
			continue
		}
		report.EventsLoaded += n
		report.FilesLoaded++
		if err := recordRestoredArchive(ctx, db, f, n); err != nil {
			report.FailedFiles = append(report.FailedFiles, f.PartitionName+" (archive_state): "+err.Error())
			continue
		}
		report.ArchiveStateRows++
		if riFormat != "json" {
			fmt.Printf("loaded %s (%d rows)\n", f.PartitionName, n)
		}
	}

	// ── Sidecar: schema snapshots + server identity ────────────────────────
	if m := newestSidecar(ctx, s3c, ids); m != nil {
		report.SidecarFound = true
		snaps, servers, serr := archive.RestoreMetaSidecar(ctx, db, m)
		report.SnapshotsRestored, report.ServersRestored = snaps, servers
		if serr != nil {
			report.FailedFiles = append(report.FailedFiles, "index-meta sidecar: "+serr.Error())
		}
	}

	report.NotRecovered = append(report.NotRecovered,
		"stream_state (replication position — deliberately never persisted: resuming a stale position would fake continuity)",
		"index_state (per-file indexing ledger)")
	if !report.SidecarFound {
		report.NotRecovered = append(report.NotRecovered,
			"schema_snapshots + server identity (no index-meta sidecar found — archives predate #1196)")
		report.NextSteps = append(report.NextSteps, "run `bintrail snapshot` against the source to re-capture table schemas")
	}
	report.NextSteps = append(report.NextSteps,
		"restart the stream (`bintrail stream` / `bintrail-console watch`) from a fresh position — the continuity verdict will honestly report the capture seam",
		"run `bintrail archive reconcile` to double-check archive_state against the layout")

	exitErr := report.ExitError()
	if riFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			return errors.Join(err, exitErr)
		}
	} else {
		writeRestoreIndexText(report)
	}
	cmd.SilenceUsage = true
	return exitErr
}

// restoreIndexTargetEmpty refuses an index that already holds events —
// restore-index is for a fresh index only (the drill guard's sibling).
func restoreIndexTargetEmpty(ctx context.Context, db *sql.DB, dbName string) error {
	var exists int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'`, dbName).Scan(&exists); err != nil {
		return fmt.Errorf("probe index: %w", err)
	}
	if exists == 0 {
		return nil
	}
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM binlog_events LIMIT 1").Scan(&one)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return fmt.Errorf("probe binlog_events: %w", err)
	default:
		return fmt.Errorf("the index already holds events — restore-index only rebuilds a FRESH index; point --index-dsn at a new, empty database")
	}
}

// buildRestorePartitionSQL builds the single ALTER that re-partitions the
// EMPTY binlog_events to exactly the archived hours plus a forward horizon —
// instant on an empty table, and it avoids the fragile arithmetic of
// reorganizing partitions backwards. Returns the statement and the number of
// named partitions (p_future excluded).
func buildRestorePartitionSQL(dbName string, archiveHours map[time.Time]bool, now time.Time, horizon int) (string, int) {
	all := map[time.Time]bool{}
	for h := range archiveHours {
		all[h.UTC().Truncate(time.Hour)] = true
	}
	start := now.Truncate(time.Hour)
	for i := 0; i < horizon; i++ {
		all[start.Add(time.Duration(i)*time.Hour)] = true
	}
	hours := make([]time.Time, 0, len(all))
	for h := range all {
		hours = append(hours, h)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i].Before(hours[j]) })
	defs := make([]string, 0, len(hours)+1)
	for _, h := range hours {
		defs = append(defs, fmt.Sprintf(
			"    PARTITION p_%s VALUES LESS THAN (TO_SECONDS('%s'))",
			h.Format("2006010215"), h.Add(time.Hour).Format("2006-01-02 15:04:05")))
	}
	defs = append(defs, "    PARTITION p_future VALUES LESS THAN MAXVALUE")
	return fmt.Sprintf("ALTER TABLE `%s`.`binlog_events` PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (\n%s\n)",
		dbName, strings.Join(defs, ",\n")), len(hours)
}

// recordRestoredArchive rebuilds this file's archive_state row (a rebuildable
// cache, #392) — same upsert shape as rotation's, with s3_uploaded_at stamped
// whenever the S3 object was just confirmed by the scan (the reconcile
// invariant: a confirmed object must be stamped or rotate refuses drops
// forever). min/max_event_ts stay NULL — the scan does not read row content;
// the planner falls back to the hour label, and `archive reconcile --deep`
// can refine later.
func recordRestoredArchive(ctx context.Context, db *sql.DB, f archive.ScannedFile, rows int64) error {
	var localPath, bucket, key, uploadedAt any
	if f.Backend == archive.BackendS3 {
		bucket, key, uploadedAt = f.S3Bucket, f.S3Key, f.LastModified.UTC()
	} else {
		localPath = f.LocalPath
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO archive_state
			(partition_name, bintrail_id, local_path, file_size_bytes, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			local_path = COALESCE(VALUES(local_path), local_path),
			s3_bucket = COALESCE(VALUES(s3_bucket), s3_bucket),
			s3_key = COALESCE(VALUES(s3_key), s3_key),
			s3_uploaded_at = COALESCE(VALUES(s3_uploaded_at), s3_uploaded_at)`,
		f.PartitionName, f.BintrailID, localPath, f.SizeBytes, rows, bucket, key, uploadedAt)
	return err
}

func downloadS3ToTemp(ctx context.Context, client *s3.Client, bucket, key string) (string, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()
	tmp, err := os.CreateTemp("", "bintrail-restore-*.parquet")
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(tmp, out.Body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// newestSidecar finds the newest index-meta sidecar across the scanned
// bintrail_ids (each source directory carries its own; the sidecar holds a
// FULL table dump, so restoring more than one would duplicate rows).
func newestSidecar(ctx context.Context, s3c *s3.Client, ids map[string]bool) *archive.MetaSidecar {
	var newest *archive.MetaSidecar
	for id := range ids {
		var m *archive.MetaSidecar
		if riArchiveDir != "" {
			path := filepath.Join(riArchiveDir, "bintrail_id="+id, archive.MetaSidecarName)
			got, err := archive.ReadMetaSidecar(path)
			if err != nil {
				if !os.IsNotExist(err) {
					fmt.Fprintf(os.Stderr, "warning: unreadable sidecar %s: %v\n", path, err)
				}
				continue
			}
			m = got
		} else {
			bucket, prefix, err := storage.ParseS3URL(riArchiveS3)
			if err != nil {
				continue
			}
			key := strings.TrimSuffix(prefix, "/")
			if key != "" {
				key += "/"
			}
			key += "bintrail_id=" + id + "/" + archive.MetaSidecarName
			path, err := downloadS3ToTemp(ctx, s3c, bucket, key)
			if err != nil {
				continue // absent sidecar is the routine pre-#1196 case
			}
			got, rerr := archive.ReadMetaSidecar(path)
			os.Remove(path)
			if rerr != nil {
				fmt.Fprintf(os.Stderr, "warning: unreadable sidecar s3://%s/%s: %v\n", bucket, key, rerr)
				continue
			}
			m = got
		}
		if newest == nil || m.WrittenAt.After(newest.WrittenAt) {
			newest = m
		}
	}
	return newest
}

func writeRestoreIndexText(r *restoreIndexReport) {
	fmt.Println("=== restore-index ===")
	fmt.Printf("Events loaded:      %d (%d file(s), %d partition(s) created)\n", r.EventsLoaded, r.FilesLoaded, r.PartitionsCreated)
	fmt.Printf("archive_state rows: %d\n", r.ArchiveStateRows)
	if r.SidecarFound {
		fmt.Printf("Sidecar restored:   %d schema-snapshot row(s), %d server identity row(s)\n", r.SnapshotsRestored, r.ServersRestored)
	}
	for _, f := range r.FailedFiles {
		fmt.Printf("FAILED: %s\n", f)
	}
	fmt.Println("NOT recovered:")
	for _, s := range r.NotRecovered {
		fmt.Printf("  - %s\n", s)
	}
	fmt.Println("Next steps:")
	for _, s := range r.NextSteps {
		fmt.Printf("  - %s\n", s)
	}
}
