package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// archiveCmd is the parent for archive-maintenance subcommands (the
// `config init` parent+sub precedent).
var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive maintenance commands",
}

var archiveReconcileCmd = &cobra.Command{
	Use:   "reconcile",
	Short: "Re-sync archive_state with the Parquet files actually on disk / in S3",
	Long: `Treats archive_state as a rebuildable cache over the self-describing
Hive-partitioned archive layout (#392). Scans the given backends for
bintrail_id=<uuid>/event_date=<d>/event_hour=<h>/*.parquet files, derives the
registry row each file implies, and diffs against archive_state:

  - files without rows   → --repair re-registers them (restores archive
                           auto-discovery and planner coverage after an
                           index rebuild)
  - rows without files   → reported; --prune deletes them (registry rows
                           only — data files are NEVER touched)
  - metadata drift       → --repair updates (file size always; row counts
                           only under --deep, which reads Parquet footers)

The default is a DRY-RUN that prints the drift and exits non-zero when any
exists — safe to run from cron as a drift monitor.

Safety rules:
  - a row is only a prune candidate when EVERY backend it references was
    scanned by this invocation and came up empty; a row referencing S3
    during a --archive-dir-only run is reported as unverified, never pruned
  - rows younger than --prune-min-age are never pruned (a concurrent
    rotate may still be mid-write)
  - repair is backend-scoped: a local-only run never touches S3 columns

Examples:

  # cron drift monitor (read-only; non-zero exit on drift)
  bintrail archive reconcile --index-dsn "$IDX" \
    --archive-dir /var/lib/bintrail/archives --archive-s3 s3://bkt/archives/

  # rebuild the registry after an index loss
  bintrail archive reconcile --index-dsn "$IDX" --archive-s3 s3://bkt/archives/ --repair

  # also drop registrations whose files are gone from BOTH backends
  bintrail archive reconcile --index-dsn "$IDX" \
    --archive-dir /var/lib/bintrail/archives --archive-s3 s3://bkt/archives/ \
    --repair --prune`,
	RunE: runArchiveReconcile,
}

var (
	arcIndexDSN    string
	arcDir         string
	arcS3          string
	arcRegion      string
	arcRepair      bool
	arcPrune       bool
	arcDeep        bool
	arcPruneMinAge time.Duration
	arcFormat      string
)

func init() {
	archiveReconcileCmd.Flags().StringVar(&arcIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	archiveReconcileCmd.Flags().StringVar(&arcDir, "archive-dir", "", "Local archive root to scan (the directory given to rotate --archive-dir)")
	archiveReconcileCmd.Flags().StringVar(&arcS3, "archive-s3", "", "S3 archive root to scan (e.g. s3://bucket/prefix/); uses the standard AWS credential chain")
	archiveReconcileCmd.Flags().StringVar(&arcRegion, "region", "", "AWS region (default: from AWS_REGION env var or ~/.aws/config)")
	archiveReconcileCmd.Flags().BoolVar(&arcRepair, "repair", false, "Execute inserts/updates that bring archive_state in line with the scanned files")
	archiveReconcileCmd.Flags().BoolVar(&arcPrune, "prune", false, "Delete registry rows whose every referenced backend was scanned and holds no file (data files are never touched)")
	archiveReconcileCmd.Flags().BoolVar(&arcDeep, "deep", false, "Also verify row counts (reads Parquet footers — one metadata GET per S3 object)")
	archiveReconcileCmd.Flags().DurationVar(&arcPruneMinAge, "prune-min-age", time.Hour, "Never prune rows whose archived_at is younger than this (concurrent-rotate safety margin)")
	archiveReconcileCmd.Flags().StringVar(&arcFormat, "format", "text", "Output format: text or json")
	_ = archiveReconcileCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(archiveReconcileCmd)
	archiveCmd.AddCommand(archiveReconcileCmd)
	rootCmd.AddCommand(archiveCmd)
}

func runArchiveReconcile(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(arcFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", arcFormat)
	}
	if arcDir == "" && arcS3 == "" {
		return fmt.Errorf("nothing to scan: pass --archive-dir and/or --archive-s3")
	}
	ctx := cmd.Context()

	var files []archive.ScannedFile
	if arcDir != "" {
		local, err := scanLocalArchive(arcDir)
		if err != nil {
			return fmt.Errorf("scan --archive-dir: %w", err)
		}
		files = append(files, local...)
	}
	if arcS3 != "" {
		remote, err := scanS3Archive(ctx, arcS3, arcRegion, arcDeep)
		if err != nil {
			return fmt.Errorf("scan --archive-s3: %w", err)
		}
		files = append(files, remote...)
	}

	db, err := config.Connect(arcIndexDSN)
	if err != nil {
		return fmt.Errorf("connect index database: %w", err)
	}
	defer db.Close()

	rows, err := loadArchiveStateRows(ctx, db)
	if err != nil {
		return fmt.Errorf("load archive_state: %w", err)
	}

	report := archive.Diff(files, rows, archive.DiffOptions{
		ScannedLocal: arcDir != "",
		ScannedS3:    arcS3 != "",
		Deep:         arcDeep,
		PruneMinAge:  arcPruneMinAge,
		Now:          time.Now().UTC(),
	})

	executed, execErrs := executeReconcileActions(ctx, db, report.Actions, arcRepair, arcPrune)

	if err := writeReconcileReport(os.Stdout, arcFormat, &report, executed, execErrs, arcRepair, arcPrune); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	if len(execErrs) > 0 {
		return fmt.Errorf("%d reconcile action(s) failed", len(execErrs))
	}
	if !arcRepair && !arcPrune {
		// Dry-run: non-zero exit on any drift (the cron monitor contract).
		return report.Err()
	}
	// Execute mode: exit 0 ⟺ no unaddressed drift remains. EVERY action
	// this invocation's flags didn't execute counts — --prune without
	// --repair must not silently mask insert/update drift the dry-run
	// would have flagged (and vice versa).
	pendingRepairs, pendingPrunes := 0, 0
	if !arcRepair {
		pendingRepairs = report.Inserts + report.Updates
	}
	if !arcPrune {
		pendingPrunes = report.Prunes
	}
	if pendingRepairs+pendingPrunes+report.SkippedUnverified+report.SkippedRecent > 0 {
		return fmt.Errorf("drift remains: %d insert/update(s) (need --repair), %d prune candidate(s) (need --prune), %d unverified, %d too recent",
			pendingRepairs, pendingPrunes, report.SkippedUnverified, report.SkippedRecent)
	}
	return nil
}

// scanLocalArchive walks root for Hive-layout parquet files. Footer row
// counts are always read locally — a local footer read is cheap and makes
// repaired rows feed status totals correctly.
func scanLocalArchive(root string) ([]archive.ScannedFile, error) {
	var out []archive.ScannedFile
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == root {
				return walkErr
			}
			slog.Warn("reconcile: skipping unreadable entry", "path", path, "error", walkErr)
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".parquet") {
			return nil
		}
		id, part := parseArchivePath(path)
		if id == "" {
			slog.Debug("reconcile: parquet outside the archive layout, ignoring", "path", path)
			return nil
		}
		info, err := d.Info()
		if err != nil {
			slog.Warn("reconcile: cannot stat file, skipping", "path", path, "error", err)
			return nil
		}
		f := archive.ScannedFile{
			PartitionName: part, BintrailID: id, Backend: archive.BackendLocal,
			LocalPath: path, SizeBytes: info.Size(), LastModified: info.ModTime().UTC(),
		}
		if n, err := localParquetRowCount(path, info.Size()); err == nil {
			f.RowCount = sql.NullInt64{Int64: n, Valid: true}
		} else {
			slog.Warn("reconcile: cannot read parquet footer, row_count left unset", "path", path, "error", err)
		}
		out = append(out, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func localParquetRowCount(path string, size int64) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	pf, err := parquet.OpenFile(f, size)
	if err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}

// scanS3Archive lists the prefix for Hive-layout parquet objects. Size and
// LastModified come free with the listing; row counts cost one metadata
// read per object and are only fetched under --deep.
func scanS3Archive(ctx context.Context, s3URL, region string, deep bool) ([]archive.ScannedFile, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewS3Client(ctx, region)
	if err != nil {
		return nil, err
	}

	var out []archive.ScannedFile
	var token *string
	for {
		page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: &bucket, Prefix: &prefix, ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil || !strings.HasSuffix(*obj.Key, ".parquet") {
				continue
			}
			id, part := parseArchivePath(*obj.Key)
			if id == "" {
				slog.Debug("reconcile: s3 parquet outside the archive layout, ignoring", "key", *obj.Key)
				continue
			}
			f := archive.ScannedFile{
				PartitionName: part, BintrailID: id, Backend: archive.BackendS3,
				S3Bucket: bucket, S3Key: *obj.Key,
			}
			if obj.Size != nil {
				f.SizeBytes = *obj.Size
			}
			if obj.LastModified != nil {
				f.LastModified = obj.LastModified.UTC()
			}
			if deep {
				if n, err := s3ParquetRowCount(ctx, bucket, *obj.Key); err == nil {
					f.RowCount = sql.NullInt64{Int64: n, Valid: true}
				} else {
					slog.Warn("reconcile: cannot read s3 parquet footer, row_count left unset",
						"key", *obj.Key, "error", err)
				}
			}
			out = append(out, f)
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
	}
	return out, nil
}

// s3ParquetRowCount reads num_rows from a Parquet footer in S3 via DuckDB
// httpfs (the ReadParquetMetadataAny pattern in internal/baseline).
func s3ParquetRowCount(ctx context.Context, bucket, key string) (int64, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return 0, fmt.Errorf("load httpfs extension: %w", err)
	}
	duckdbutil.EnableS3CredentialChain(ctx, db)
	safe := strings.ReplaceAll("s3://"+bucket+"/"+key, "'", "''")
	var n int64
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT num_rows FROM parquet_file_metadata('%s')", safe)).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func loadArchiveStateRows(ctx context.Context, db *sql.DB) ([]archive.StateRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT partition_name, bintrail_id, local_path, file_size_bytes,
		       row_count, s3_bucket, s3_key, s3_uploaded_at, archived_at
		FROM archive_state
		WHERE bintrail_id IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []archive.StateRow
	for rows.Next() {
		var r archive.StateRow
		if err := rows.Scan(&r.PartitionName, &r.BintrailID, &r.LocalPath, &r.FileSizeBytes,
			&r.RowCount, &r.S3Bucket, &r.S3Key, &r.S3UploadedAt, &r.ArchivedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// reconcileColumns is the allowlist of archive_state columns an action may
// write — the FieldChange column names come from internal/archive, never
// from user input, but the allowlist makes that contract explicit.
var reconcileColumns = map[string]bool{
	"local_path": true, "file_size_bytes": true, "row_count": true,
	"s3_bucket": true, "s3_key": true, "s3_uploaded_at": true,
}

// executeReconcileActions applies insert/update actions under --repair and
// prune actions under --prune. Returns the number executed and any errors
// (one entry per failed action; execution continues past failures so a
// single bad row doesn't abort a large repair).
func executeReconcileActions(ctx context.Context, db *sql.DB, actions []archive.Action, repair, prune bool) (int, []error) {
	executed := 0
	var errs []error
	for _, a := range actions {
		switch a.Kind {
		case archive.ActionInsert, archive.ActionUpdate:
			if !repair {
				continue
			}
			if err := applyUpsert(ctx, db, a); err != nil {
				errs = append(errs, fmt.Errorf("%s %s/%s: %w", a.Kind, a.PartitionName, a.BintrailID, err))
				continue
			}
			executed++
		case archive.ActionPrune:
			if !prune {
				continue
			}
			if _, err := db.ExecContext(ctx,
				`DELETE FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
				a.PartitionName, a.BintrailID); err != nil {
				errs = append(errs, fmt.Errorf("prune %s/%s: %w", a.PartitionName, a.BintrailID, err))
				continue
			}
			executed++
		}
	}
	return executed, errs
}

// applyUpsert writes one action's field changes. Inserts and updates share
// the INSERT … ON DUPLICATE KEY UPDATE shape (rotate's upsert model) so a
// concurrent rotate inserting the same key never races us into a duplicate
// error; only the action's own columns are written (backend-scoped).
func applyUpsert(ctx context.Context, db *sql.DB, a archive.Action) error {
	cols := []string{"partition_name", "bintrail_id"}
	vals := []any{a.PartitionName, a.BintrailID}
	var updates []string
	for _, c := range a.Changes {
		if !reconcileColumns[c.Column] {
			return fmt.Errorf("column %q is not reconcile-writable", c.Column)
		}
		cols = append(cols, c.Column)
		vals = append(vals, c.Value)
		updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", c.Column, c.Column))
	}
	q := fmt.Sprintf(
		"INSERT INTO archive_state (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		strings.Join(cols, ", "),
		strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", "),
		strings.Join(updates, ", "),
	)
	_, err := db.ExecContext(ctx, q, vals...)
	return err
}

// reconcileReportJSON is the --format json shape.
type reconcileReportJSON struct {
	Actions  []reconcileActionJSON `json:"actions"`
	Inserts  int                   `json:"inserts"`
	Updates  int                   `json:"updates"`
	Prunes   int                   `json:"prune_candidates"`
	Skipped  int                   `json:"skipped"`
	InSync   int                   `json:"in_sync"`
	Executed int                   `json:"executed"`
	Errors   []string              `json:"errors,omitempty"`
}

type reconcileActionJSON struct {
	Kind       string `json:"kind"`
	Partition  string `json:"partition"`
	BintrailID string `json:"bintrail_id"`
	Reason     string `json:"reason,omitempty"`
}

func writeReconcileReport(w io.Writer, format string, rep *archive.Report, executed int, execErrs []error, repair, prune bool) error {
	if format == "json" {
		j := reconcileReportJSON{
			Inserts: rep.Inserts, Updates: rep.Updates, Prunes: rep.Prunes,
			Skipped: rep.SkippedUnverified + rep.SkippedRecent, InSync: rep.InSync, Executed: executed,
		}
		for _, a := range rep.Actions {
			j.Actions = append(j.Actions, reconcileActionJSON{
				Kind: string(a.Kind), Partition: a.PartitionName, BintrailID: a.BintrailID, Reason: a.Reason,
			})
		}
		for _, e := range execErrs {
			j.Errors = append(j.Errors, e.Error())
		}
		return cliutil.OutputJSON(j)
	}

	mode := "dry-run"
	if repair || prune {
		mode = "execute"
	}
	fmt.Fprintf(w, "archive reconcile (%s): %d in sync, %d to insert, %d to update, %d prune candidate(s), %d skipped\n",
		mode, rep.InSync, rep.Inserts, rep.Updates, rep.Prunes, rep.SkippedUnverified+rep.SkippedRecent)
	for _, a := range rep.Actions {
		fmt.Fprintf(w, "  [%s] %s / %s — %s\n", a.Kind, a.PartitionName, a.BintrailID, a.Reason)
	}
	if repair || prune {
		fmt.Fprintf(w, "executed: %d action(s)\n", executed)
	}
	for _, e := range execErrs {
		fmt.Fprintf(w, "ERROR: %v\n", e)
	}
	return nil
}
