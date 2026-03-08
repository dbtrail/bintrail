package query

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"strings"
)

// ResolveArchiveSources auto-discovers Parquet archive source paths from the
// archive_state table. For each distinct bintrail_id it returns the base path
// (local directory or S3 URL) that can be passed to parquetquery.Fetch.
//
// Local paths are preferred over S3 when the directory exists on disk.
// Returns nil when no archives are configured or the table does not exist.
func ResolveArchiveSources(ctx context.Context, db *sql.DB) []string {
	if db == nil {
		return nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT bintrail_id,
		       MIN(local_path)  AS sample_local,
		       MIN(s3_bucket)   AS sample_bucket,
		       MIN(s3_key)      AS sample_key
		FROM archive_state
		WHERE bintrail_id IS NOT NULL
		GROUP BY bintrail_id`)
	if err != nil {
		// archive_state may not exist in older indexes.
		slog.Debug("could not query archive_state", "error", err)
		return nil
	}
	defer rows.Close()

	var sources []string
	for rows.Next() {
		var bintrailID string
		var localPath, s3Bucket, s3Key sql.NullString
		if err := rows.Scan(&bintrailID, &localPath, &s3Bucket, &s3Key); err != nil {
			slog.Debug("could not scan archive_state row", "error", err)
			continue
		}

		// Prefer local path if the base directory exists on disk.
		if localPath.Valid && localPath.String != "" {
			base := extractBasePath(localPath.String)
			if base != "" {
				if _, err := os.Stat(base); err == nil {
					sources = append(sources, base)
					continue
				}
			}
		}

		// Fall back to S3.
		if s3Bucket.Valid && s3Bucket.String != "" && s3Key.Valid && s3Key.String != "" {
			base := extractBasePath(s3Key.String)
			if base != "" {
				sources = append(sources, "s3://"+s3Bucket.String+"/"+base)
			}
		}
	}
	if err := rows.Err(); err != nil {
		slog.Debug("archive_state iteration error", "error", err)
	}

	return sources
}

// extractBasePath returns the portion of an archive file path up to and
// including the "bintrail_id=<uuid>" directory component.
//
// Example:
//
//	"/data/archives/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet"
//	→ "/data/archives/bintrail_id=abc-123"
//
//	"prefix/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet"
//	→ "prefix/bintrail_id=abc-123"
func extractBasePath(path string) string {
	const marker = "bintrail_id="
	idx := strings.Index(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	slashIdx := strings.Index(rest, "/")
	if slashIdx < 0 {
		return path // entire path is the base (no trailing components)
	}
	return path[:idx+len(marker)+slashIdx]
}
