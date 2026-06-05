package query

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// ResolveArchiveSources auto-discovers Parquet archive source paths from the
// archive_state table. For each distinct bintrail_id it returns the base path
// (local directory or S3 URL) that can be passed to parquetquery.Fetch.
//
// Local paths are preferred over S3 when the directory exists on disk AND
// holds at least one .parquet file — an empty local tree (files pruned after
// upload) falls back to the S3 copy instead of shadowing it (#383). A
// registered source is never omitted from the result: the planner counts
// archived hours straight from archive_state, so omission would make strict
// mode (#377) silently miss the coverage hole.
// Returns nil when no archives are configured, the table does not exist, or
// db is nil.
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
		// archive_state may not exist in older indexes (table-not-found is
		// expected). Other errors (permission denied, timeout) are unexpected.
		slog.Warn("could not query archive_state", "error", err)
		return nil
	}
	defer rows.Close()

	var sources []string
	for rows.Next() {
		var bintrailID string
		var localPath, s3Bucket, s3Key sql.NullString
		if err := rows.Scan(&bintrailID, &localPath, &s3Bucket, &s3Key); err != nil {
			slog.Warn("could not scan archive_state row", "error", err)
			continue
		}

		var localBase string
		if localPath.Valid && localPath.String != "" {
			localBase = extractBasePath(localPath.String)
		}
		var s3Source string
		if s3Bucket.Valid && s3Bucket.String != "" && s3Key.Valid && s3Key.String != "" {
			if base := extractBasePath(s3Key.String); base != "" {
				s3Source = "s3://" + s3Bucket.String + "/" + base
			}
		}

		// Prefer the local copy only when it actually holds data. A base
		// dir that exists but contains no .parquet files — the "archive
		// locally → upload to S3 → prune the local files, keep the tree"
		// cleanup pattern; rotate writes BOTH paths into the same
		// archive_state row — must not shadow a healthy S3 copy (#383).
		localExists := false
		localUsable := false
		var localRootErr error
		if localBase != "" {
			if _, err := os.Stat(localBase); err == nil {
				localExists = true
				localUsable, localRootErr = localBaseHasParquet(localBase)
			}
		}

		switch {
		case localUsable:
			sources = append(sources, localBase)
		case s3Source != "":
			// Warn only for the surprising cases: an UNREADABLE base (a
			// real local misconfiguration the fallback would otherwise
			// hide indefinitely) or an existing-but-fileless tree (the
			// shadow case). A fully-pruned local path falling back to S3
			// is the normal post-cleanup state and stays quiet, as before.
			switch {
			case localRootErr != nil:
				slog.Warn("local archive base is unreadable; falling back to S3",
					"bintrail_id", bintrailID, "local_base", localBase, "s3_source", s3Source, "error", localRootErr)
			case localExists:
				slog.Warn("local archive base has no parquet files; falling back to S3",
					"bintrail_id", bintrailID, "local_base", localBase, "s3_source", s3Source)
			}
			sources = append(sources, s3Source)
		case localBase != "":
			// NEVER omit a registered source: the planner counts these
			// hours as covered straight from archive_state (independent of
			// this list), so dropping the source would leave strict mode
			// (#377) nothing to fail on — a silently incomplete result.
			// Keep the unusable local base; the fetch fails loud instead
			// (DuckDB "No files found" / stat error).
			sources = append(sources, localBase)
		}
		// A row with neither a parseable local base nor S3 columns
		// contributes nothing, as before.
	}
	if err := rows.Err(); err != nil {
		slog.Warn("archive_state iteration error", "error", err)
	}

	return sources
}

// errFoundParquet is the sentinel localBaseHasParquet uses to stop the walk
// at the first hit.
var errFoundParquet = errors.New("found parquet")

// localBaseHasParquet reports whether base contains at least one .parquet
// file anywhere under it (layout-agnostic — fixtures place files directly
// under the base, rotate uses event_date=/event_hour= subtrees). It is a
// routing HINT, not a correctness gate: parquetquery.Fetch is the real
// fail-loud guard, so races between this check and the fetch (files pruned
// in between) are harmless — worst case the fetch errors and strict mode
// (#377) reports it.
//
// rootErr is non-nil when the BASE ITSELF could not be walked (EACCES,
// not-a-dir, broken symlink): callers must distinguish "unreadable" from
// "legitimately pruned" — silently demoting a permission problem to the
// S3 fallback would hide a real local misconfiguration indefinitely.
// Unreadable entries DEEPER in the tree are skipped (a routing hint must
// not abort on one bad leaf); the worst case there is a false "fileless"
// that the keep-or-fallback routing handles safely.
func localBaseHasParquet(base string) (found bool, rootErr error) {
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == base {
				// Root unreadable — propagate, don't swallow.
				return walkErr
			}
			// Unreadable entry deeper in: skip it, keep scanning.
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".parquet") {
			return errFoundParquet
		}
		return nil
	})
	if errors.Is(err, errFoundParquet) {
		return true, nil
	}
	return false, err
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
