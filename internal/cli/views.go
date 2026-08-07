package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

var viewsCmd = &cobra.Command{
	Use:   "views",
	Short: "Generate DuckDB view definitions over your Parquet archive layout",
	Long: `Write a .sql file of DuckDB views over the archives and baseline snapshots
this index already knows about, so any DuckDB — the CLI, a notebook, an
embedded process — can query the Parquet tier without hand-writing
read_parquet globs.

The generated file defines:

  events                     every archived binlog event across all archive
                             sources, with event_type decoded and the commit
                             timestamp typed
  state_<schema>_<table>     each table's contents as of the newest
                             discoverable baseline snapshot

bintrail only writes the text: it never opens DuckDB and never runs what it
prints. Nothing in the file writes, and no credentials appear in it — S3
access goes through DuckDB's credential chain, the same way bintrail's own
S3 reads do.

Archive sources come from the index's archive_state registry by default. Pass
--archive-dir/--archive-s3 with --bintrail-id to name one explicitly instead;
in that case --index-dsn is not needed at all.

The file is a snapshot of the layout, not a live binding. The event globs keep
picking up newly rotated partitions on their own, but the state views point at
one snapshot — regenerate after taking or refreshing a baseline.

Examples:
  # From the index's own registry, plus a local baseline directory
  bintrail views --index-dsn "..." --baseline-dir /data/baselines --out views.sql

  # Straight to stdout, piped into DuckDB
  bintrail views --index-dsn "..." --baseline-dir /data/baselines --out - | duckdb lake.db

  # Without an index: name the archive and baseline locations directly
  bintrail views --archive-s3 s3://bucket/archives/ --bintrail-id <uuid> \
    --baseline-s3 s3://bucket/baselines/ --out views.sql`,
	RunE: runViews,
}

var (
	vIndexDSN    string
	vArchiveDir  string
	vArchiveS3   string
	vBintrailID  string
	vRegion      string
	vBaselineDir string
	vBaselineS3  string
	vOut         string
	vNoBaselines bool
)

func init() {
	viewsCmd.Flags().StringVar(&vIndexDSN, "index-dsn", "", "DSN for the index MySQL database (used to discover archive sources; not needed with --archive-dir/--archive-s3)")
	viewsCmd.Flags().StringVar(&vArchiveDir, "archive-dir", "", "Local root directory of Parquet archives (requires --bintrail-id)")
	viewsCmd.Flags().StringVar(&vArchiveS3, "archive-s3", "", "S3 root URL prefix of Parquet archives (requires --bintrail-id; e.g. s3://bucket/prefix/)")
	viewsCmd.Flags().StringVar(&vBintrailID, "bintrail-id", "", "Server identity UUID (required when --archive-dir or --archive-s3 is set)")
	viewsCmd.Flags().StringVar(&vRegion, "region", "", "AWS region to pin in the generated S3 secret (default: resolved by the credential chain)")
	viewsCmd.Flags().StringVar(&vBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots")
	viewsCmd.Flags().StringVar(&vBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline Parquet snapshots (e.g. s3://bucket/baselines/)")
	viewsCmd.Flags().BoolVar(&vNoBaselines, "no-baselines", false, "Emit only the events view, skipping the baseline state views")
	viewsCmd.Flags().StringVar(&vOut, "out", "views.sql", "Output file, or - for stdout")
}

func runViews(cmd *cobra.Command, _ []string) error {
	if (vArchiveDir != "" || vArchiveS3 != "") && vBintrailID == "" {
		return fmt.Errorf("--bintrail-id is required when --archive-dir or --archive-s3 is set")
	}
	if vBaselineDir != "" && vBaselineS3 != "" {
		return fmt.Errorf("--baseline-dir and --baseline-s3 are mutually exclusive")
	}
	explicitArchives := vArchiveDir != "" || vArchiveS3 != ""
	if vIndexDSN == "" && !explicitArchives {
		return fmt.Errorf("--index-dsn is required (it is where archive sources are discovered); " +
			"or name a source directly with --archive-dir/--archive-s3 plus --bintrail-id")
	}

	in := views.Input{
		GeneratedAt: time.Now().UTC(),
		Version:     buildVersion,
		// Region is only meaningful for the S3 secret; a local-only layout
		// never emits the preamble that would use it.
		ArchiveRegion: vRegion,
	}

	var err error
	if explicitArchives {
		in.ArchiveSources = explicitArchiveSources()
	} else {
		in.ArchiveSources, err = discoverArchiveSources(cmd.Context(), vIndexDSN)
		if err != nil {
			return err
		}
	}

	if !vNoBaselines {
		if err := resolveBaselineViews(cmd.Context(), &in); err != nil {
			return err
		}
	}

	sql := views.Generate(in)
	if err := writeViewsOutput(cmd, sql); err != nil {
		return err
	}

	// Deliberately NOT audited. ext.Record's contract covers surfaces that serve
	// historical ROW DATA; this one emits view DEFINITIONS — paths and column
	// names, no row ever read — which puts it in the same metadata-only class as
	// `status`. Auditing it would report a data access that did not happen.
	if vOut != "-" {
		fmt.Fprintf(cmd.OutOrStdout(), "wrote %s (%d archive source(s), %d state view(s))\n",
			vOut, len(in.ArchiveSources), len(in.Baselines))
	}
	return nil
}

// explicitArchiveSources scopes the operator-named roots to this server's
// bintrail_id subdirectory, exactly like `query`'s equivalent — a view over the
// unscoped root would mix servers' events under one name.
func explicitArchiveSources() []string {
	var sources []string
	if vArchiveDir != "" {
		sources = append(sources, filepath.Join(vArchiveDir, "bintrail_id="+vBintrailID))
	}
	if vArchiveS3 != "" {
		sources = append(sources, strings.TrimSuffix(vArchiveS3, "/")+"/bintrail_id="+vBintrailID)
	}
	return sources
}

// discoverArchiveSources reads the archive_state registry. An empty result is
// not an error: an index whose partitions have never been archived legitimately
// has no archive tier yet, and the generated file says so in a comment rather
// than failing a command whose whole job is to describe what exists.
func discoverArchiveSources(ctx context.Context, dsn string) ([]string, error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to index DB: %w", err)
	}
	defer db.Close()
	sources, err := query.ResolveArchiveSources(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("resolve archive sources from archive_state: %w", err)
	}
	return sources, nil
}

// resolveBaselineViews picks the NEWEST discoverable snapshot and takes its
// tables.
//
// Only one snapshot: rows from two snapshots are two different points in time,
// and a view spanning them would present a state that never existed. Which
// snapshot is newest per table is not a per-table question here either —
// ListBaselines already skips incomplete snapshots (#467), and taking the single
// newest snapshot timestamp keeps every state view in the file consistent with
// every other one.
func resolveBaselineViews(ctx context.Context, in *views.Input) error {
	src := vBaselineDir
	if src == "" {
		src = vBaselineS3
	}
	if src == "" {
		return nil
	}
	in.BaselineSource = src

	files, err := reconstruct.ListBaselines(ctx, src)
	if err != nil {
		return fmt.Errorf("list baseline snapshots under %s: %w", src, err)
	}
	if len(files) == 0 {
		return nil
	}
	// ListBaselines returns newest snapshot first.
	newest := files[0].SnapshotTime
	in.BaselineSnapshot = newest
	for _, f := range files {
		if !f.SnapshotTime.Equal(newest) {
			continue
		}
		in.Baselines = append(in.Baselines, views.BaselineTable{
			Schema: f.Schema,
			Table:  f.Table,
			Path:   f.Path,
		})
	}
	return nil
}

// writeViewsOutput sends the generated SQL to --out, or to stdout for "-".
//
// A file is written whole via os.WriteFile rather than streamed: the generator
// already produced the complete text, and a partial file left behind by a
// mid-write failure is a file someone would paste into DuckDB.
func writeViewsOutput(cmd *cobra.Command, sql string) error {
	if vOut == "-" {
		_, err := fmt.Fprint(cmd.OutOrStdout(), sql)
		return err
	}
	if err := os.WriteFile(vOut, []byte(sql), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", vOut, err)
	}
	return nil
}
