package console

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// handleViewsSQL serves GET /api/views.sql: the same DuckDB view definitions
// `bintrail views` generates, with the paths resolved from the selected
// server's bundle.
//
// The point of the surface is what it is NOT. The console executes no SQL and
// gains no query engine here — it hands the operator a text file their own
// DuckDB runs, in their own process, on their own machine, against their own
// Parquet. That is why the "unrestricted SQL over the lake" the UI advertises
// costs nothing to secure: there is no sandbox to escape, no timeout to tune
// and no result cap to argue about, because none of it runs in the daemon.
//
// Deliberately NOT audited. ext.Record's contract covers surfaces that serve
// historical ROW DATA; this emits view definitions — paths and column names,
// with no row ever read — the same metadata-only class as `status`. Recording
// it would report a data access that did not happen.
func (s *Server) handleViewsSQL(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}

	// An active data profile refuses this file, exactly as it refuses the
	// baseline listing (#1075). The reasoning is the same and it is not about
	// the bytes: RBAC redaction happens in the console's own read path, and this
	// file is a map straight past it — a ready-made schema over the unredacted
	// Parquet the profile exists to withhold. Whether the session ALSO has
	// filesystem or S3 access is not the console's judgement to make; handing
	// over the map is.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "views")
		writeJSONError(w, http.StatusForbidden,
			"the DuckDB view file is unavailable while an access-control profile is active — "+
				"it maps directly onto the unredacted Parquet files")
		return
	}
	if b.noArchive {
		writeJSONError(w, http.StatusNotFound,
			"archive access is disabled for this server, so there is no Parquet layout to describe")
		return
	}

	in := views.Input{
		GeneratedAt: time.Now().UTC(),
		Version:     s.version,
	}
	in.ArchiveSources = consoleArchiveSources(r.Context(), b.db)
	if b.baselineSrc != "" {
		in.BaselineSource = b.baselineSrc
		files, err := reconstruct.ListBaselines(r.Context(), b.baselineSrc)
		if err != nil {
			// Configured but unreadable is an upstream fault worth naming, and
			// the same 502 the baseline listing returns for it. Degrading to
			// "archives only" would silently hand over a file missing every
			// state view the operator came for.
			writeJSONError(w, http.StatusBadGateway, "list baselines: "+err.Error())
			return
		}
		if len(files) > 0 {
			newest := files[0].SnapshotTime // ListBaselines returns newest first
			in.BaselineSnapshot = newest
			for _, f := range files {
				if !f.SnapshotTime.Equal(newest) {
					continue
				}
				in.Baselines = append(in.Baselines, views.BaselineTable{
					Schema: f.Schema, Table: f.Table, Path: f.Path,
				})
			}
		}
	}

	if len(in.ArchiveSources) == 0 && len(in.Baselines) == 0 {
		// Nothing to describe. A file of comments explaining that would be a
		// worse answer than the UI simply not offering the button, which is
		// what the matching capability arranges.
		writeJSONError(w, http.StatusNotFound,
			"this server has no archived partitions and no baseline snapshot yet — nothing to generate views over")
		return
	}

	sqlText := views.Generate(in)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="views.sql"`)
	// The file names paths, not data, and it is regenerated from live state on
	// every request — a cached copy would quietly describe a layout that has
	// since rotated.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(sqlText)); err != nil {
		slog.Debug("console: client went away while receiving views.sql", "error", err)
	}
}

// consoleArchiveSources reads the selected server's archive registry,
// best-effort.
//
// A registry read failure degrades to "no archive sources" rather than failing
// the request: the baseline half of the file is still useful on its own, and the
// generated header states which archive sources were resolved — so an empty list
// shows up in the artifact itself rather than only in a log. Returns nil for a
// server whose connection is not open.
func consoleArchiveSources(ctx context.Context, db *sql.DB) []string {
	if db == nil {
		return nil
	}
	sources, err := query.ResolveArchiveSources(ctx, db)
	if err != nil {
		slog.Warn("console: could not resolve archive sources for views.sql; the file will describe baselines only",
			"error", err)
		return nil
	}
	return sources
}

// viewsAvailable reports whether the selected server has a Parquet layout worth
// generating views over — the capability the UI gates the download button on.
//
// It runs the SAME checks the handler does, in the same order, so the advertised
// capability cannot over-promise: a button that only 404s is a lie, and this
// codebase already refuses that trade for reconstruct and verify.
func (s *Server) viewsAvailable(r *http.Request, b *bundle) bool {
	if b == nil || b.noArchive || sessionRestricted(r) {
		return false
	}
	if b.baselineSrc != "" {
		return true
	}
	return len(consoleArchiveSources(r.Context(), b.db)) > 0
}
