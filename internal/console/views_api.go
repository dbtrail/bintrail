package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// errNoViewSources: the selected server has neither archived partitions nor a
// baseline snapshot — there is no Parquet layout to describe (views.sql) or to
// query (the SQL panel).
var errNoViewSources = errors.New("this server has no archived partitions and no baseline snapshot yet")

// buildViewsInput resolves the selected server's Parquet layout — archive
// sources from archive_state plus the NEWEST baseline snapshot's tables — into
// the generator input shared by GET /api/views.sql and the SQL panel. A
// baseline root that is configured but unlistable is an upstream fault worth
// naming, not a degrade: silently dropping the baseline half would hand over a
// layout missing every state view.
//
// portable selects which registered location names each archive. The download
// runs on the operator's machine, where this host's local copy does not exist,
// so it takes the S3 location whenever one is registered (#1456); the SQL panel
// runs in this daemon and keeps the local-first routing every other console
// read uses.
func (s *Server) buildViewsInput(ctx context.Context, b *bundle, portable bool) (views.Input, error) {
	in := views.Input{
		GeneratedAt: time.Now().UTC(),
		Version:     s.version,
	}
	var archiveErr error
	in.ArchiveSources, archiveErr = consoleArchiveSources(ctx, b.db, portable)
	in.PortableRouting = portable
	in.ArchiveDiscoveryFailed = archiveErr != nil
	if b.baselineSrc != "" {
		in.BaselineSource = b.baselineSrc
		files, err := reconstruct.ListBaselines(ctx, b.baselineSrc)
		if err != nil {
			return views.Input{}, fmt.Errorf("list baselines: %w", err)
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
		if archiveErr != nil {
			// Not "nothing archived": the registry could not be read, and
			// with no baseline half to carry the file there is nothing
			// honest to serve. Same 502-for-upstream-fault rule as an
			// unlistable baseline root.
			return views.Input{}, fmt.Errorf("read archive_state: %w", archiveErr)
		}
		return views.Input{}, errNoViewSources
	}
	return in, nil
}

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
			"the DuckDB view file is unavailable while an access-control profile is active: "+
				"it maps directly onto the unredacted Parquet files")
		return
	}
	if b.noArchive {
		writeJSONError(w, http.StatusNotFound,
			"archive access is disabled for this server, so there is no Parquet layout to describe")
		return
	}

	in, err := s.buildViewsInput(r.Context(), b, true)
	switch {
	case errors.Is(err, errNoViewSources):
		// Nothing to describe. A file of comments explaining that would be a
		// worse answer than the UI simply not offering the button, which is
		// what the matching capability arranges.
		writeJSONError(w, http.StatusNotFound,
			errNoViewSources.Error()+"; nothing to generate views over")
		return
	case err != nil:
		// Configured but unreadable is an upstream fault worth naming, and
		// the same 502 the baseline listing returns for it.
		writeJSONError(w, http.StatusBadGateway, err.Error())
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

// consoleArchiveSources reads the selected server's archive registry. Returns
// (nil, nil) for a server whose connection is not open.
//
// A registry read failure is returned, not swallowed: the caller decides
// whether the baseline half can still carry the file (then the header names
// the failure where the operator reads it) or nothing honest is left to serve.
// It is also logged, because the file is what leaves the host and the log is
// what stays.
//
// portable picks query.PortableArchiveSources (S3 wherever registered, for a
// file that leaves this host) over the local-first ResolveArchiveSources.
func consoleArchiveSources(ctx context.Context, db *sql.DB, portable bool) ([]string, error) {
	if db == nil {
		return nil, nil
	}
	resolve := query.ResolveArchiveSources
	if portable {
		resolve = query.PortableArchiveSources
	}
	sources, err := resolve(ctx, db)
	if err != nil {
		slog.Warn("console: could not resolve archive sources for views.sql",
			"error", err)
		return nil, err
	}
	return sources, nil
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
	// Either routing yields the same count; the portable read skips the
	// per-source filesystem walk, so it is the cheaper gate. A registry read
	// failure hides the button, as before: the handler would 502.
	sources, err := consoleArchiveSources(r.Context(), b.db, true)
	// sources is nil whenever err is set, so the err check is intent, not a
	// distinct branch: the gate must never say yes on a failed read.
	return err == nil && len(sources) > 0
}
