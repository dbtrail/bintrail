package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
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
// omitEvents is a PARAMETER rather than something the caller sets afterwards:
// NeedsS3 below asks whether the rendered file reads any s3:// path, and the
// events view is the half that reads the archives. Set after this returns, the
// gate ran with the zero value and a default download 502'd over an S3 variable
// its file never touches -- while the CLI, which knows before it asks, did not.
// The SQL panel passes false: it decides through OnlyViews.
func (s *Server) buildViewsInput(ctx context.Context, b *bundle, portable, omitEvents bool) (views.Input, error) {
	in := views.Input{
		GeneratedAt: time.Now().UTC(),
		Version:     s.version,
		OmitEvents:  omitEvents,
	}
	// The download CAN carry the hot leg now (the "Include the live index"
	// box), so the archives-only note names that box rather than a flag the
	// reader is not at a command line to pass. A server this console cannot
	// reach by host and port has no route at all, and says so instead.
	if consoleCanOfferLiveLeg(b) {
		in.LiveLegHowTo = liveLegHowTo
	} else {
		in.LiveLegUnavailable = true
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
			// Column types for the state views' decimal casts. Best-effort and
			// memoized per snapshot; serves the download and the SQL panel
			// alike, both of which reach the same Parquet through the same
			// views.
			s.resolveBaselineDecimals(ctx, &in)
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
	// After the layout is known: a server whose archives are all local reads
	// nothing through httpfs, so an unrelated S3 variable must not 502 its
	// page. Where the file does name s3:// paths, the store this daemon reads
	// from is the store the file must name, and an invalid value is an
	// upstream fault worth reporting rather than a file that points at AWS.
	if in.NeedsS3() {
		ep, err := storage.S3EndpointFromEnv()
		if err != nil {
			return views.Input{}, fmt.Errorf("S3 endpoint configuration: %w", err)
		}
		in.S3Endpoint = ep
		// The region for this layout, when it was actually DETECTED (#1462).
		// Not "the region our own reads use": those fall back to the ambient
		// one and are right to, since they fail here and loudly. A file that
		// leaves the host pins nothing rather than a guess.
		in.ArchiveRegion, in.RegionAmbiguous = s.archiveRegion(ctx, in)
	}
	return in, nil
}

// parseIncludeLive reads the include_live parameter.
//
// isTrue is the house convention for a boolean query parameter and stays that
// everywhere else, but it treats every unrecognized value as false, and here
// false is not a harmless default: the reader gets a 200 and an archives-only
// file whose own note tells them to tick the box they believe they ticked.
// "on" is what a bare HTML checkbox posts, so it is a value a client really
// sends. An unrecognized value is refused with the ones that work.
func parseIncludeLive(v string) (bool, error) {
	return parseStrictInclude("include_live", "add the live index leg", v)
}

// parseIncludeEvents reads the include_events parameter (#1535). Same strictness
// and for the same reason as include_live: a silently-false unrecognized value
// hands back a 200 and a file missing the very view the reader asked for, and
// the file's own note then tells them to ask for it.
func parseIncludeEvents(v string) (bool, error) {
	return parseStrictInclude("include_events", "add the events view", v)
}

func parseStrictInclude(name, does, v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "":
		return false, nil
	case "1", "true":
		return true, nil
	case "0", "false":
		return false, nil
	}
	return false, fmt.Errorf("%s=%q is not a value this route understands; "+
		"use %s=1 to %s, or leave it out", name, v, name, does)
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

	includeLive, err := parseIncludeLive(r.URL.Query().Get("include_live"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	includeEvents, err := parseIncludeEvents(r.URL.Query().Get("include_events"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	// The same refusal the CLI gives, for the same reason: the live leg hangs
	// on the events view, so asking for it without the view is a request the
	// route cannot honour. Refused rather than quietly upgraded — the events
	// view is the expensive one and turning it on unasked is a cost the reader
	// pays on every query.
	if includeLive && !includeEvents {
		writeJSONError(w, http.StatusBadRequest,
			"include_live adds a leg to the events view, which this file would not define: "+
				"pass include_events=1 with it")
		return
	}

	in, err := s.buildViewsInput(r.Context(), b, true, !includeEvents)
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

	// The hot leg, only when the reader asked for it (#1480). Opt-in and never
	// the default: it names the index by host and port in a file meant to be
	// shared, and a query against the two-leg view reads the live capture
	// index. Resolved AFTER the layout, so a server with nothing to describe
	// still 404s on the cheaper answer.
	if includeLive {
		li, err := resolveConsoleLiveIndex(r.Context(), b)
		var cfgErr *liveLegConfigError
		switch {
		case errors.As(err, &cfgErr):
			// This server cannot carry the leg however it is asked for.
			writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
			return
		case err != nil:
			// The index could not be asked. Same upstream-fault answer the
			// baseline listing gives, and never a file that silently drops
			// the half the reader ticked a box for.
			writeJSONError(w, http.StatusBadGateway, scrubDSNError(err, b.dsn))
			return
		}
		in.LiveIndex = li
		// A file that HAS the leg needs no note about how to add one.
		in.LiveLegHowTo = ""
	}

	// AFTER the live leg is resolved, since that leg can be the only thing
	// defining the events view. errNoViewSources above answers "this server has
	// nothing to describe"; this answers the narrower and newer case: sources
	// exist, but with the events view left out and no baseline snapshot to
	// build state views from, the file would carry no view at all. Served as a
	// 200 it looked like a successful download of an empty schema.
	if !in.RendersAnyView() {
		writeJSONError(w, http.StatusNotFound,
			"this would define no view at all: no baseline snapshot was found to build "+
				"state views from, and the change log is not included. Take a backup, or "+
				"tick \"Include the change log\" to get a view over the archived changes")
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
