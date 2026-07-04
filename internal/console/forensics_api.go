package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/forensics"
	"github.com/dbtrail/dbtrail/internal/query"
)

// whoChangedDefaultLimit/whoChangedMaxLimit bound POST /api/forensics/who-changed's
// limit. Every other console surface bounds its top end (events'
// eventsMaxLimit, activity queries at 1000 inside the forensics library
// itself); forensics.WhoChangedParams only floors an unset limit (mirrored
// here as the default, since that unexported constant isn't reachable from
// this package), so the handler must clamp the ceiling itself.
const (
	whoChangedDefaultLimit = 100
	whoChangedMaxLimit     = 1000
)

// forensicsCapabilitiesResponse flattens forensics.Capabilities (embedding
// promotes performance_schema/audit_log/server_info to top-level JSON keys,
// matching the SaaS wire contract) and adds the tailored setup guide plus
// whether the selected server even has a source connection to detect against.
type forensicsCapabilitiesResponse struct {
	forensics.Capabilities
	SetupGuide       *forensics.SetupGuide `json:"setup_guide,omitempty"`
	SourceConfigured bool                  `json:"source_configured"`
}

// forensicsUsersResponse is the wire view for GET /api/forensics/users — a
// MySQL user list for filter dropdowns.
type forensicsUsersResponse struct {
	Users []string `json:"users"`
}

// whoChangedRequest is the JSON body accepted by POST /api/forensics/who-changed.
type whoChangedRequest struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	PK     string `json:"pk"`
	Since  string `json:"since"`
	Until  string `json:"until"`
	Limit  int    `json:"limit"`
	Order  string `json:"order"`
}

// forensicsActivityRequest is the JSON body accepted by POST
// /api/forensics/activity. QueryType selects one of forensics'
// QueryUserActivity/QueryConnectionHistory/QueryDDLHistory.
type forensicsActivityRequest struct {
	QueryType string `json:"query_type"`
	User      string `json:"user"`
	Host      string `json:"host"`
	Schema    string `json:"schema"`
	Since     string `json:"since"`
	Until     string `json:"until"`
	Limit     int    `json:"limit"`
	Order     string `json:"order"`
}

// forensicsRefused checks the two gates every forensics endpoint shares —
// the entitlement seam (epic #701 D1) and the RBAC-profile refusal #708
// specifies for v1 (forensic output includes unredacted SQL text and session
// identity, which the redaction pipeline does not yet cover, matching the
// Verify/recover-cascade precedent) — writing the error response and
// reporting true when the caller must stop.
func (s *Server) forensicsRefused(w http.ResponseWriter) bool {
	if !forensics.Enabled() {
		writeJSONError(w, http.StatusForbidden, "forensics is not enabled in this build")
		return true
	}
	if s.rbacActive() {
		writeJSONError(w, http.StatusForbidden,
			"forensics isn't available while an access-control profile is active — forensic output includes unredacted SQL text and session identity")
		return true
	}
	return false
}

// errSourceNotConfigured is openForensicsSource's sentinel for "this server
// has no source connection set up at all" — distinct from any other error
// (which means a source IS configured but couldn't be reached: a genuine
// misconfiguration or outage the caller must surface, not silently relabel
// as "not configured"). Conflating the two used to leave an operator staring
// at "add a source connection" for a server that already has one. Encoding
// this as the error itself (rather than a separate bool alongside err) makes
// the fourth, illegal combination — "not configured" AND "some other error"
// — unrepresentable, instead of relying on every call site to check them in
// the right order.
var errSourceNotConfigured = errors.New("no source connection configured")

// openForensicsSource opens the selected server's source connection for the
// forensic data-source queries (performance_schema, audit log, mysql.user).
// Callers distinguish the three outcomes with errors.Is(err, errSourceNotConfigured)
// then a plain err != nil check. The returned db must be closed by the
// caller whenever err is nil.
func (s *Server) openForensicsSource(r *http.Request) (db *sql.DB, host string, err error) {
	e, found := s.selectedEntry(r)
	if !found || e.SourceDSN == "" {
		return nil, "", errSourceNotConfigured
	}
	db, cerr := config.Connect(e.SourceDSN)
	if cerr != nil {
		slog.Warn("forensics: source connect failed", "server", e.Name, "error", cerr)
		return nil, "", errors.New(scrubDSNError(cerr, e.SourceDSN))
	}
	// A DSN shape Connect accepts but ParseSourceDSN rejects (e.g. a
	// unix-socket source) just loses SourceHost — degrading the RDS/Aurora
	// audit-log-over-file-API tier per WhoChangedDeps' own doc comment — not
	// the connection itself; log it so that degradation is diagnosable rather
	// than silently invisible.
	host, _, _, _, perr := config.ParseSourceDSN(e.SourceDSN)
	if perr != nil {
		slog.Warn("forensics: could not derive source host for RDS/Aurora audit-log discovery", "server", e.Name, "error", perr)
	}
	return db, host, nil
}

// handleForensicsCapabilities serves GET /api/forensics/capabilities:
// detected forensic data sources (performance_schema, audit log) for the
// selected server, plus tailored setup guidance. SourceConfigured=false
// (rather than an error) when the server has no source connection set up —
// the frontend renders that as a setup prompt, not a failure. A source that
// IS configured but unreachable is a different, genuine failure (bad
// credentials, network outage) and gets a 502, matching resolveOr's
// precedent for "a server whose connection cannot be established" — it must
// not read as "you never set this up."
func (s *Server) handleForensicsCapabilities(w http.ResponseWriter, r *http.Request) {
	if s.forensicsRefused(w) {
		return
	}
	sourceDB, _, err := s.openForensicsSource(r)
	if errors.Is(err, errSourceNotConfigured) {
		writeJSON(w, http.StatusOK, forensicsCapabilitiesResponse{})
		return
	}
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "could not reach this server's source connection: "+err.Error())
		return
	}
	defer sourceDB.Close()

	caps, err := forensics.DetectCapabilities(r.Context(), sourceDB)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "could not detect forensic capabilities: "+err.Error())
		return
	}
	guide := forensics.BuildSetupGuide(caps)
	writeJSON(w, http.StatusOK, forensicsCapabilitiesResponse{
		Capabilities:     caps,
		SetupGuide:       &guide,
		SourceConfigured: true,
	})
}

// handleForensicsUsers serves GET /api/forensics/users: known MySQL user
// accounts for the selected server, for filter dropdowns. An empty list
// (never an error) when no source is configured; a configured-but-unreachable
// source is a 502, same distinction as handleForensicsCapabilities.
func (s *Server) handleForensicsUsers(w http.ResponseWriter, r *http.Request) {
	if s.forensicsRefused(w) {
		return
	}
	sourceDB, _, err := s.openForensicsSource(r)
	if errors.Is(err, errSourceNotConfigured) {
		writeJSON(w, http.StatusOK, forensicsUsersResponse{Users: []string{}})
		return
	}
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "could not reach this server's source connection: "+err.Error())
		return
	}
	defer sourceDB.Close()

	users, err := forensics.ListUsers(r.Context(), sourceDB)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, forensicsUsersResponse{Users: users})
}

// handleForensicsWhoChanged serves POST /api/forensics/who-changed: attributes
// indexed binlog events for a table (optionally one row) to the database
// sessions that produced them. Always resolves — a missing/unreachable source
// degrades the attribution tiers rather than failing the request; only bad
// parameters or the underlying index fetch can error.
func (s *Server) handleForensicsWhoChanged(w http.ResponseWriter, r *http.Request) {
	if s.forensicsRefused(w) {
		return
	}
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	var body whoChangedRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "malformed request body: "+err.Error())
		return
	}
	if body.Schema == "" || body.Table == "" {
		writeJSONError(w, http.StatusBadRequest, "schema and table are required")
		return
	}
	since, err := cliutil.ParseTime(body.Since)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid since: "+err.Error())
		return
	}
	until, err := cliutil.ParseTime(body.Until)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid until: "+err.Error())
		return
	}

	deps := forensics.WhoChangedDeps{IndexDB: b.db}
	var gapHours []time.Time
	deps.Fetch = func(ctx context.Context, opts query.Options) ([]query.ResultRow, error) {
		rows, plan, ferr := s.fetch(ctx, b, opts)
		if plan != nil {
			gapHours = plan.GapHours
		}
		return rows, ferr
	}
	var sourceUnreachable string
	switch sourceDB, host, serr := s.openForensicsSource(r); {
	case errors.Is(serr, errSourceNotConfigured):
		// No source at all — same as always, deps.SourceDB stays nil and
		// WhoChanged runs index-only tiers with no note (nothing changed).
	case serr != nil:
		// Degrade, per the library's own "degradation is an answer" design
		// (deps.SourceDB stays nil, same as index-only mode) — but say so,
		// exactly like DetectCapabilities' own unreachable-source note
		// would have, so this doesn't read as a silent downgrade to a
		// worse attribution tier.
		sourceUnreachable = "This server's source connection is configured but could not be reached, " +
			"so the audit-log and performance_schema attribution tiers were not consulted; " +
			"only index-side sources ran."
	default:
		defer sourceDB.Close()
		deps.SourceDB = sourceDB
		deps.SourceHost = host
	}

	res, err := forensics.WhoChanged(r.Context(), deps, forensics.WhoChangedParams{
		Schema: body.Schema,
		Table:  body.Table,
		PK:     body.PK,
		Since:  since,
		Until:  until,
		Limit:  clampLimit(body.Limit, whoChangedDefaultLimit, whoChangedMaxLimit),
		Order:  body.Order,
	})
	if err != nil {
		// By this point schema/table/deps.Fetch are all set, so the only
		// realistic error WhoChanged returns is its wrapped index-fetch
		// failure — route it through the same classifier handleEvents uses
		// (writeFetchError) so a pre-connection_id-column index gets the
		// same actionable 422 migration guidance instead of a bare 400.
		writeFetchError(w, err)
		return
	}
	if sourceUnreachable != "" {
		res.Notes = append(res.Notes, sourceUnreachable)
	}
	if len(gapHours) > 0 {
		res.Notes = append(res.Notes, "Index coverage warning: "+query.FormatGapWarning(gapHours))
	}
	writeJSON(w, http.StatusOK, res)
}

// handleForensicsActivity serves POST /api/forensics/activity: the three
// general investigation modes (user_activity / connection_history /
// ddl_history) against the selected server's performance_schema. Unlike
// who-changed, these modes have no index-side fallback — they require a
// configured source connection.
func (s *Server) handleForensicsActivity(w http.ResponseWriter, r *http.Request) {
	if s.forensicsRefused(w) {
		return
	}
	var body forensicsActivityRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "malformed request body: "+err.Error())
		return
	}
	switch body.QueryType {
	case forensics.QueryUserActivity, forensics.QueryConnectionHistory, forensics.QueryDDLHistory:
	default:
		writeJSONError(w, http.StatusBadRequest,
			"query_type must be one of user_activity, connection_history, ddl_history")
		return
	}
	if body.QueryType == forensics.QueryUserActivity && body.User == "" {
		writeJSONError(w, http.StatusBadRequest, "user_activity requires a user")
		return
	}
	if body.QueryType == forensics.QueryConnectionHistory && body.User == "" && body.Host == "" {
		writeJSONError(w, http.StatusBadRequest, "connection_history requires a user or host")
		return
	}

	sourceDB, _, err := s.openForensicsSource(r)
	if errors.Is(err, errSourceNotConfigured) {
		writeJSONError(w, http.StatusBadRequest,
			"this server has no source connection configured; set the source connection first")
		return
	}
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "could not reach this server's source connection: "+err.Error())
		return
	}
	defer sourceDB.Close()

	res, err := forensics.Activity(r.Context(), sourceDB, forensics.ActivityQuery{
		Type:   body.QueryType,
		User:   body.User,
		Host:   body.Host,
		Schema: body.Schema,
		Since:  body.Since,
		Until:  body.Until,
		Limit:  body.Limit,
		Order:  body.Order,
	})
	if err != nil {
		// By this point query_type/user/host preconditions are already
		// validated above and forensics.Activity re-checks nothing new — any
		// error here is a genuine query failure against a source that DID
		// connect (e.g. missing performance_schema/mysql.user grants), never
		// a client mistake. A bare 400 would tell an operator their filters
		// are wrong when the real problem is server-side; match the other
		// three forensics handlers' precedent (502/writeFetchError) instead.
		writeJSONError(w, http.StatusBadGateway, "could not run the forensic query: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}
