package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/bintrail/internal/config"
)

// testConnectTimeout is the dial timeout injected into test-connection probes
// when the DSN doesn't set one. Deliberately shorter than config.Connect's 10s
// default: a dead host should fail the health check fast, not stall the UI.
const testConnectTimeout = 3 * time.Second

// serverDTO is the masked wire view of a server entry. It NEVER carries the
// DSN string or the password — only parsed non-secret parts plus has_password,
// so the edit form can prefill everything except the secret (which it keeps
// via the omitted-password semantics of PUT).
type serverDTO struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Kind        string            `json:"kind"` // "registry" | "ephemeral"
	Host        string            `json:"host"`
	Port        string            `json:"port"`
	User        string            `json:"user"`
	DBName      string            `json:"dbname"`
	Params      map[string]string `json:"params,omitempty"`
	HasPassword bool              `json:"has_password"`
	BaselineDir string            `json:"baseline_dir,omitempty"`
	BaselineS3  string            `json:"baseline_s3,omitempty"`
	NoArchive   bool              `json:"no_archive"`
	// Reconstruct is the per-server Time-travel capability, derived from pure
	// config (no connection is opened to compute it).
	Reconstruct bool `json:"reconstruct"`
	Editable    bool `json:"editable"`
	Deletable   bool `json:"deletable"`
	// Connected reports whether a live connection is currently cached.
	Connected bool `json:"connected"`
}

type serversResponse struct {
	Servers []serverDTO `json:"servers"`
	// DefaultID is the entry served when the browser sends no selection
	// header: the command-line (ephemeral) entry when present, else the first
	// registry entry.
	DefaultID string `json:"default_id"`
}

// serverRequest is the JSON body for POST/PUT /api/servers and test probes.
// Either a full dsn or the structured fields. Password is a *string so PUT
// distinguishes "omitted = keep the stored password" from `"" = clear it`.
type serverRequest struct {
	Name        string  `json:"name"`
	DSN         string  `json:"dsn"`
	Host        string  `json:"host"`
	Port        string  `json:"port"`
	User        string  `json:"user"`
	Password    *string `json:"password"`
	DBName      string  `json:"dbname"`
	BaselineDir string  `json:"baseline_dir"`
	BaselineS3  string  `json:"baseline_s3"`
	NoArchive   bool    `json:"no_archive"`
}

// testResponse is the probe result. HasIndex/SchemaCurrent are tri-state
// (*bool): nil means the metadata lookup itself failed — UNKNOWN, omitted from
// the JSON — which must never be rendered as the affirmative "outdated/not an
// index" claim a literal false carries (a swallowed error would otherwise tell
// the operator to run a migration they don't need).
type testResponse struct {
	OK            bool   `json:"ok"`
	Error         string `json:"error,omitempty"`
	ServerVersion string `json:"server_version,omitempty"`
	DBName        string `json:"dbname,omitempty"`
	LatencyMS     int64  `json:"latency_ms"`
	// HasIndex: the database contains a binlog_events table (it looks like a
	// bintrail index at all).
	HasIndex *bool `json:"has_index,omitempty"`
	// SchemaCurrent: binlog_events carries the connection_id column. The
	// console never migrates registry servers (that ALTER is confined to the
	// command-line DSN), so a stale index must be migrated by a writer command
	// (index/stream/agent) before this console can query it.
	SchemaCurrent *bool `json:"schema_current,omitempty"`
}

// handleServersList serves GET /api/servers.
func (s *Server) handleServersList(w http.ResponseWriter, r *http.Request) {
	out := []serverDTO{}
	if dto, ok := s.bootDTO(); ok {
		out = append(out, dto)
	}
	for _, e := range s.cm.reg.List() {
		out = append(out, s.entryDTO(e))
	}
	writeJSON(w, http.StatusOK, serversResponse{Servers: out, DefaultID: s.cm.defaultID()})
}

// handleServersGet serves GET /api/servers/{id} — the masked single-entry view
// used to prefill the edit form.
func (s *Server) handleServersGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		if dto, ok := s.bootDTO(); ok {
			writeJSON(w, http.StatusOK, dto)
			return
		}
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	e, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.entryDTO(e))
}

// handleServersCreate serves POST /api/servers. It validates and persists the
// entry — it does NOT connect (opens are lazy, on first selection) and runs no
// DDL, ever.
func (s *Server) handleServersCreate(w http.ResponseWriter, r *http.Request) {
	var req serverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	dsn, err := buildDSN(req, "")
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	entry := ServerEntry{
		Name:        strings.TrimSpace(req.Name),
		DSN:         dsn,
		BaselineDir: req.BaselineDir,
		BaselineS3:  req.BaselineS3,
		NoArchive:   req.NoArchive,
	}
	added, err := s.cm.reg.Add(entry)
	if err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, s.entryDTO(added))
}

// handleServersUpdate serves PUT /api/servers/{id}. Password semantics:
// omitted/null keeps the stored one, "" clears it, a value replaces it. A
// DSN-affecting change evicts (and closes) the cached connection; a
// baseline/no-archive-only change rebuilds the derived flags in place.
func (s *Server) handleServersUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, "the command-line server cannot be edited; it mirrors --index-dsn")
		return
	}
	old, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	var req serverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	dsn, err := buildDSN(req, old.DSN)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	entry := ServerEntry{
		ID:          id,
		Name:        strings.TrimSpace(req.Name),
		DSN:         dsn,
		BaselineDir: req.BaselineDir,
		BaselineS3:  req.BaselineS3,
		NoArchive:   req.NoArchive,
	}
	if err := s.cm.reg.Update(entry); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	if dsn != old.DSN {
		s.cm.evict(id) // connection points at the old DSN; close and reopen lazily
	} else {
		s.cm.rebuildDerived(entry) // keep the db, recompute baseline/no-archive gates
	}
	writeJSON(w, http.StatusOK, s.entryDTO(entry))
}

// handleServersDelete serves DELETE /api/servers/{id}.
func (s *Server) handleServersDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, "the command-line server cannot be deleted; stop the console instead")
		return
	}
	if err := s.cm.reg.Delete(id); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	s.cm.evict(id)
	w.WriteHeader(http.StatusNoContent)
}

// handleServersTest serves POST /api/servers/test (unsaved candidate) and
// POST /api/servers/{id}/test (stored entry, optionally overridden by a body —
// with keep-password merge, so "edit then test before saving" works without
// retyping the secret).
//
// The probe is write-free by construction: Connect (Ping), SELECT VERSION(),
// two information_schema lookups, Close. No EnsureSchema, no caching. It
// returns 200 with ok:false on an unreachable server — a failed probe is a
// RESULT, not a transport error.
func (s *Server) handleServersTest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	stored := ""
	if id != "" && id != bootServerID {
		e, ok := s.cm.reg.Get(id)
		if !ok {
			writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
			return
		}
		stored = e.DSN
	}
	if id == bootServerID {
		_, stored = s.cm.bootInfo()
	}

	// An empty body means "test the stored DSN as-is"; anything else must parse.
	var req serverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	dsn := stored
	if req.DSN != "" || req.Host != "" || req.User != "" || req.DBName != "" || req.Password != nil {
		built, err := buildDSN(req, stored)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		dsn = built
	}
	if dsn == "" {
		writeJSONError(w, http.StatusBadRequest, "nothing to test: no stored DSN and no candidate supplied")
		return
	}
	writeJSON(w, http.StatusOK, probeServer(r, dsn))
}

// probeServer runs the write-free reachability probe against one DSN.
func probeServer(r *http.Request, dsn string) testResponse {
	short, dbName, err := shortTimeoutDSN(dsn)
	if err != nil {
		return testResponse{Error: scrubDSNError(err, dsn)}
	}
	start := time.Now()
	db, err := config.Connect(short)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return testResponse{Error: scrubDSNError(err, dsn), LatencyMS: latency}
	}
	defer db.Close()

	resp := testResponse{OK: true, DBName: dbName, LatencyMS: latency}
	ctx := r.Context()
	// Best-effort enrichments: a probe that Pings but can't read metadata is
	// still "reachable", so these never flip OK back to false. A FAILED lookup
	// leaves the tri-state nil (unknown) and is logged — collapsing it to
	// false would render a confident wrong claim ("schema outdated") out of a
	// transient error.
	_ = db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&resp.ServerVersion)
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'",
		dbName).Scan(&n); err == nil {
		v := n > 0
		resp.HasIndex = &v
	} else {
		slog.Warn("console: test probe could not check for binlog_events", "db", dbName, "error", err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND COLUMN_NAME = 'connection_id'",
		dbName).Scan(&n); err == nil {
		v := n > 0
		resp.SchemaCurrent = &v
	} else {
		slog.Warn("console: test probe could not check the index schema", "db", dbName, "error", err)
	}
	return resp
}

// shortTimeoutDSN injects the probe dial timeout (when the DSN doesn't set its
// own) and returns the database name for the probe queries.
func shortTimeoutDSN(dsn string) (string, string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", "", fmt.Errorf("invalid DSN: %w", err)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = testConnectTimeout
	}
	return cfg.FormatDSN(), cfg.DBName, nil
}

// buildDSN assembles the stored DSN for a create/update request. Either a raw
// dsn (used verbatim) or structured fields layered over the stored DSN (PUT)
// or a blank config (POST). Password merge: nil keeps the stored secret, ""
// clears it, a value replaces it. The result must name a database — every
// console query is scoped to the index DB.
func buildDSN(req serverRequest, stored string) (string, error) {
	if req.DSN != "" {
		// The raw DSN carries its own password; accepting a structured
		// password alongside it would have to silently drop one of the two.
		if req.Password != nil {
			return "", errors.New("specify either dsn or the structured password field, not both (a dsn carries its own password)")
		}
		cfg, err := mysql.ParseDSN(req.DSN)
		if err != nil {
			return "", fmt.Errorf("invalid dsn: %w", err)
		}
		if cfg.DBName == "" {
			return "", errors.New("dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
		}
		return req.DSN, nil
	}

	var cfg *mysql.Config
	if stored != "" {
		parsed, err := mysql.ParseDSN(stored)
		if err != nil {
			// A stored DSN that no longer parses can't be merged; require a
			// full replacement via the dsn field. Scrubbed: secrecy must not
			// depend on the driver keeping its parse messages static.
			return "", fmt.Errorf("stored DSN is invalid; resubmit with a full dsn: %s", scrubDSNError(err, stored))
		}
		cfg = parsed
	} else {
		cfg = mysql.NewConfig()
		cfg.Net = "tcp"
	}

	host, port := req.Host, req.Port
	if host != "" || port != "" {
		if host == "" {
			// Port-only change: keep the stored host.
			if h, _, err := net.SplitHostPort(cfg.Addr); err == nil {
				host = h
			} else {
				host = cfg.Addr
			}
		}
		if port == "" {
			// Host-only change: keep the stored port (symmetric with the
			// host recovery above — defaulting here would silently rewrite a
			// non-default port like :3307 to :3306). 3306 only when the
			// stored address genuinely has no port (or this is a create).
			if _, p, err := net.SplitHostPort(cfg.Addr); err == nil && p != "" {
				port = p
			} else {
				port = "3306"
			}
		}
		cfg.Net = "tcp"
		cfg.Addr = net.JoinHostPort(host, port)
	}
	if req.User != "" {
		cfg.User = req.User
	}
	if req.DBName != "" {
		cfg.DBName = req.DBName
	}
	if req.Password != nil {
		cfg.Passwd = *req.Password
	}
	if cfg.Addr == "" {
		return "", errors.New("host is required")
	}
	if cfg.User == "" {
		return "", errors.New("user is required")
	}
	if cfg.DBName == "" {
		return "", errors.New("dbname is required (the index database, e.g. binlog_index)")
	}
	return cfg.FormatDSN(), nil
}

// entryDTO masks a registry entry for the wire: parsed non-secret DSN parts
// plus has_password. The DSN string itself never leaves the process.
func (s *Server) entryDTO(e ServerEntry) serverDTO {
	dto := serverDTO{
		ID:          e.ID,
		Name:        e.Name,
		Kind:        "registry",
		BaselineDir: e.BaselineDir,
		BaselineS3:  e.BaselineS3,
		NoArchive:   e.NoArchive,
		Reconstruct: s.cm.capability(e),
		Editable:    !s.cm.reg.ReadOnly(),
		Deletable:   !s.cm.reg.ReadOnly(),
		Connected:   s.cm.cached(e.ID),
	}
	fillDSNParts(&dto, e.DSN)
	return dto
}

// bootDTO renders the ephemeral command-line entry, when one exists.
func (s *Server) bootDTO() (serverDTO, bool) {
	boot, dsn := s.cm.bootInfo()
	if boot == nil {
		return serverDTO{}, false
	}
	dto := serverDTO{
		ID:          bootServerID,
		Name:        bootServerID,
		Kind:        "ephemeral",
		DBName:      boot.dbName,
		NoArchive:   boot.noArchive,
		Reconstruct: boot.baselineConfigured,
		BaselineDir: "",
		BaselineS3:  "",
		Editable:    false,
		Deletable:   false,
		Connected:   true,
	}
	if boot.baselineSrc != "" {
		if strings.HasPrefix(boot.baselineSrc, "s3://") {
			dto.BaselineS3 = boot.baselineSrc
		} else {
			dto.BaselineDir = boot.baselineSrc
		}
	}
	if dsn != "" {
		fillDSNParts(&dto, dsn)
	}
	return dto, true
}

// fillDSNParts decomposes a DSN into the masked DTO fields. Parse failures
// leave the fields blank rather than leaking the raw string.
func fillDSNParts(dto *serverDTO, dsn string) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return
	}
	if h, p, err := net.SplitHostPort(cfg.Addr); err == nil {
		dto.Host, dto.Port = h, p
	} else {
		dto.Host = cfg.Addr
	}
	dto.User = cfg.User
	dto.DBName = cfg.DBName
	dto.HasPassword = cfg.Passwd != ""
	if len(cfg.Params) > 0 {
		dto.Params = cfg.Params
	}
}

// scrubDSNError strips the DSN and its password from an error message before
// it reaches the browser. config.Connect errors can embed the full DSN
// ("invalid DSN: ..."), and driver errors may echo credentials.
func scrubDSNError(err error, dsn string) string {
	msg := err.Error()
	if dsn != "" {
		msg = strings.ReplaceAll(msg, dsn, "<dsn>")
	}
	if cfg, perr := mysql.ParseDSN(dsn); perr == nil && cfg.Passwd != "" {
		msg = strings.ReplaceAll(msg, cfg.Passwd, "***")
	}
	return msg
}

// registryErrStatus maps registry errors onto HTTP statuses.
func registryErrStatus(err error) int {
	switch {
	case errors.Is(err, ErrDuplicateName), errors.Is(err, ErrRegistryReadOnly):
		return http.StatusConflict
	case errors.Is(err, ErrUnknownServer):
		return http.StatusNotFound
	default:
		if strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "reserved") {
			return http.StatusBadRequest
		}
		return http.StatusInternalServerError
	}
}
