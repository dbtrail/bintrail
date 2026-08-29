package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/accessprofiles"
)

// The Access profiles page (#1445): the console authors the flags,
// profiles and rules that `--profile` and a session's data profile enforce.
// This is a write to the SELECTED SERVER'S INDEX DATABASE, a class of
// console write the registry file and the rotation override are not, and it
// is allowed under four rules, all pinned by tests:
//
//   - every mutation needs settings:write (the permission that already
//     governs console administration), reads need settings:read;
//   - the validation and the SQL are internal/accessprofiles, the SAME code
//     the CLI verbs run, so a profile authored here is the rows the CLI
//     would write and is refused with the CLI's words;
//   - the target is the selected server (X-Bintrail-Server), the boot entry
//     included: it is a real index;
//   - every mutation is audited (console/flag.add, ... access.remove).
//
// One more, which the permission alone does not give: a session that
// carries a data profile is refused, whatever its permissions. Such a
// session editing the rules could lift its own redaction, and the profile
// gate on the row-data surfaces (reconstruct, verify, cascade) has the same
// shape: refused rather than trusted to redact itself.

// accessProfilesDoc is the whole configuration in one document: what GET
// returns and what every mutation returns once it has applied, so the page
// always repaints from the server's own view and never from what it thinks
// it just did.
type accessProfilesDoc struct {
	Flags    []accessFlagDTO    `json:"flags"`
	Profiles []accessProfileDTO `json:"profiles"`
	Rules    []accessRuleDTO    `json:"rules"`
}

type accessFlagDTO struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Column    string `json:"column"`
	Flag      string `json:"flag"`
	CreatedAt string `json:"created_at"`
}

type accessProfileDTO struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
}

type accessRuleDTO struct {
	Profile    string `json:"profile"`
	Flag       string `json:"flag"`
	Permission string `json:"permission"`
	CreatedAt  string `json:"created_at"`
}

// Request bodies. Flag add/remove share one; so do profile and rule.
type accessFlagRequest struct {
	Flag   string `json:"flag"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Column string `json:"column"`
}

type accessProfileRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type accessRuleRequest struct {
	Profile    string `json:"profile"`
	Flag       string `json:"flag"`
	Permission string `json:"permission"`
}

// accessProfilesRefusal is the message a data-profile session gets on a
// mutation (see the file comment).
const accessProfilesRefusal = "your session has a data profile, so it cannot change access profiles; sign in with an account that has none"

// loadAccessProfilesDoc reads the three tables of the selected index.
func loadAccessProfilesDoc(ctx context.Context, db accessprofiles.DBExecer) (accessProfilesDoc, error) {
	doc := accessProfilesDoc{Flags: []accessFlagDTO{}, Profiles: []accessProfileDTO{}, Rules: []accessRuleDTO{}}
	flags, err := accessprofiles.ListFlags(ctx, db, "", "")
	if err != nil {
		return doc, err
	}
	for _, f := range flags {
		doc.Flags = append(doc.Flags, accessFlagDTO{Schema: f.Schema, Table: f.Table, Column: f.Column, Flag: f.Name,
			CreatedAt: f.CreatedAt.UTC().Format(consoleTSFormat)})
	}
	profiles, err := accessprofiles.ListProfiles(ctx, db)
	if err != nil {
		return doc, err
	}
	for _, p := range profiles {
		doc.Profiles = append(doc.Profiles, accessProfileDTO{Name: p.Name, Description: p.Description,
			CreatedAt: p.CreatedAt.UTC().Format(consoleTSFormat)})
	}
	rules, err := accessprofiles.ListRules(ctx, db, "")
	if err != nil {
		return doc, err
	}
	for _, r := range rules {
		doc.Rules = append(doc.Rules, accessRuleDTO{Profile: r.Profile, Flag: r.Flag, Permission: r.Permission,
			CreatedAt: r.CreatedAt.UTC().Format(consoleTSFormat)})
	}
	return doc, nil
}

// writeAccessProfilesError maps a shared-package refusal onto a status: bad
// input is 400, a row that is not there is 404, an index without the three
// tables is 422 (it predates them; nothing here can create them), anything
// else is the database's own error at 500.
func writeAccessProfilesError(w http.ResponseWriter, err error) {
	var myErr *mysql.MySQLError
	switch {
	case accessprofiles.IsNotFound(err):
		writeJSONError(w, http.StatusNotFound, err.Error())
	case accessprofiles.IsRefusal(err):
		writeJSONError(w, http.StatusBadRequest, err.Error())
	case errors.As(err, &myErr) && myErr.Number == 1146:
		writeJSONError(w, http.StatusUnprocessableEntity,
			"this index has no access profile tables; it was created before they existed, so access profiles cannot be stored on it")
	default:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleAccessProfilesGet serves GET /api/access-profiles for the selected
// server. Configuration, not row data: flag names, table and column names,
// profile names and their rules.
func (s *Server) handleAccessProfilesGet(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	doc, err := loadAccessProfilesDoc(r.Context(), b.db)
	if err != nil {
		writeAccessProfilesError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

// accessMutation is one verb's body: apply it to the selected index and
// report what to audit (the schema and table the change names, when it
// names one, plus the fields that identify it).
type accessMutation func(ctx context.Context, db accessprofiles.DBExecer) (schema, table string, detail map[string]string, err error)

// mutateAccessProfiles is the shared shape of the six verbs: refuse a
// data-profile session, resolve the server, run the shared code, drop the
// cached profile rules for that server (a profiled session's next request
// must see the change, not the 30-second cache), audit, and answer with the
// fresh document.
func (s *Server) mutateAccessProfiles(w http.ResponseWriter, r *http.Request, action string, run accessMutation) {
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "access_profiles")
		writeJSONError(w, http.StatusForbidden, accessProfilesRefusal)
		return
	}
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	schema, table, detail, err := run(r.Context(), b.db)
	if err != nil {
		writeAccessProfilesError(w, err)
		return
	}
	s.sessionProfiles.invalidate(s.selectedID(r))
	recordConsoleAccess(r, action, schema, table, detail)
	doc, err := loadAccessProfilesDoc(r.Context(), b.db)
	if err != nil {
		// The change is in; only the readback failed. Say so rather than
		// reporting a failure the operator would then repeat.
		writeAccessProfilesError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func decodeAccessBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeBodyDecodeError(w, err)
		return false
	}
	return true
}

func flagDetail(f accessprofiles.Flag) map[string]string {
	return map[string]string{"flag": f.Name, "column": f.Column}
}

// handleAccessFlagAdd serves POST /api/access-profiles/flags.
func (s *Server) handleAccessFlagAdd(w http.ResponseWriter, r *http.Request) {
	var req accessFlagRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	f := accessprofiles.Flag{Schema: req.Schema, Table: req.Table, Column: req.Column, Name: req.Flag}
	s.mutateAccessProfiles(w, r, "flag.add", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return f.Schema, f.Table, flagDetail(f), accessprofiles.AddFlag(ctx, db, f)
	})
}

// handleAccessFlagRemove serves POST /api/access-profiles/flags/remove. A
// POST with a body rather than a DELETE: a flag's key is four values, which
// have no place in a path.
func (s *Server) handleAccessFlagRemove(w http.ResponseWriter, r *http.Request) {
	var req accessFlagRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	f := accessprofiles.Flag{Schema: req.Schema, Table: req.Table, Column: req.Column, Name: req.Flag}
	s.mutateAccessProfiles(w, r, "flag.remove", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return f.Schema, f.Table, flagDetail(f), accessprofiles.RemoveFlag(ctx, db, f)
	})
}

// handleAccessProfileAdd serves POST /api/access-profiles/profiles.
func (s *Server) handleAccessProfileAdd(w http.ResponseWriter, r *http.Request) {
	var req accessProfileRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	p := accessprofiles.Profile{Name: req.Name, Description: req.Description}
	s.mutateAccessProfiles(w, r, "profile.add", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return "", "", map[string]string{"profile": p.Name}, accessprofiles.AddProfile(ctx, db, p)
	})
}

// handleAccessProfileRemove serves POST /api/access-profiles/profiles/remove.
// The profile's rules go with it (the table's foreign key cascades), which
// the page says before asking.
func (s *Server) handleAccessProfileRemove(w http.ResponseWriter, r *http.Request) {
	var req accessProfileRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	s.mutateAccessProfiles(w, r, "profile.remove", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return "", "", map[string]string{"profile": req.Name}, accessprofiles.RemoveProfile(ctx, db, req.Name)
	})
}

// handleAccessRuleAdd serves POST /api/access-profiles/rules. Adding a rule
// for a pair that has one replaces its permission, as the CLI does.
func (s *Server) handleAccessRuleAdd(w http.ResponseWriter, r *http.Request) {
	var req accessRuleRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	rule := accessprofiles.Rule{Profile: req.Profile, Flag: req.Flag, Permission: req.Permission}
	s.mutateAccessProfiles(w, r, "access.add", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return "", "", map[string]string{"profile": rule.Profile, "flag": rule.Flag, "permission": rule.Permission},
			accessprofiles.AddRule(ctx, db, rule)
	})
}

// handleAccessRuleRemove serves POST /api/access-profiles/rules/remove.
func (s *Server) handleAccessRuleRemove(w http.ResponseWriter, r *http.Request) {
	var req accessRuleRequest
	if !decodeAccessBody(w, r, &req) {
		return
	}
	s.mutateAccessProfiles(w, r, "access.remove", func(ctx context.Context, db accessprofiles.DBExecer) (string, string, map[string]string, error) {
		return "", "", map[string]string{"profile": req.Profile, "flag": req.Flag},
			accessprofiles.RemoveRule(ctx, db, req.Profile, req.Flag)
	})
}
