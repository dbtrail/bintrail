package console

import (
	"net/http"

	"github.com/dbtrail/dbtrail/internal/query"
)

// profilesResponse is the wire view of GET /api/profiles.
type profilesResponse struct {
	Profiles []string `json:"profiles"`
}

// handleProfiles serves GET /api/profiles: the RBAC data-profile NAMES
// defined on the selected server's index. Vocabulary, not row data — never
// the rules or the flagged tables/columns behind a name. It exists for
// administration surfaces (an installed settings panel offering a profile
// picker instead of a free-text guess — see ext.ConsoleSettingsProvider),
// and is tiered settings:read accordingly: the profile names describe the
// index's access-control configuration, which is the Settings surface's
// subject, not an events read.
func (s *Server) handleProfiles(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	names, err := query.ListProfiles(r.Context(), b.db)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, profilesResponse{Profiles: names})
}
