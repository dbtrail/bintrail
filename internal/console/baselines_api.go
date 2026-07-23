package console

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// baselinesMaxSnapshots caps the snapshots GET /api/baselines returns — the
// Storage panel is a recency view ("do I have a usable baseline, how stale is
// it"), not an inventory dump.
const baselinesMaxSnapshots = 50

// baselineSnapshotDTO is one snapshot (one timestamped dump) in the listing:
// the grouped view of its per-table Parquet files.
type baselineSnapshotDTO struct {
	Time     string  `json:"time"`
	AgeHours float64 `json:"age_hours"`
	// Tables lists schema.table, sorted (the lister's stable order).
	Tables []string `json:"tables"`
	// Binlog coordinates from Parquet metadata — where this snapshot's deltas
	// start. Local sources only: reading per-snapshot Parquet footers over S3
	// is not worth the listing latency.
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  int64  `json:"binlog_pos,omitempty"`
	GTIDSet    string `json:"gtid_set,omitempty"`
}

type baselinesResponse struct {
	// Configured reports whether the selected server has a baseline source at
	// all (dir or s3). Reconstruct additionally requires archives enabled and
	// no RBAC profile (the bundle's gate) — a server can list baselines that
	// Time-travel still refuses to use.
	Configured  bool                  `json:"configured"`
	Source      string                `json:"source,omitempty"`
	Kind        string                `json:"kind,omitempty"` // "dir" | "s3"
	Reconstruct bool                  `json:"reconstruct"`
	Snapshots   []baselineSnapshotDTO `json:"snapshots"`
	Truncated   bool                  `json:"truncated,omitempty"`
}

// handleBaselines serves GET /api/baselines: a read-only listing of the
// selected server's baseline snapshots (the inputs Time-travel reconstructs
// from), grouped per snapshot timestamp, newest first. The listing is
// path-derived; only local sources additionally read one Parquet footer per
// snapshot for its binlog coordinates (best-effort — a missing/corrupt footer
// just omits them).
func (s *Server) handleBaselines(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	// Baseline snapshot reads bypass RBAC redaction, so a session carrying a data
	// profile is refused the listing (#1075) — the same invariant that gates
	// reconstruct. A startup profile already forced baselineConfigured false.
	if sessionRestricted(r) {
		writeJSONError(w, http.StatusForbidden,
			"baseline listings are unavailable while an access-control profile is active — baseline reads aren't redacted")
		return
	}
	resp := baselinesResponse{Reconstruct: b.baselineConfigured, Snapshots: []baselineSnapshotDTO{}}
	if b.baselineSrc == "" {
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp.Configured = true
	resp.Source = b.baselineSrc
	resp.Kind = "dir"
	if strings.HasPrefix(b.baselineSrc, "s3://") {
		resp.Kind = "s3"
	}

	files, err := reconstruct.ListBaselines(r.Context(), b.baselineSrc)
	if err != nil {
		// The source is configured but unreadable (missing dir, unreachable
		// bucket) — an upstream fault from the console's point of view, and an
		// actionable message for the operator.
		writeJSONError(w, http.StatusBadGateway, "list baselines: "+err.Error())
		return
	}

	now := time.Now().UTC()
	var cur *baselineSnapshotDTO
	var curTime time.Time
	for _, f := range files {
		if cur == nil || !f.SnapshotTime.Equal(curTime) {
			if len(resp.Snapshots) >= baselinesMaxSnapshots {
				resp.Truncated = true
				break
			}
			curTime = f.SnapshotTime
			dto := baselineSnapshotDTO{
				Time:     f.SnapshotTime.Format(consoleTSFormat),
				AgeHours: now.Sub(f.SnapshotTime).Hours(),
			}
			if resp.Kind == "dir" {
				if meta, err := baseline.ReadParquetMetadata(f.Path); err == nil {
					dto.BinlogFile = meta.BinlogFile
					dto.BinlogPos = meta.BinlogPos
					dto.GTIDSet = meta.GTIDSet
				} else {
					// Tolerated (the listing must not die on one bad footer), but
					// never silent: a corrupt snapshot rendered as merely
					// "coordinate-less" would look identical to a healthy
					// pre-metadata baseline.
					slog.Warn("console: baseline Parquet metadata unreadable", "path", f.Path, "error", err)
				}
			}
			resp.Snapshots = append(resp.Snapshots, dto)
			cur = &resp.Snapshots[len(resp.Snapshots)-1]
		}
		cur.Tables = append(cur.Tables, f.Schema+"."+f.Table)
	}
	writeJSON(w, http.StatusOK, resp)
}
