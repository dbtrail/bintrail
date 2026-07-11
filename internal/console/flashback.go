package console

import (
	"context"
	"database/sql"
	"strings"

	drivermysql "github.com/go-sql-driver/mysql"
)

// FlashbackTarget is the go-mysql-free resolution of a flashback connection's
// target server (issue #996): the per-source index handle, baseline source, and
// default schema the MySQL-protocol serving layer needs to build a shim handler.
//
// It lives here — but the serving code does NOT — because internal/console is a
// read-layer package barred from linking go-mysql (#528,
// TestReadLayerDoesNotLinkGoMySQL). cmd/bintrail-console owns the protocol
// server and consumes this plain struct; nothing here imports the protocol or
// capture libraries.
type FlashbackTarget struct {
	// IndexDB is the open connection to the server's per-source index. It is
	// owned by the connManager — the serving layer must not Close it.
	IndexDB *sql.DB
	// IndexDBName is the schema where binlog_events lives (the query planner
	// scopes information_schema.PARTITIONS to it).
	IndexDBName string
	// BaselineDir / BaselineS3 are the resolved _snapshot baseline source, split
	// by scheme (dir-preferred, mirroring the console's Time-travel tab). The
	// #766 local→S3 fallback the console bundle also carries is intentionally
	// dropped — a documented single-source-parity edge for the embedded port
	// (see docs/time-travel-sql.md).
	BaselineDir string
	BaselineS3  string
	// NoArchive disables archive auto-discovery for this server.
	NoArchive bool
	// DefaultSchema is the source database name (from the registry SourceDSN),
	// seeded so USE-less `_flashback.<table>` queries resolve; empty for the
	// boot entry (no registry SourceDSN).
	DefaultSchema string
}

// ResolveFlashback maps a flashback connection username to its target server's
// per-source index + baseline, opening the connection lazily via connManager.
// The username selects the server by registry ID, display Name, or "default"
// (the boot entry). Returns ErrUnknownServer when the selector matches no
// selectable server — the serving layer turns that into a MySQL "no such
// database" error on the client's first query. The registry is read live, so
// servers added in the console mid-session are reachable without a restart.
func (s *Server) ResolveFlashback(ctx context.Context, selector string) (FlashbackTarget, error) {
	id, ok := s.flashbackTarget(selector)
	if !ok {
		return FlashbackTarget{}, ErrUnknownServer
	}
	b, err := s.cm.Resolve(ctx, id)
	if err != nil {
		return FlashbackTarget{}, err
	}
	dir, s3 := splitBaselineSource(b.baselineSrc)
	return FlashbackTarget{
		IndexDB:       b.db,
		IndexDBName:   b.dbName,
		BaselineDir:   dir,
		BaselineS3:    s3,
		NoArchive:     b.noArchive,
		DefaultSchema: s.flashbackDefaultSchema(id),
	}, nil
}

// flashbackTarget maps a connection username to a canonical server id: a
// registry ID, a registry display Name, or "default" for the boot entry. The
// registry is read live so servers added in the UI mid-session are reachable
// without restarting the port.
func (s *Server) flashbackTarget(selector string) (string, bool) {
	if selector == "" {
		return "", false
	}
	if _, ok := s.cm.reg.Get(selector); ok {
		return selector, true // matched by id
	}
	for _, e := range s.cm.reg.List() {
		if e.Name == selector {
			return e.ID, true // matched by display name
		}
	}
	if selector == bootServerID && s.cm.bootSelectable() {
		return bootServerID, true
	}
	return "", false
}

// flashbackDefaultSchema derives the source database name for a target server
// from its registry SourceDSN, for `USE`-less fully qualified queries. Empty
// for the boot entry (no registry SourceDSN) or an unparseable/absent DSN.
func (s *Server) flashbackDefaultSchema(id string) string {
	entry, ok := s.cm.reg.Get(id)
	if !ok || entry.SourceDSN == "" {
		return ""
	}
	cfg, err := drivermysql.ParseDSN(entry.SourceDSN)
	if err != nil {
		return ""
	}
	return cfg.DBName
}

// splitBaselineSource maps a resolved baseline source (the console bundle's
// already dir-preferred baselineSrc) onto the shim's dir/S3 config fields by
// scheme. The #766 local→S3 fallback the console bundle also carries is
// deliberately NOT represented — a documented single-source-parity limitation
// for the embedded port: a server with BOTH a local dir and an S3 copy reads
// `_snapshot` only from the local dir here (see docs/time-travel-sql.md).
func splitBaselineSource(src string) (dir, s3 string) {
	if strings.HasPrefix(src, "s3://") {
		return "", src
	}
	if src != "" {
		return src, ""
	}
	return "", ""
}
