package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// errNoServers is returned when a request arrives with no selectable server:
// no --index-dsn boot entry and an empty registry.
var errNoServers = errors.New("no servers configured: pass --index-dsn or add a server in the UI")

// bundle is the per-server connection state: everything that used to live on
// Server when the console spoke to exactly one index. One bundle per selected
// server, built lazily and cached by connManager.
type bundle struct {
	db       *sql.DB
	dbName   string
	engine   *query.Engine
	resolver *metadata.Resolver
	// noArchive disables Parquet archive auto-discovery for this server. It is
	// the entry's own flag OR'd with the process-global profileActive: archives
	// do not enforce RBAC, so an active profile forces it on every server.
	noArchive bool
	// baselineSrc is the resolved reconstruct baseline source (local dir wins
	// over s3:// prefix); empty when reconstruct is not configured.
	baselineSrc string
	// baselineConfigured gates the reconstruct surface per server: a baseline
	// is present AND archives are enabled AND no RBAC profile is active. See
	// the rationale on newBundleDerived.
	baselineConfigured bool
}

// connManager owns the per-server connection lifecycle: lazy open on first
// selection, caching, single-flight, eviction on edit/delete, and shutdown.
//
// Critically, it NEVER runs indexer.EnsureSchema on a registry DSN: schema
// migration is an ALTER (DDL), and the console's read-only contract only
// tolerates it on the DSN the operator typed on the command line (the boot
// entry, migrated by cmd/bintrail before the server starts). A registry index
// missing a newer column surfaces as an actionable error instead — see
// writeFetchError in api.go.
type connManager struct {
	reg *Registry
	// profileActive mirrors "any RBAC rule is loaded" — it forces noArchive
	// (and thus disables reconstruct) on every bundle, because neither archives
	// nor baseline reads apply RBAC redaction.
	profileActive bool

	// demoteBoot prefers a registry entry over the boot entry as the default
	// (no-header) selection when registry entries exist. Set by source-less
	// `bintrail-console watch`: its boot index is only the control plane's
	// anchor database — no stream ever writes to it — so landing new tabs
	// there shows a permanently empty index. The boot entry stays listed and
	// selectable. Written once before serving (console.New); read under mu.
	demoteBoot bool

	mu      sync.Mutex
	bundles map[string]*bundle
	locks   map[string]*sync.Mutex // per-id single-flight for lazy opens
	// boot is the ephemeral command-line entry (--index-dsn / `up`'s stream
	// DSN): opened eagerly by the cmd layer (which also owns closing its db),
	// never persisted, never evicted. nil when the console is registry-only.
	boot *bundle
	// bootDSN is the boot entry's DSN, kept ONLY to render the masked DTO in
	// /api/servers (host/user/dbname). Never serialized whole.
	bootDSN string
}

func newConnManager(reg *Registry, profileActive bool) *connManager {
	if reg == nil {
		reg, _ = LoadRegistry("") // in-memory; "" never errors
	}
	return &connManager{
		reg:           reg,
		profileActive: profileActive,
		bundles:       map[string]*bundle{},
		locks:         map[string]*sync.Mutex{},
	}
}

// Resolve returns the bundle for a server id, lazily opening the connection on
// first selection. id == "" selects the default: the boot entry when present,
// else the first registry entry. The open is single-flighted per id so the
// fan-out a tab switch fires (capabilities + schemas + events) opens ONE
// connection, not four.
func (cm *connManager) Resolve(ctx context.Context, id string) (*bundle, error) {
	if id == "" {
		// The no-header default must match defaultID() exactly — /api/servers
		// reports that id as default_id and the switcher renders it selected,
		// so resolving "" anywhere else would render one server while
		// querying another.
		if id = cm.defaultID(); id == "" {
			return nil, errNoServers
		}
	}
	cm.mu.Lock()
	if id == bootServerID {
		if b := cm.boot; b != nil {
			cm.mu.Unlock()
			return b, nil
		}
		cm.mu.Unlock()
		return nil, ErrUnknownServer
	}
	if b, ok := cm.bundles[id]; ok {
		cm.mu.Unlock()
		return b, nil
	}
	lock, ok := cm.locks[id]
	if !ok {
		lock = &sync.Mutex{}
		cm.locks[id] = lock
	}
	cm.mu.Unlock()

	// Single-flight: only one goroutine builds the bundle for this id; the
	// rest block here and find it cached on re-check.
	lock.Lock()
	defer lock.Unlock()
	cm.mu.Lock()
	if b, ok := cm.bundles[id]; ok {
		cm.mu.Unlock()
		return b, nil
	}
	cm.mu.Unlock()

	// Build outside cm.mu (network I/O), then publish under ONE critical
	// section that re-validates the entry. An edit racing this open
	// (handleServersUpdate runs reg.Update→evict WITHOUT the per-id lock)
	// would otherwise leave a bundle for the OLD DSN cached forever: evict
	// only closes what is already in the map, and this open isn't yet.
	// Re-reading under cm.mu pairs with evict's cm.mu — the edit either
	// happened before our re-read (we rebuild from the current entry) or its
	// evict runs after our publish and closes what we cached.
	for attempt := 0; ; attempt++ {
		entry, ok := cm.reg.Get(id)
		if !ok {
			return nil, ErrUnknownServer
		}
		b, err := cm.buildBundle(entry)
		if err != nil {
			// Not cached: a failed open is retried on the next selection rather
			// than poisoning the entry until restart.
			return nil, err
		}
		cm.mu.Lock()
		cur, stillThere := cm.reg.Get(id)
		if stillThere && cur.DSN == entry.DSN {
			// Derive the published gates from the CURRENT entry: a
			// baseline/no-archive-only edit during the open keeps this db but
			// must not publish the stale entry's reconstruct gate.
			nb := newBundleDerived(b.db, b.dbName, cur, cm.profileActive)
			nb.resolver = b.resolver
			cm.bundles[id] = nb
			cm.mu.Unlock()
			return nb, nil
		}
		cm.mu.Unlock()
		_ = b.db.Close() // built against a deleted or DSN-edited entry
		if !stillThere {
			return nil, ErrUnknownServer
		}
		if attempt >= 2 {
			return nil, fmt.Errorf("server %q is being edited concurrently; retry", cur.Name)
		}
	}
}

// buildBundle opens a registry server's connection and derives its per-server
// state. config.Connect Pings eagerly, so a dead entry fails here — on
// selection, exactly when the operator switches to it — with a scrubbed error.
// NO EnsureSchema: see the connManager doc comment.
func (cm *connManager) buildBundle(entry ServerEntry) (*bundle, error) {
	cfg, err := mysql.ParseDSN(entry.DSN)
	if err != nil {
		// The driver's ParseDSN messages are static today, but scrub anyway so
		// DSN secrecy holds on every error path, not by driver-version luck.
		return nil, fmt.Errorf("server %q: invalid DSN: %s", entry.Name, scrubDSNError(err, entry.DSN))
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("server %q: DSN must include a database name", entry.Name)
	}
	db, err := config.Connect(entry.DSN)
	if err != nil {
		return nil, fmt.Errorf("server %q: %s", entry.Name, scrubDSNError(err, entry.DSN))
	}
	b := newBundleDerived(db, cfg.DBName, entry, cm.profileActive)
	b.resolver = loadResolver(db)
	return b, nil
}

// newBundleDerived computes the pure-config per-server state shared by lazy
// opens and derived-only rebuilds. The reconstruct gate mirrors what New()
// enforced process-globally before multi-server:
//  1. a baseline must be configured (dir wins over s3);
//  2. no RBAC profile may be active — baseline reads bypass redaction; and
//  3. archives must be enabled — the planner can only fail loud on coverage
//     gaps of rotated-out hours if their archives are actually fetched.
//
// Conditions 2 and 3 collapse into !noArchive, exactly as in New().
func newBundleDerived(db *sql.DB, dbName string, entry ServerEntry, profileActive bool) *bundle {
	noArchive := entry.NoArchive || profileActive
	src := entry.BaselineDir
	if src == "" {
		src = entry.BaselineS3
	}
	return &bundle{
		db:                 db,
		dbName:             dbName,
		engine:             query.New(db),
		noArchive:          noArchive,
		baselineSrc:        src,
		baselineConfigured: src != "" && !noArchive,
	}
}

// loadResolver loads the latest schema snapshot, best-effort: a missing
// snapshot just means recovery falls back to all-column WHERE clauses.
func loadResolver(db *sql.DB) *metadata.Resolver {
	if db == nil {
		return nil
	}
	r, err := metadata.NewResolver(db, 0)
	switch {
	case err == nil:
		return r
	case errors.Is(err, metadata.ErrNoSnapshots):
		slog.Debug("console: no schema snapshots; recovery will use all-column WHERE clauses")
	default:
		slog.Warn("console: failed to load schema resolver; recovery will use all-column WHERE clauses", "error", err)
	}
	return nil
}

// seedBoot registers the ephemeral command-line bundle. The cmd layer built it
// eagerly (config.Connect + EnsureSchema, fail-fast preserved) and keeps
// ownership of closing its db.
func (cm *connManager) seedBoot(b *bundle, dsn string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.boot = b
	cm.bootDSN = dsn
}

// bootInfo returns the boot bundle and its display DSN under the lock —
// seedBoot runs once before serving, but readers stay uniformly synchronized.
func (cm *connManager) bootInfo() (*bundle, string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.boot, cm.bootDSN
}

// evict drops and closes a cached bundle (DSN edit or delete). The boot entry
// is not evictable. Closing while a request is in flight is acceptable for a
// single-operator tool: the in-flight query errors, the next selection reopens.
func (cm *connManager) evict(id string) {
	cm.mu.Lock()
	b := cm.bundles[id]
	delete(cm.bundles, id)
	cm.mu.Unlock()
	if b != nil && b.db != nil {
		if err := b.db.Close(); err != nil {
			slog.Warn("console: closing evicted server connection", "server", id, "error", err)
		}
	}
}

// rebuildDerived recomputes a cached bundle's derived flags after a
// baseline/no-archive-only edit, keeping the open db (no needless re-Ping).
// The bundle is replaced wholesale (handlers hold *bundle snapshots, which
// must stay immutable once published). No-op when the bundle isn't cached —
// the next lazy open reads the updated entry anyway.
func (cm *connManager) rebuildDerived(entry ServerEntry) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	old, ok := cm.bundles[entry.ID]
	if !ok {
		return
	}
	nb := newBundleDerived(old.db, old.dbName, entry, cm.profileActive)
	nb.engine = old.engine
	nb.resolver = old.resolver
	cm.bundles[entry.ID] = nb
}

// cached reports whether a live bundle exists for id (the UI's "connected" dot).
func (cm *connManager) cached(id string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	_, ok := cm.bundles[id]
	return ok
}

// capability reports the reconstruct gate for a registry entry as pure config —
// no connection is opened, so /api/servers can label every entry instantly.
func (cm *connManager) capability(entry ServerEntry) bool {
	src := entry.BaselineDir
	if src == "" {
		src = entry.BaselineS3
	}
	return src != "" && !(entry.NoArchive || cm.profileActive)
}

// defaultID returns the id the browser falls back to with no header: the boot
// entry when present, else the first registry entry, else "". Under demoteBoot
// the preference inverts when registry entries exist: the first entry with a
// source (a monitored server — where events actually land), else the first
// entry. The boot entry stays selectable; it just stops being the landing
// default for fresh tabs.
func (cm *connManager) defaultID() string {
	cm.mu.Lock()
	boot := cm.boot
	demote := cm.demoteBoot
	cm.mu.Unlock()
	entries := cm.reg.List()
	if boot != nil {
		if !demote || len(entries) == 0 {
			return bootServerID
		}
		for _, e := range entries {
			if e.SourceDSN != "" {
				return e.ID
			}
		}
		return entries[0].ID
	}
	// boot == nil: registry-only console. demoteBoot is unreachable here
	// today (only watch sets it, and watch always seeds a boot bundle), so the
	// sourced-entry preference deliberately does not apply.
	if len(entries) > 0 {
		return entries[0].ID
	}
	return ""
}

// CloseAll closes every cached registry connection. The boot entry's db is
// NOT closed here — the cmd layer opened it and owns its defer Close().
func (cm *connManager) CloseAll() {
	cm.mu.Lock()
	bundles := cm.bundles
	cm.bundles = map[string]*bundle{}
	cm.mu.Unlock()
	for id, b := range bundles {
		if b.db != nil {
			if err := b.db.Close(); err != nil {
				slog.Warn("console: closing server connection at shutdown", "server", id, "error", err)
			}
		}
	}
}
