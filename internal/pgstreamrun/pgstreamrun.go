// Package pgstreamrun is the PostgreSQL streaming CONSUMER: it drives the
// internal/pgcapture capturer (the producer) into the shared indexer, persisting a
// durable LSN checkpoint to stream_state. It is the PostgreSQL analog of
// internal/streamrun, deliberately a SEPARATE package — the go-mysql GTID/position
// types and binlog gap detection in streamrun do not parameterize. It reuses the
// source-agnostic core: event.Event, indexer.Indexer, and the batch→flush→checkpoint
// loop shape.
//
// #534 (the consumer); closes #530's end-to-end acceptance (a live PG change stream
// indexed into binlog_events via the existing indexer).
package pgstreamrun

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// pgFlavor is the stream_state.flavor value for a PostgreSQL source. The checkpoint
// rides the existing gtid-mode columns (the cursor is a single monotonic LSN, like a
// GTID set, advanced only at commit boundaries) — mode='gtid' needs no shared-schema
// change; flavor='postgres' is what actually distinguishes a PG checkpoint.
const pgFlavor = "postgres"

// defaultEventBuffer is the size of the channel between the capturer and the loop.
const defaultEventBuffer = 1000

// Config binds everything One needs.
type Config struct {
	IndexDSN    string // the index MySQL database (same store as a MySQL source)
	ReplDSN     string // PostgreSQL replication connection (must carry replication=database)
	QueryDSN    string // PostgreSQL normal connection (catalog PK lookup + slot/publication checks)
	SlotName    string
	Publication string
	ServerID    uint32 // identifier recorded in stream_state (server_id is NOT NULL)
	StartLSN    uint64 // explicit start LSN; ignored once a checkpoint exists; 0 = let the slot's ConsistentPoint decide on first run
	Schemas     string // comma-separated schema filter (empty = all)
	Tables      string // comma-separated table filter (empty = all)
	BatchSize   int
	Partitions  int           // binlog_events partitions for the one-time bootstrap (0 → 48)
	Checkpoint  time.Duration // checkpoint interval (0 → 5s)
	Logger      *slog.Logger
}

// pgStreamState mirrors the streamrun checkpoint row, scoped to what a PG source
// needs. The live cursor (last committed LSN) is tracked in the loop, NOT here — this
// holds the loaded resume point and the running counters.
type pgStreamState struct {
	lsn           uint64 // resume LSN (loaded from the checkpoint; 0 on first run)
	eventsIndexed int64
	lastEventTime sql.NullTime
	serverID      uint32
}

// One runs a complete PostgreSQL replication stream until ctx is cancelled. It
// returns nil on graceful shutdown and a non-nil error on a fatal failure (so a
// supervisor reconnects).
func One(ctx context.Context, cfg Config) error {
	if cfg.IndexDSN == "" || cfg.ReplDSN == "" || cfg.QueryDSN == "" || cfg.SlotName == "" || cfg.Publication == "" {
		return fmt.Errorf("pgstreamrun: One requires IndexDSN, ReplDSN, QueryDSN, SlotName, and Publication")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	partitions := cfg.Partitions
	if partitions <= 0 {
		partitions = 48
	}
	checkpointInterval := cfg.Checkpoint
	if checkpointInterval <= 0 {
		checkpointInterval = 5 * time.Second
	}

	indexDB, err := config.Connect(cfg.IndexDSN)
	if err != nil {
		return fmt.Errorf("pgstreamrun: connect index database: %w", err)
	}
	defer indexDB.Close()

	// Bootstrap (idempotent) then migrate: CreateIndexTables handles a fresh index DB
	// (EnsureSchema alone fails on one), EnsureSchema adds any columns on an existing
	// one.
	if err := indexer.CreateIndexTables(ctx, indexDB, partitions, false, nil); err != nil {
		return fmt.Errorf("pgstreamrun: bootstrap index schema: %w", err)
	}
	if err := indexer.EnsureSchema(indexDB); err != nil {
		return fmt.Errorf("pgstreamrun: migrate index schema: %w", err)
	}

	saved, err := loadStreamStatePG(indexDB)
	if err != nil {
		return err
	}
	startLSN := resolveStartLSN(saved, cfg.StartLSN, logger)

	state := &pgStreamState{lsn: startLSN, serverID: cfg.ServerID}
	if saved != nil {
		state.eventsIndexed = saved.eventsIndexed
	}

	cap := pgcapture.New(pgcapture.Config{
		ReplDSN:            cfg.ReplDSN,
		QueryDSN:           cfg.QueryDSN,
		SlotName:           cfg.SlotName,
		Publication:        cfg.Publication,
		Filters:            cliutil.BuildIndexFilters(cfg.Schemas, cfg.Tables),
		StartLSN:           pglogrepl.LSN(startLSN),
		ExpectExistingSlot: saved != nil, // resuming → the slot must still exist + be valid
		Logger:             logger,
	})
	idx := indexer.New(indexDB, cfg.BatchSize)

	// Bridge loop-exit and capturer-exit (mirrors streamrun.One). The capturer's
	// Run does NOT close the events channel (its contract), so the wrapper goroutine
	// closes it when Run returns: a capturer that dies on its own (slot lost
	// mid-stream, decode desync, network drop) then makes streamLoopPG drain the
	// remaining events and exit on the close, where One reads the real error and
	// returns it for the supervisor to reconnect — instead of hanging forever on a
	// never-closed channel under a never-cancelled parent ctx. The explicit cancel()
	// after the loop covers the other direction: if streamLoopPG returns first, it
	// unblocks the capturer's emit/receive so it can return.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := make(chan event.Event, defaultEventBuffer)
	captureErr := make(chan error, 1)
	go func() {
		defer close(events)
		captureErr <- cap.Run(ctx, events)
	}()

	loopErr := streamLoopPG(ctx, events, idx, indexDB, cap, checkpointInterval, state, logger)
	cancel() // loop exited first → unblock the capturer's emit/receive so it returns

	// Run returns nil on ctx-cancel; only a real capture error matters.
	capErr := <-captureErr
	if loopErr != nil {
		return loopErr
	}
	if capErr != nil && !errors.Is(capErr, context.Canceled) {
		return capErr
	}
	logger.Info("pgstreamrun: stopped", "events_indexed", state.eventsIndexed,
		"last_lsn", pglogrepl.LSN(state.lsn))
	return nil
}

// streamLoopPG batches row events into the indexer and checkpoints the durable LSN.
//
// The checkpoint cursor is the last COMPLETE transaction's commit LSN (lastCommitLSN)
// and nothing else — never a per-row LSN. A batch-full or ticker flush can land rows
// of an in-flight transaction, but the checkpoint stays at the last commit, so a
// crash resumes at a transaction boundary and PostgreSQL re-delivers the in-flight
// tail (at-least-once). This is the #491 invariant. The order at every checkpoint is
// flush → saveCheckpointPG → AckCommitted: PostgreSQL WAL is released only after the
// rows are durably indexed AND the checkpoint is durably persisted.
func streamLoopPG(
	ctx context.Context,
	events <-chan event.Event,
	idx *indexer.Indexer,
	indexDB *sql.DB,
	cap *pgcapture.Capturer,
	checkpointInterval time.Duration,
	state *pgStreamState,
	logger *slog.Logger,
) error {
	batch := make([]event.Event, 0, idx.BatchSize())
	ticker := time.NewTicker(checkpointInterval)
	defer ticker.Stop()

	var lastCommitLSN uint64

	// snapshotByTable maps "schema.table" → the snapshot_id of that table's most
	// recent EventRelation (#533). Per-TABLE, not a single scalar: pgoutput sends a
	// RelationMessage once per relation per session, so a second table's snapshot
	// would otherwise clobber the first, and every later row of the first table would
	// be stamped the wrong snapshot_id (recovery would silently fall back to an
	// all-columns WHERE — #531 left unclosed for all but the last table). A map miss
	// yields 0 → the safe SchemaVersion-0 all-columns fallback, which an in-scope row
	// never hits (its RelationMessage always precedes it).
	snapshotByTable := make(map[string]uint32)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := idx.InsertBatch(batch)
		state.eventsIndexed += n
		// Discard the batch unconditionally: InsertBatch is a single atomic INSERT
		// (0 rows on error), and every flush-error path here is fatal — the loop
		// aborts and PostgreSQL re-streams the whole tail from the unadvanced
		// checkpoint. If a future change ever retries or continues past a flush
		// error, this clear must move into the success branch or those rows are lost.
		batch = batch[:0]
		return err
	}

	checkpoint := func() error {
		if err := flush(); err != nil {
			return err
		}
		if lastCommitLSN == 0 {
			return nil // nothing committed yet — nothing durable to checkpoint
		}
		if err := saveCheckpointPG(indexDB, state, lastCommitLSN); err != nil {
			return err
		}
		// Only now is it safe to let PostgreSQL release WAL up to lastCommitLSN.
		cap.AckCommitted(lastCommitLSN)
		state.lsn = lastCommitLSN
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			if err := checkpoint(); err != nil {
				logger.Warn("pgstreamrun: final checkpoint failed on shutdown", "error", err)
			}
			return nil

		case <-ticker.C:
			if err := checkpoint(); err != nil {
				return err
			}

		case ev, ok := <-events:
			if !ok {
				if err := checkpoint(); err != nil {
					logger.Warn("pgstreamrun: final checkpoint failed", "error", err)
				}
				return nil
			}
			if !ev.Timestamp.IsZero() {
				state.lastEventTime = sql.NullTime{Time: ev.Timestamp, Valid: true}
			}
			switch ev.EventType {
			case event.EventCommit:
				// The boundary: record the commit LSN; its rows are already in the
				// batch (pgoutput delivers them before the commit).
				lastCommitLSN = ev.EndPos
			case event.EventDDL:
				if err := flush(); err != nil {
					return err
				}
				lastCommitLSN = ev.EndPos
			case event.EventRelation:
				// A relation's shape (#533): persist it as a schema snapshot and record
				// its snapshot_id for stamping this table's subsequent rows. The snapshot
				// commits immediately (its own txn), BEFORE the rows referencing it are
				// flushed — so "snapshot durable before its rows durable" holds; it sits
				// outside the flush→checkpoint→ack sequence, so it does not advance the
				// cursor and does NOT force a flush (already-batched rows carry their own,
				// already-persisted snapshot_ids — per-row schema_version makes a mixed
				// batch correct). A crash before the next checkpoint just re-delivers and
				// re-snapshots (a benign orphan id).
				id, werr := metadata.WritePGSnapshot(indexDB, ev.Relation)
				if werr != nil {
					return fmt.Errorf("pgstreamrun: persist schema snapshot for %s.%s: %w", ev.Schema, ev.Table, werr)
				}
				snapshotByTable[ev.Schema+"."+ev.Table] = uint32(id)
			case event.EventGTID:
				// PostgreSQL does not emit a leading GTID marker; ignore defensively.
			default:
				ev.SchemaVersion = snapshotByTable[ev.Schema+"."+ev.Table]
				batch = append(batch, ev)
				if len(batch) >= idx.BatchSize() {
					if err := flush(); err != nil {
						return err
					}
				}
			}
		}
	}
}

// loadStreamStatePG loads the single-row checkpoint (id=1) for a PostgreSQL source.
// It returns nil on first run (no row). If a row exists but is NOT a PostgreSQL
// checkpoint, it fails loud rather than misread/clobber a MySQL/MariaDB checkpoint
// (stream_state is single-row; an index DB serves one source).
func loadStreamStatePG(db *sql.DB) (*pgStreamState, error) {
	var (
		flavor        string
		lsn           uint64
		eventsIndexed int64
		serverID      uint32
		lastEventTime sql.NullTime
	)
	err := db.QueryRow(`SELECT flavor, binlog_position, events_indexed, server_id, last_event_time
		FROM stream_state WHERE id = 1`).Scan(&flavor, &lsn, &eventsIndexed, &serverID, &lastEventTime)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgstreamrun: load checkpoint: %w", err)
	}
	if flavor != pgFlavor {
		return nil, fmt.Errorf("pgstreamrun: index database holds a %q checkpoint, not %q — refusing to stream a PostgreSQL source into a non-PostgreSQL index", flavor, pgFlavor)
	}
	return &pgStreamState{lsn: lsn, eventsIndexed: eventsIndexed, serverID: serverID, lastEventTime: lastEventTime}, nil
}

// saveCheckpointPG persists the durable cursor (commitLSN — the last complete
// transaction) and the running counters. mode='gtid'/flavor='postgres': the LSN
// rides binlog_position (uint64) and binlog_file/gtid_set (the "X/Y" string).
func saveCheckpointPG(db *sql.DB, state *pgStreamState, commitLSN uint64) error {
	lsnStr := pglogrepl.LSN(commitLSN).String()
	var lastEventTime any
	if state.lastEventTime.Valid {
		lastEventTime = state.lastEventTime.Time
	}
	_, err := db.Exec(`
		INSERT INTO stream_state
			(id, mode, binlog_file, binlog_position, gtid_set, flavor,
			 events_indexed, last_event_time, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), ?)
		ON DUPLICATE KEY UPDATE
			binlog_file     = VALUES(binlog_file),
			binlog_position = VALUES(binlog_position),
			gtid_set        = VALUES(gtid_set),
			flavor          = VALUES(flavor),
			events_indexed  = VALUES(events_indexed),
			last_event_time = VALUES(last_event_time),
			last_checkpoint = UTC_TIMESTAMP(),
			server_id       = VALUES(server_id)`,
		lsnStr, commitLSN, lsnStr, pgFlavor, state.eventsIndexed, lastEventTime, state.serverID)
	if err != nil {
		return fmt.Errorf("pgstreamrun: save checkpoint at %s: %w", lsnStr, err)
	}
	return nil
}

// resolveStartLSN picks the start LSN: a saved checkpoint wins (idempotent resume);
// otherwise the explicit Config.StartLSN; otherwise 0 — on first run 0 is correct,
// the capturer creates the slot and starts from its ConsistentPoint.
func resolveStartLSN(saved *pgStreamState, flagLSN uint64, logger *slog.Logger) uint64 {
	if saved != nil {
		logger.Info("pgstreamrun: resuming from checkpoint", "lsn", pglogrepl.LSN(saved.lsn))
		return saved.lsn
	}
	if flagLSN != 0 {
		logger.Info("pgstreamrun: starting from configured LSN", "lsn", pglogrepl.LSN(flagLSN))
		return flagLSN
	}
	logger.Info("pgstreamrun: first run — starting from the slot's consistent point")
	return 0
}
