//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// driveHealthOnce runs streamLoopPG with the given health probe and a tiny interval,
// waits (bounded) for the first source_health snapshot to land, then stops the loop and
// returns the index DB for assertions. No live PostgreSQL is needed: the probe is
// injected, and a health-only tick never touches `cap` (the only cap deref is
// cap.AckCommitted inside checkpoint(), gated behind lastCommitLSN != 0 — and no commit
// event is ever sent here), so a nil cap is safe. checkpointInterval is an hour so only
// the health ticker fires.
func driveHealthOnce(t *testing.T, probe func(context.Context) (pgcapture.HealthSnapshot, error)) *sql.DB {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	indexDB, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, indexDB, 4, false, nil); err != nil {
		cancel()
		t.Fatalf("CreateIndexTables: %v", err)
	}
	idx := indexer.New(indexDB, 100)
	events := make(chan event.Event) // never sent → no commit → nil cap untouched
	state := &pgStreamState{serverID: 88}

	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(ctx, events, idx, indexDB, nil, time.Hour, probe, 15*time.Millisecond, state, slog.New(slog.DiscardHandler), nil)
	}()

	present := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var sh sql.NullString
		if err := indexDB.QueryRow(`SELECT source_health FROM stream_state WHERE id=1`).Scan(&sh); err == nil && sh.Valid {
			present = true
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	cancel()
	<-done
	if !present {
		t.Fatal("no source_health snapshot landed — the healthTicker → pollHealth → saveSourceHealth wiring is dead")
	}
	return indexDB
}

// TestStreamLoopPG_HealthTickWiring exercises the full loop wiring (#599 review, sev-6):
// the three pieces (probe / buildSourceHealthJSON / saveSourceHealth) are unit-tested
// separately, but broken wiring would silently write nothing while every separate test
// stays green. A successful probe must land its snapshot in stream_state.source_health.
func TestStreamLoopPG_HealthTickWiring(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db := driveHealthOnce(t, func(context.Context) (pgcapture.HealthSnapshot, error) {
		return pgcapture.HealthSnapshot{Slot: pgcapture.SlotHealth{Exists: true, WalStatus: "reserved"}}, nil
	})
	var ws sql.NullString
	if err := db.QueryRow(`SELECT source_health->>'$.wal_status' FROM stream_state WHERE id=1`).Scan(&ws); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if ws.String != "reserved" {
		t.Errorf("the loop did not persist the probe's snapshot: %q", ws.String)
	}
}

// TestStreamLoopPG_HealthProbeErrorPersists proves the never-written-snapshot fix (#599
// review HIGH): a probe that always errors must STILL persist a snapshot — carrying
// probe_error — so the console shows "probe failing" rather than a blank panel.
func TestStreamLoopPG_HealthProbeErrorPersists(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db := driveHealthOnce(t, func(context.Context) (pgcapture.HealthSnapshot, error) {
		return pgcapture.HealthSnapshot{}, errors.New("recovery is in progress")
	})
	var pe sql.NullString
	if err := db.QueryRow(`SELECT source_health->>'$.probe_error' FROM stream_state WHERE id=1`).Scan(&pe); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !pe.Valid || !strings.Contains(pe.String, "recovery is in progress") {
		t.Errorf("a persistently failing probe must record probe_error (the never-written-snapshot gap), got: %v", pe)
	}
}
