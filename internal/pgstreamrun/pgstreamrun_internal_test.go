package pgstreamrun

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// TestBuildSourceHealthJSON pins the wire shape written to stream_state.source_health
// (#599): SlotHealth's sql.NullInt64 and pglogrepl.LSN must flatten cleanly (NULL safe
// margin → JSON null, LSN → "X/Y" string), an empty not-full list must marshal as []
// (not null) for a stable frontend contract, and checked_at must be RFC3339 UTC.
func TestBuildSourceHealthJSON(t *testing.T) {
	checkedAt := time.Date(2026, 6, 23, 18, 30, 0, 0, time.UTC)

	// Unlimited retention (invalid SafeWalSize → null) + a non-FULL table.
	snap := pgcapture.HealthSnapshot{
		Slot: pgcapture.SlotHealth{
			Exists: true, Active: true, WalStatus: "reserved",
			RetainedBytes: 16384, RestartLSN: pglogrepl.LSN(0x1A2B3C4),
			SafeWalSize: sql.NullInt64{}, // invalid → null
		},
		ReplicaIdentityNotFull: []string{"app.t1 (relreplident=d)"},
	}
	var got map[string]any
	if b, err := buildSourceHealthJSON(snap, checkedAt); err != nil {
		t.Fatalf("buildSourceHealthJSON: %v", err)
	} else if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["wal_status"] != "reserved" {
		t.Errorf("wal_status = %v", got["wal_status"])
	}
	// Pin the key NAMES the frontend (pgHealthCard) reads directly — a json-tag rename on
	// any of these would pass a looser test green and silently blank the panel.
	if got["exists"] != true {
		t.Errorf("exists = %v, want true", got["exists"])
	}
	if got["active"] != true {
		t.Errorf("active = %v, want true", got["active"])
	}
	if got["retained_bytes"] != float64(16384) {
		t.Errorf("retained_bytes = %v, want 16384", got["retained_bytes"])
	}
	if got["safe_wal_size"] != nil {
		t.Errorf("safe_wal_size must be null when SafeWalSize is invalid, got %v", got["safe_wal_size"])
	}
	if got["checked_at"] != "2026-06-23T18:30:00Z" {
		t.Errorf("checked_at = %v, want RFC3339 UTC", got["checked_at"])
	}
	if got["restart_lsn"] != pglogrepl.LSN(0x1A2B3C4).String() {
		t.Errorf("restart_lsn = %v, want %q", got["restart_lsn"], pglogrepl.LSN(0x1A2B3C4).String())
	}
	if nf, _ := got["replica_identity_not_full"].([]any); len(nf) != 1 || nf[0] != "app.t1 (relreplident=d)" {
		t.Errorf("replica_identity_not_full = %v", got["replica_identity_not_full"])
	}

	// Valid SafeWalSize → a number; empty not-full → [] (a non-nil slice), never null.
	snap2 := pgcapture.HealthSnapshot{
		Slot:                   pgcapture.SlotHealth{Exists: true, SafeWalSize: sql.NullInt64{Int64: 1 << 30, Valid: true}},
		ReplicaIdentityNotFull: nil,
	}
	var got2 map[string]any
	if b, err := buildSourceHealthJSON(snap2, checkedAt); err != nil {
		t.Fatalf("buildSourceHealthJSON(2): %v", err)
	} else if err := json.Unmarshal(b, &got2); err != nil {
		t.Fatalf("unmarshal(2): %v", err)
	}
	if got2["safe_wal_size"] == nil {
		t.Errorf("safe_wal_size must be a number when SafeWalSize is valid")
	}
	if nf, ok := got2["replica_identity_not_full"].([]any); !ok || nf == nil {
		t.Errorf("empty replica_identity_not_full must marshal as [], got %v", got2["replica_identity_not_full"])
	}
}

// TestBuildProbeErrorJSON pins the failed-probe wire shape (#599 review): a probe error
// is persisted as a snapshot carrying probe_error + checked_at (so the console shows
// "probe failing", not a blank panel), with replica_identity_not_full normalized to [];
// and a healthy snapshot must OMIT probe_error.
func TestBuildProbeErrorJSON(t *testing.T) {
	checkedAt := time.Date(2026, 6, 23, 18, 30, 0, 0, time.UTC)
	b, err := buildProbeErrorJSON("recovery is in progress", checkedAt)
	if err != nil {
		t.Fatalf("buildProbeErrorJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["probe_error"] != "recovery is in progress" {
		t.Errorf("probe_error = %v", got["probe_error"])
	}
	if got["checked_at"] != "2026-06-23T18:30:00Z" {
		t.Errorf("checked_at = %v", got["checked_at"])
	}
	if nf, ok := got["replica_identity_not_full"].([]any); !ok || nf == nil {
		t.Errorf("replica_identity_not_full must be [], got %v", got["replica_identity_not_full"])
	}

	// A successful snapshot must NOT carry probe_error (omitempty).
	healthy, _ := buildSourceHealthJSON(pgcapture.HealthSnapshot{}, checkedAt)
	var gm map[string]any
	if err := json.Unmarshal(healthy, &gm); err != nil {
		t.Fatalf("unmarshal healthy: %v", err)
	}
	if _, present := gm["probe_error"]; present {
		t.Errorf("a healthy snapshot must omit probe_error, got %v", gm["probe_error"])
	}
}

func TestResolveStartLSN(t *testing.T) {
	log := slog.New(slog.DiscardHandler)

	// A saved checkpoint wins over the flag (idempotent resume).
	if got := resolveStartLSN(&pgStreamState{lsn: 500}, 999, log); got != 500 {
		t.Errorf("saved checkpoint: got %d, want 500", got)
	}
	// No checkpoint → the explicit flag.
	if got := resolveStartLSN(nil, 999, log); got != 999 {
		t.Errorf("flag start: got %d, want 999", got)
	}
	// No checkpoint, no flag → 0: first run, the capturer starts from the slot's
	// ConsistentPoint (this must NOT be an error).
	if got := resolveStartLSN(nil, 0, log); got != 0 {
		t.Errorf("first run: got %d, want 0", got)
	}
}
