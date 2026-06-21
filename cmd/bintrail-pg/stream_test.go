package main

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

// resetPGFlags restores the stream command's package-global flag vars to their
// declared defaults so each subtest starts clean — pgStreamConfigFromFlags
// writes the env fallback back into the globals, so they must be reset between
// cases. These subtests mutate shared globals and must NOT run in parallel.
func resetPGFlags() {
	pgIndexDSN = ""
	pgReplDSN = ""
	pgQueryDSN = ""
	pgSlot = ""
	pgPublication = ""
	pgServerID = 0
	pgStartLSN = ""
	pgSchemas = ""
	pgTables = ""
	pgBatchSize = 1000
	pgCheckpoint = 5
	pgPartitions = 48
}

// clearPGEnv blanks the BINTRAIL_PG_* vars for the test. t.Setenv to "" reads as
// unset in applyEnvFallback (which treats empty as absent), so a developer's
// shell environment cannot leak into these table-driven cases.
func clearPGEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{
		"BINTRAIL_PG_REPL_DSN", "BINTRAIL_PG_QUERY_DSN", "BINTRAIL_PG_SLOT",
		"BINTRAIL_PG_PUBLICATION", "BINTRAIL_PG_START_LSN",
	} {
		t.Setenv(v, "")
	}
}

func TestPGStreamConfigFromFlags_MissingRequired(t *testing.T) {
	clearPGEnv(t)
	resetPGFlags()
	// index-dsn/server-id are enforced by MarkFlagRequired at the cobra layer,
	// not here; this seam only validates the PG-specific connection settings.

	_, err := pgStreamConfigFromFlags()
	if err == nil {
		t.Fatal("expected an error when all PG connection settings are missing, got nil")
	}
	for _, want := range []string{"--repl-dsn", "--query-dsn", "--slot", "--publication"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should name the missing flag %q", err, want)
		}
	}
}

func TestPGStreamConfigFromFlags_HappyPathFromFlags(t *testing.T) {
	clearPGEnv(t)
	resetPGFlags()
	pgIndexDSN = "user:pass@tcp(localhost:3306)/bintrail_index"
	pgReplDSN = "postgres://u@localhost/db?replication=database"
	pgQueryDSN = "postgres://u@localhost/db"
	pgSlot = "bintrail_slot"
	pgPublication = "bintrail_pub"
	pgServerID = 42
	pgSchemas = "public"
	pgTables = "public.orders"
	pgBatchSize = 500
	pgCheckpoint = 12
	pgPartitions = 24

	cfg, err := pgStreamConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.IndexDSN != pgIndexDSN || cfg.ReplDSN != pgReplDSN || cfg.QueryDSN != pgQueryDSN {
		t.Errorf("DSN passthrough mismatch: %+v", cfg)
	}
	if cfg.SlotName != "bintrail_slot" || cfg.Publication != "bintrail_pub" {
		t.Errorf("slot/publication mismatch: %+v", cfg)
	}
	if cfg.ServerID != 42 || cfg.Schemas != "public" || cfg.Tables != "public.orders" {
		t.Errorf("filter/server-id mismatch: %+v", cfg)
	}
	if cfg.BatchSize != 500 || cfg.Partitions != 24 {
		t.Errorf("batch/partitions mismatch: %+v", cfg)
	}
	if cfg.Checkpoint != 12*time.Second {
		t.Errorf("Checkpoint = %v, want 12s (the --checkpoint int is seconds)", cfg.Checkpoint)
	}
	if cfg.StartLSN != 0 {
		t.Errorf("StartLSN = %d, want 0 when --start-lsn is unset", cfg.StartLSN)
	}
}

func TestPGStreamConfigFromFlags_EnvFallback(t *testing.T) {
	clearPGEnv(t)
	resetPGFlags()
	pgIndexDSN = "idx"
	pgServerID = 7
	// All PG-specific settings supplied via env only (no CLI flag).
	t.Setenv("BINTRAIL_PG_REPL_DSN", "repl-from-env")
	t.Setenv("BINTRAIL_PG_QUERY_DSN", "query-from-env")
	t.Setenv("BINTRAIL_PG_SLOT", "slot-from-env")
	t.Setenv("BINTRAIL_PG_PUBLICATION", "pub-from-env")

	cfg, err := pgStreamConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ReplDSN != "repl-from-env" || cfg.QueryDSN != "query-from-env" ||
		cfg.SlotName != "slot-from-env" || cfg.Publication != "pub-from-env" {
		t.Errorf("env fallback not applied: %+v", cfg)
	}
}

func TestPGStreamConfigFromFlags_FlagWinsOverEnv(t *testing.T) {
	clearPGEnv(t)
	resetPGFlags()
	pgIndexDSN = "idx"
	pgServerID = 7
	pgReplDSN = "repl-from-flag" // CLI flag set
	pgQueryDSN = "query-from-flag"
	pgSlot = "slot-from-flag"
	pgPublication = "pub-from-flag"
	t.Setenv("BINTRAIL_PG_REPL_DSN", "repl-from-env") // env also set — flag must win

	cfg, err := pgStreamConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ReplDSN != "repl-from-flag" {
		t.Errorf("ReplDSN = %q, want the CLI flag value to win over env", cfg.ReplDSN)
	}
}

func TestPGStreamConfigFromFlags_StartLSN(t *testing.T) {
	clearPGEnv(t)

	t.Run("invalid", func(t *testing.T) {
		resetPGFlags()
		setPGRequired()
		pgStartLSN = "not-an-lsn"
		_, err := pgStreamConfigFromFlags()
		if err == nil || !strings.Contains(err.Error(), "start-lsn") {
			t.Fatalf("expected a start-lsn parse error, got %v", err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		resetPGFlags()
		setPGRequired()
		pgStartLSN = "16/B374D848"
		cfg, err := pgStreamConfigFromFlags()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want, _ := pglogrepl.ParseLSN("16/B374D848")
		if cfg.StartLSN != uint64(want) {
			t.Errorf("StartLSN = %d, want %d", cfg.StartLSN, uint64(want))
		}
	})
}

// setPGRequired fills the PG-specific required settings so a test can exercise
// downstream parsing (e.g. --start-lsn) without tripping the missing-settings
// guard. Callers must resetPGFlags() first.
func setPGRequired() {
	pgReplDSN = "repl"
	pgQueryDSN = "query"
	pgSlot = "slot"
	pgPublication = "pub"
}
