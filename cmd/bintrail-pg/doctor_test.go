package main

import (
	"strings"
	"testing"
)

// resetPGDoctorFlags restores the doctor command's package-global flag vars to their
// declared defaults so each subtest starts clean (pgDoctorConfigFromFlags writes the
// env fallback back into the globals). These subtests mutate shared globals and must
// NOT run in parallel.
func resetPGDoctorFlags() {
	pgDoctorQueryDSN = ""
	pgDoctorSlot = ""
	pgDoctorPublication = ""
	pgDoctorSchemas = ""
	pgDoctorTables = ""
	pgDoctorFormat = "text"
}

func clearPGDoctorEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{"BINTRAIL_PG_QUERY_DSN", "BINTRAIL_PG_SLOT", "BINTRAIL_PG_PUBLICATION"} {
		t.Setenv(v, "")
	}
}

func TestPGDoctorConfigFromFlags_MissingRequired(t *testing.T) {
	clearPGDoctorEnv(t)
	resetPGDoctorFlags()

	_, err := pgDoctorConfigFromFlags()
	if err == nil {
		t.Fatal("expected an error when required settings are missing, got nil")
	}
	for _, want := range []string{"--query-dsn", "--slot", "--publication"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should name the missing flag %q", err, want)
		}
	}
}

func TestPGDoctorConfigFromFlags_HappyPath(t *testing.T) {
	clearPGDoctorEnv(t)
	resetPGDoctorFlags()
	pgDoctorQueryDSN = "postgres://u@localhost/db"
	pgDoctorSlot = "bintrail_slot"
	pgDoctorPublication = "bintrail_pub"
	pgDoctorSchemas = "public"
	pgDoctorTables = "public.orders"

	cfg, err := pgDoctorConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.QueryDSN != "postgres://u@localhost/db" || cfg.SlotName != "bintrail_slot" || cfg.Publication != "bintrail_pub" {
		t.Errorf("passthrough mismatch: %+v", cfg)
	}
	if cfg.Schemas != "public" || cfg.Tables != "public.orders" {
		t.Errorf("filter mismatch: %+v", cfg)
	}
}

func TestPGDoctorConfigFromFlags_EnvFallback(t *testing.T) {
	clearPGDoctorEnv(t)
	resetPGDoctorFlags()
	t.Setenv("BINTRAIL_PG_QUERY_DSN", "query-from-env")
	t.Setenv("BINTRAIL_PG_SLOT", "slot-from-env")
	t.Setenv("BINTRAIL_PG_PUBLICATION", "pub-from-env")

	cfg, err := pgDoctorConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.QueryDSN != "query-from-env" || cfg.SlotName != "slot-from-env" || cfg.Publication != "pub-from-env" {
		t.Errorf("env fallback not applied: %+v", cfg)
	}
}

func TestPGDoctorConfigFromFlags_FlagWinsOverEnv(t *testing.T) {
	clearPGDoctorEnv(t)
	resetPGDoctorFlags()
	pgDoctorQueryDSN = "query-from-flag"
	pgDoctorSlot = "slot-from-flag"
	pgDoctorPublication = "pub-from-flag"
	t.Setenv("BINTRAIL_PG_QUERY_DSN", "query-from-env")

	cfg, err := pgDoctorConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.QueryDSN != "query-from-flag" {
		t.Errorf("QueryDSN = %q, want the CLI flag to win over env", cfg.QueryDSN)
	}
}

func TestPGDoctorConfigFromFlags_InvalidFormat(t *testing.T) {
	clearPGDoctorEnv(t)
	resetPGDoctorFlags()
	pgDoctorQueryDSN = "q"
	pgDoctorSlot = "s"
	pgDoctorPublication = "p"
	pgDoctorFormat = "yaml"

	if _, err := pgDoctorConfigFromFlags(); err == nil || !strings.Contains(err.Error(), "format") {
		t.Fatalf("expected an invalid-format error, got %v", err)
	}
}

// TestPGDoctorCmd_defaults pins the registered --format default ("text").
func TestPGDoctorCmd_defaults(t *testing.T) {
	f := doctorCmd.Flag("format")
	if f == nil {
		t.Fatal("flag --format not registered")
	}
	if f.DefValue != "text" {
		t.Errorf("--format default = %q, want text", f.DefValue)
	}
}
