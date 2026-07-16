package consoleapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

func TestPGBaselineConfig(t *testing.T) {
	req := console.BaselineRequest{
		SourceDSN:   "postgres://repl:secret@pg:5432/appdb",
		Slot:        "bintrail_slot",
		Publication: "bintrail_pub",
		Schemas:     []string{"public", "shop"},
		Flavor:      "postgres",
	}
	cfg, err := pgBaselineConfig(req, "/out")
	if err != nil {
		t.Fatalf("pgBaselineConfig: %v", err)
	}
	if cfg.QueryDSN != req.SourceDSN {
		t.Errorf("QueryDSN = %q, want the source DSN", cfg.QueryDSN)
	}
	if !strings.Contains(cfg.ReplDSN, "replication=database") {
		t.Errorf("ReplDSN missing replication=database: %q", cfg.ReplDSN)
	}
	if cfg.SlotName != "bintrail_slot" || cfg.Publication != "bintrail_pub" {
		t.Errorf("slot/publication wrong: %q / %q", cfg.SlotName, cfg.Publication)
	}
	if cfg.OutputDir != "/out" || cfg.Compression != "zstd" {
		t.Errorf("output/compression wrong: %q / %q", cfg.OutputDir, cfg.Compression)
	}
}

func TestPGBaselineConfig_badDSN(t *testing.T) {
	// A DSN already carrying replication is rejected by PGReplDSN.
	if _, err := pgBaselineConfig(console.BaselineRequest{SourceDSN: "postgres://h:5432/db?replication=database"}, "/out"); err == nil {
		t.Error("expected error for a repl-carrying source DSN")
	}
}
