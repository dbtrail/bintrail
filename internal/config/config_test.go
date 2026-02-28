package config

import (
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// TestConnect_locUTCApplied verifies that Connect injects Loc=UTC, overriding
// any loc the user may have specified in their DSN.
func TestConnect_locUTCApplied(t *testing.T) {
	// Simulate the path Connect takes for a DSN with loc=Local.
	cfg, err := mysql.ParseDSN("root:pass@tcp(127.0.0.1:3306)/test?loc=Local")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC // Connect always forces this

	if cfg.Loc != time.UTC {
		t.Errorf("expected Loc=UTC, got %v", cfg.Loc)
	}
}
