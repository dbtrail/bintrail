//go:build integration

package config

import (
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/testutil"
)

func TestConnect_validDSN(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	dsn := testutil.IntegrationDSN("")
	db, err := Connect(dsn)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer db.Close()

	// Verify the connection works.
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if one != 1 {
		t.Errorf("expected 1, got %d", one)
	}
}

func TestConnect_parseTimeInjected(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	// Even without parseTime in the DSN, Connect should inject it.
	dsn := testutil.BaseDSN() + "/"
	db, err := Connect(dsn)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer db.Close()

	// SELECT NOW() should scan into time.Time, not []uint8.
	var ts time.Time
	if err := db.QueryRow("SELECT NOW()").Scan(&ts); err != nil {
		t.Fatalf("SELECT NOW() scan failed (parseTime not working?): %v", err)
	}
	if ts.IsZero() {
		t.Error("expected non-zero time from NOW()")
	}
}

func TestConnect_invalidDSN(t *testing.T) {
	_, err := Connect("not a valid dsn!@#$%")
	if err == nil {
		t.Error("expected error for invalid DSN, got nil")
	}
}

func TestConnect_unreachableHost(t *testing.T) {
	// Use a non-routable address to trigger a connection error.
	_, err := Connect("root:root@tcp(192.0.2.1:3306)/test?timeout=1s")
	if err == nil {
		t.Error("expected error for unreachable host, got nil")
	}
}
