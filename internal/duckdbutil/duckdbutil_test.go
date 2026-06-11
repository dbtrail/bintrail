package duckdbutil

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestEnableS3CredentialChainKeepsSessionUsable: the helper is best-effort —
// whether the aws extension installs (network) or not (offline), it must
// never break the session it was called on.
func TestEnableS3CredentialChainKeepsSessionUsable(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChain(context.Background(), db)

	var one int
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after EnableS3CredentialChain: %v (got %d)", err, one)
	}
}
