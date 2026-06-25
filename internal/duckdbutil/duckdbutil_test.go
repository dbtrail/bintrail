package duckdbutil

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestHomeDirPragma: no override when $HOME resolves to an existing directory
// (the normal case), and a SET home_directory when it does not — the homeless
// user case (`useradd --no-create-home`) that broke DuckDB's INSTALL.
func TestHomeDirPragma(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // exists ⇒ leave DuckDB's default alone
	if p := homeDirPragma(); p != "" {
		t.Errorf("expected no pragma when $HOME exists, got %q", p)
	}

	t.Setenv("HOME", filepath.Join(t.TempDir(), "does-not-exist"))
	p := homeDirPragma()
	if !strings.HasPrefix(p, "SET home_directory='") || !strings.HasSuffix(p, "'; ") {
		t.Errorf("expected a SET home_directory pragma for a missing $HOME, got %q", p)
	}
}

// TestLoadHTTPFSHomelessUser is the regression guard for the reported bug: a
// process whose $HOME points at a non-existent directory (a container user
// created with `useradd --no-create-home`) failed httpfs INSTALL with "Can't
// find the home directory at '/home/...'". With the home_directory pin,
// LoadHTTPFS must never surface that error — it may still fail on an offline
// host (extension download), but never on the home directory — and the session
// must stay usable.
func TestLoadHTTPFSHomelessUser(t *testing.T) {
	t.Setenv("HOME", filepath.Join(t.TempDir(), "no-home-here"))

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := LoadHTTPFS(context.Background(), db); err != nil &&
		strings.Contains(err.Error(), "home directory") {
		t.Fatalf("LoadHTTPFS must not fail on the home directory for a homeless user: %v", err)
	}

	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after LoadHTTPFS: %v (got %d)", err, one)
	}
}

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

// TestEnableS3CredentialChainCreatesSecret pins the success path: when the
// aws extension is available, the chain secret must actually exist — without
// this, renaming the secret, typoing the SQL, or deleting the CREATE entirely
// keeps the suite green. Dummy env creds make the chain always resolvable
// (creds-less CI would otherwise hit the legitimate validation failure).
func TestEnableS3CredentialChainCreatesSecret(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATESTDUMMY0000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChain(context.Background(), db)

	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host) — the best-effort fallback branch applies")
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("chain secret missing after a successful extension load: n=%d err=%v", n, err)
	}
}

// TestEnableS3CredentialChainEscapeHatch: BINTRAIL_DUCKDB_NO_AWS_EXT must
// skip the setup entirely (no INSTALL attempt that could stall behind a
// blackholing proxy) — no secret, session untouched.
func TestEnableS3CredentialChainEscapeHatch(t *testing.T) {
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChain(context.Background(), db)

	var n int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("escape hatch did not skip the secret setup: n=%d err=%v", n, err)
	}
}

// TestEnableS3CredentialChainRegion: pinning a region must keep the session
// usable and, when the aws extension is available, still create the chain
// secret (now carrying REGION). The region travels with the secret so a
// cross-region httpfs read doesn't 301 regardless of secret-vs-SET precedence
// (#511). Dummy env creds make the chain resolvable on creds-less CI.
func TestEnableS3CredentialChainRegion(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATESTDUMMY0000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChainRegion(context.Background(), db, "eu-west-1")

	// Session must remain usable whether or not the aws extension loaded.
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after EnableS3CredentialChainRegion: %v (got %d)", err, one)
	}

	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host) — region pin not exercised")
	}
	// Assert the region actually TRAVELS WITH the secret — not just that a
	// secret exists (which TestEnableS3CredentialChainCreatesSecret already
	// covers). secret_string carries the region in plaintext, so this pins the
	// commit's actual change: stripping the REGION clause would fail here.
	var secretStr string
	if err := db.QueryRow("SELECT secret_string FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&secretStr); err != nil {
		t.Fatalf("region-pinned chain secret missing after a successful extension load: %v", err)
	}
	if !strings.Contains(secretStr, "region=eu-west-1") {
		t.Fatalf("chain secret does not carry the pinned region; got secret_string=%q", secretStr)
	}
}
