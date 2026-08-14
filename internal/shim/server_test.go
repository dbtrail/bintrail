package shim

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// TestNewMySQLServerAcceptsKnownMethods pins the four spellings the
// shim accepts: empty (default), and the three constants from
// go-mysql/mysql. Each must produce a non-nil *server.Server without
// panicking — go-mysql's NewServer panics on an unsupported method,
// so a regression that bypassed our switch and forwarded an unknown
// string would crash the shim at handshake time. The error path is
// covered by TestNewMySQLServerRejectsUnknownMethod.
func TestNewMySQLServerAcceptsKnownMethods(t *testing.T) {
	cases := []string{
		"",
		mysql.AUTH_NATIVE_PASSWORD,
		mysql.AUTH_CACHING_SHA2_PASSWORD,
		mysql.AUTH_SHA256_PASSWORD,
	}
	for _, m := range cases {
		t.Run(m, func(t *testing.T) {
			srv, err := NewMySQLServer(m)
			if err != nil {
				t.Fatalf("NewMySQLServer(%q): %v", m, err)
			}
			if srv == nil {
				t.Fatalf("NewMySQLServer(%q): nil *server.Server", m)
			}
		})
	}
}

// TestNewMySQLServerRejectsUnknownMethod pins the error path: an
// unrecognised auth_method must return a typed error before reaching
// go-mysql's NewServer (which panics). The CLI surface relies on this
// to convert operator typos into a clean cobra error rather than a
// process-wide crash on the first incoming connection.
func TestNewMySQLServerRejectsUnknownMethod(t *testing.T) {
	srv, err := NewMySQLServer("not_a_real_method")
	if srv != nil {
		t.Errorf("expected nil server on error, got %v", srv)
	}
	if err == nil {
		t.Fatal("expected error for unknown auth method")
	}
	if !strings.Contains(err.Error(), "unsupported auth_method") {
		t.Errorf("error should mention unsupported auth_method, got: %v", err)
	}
}

// TestGenerateSelfSignedTLSProducesUsableArtifacts verifies the helper
// returns the RSA private key go-mysql/server needs — NewServer takes it
// directly (v1.15.0+) and derives the public-key bytes it serves to
// clients from it — and a TLS config whose cert carries that SAME key
// (the RSA-OAEP decrypt branch in (*Conn).handleAuthSwitchResponse
// dereferences it on a non-TLS SHA2 full auth). A regression that
// returned a nil tlsConfig, an EC key, or a cert/key split would
// surface as a nil-deref / type assertion panic on the first SHA2
// cache miss — catch it at unit test time instead.
//
// The OAEP round-trip at the end is the load-bearing assertion: a
// keypair mis-sized for OAEP-SHA1 (or a future change that swapped
// SHA1 for a hash the upstream library doesn't use) would produce a
// unit-green result and fail at runtime. Performing the actual
// encrypt/decrypt the wire path performs pins the contract.
func TestGenerateSelfSignedTLSProducesUsableArtifacts(t *testing.T) {
	priv, tlsConfig, err := generateSelfSignedTLS()
	if err != nil {
		t.Fatalf("generateSelfSignedTLS: %v", err)
	}
	if priv == nil {
		t.Fatal("generateSelfSignedTLS must return the RSA private key NewServer is handed")
	}

	if tlsConfig == nil || len(tlsConfig.Certificates) == 0 {
		t.Fatal("tlsConfig must carry at least one Certificate")
	}
	certKey, ok := tlsConfig.Certificates[0].PrivateKey.(*rsa.PrivateKey)
	if !ok || certKey == nil {
		t.Fatalf("tlsConfig.Certificates[0].PrivateKey must be *rsa.PrivateKey for go-mysql full-auth path; got %T",
			tlsConfig.Certificates[0].PrivateKey)
	}
	if certKey != priv {
		t.Error("the TLS cert must carry the SAME private key NewServer is handed (key pair split — full auth would fail to round-trip)")
	}

	// OAEP round-trip with sha1.New — this is the exact algorithm
	// go-mysql's RSA-OAEP decrypt branch uses
	// (rsa.DecryptOAEP(sha1.New(), rand.Reader, ...)). A regression
	// where the keypair were mis-sized for OAEP-SHA1 — e.g. a 512-bit
	// key would fail "message too long for RSA key size" at runtime —
	// fails this assertion instead.
	plaintext := []byte("password-secret_a-salt-AAAAAAAAAAAAAAAAAAAA")
	ciphertext, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, &priv.PublicKey, plaintext, nil)
	if err != nil {
		t.Fatalf("rsa.EncryptOAEP with returned pubKey: %v", err)
	}
	decrypted, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, priv, ciphertext, nil)
	if err != nil {
		t.Fatalf("rsa.DecryptOAEP with returned privKey: %v", err)
	}
	if string(decrypted) != string(plaintext) {
		t.Errorf("OAEP round-trip mismatch: got %q, want %q", decrypted, plaintext)
	}
}
