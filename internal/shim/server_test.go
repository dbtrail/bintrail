package shim

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
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
// returns a TLS config whose cert exposes an RSA private key (the
// thing go-mysql/server's full-auth path dereferences at
// auth_switch_response.go:98) and a PEM-encoded RSA public key (the
// pubKey argument NewServer requires). A regression that returned a
// nil tlsConfig, an EC key, or an empty PEM block would surface as a
// nil-deref / type assertion panic on the first SHA2 cache miss —
// catch it at unit test time instead.
func TestGenerateSelfSignedTLSProducesUsableArtifacts(t *testing.T) {
	pubKeyPEM, tlsConfig, err := generateSelfSignedTLS()
	if err != nil {
		t.Fatalf("generateSelfSignedTLS: %v", err)
	}

	if tlsConfig == nil || len(tlsConfig.Certificates) == 0 {
		t.Fatal("tlsConfig must carry at least one Certificate")
	}
	priv, ok := tlsConfig.Certificates[0].PrivateKey.(*rsa.PrivateKey)
	if !ok || priv == nil {
		t.Fatalf("tlsConfig.Certificates[0].PrivateKey must be *rsa.PrivateKey for go-mysql full-auth path; got %T",
			tlsConfig.Certificates[0].PrivateKey)
	}

	block, _ := pem.Decode(pubKeyPEM)
	if block == nil || block.Type != "PUBLIC KEY" {
		t.Fatalf("pubKeyPEM must be a PEM block of type PUBLIC KEY; got %v", block)
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("ParsePKIXPublicKey: %v", err)
	}
	parsedRSA, ok := parsed.(*rsa.PublicKey)
	if !ok {
		t.Fatalf("parsed pubKey must be *rsa.PublicKey, got %T", parsed)
	}
	if parsedRSA.N.Cmp(priv.PublicKey.N) != 0 {
		t.Error("pubKeyPEM does not match the private key in tlsConfig (key pair mismatch — full auth would fail to round-trip)")
	}
}

