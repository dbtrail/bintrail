//go:build integration

package config

import (
	"crypto/tls"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// sessionSSLCipher returns the active TLS cipher for the connection's session.
// An empty value means the connection is unencrypted.
func sessionSSLCipher(t *testing.T, dsn string, tlsCfg *tls.Config) string {
	t.Helper()
	db, err := ConnectWithTLS(dsn, tlsCfg)
	if err != nil {
		t.Fatalf("ConnectWithTLS: %v", err)
	}
	defer db.Close()
	// Pin one connection so the SHOW STATUS reflects the connection we opened.
	db.SetMaxOpenConns(1)

	var name, cipher string
	if err := db.QueryRow("SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); err != nil {
		t.Fatalf("read Ssl_cipher: %v", err)
	}
	return cipher
}

// #946: ConnectWithTLS must actually encrypt the wire. Proven live against the
// test MySQL (which supports TLS): a TLS connection reports a non-empty
// Ssl_cipher; a plaintext connection reports an empty one.
func TestConnectWithTLS_Encrypts(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dsn := testutil.BaseDSN() + "/?parseTime=true"

	//nolint:gosec // required/preferred bintrail modes skip cert verification by design
	encrypted := sessionSSLCipher(t, dsn, &tls.Config{InsecureSkipVerify: true})
	if encrypted == "" {
		t.Fatal("ConnectWithTLS produced an UNENCRYPTED connection (Ssl_cipher empty)")
	}
	t.Logf("TLS connection cipher: %q", encrypted)

	if plain := sessionSSLCipher(t, dsn, nil); plain != "" {
		t.Fatalf("nil tlsCfg should be plaintext, but Ssl_cipher=%q", plain)
	}
}
