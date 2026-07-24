package config

import (
	"crypto/tls"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// applyTLS must attach the given config only when the DSN itself specified no
// TLS — the operator's own tls= choice always wins (#946).
func TestApplyTLS_Precedence(t *testing.T) {
	tc := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test fixture

	t.Run("empty config gets the tls config", func(t *testing.T) {
		cfg := &mysql.Config{}
		applyTLS(cfg, tc)
		if cfg.TLS != tc {
			t.Fatalf("expected TLS attached, got %v", cfg.TLS)
		}
	})

	t.Run("nil tls config is a no-op", func(t *testing.T) {
		cfg := &mysql.Config{}
		applyTLS(cfg, nil)
		if cfg.TLS != nil {
			t.Fatalf("nil tlsCfg must not mutate config: TLS=%v", cfg.TLS)
		}
	})

	t.Run("explicit tls= name in DSN wins", func(t *testing.T) {
		// A DSN like ...?tls=skip-verify parses into TLSConfig != "".
		cfg := &mysql.Config{TLSConfig: "skip-verify"}
		applyTLS(cfg, tc)
		if cfg.TLS == tc {
			t.Fatal("operator's tls= must not be overridden by tlsCfg")
		}
		if cfg.TLSConfig != "skip-verify" {
			t.Fatalf("operator's TLSConfig mutated: %q", cfg.TLSConfig)
		}
	})

	t.Run("pre-set programmatic TLS wins", func(t *testing.T) {
		existing := &tls.Config{} //nolint:gosec // test fixture
		cfg := &mysql.Config{TLS: existing}
		applyTLS(cfg, tc)
		if cfg.TLS != existing {
			t.Fatal("pre-set cfg.TLS must not be overridden")
		}
	})
}

// DSNHost is best-effort and must never reject a DSN — a socket or unparseable
// DSN yields "" rather than failing the connection (it is only a TLS
// ServerName). This is why it replaced the stricter ParseSourceDSN on the index
// connect path (#946).
func TestDSNHost(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{"u:p@tcp(db.example.com:3306)/idx", "db.example.com"},
		{"u:p@tcp(127.0.0.1:3306)/idx", "127.0.0.1"},
		{"u:p@tcp(dbhost)/idx", "dbhost"}, // no explicit port
		{"u:p@unix(/tmp/mysql.sock)/idx", ""},
		{"@#$not a dsn", ""}, // unparseable → ""
	}
	for _, tt := range tests {
		if got := DSNHost(tt.dsn); got != tt.want {
			t.Errorf("DSNHost(%q) = %q, want %q", tt.dsn, got, tt.want)
		}
	}
}

// DSNHasExplicitTLS detects an operator's own tls= in the DSN so the stream can
// warn when it silently overrides a stronger --ssl-mode (#946).
func TestDSNHasExplicitTLS(t *testing.T) {
	tests := []struct {
		dsn  string
		want bool
	}{
		{"u:p@tcp(h:3306)/idx", false},
		{"u:p@tcp(h:3306)/idx?tls=true", true},
		{"u:p@tcp(h:3306)/idx?tls=skip-verify", true},
		{"u:p@tcp(h:3306)/idx?tls=false", true}, // explicit opt-out is still explicit
		{"u:p@tcp(h:3306)/idx?parseTime=true", false},
	}
	for _, tt := range tests {
		if got := DSNHasExplicitTLS(tt.dsn); got != tt.want {
			t.Errorf("DSNHasExplicitTLS(%q) = %v, want %v", tt.dsn, got, tt.want)
		}
	}
}

// Our TLS path must never enable the driver's own silent plaintext fallback —
// flipping AllowFallbackToPlaintext to true is exactly the leak this pins
// against, and no live test could catch it (the test MySQL has TLS on) (#946).
func TestNormalizeDSN_NoSilentFallback(t *testing.T) {
	cfg, err := normalizeDSN("u:p@tcp(h:3306)/db")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AllowFallbackToPlaintext {
		t.Fatal("normalizeDSN must not enable AllowFallbackToPlaintext")
	}
	applyTLS(cfg, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test fixture
	if cfg.AllowFallbackToPlaintext {
		t.Fatal("applyTLS must not enable AllowFallbackToPlaintext")
	}
}
