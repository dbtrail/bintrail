package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/bintrail/internal/streamrun"
)

// ─── parseSourceDSN ──────────────────────────────────────────────────────────

func TestParseSourceDSN_tcp(t *testing.T) {
	dsn := "root:secret@tcp(db.example.com:3306)/mydb"
	host, port, user, pass, err := parseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "db.example.com" {
		t.Errorf("host: expected db.example.com, got %q", host)
	}
	if port != 3306 {
		t.Errorf("port: expected 3306, got %d", port)
	}
	if user != "root" {
		t.Errorf("user: expected root, got %q", user)
	}
	if pass != "secret" {
		t.Errorf("password: expected secret, got %q", pass)
	}
}

func TestParseSourceDSN_noPassword(t *testing.T) {
	dsn := "repl@tcp(127.0.0.1:13306)/"
	host, port, user, pass, err := parseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "127.0.0.1" {
		t.Errorf("host: expected 127.0.0.1, got %q", host)
	}
	if port != 13306 {
		t.Errorf("port: expected 13306, got %d", port)
	}
	if user != "repl" {
		t.Errorf("user: expected repl, got %q", user)
	}
	if pass != "" {
		t.Errorf("password: expected empty, got %q", pass)
	}
}

func TestParseSourceDSN_unixSocket(t *testing.T) {
	dsn := "root@unix(/var/run/mysqld/mysqld.sock)/test"
	_, _, _, _, err := parseSourceDSN(dsn)
	if err == nil {
		t.Error("expected error for unix socket DSN, got nil")
	}
}

func TestParseSourceDSN_invalid(t *testing.T) {
	_, _, _, _, err := parseSourceDSN("not-a-valid-dsn::::")
	if err == nil {
		t.Error("expected error for invalid DSN, got nil")
	}
}

// TestParseSourceDSN_ipv6 verifies IPv6 addresses are parsed correctly.
func TestParseSourceDSN_ipv6(t *testing.T) {
	dsn := "root:pw@tcp([::1]:3306)/db"
	host, port, _, _, err := parseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("unexpected error for IPv6 DSN: %v", err)
	}
	if host != "::1" {
		t.Errorf("host: expected ::1, got %q", host)
	}
	if port != 3306 {
		t.Errorf("port: expected 3306, got %d", port)
	}
}

// TestParseSourceDSN_portOutOfRange verifies that a port above the uint16 max
// (65535) is rejected. go-mysql-driver accepts it syntactically, but
// parseSourceDSN uses strconv.ParseUint with bitSize=16 to catch it.
func TestParseSourceDSN_portOutOfRange(t *testing.T) {
	dsn := "root@tcp(localhost:65536)/"
	_, _, _, _, err := parseSourceDSN(dsn)
	if err == nil {
		t.Error("expected error for port 65536 (exceeds uint16 max), got nil")
	}
	if !strings.Contains(err.Error(), "port") {
		t.Errorf("expected 'port' in error message, got: %v", err)
	}
}

// ─── cobra command wiring ─────────────────────────────────────────────────────

// TestStreamCmd_registered verifies that streamCmd is wired into the root command.
func TestStreamCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "stream" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'stream' command to be registered under rootCmd")
	}
}

// TestStreamCmd_requiredFlags verifies that the three required flags are marked
// as required so cobra enforces them before RunE is called.
func TestStreamCmd_requiredFlags(t *testing.T) {
	for _, flagName := range []string{"index-dsn", "source-dsn", "server-id"} {
		ann := streamCmd.Annotations
		_ = ann
		flag := streamCmd.Flag(flagName)
		if flag == nil {
			t.Errorf("flag --%s not registered", flagName)
			continue
		}
		// cobra marks required flags in the Annotations map.
		if streamCmd.Flag(flagName).Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", flagName)
		}
	}
}

// TestStreamCmd_allFlagsRegistered verifies that all expected flags are wired up.
func TestStreamCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "source-dsn", "server-id",
		"start-file", "start-pos", "start-gtid",
		"batch-size", "schemas", "tables", "checkpoint", "metrics-addr",
		"ssl-mode", "ssl-ca", "ssl-cert", "ssl-key",
		"reset", "no-gap-fill",
	} {
		if streamCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on streamCmd", name)
		}
	}
}

// TestStreamCmd_resetDefaultFalse verifies the --reset flag defaults to false.
func TestStreamCmd_resetDefaultFalse(t *testing.T) {
	f := streamCmd.Flag("reset")
	if f == nil {
		t.Fatal("flag --reset not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default reset=false, got %q", f.DefValue)
	}
}

// TestStreamCmd_sslModeDefault verifies the default ssl-mode is "preferred".
func TestStreamCmd_sslModeDefault(t *testing.T) {
	f := streamCmd.Flag("ssl-mode")
	if f == nil {
		t.Fatal("flag --ssl-mode not registered")
	}
	if f.DefValue != "preferred" {
		t.Errorf("expected default ssl-mode=preferred, got %q", f.DefValue)
	}
}

// TestStreamCmd_sslFlagsEmptyDefaults verifies ssl-ca/cert/key default to "".
func TestStreamCmd_sslFlagsEmptyDefaults(t *testing.T) {
	for _, name := range []string{"ssl-ca", "ssl-cert", "ssl-key"} {
		f := streamCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

// TestStreamCmd_defaults verifies that optional flags have the expected defaults.
func TestStreamCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"batch-size", "1000"},
		{"checkpoint", "10"},
		{"start-pos", "4"},
	}
	for _, tc := range cases {
		f := streamCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

// TestStreamCmd_emptyStringDefaults verifies that optional string flags default to "".
func TestStreamCmd_emptyStringDefaults(t *testing.T) {
	for _, name := range []string{"start-file", "start-gtid", "schemas", "tables", "metrics-addr"} {
		f := streamCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

// TestStreamCmd_noGapFillFlagRegistered verifies the --no-gap-fill flag exists.
func TestStreamCmd_noGapFillFlagRegistered(t *testing.T) {
	f := streamCmd.Flag("no-gap-fill")
	if f == nil {
		t.Fatal("flag --no-gap-fill not registered on streamCmd")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default no-gap-fill=false, got %q", f.DefValue)
	}
}

// TestStreamConfigFromFlags asserts the strm* → streamrun.Config snapshot — the
// single seam where the flag globals become a by-value config that streamrun.One
// (and, later, the control-plane supervisor) consumes. A flag added to stream
// but not wired through here would silently read its zero value inside
// streamrun.One, so every field is checked with a distinctive value. Mirrors
// TestPopulateStreamFlags' save-and-restore discipline: no t.Parallel().
func TestStreamConfigFromFlags(t *testing.T) {
	orig := struct {
		src, idx, file, gtid, sch, tbl, met, fmtv, ssl, ca, cert, key string
		sid, pos                                                      uint32
		batch, chk, gap                                               int
		reset, noGap                                                  bool
	}{
		src: strmSourceDSN, idx: strmIndexDSN, file: strmStartFile,
		gtid: strmStartGTID, sch: strmSchemas, tbl: strmTables,
		met: strmMetricsAddr, fmtv: strmFormat, ssl: strmSSLMode,
		ca: strmSSLCA, cert: strmSSLCert, key: strmSSLKey,
		sid: strmServerID, pos: strmStartPos,
		batch: strmBatchSize, chk: strmCheckpoint, gap: strmGapTimeout,
		reset: strmReset, noGap: strmNoGapFill,
	}
	t.Cleanup(func() {
		strmSourceDSN, strmIndexDSN, strmStartFile = orig.src, orig.idx, orig.file
		strmStartGTID, strmSchemas, strmTables = orig.gtid, orig.sch, orig.tbl
		strmMetricsAddr, strmFormat, strmSSLMode = orig.met, orig.fmtv, orig.ssl
		strmSSLCA, strmSSLCert, strmSSLKey = orig.ca, orig.cert, orig.key
		strmServerID, strmStartPos = orig.sid, orig.pos
		strmBatchSize, strmCheckpoint, strmGapTimeout = orig.batch, orig.chk, orig.gap
		strmReset, strmNoGapFill = orig.reset, orig.noGap
	})

	strmIndexDSN = "ix:pw@tcp(127.0.0.1:3306)/binlog_index"
	strmSourceDSN = "user:pass@tcp(source.example.com:3306)/src"
	strmServerID = 424242
	strmStartFile = "mysql-bin.000777"
	strmStartPos = 1234
	strmStartGTID = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	strmBatchSize = 333
	strmSchemas = "mydb,otherdb"
	strmTables = "mydb.orders"
	strmCheckpoint = 17
	strmMetricsAddr = ":9191"
	strmSSLMode = "verify-ca"
	strmSSLCA = "/tmp/ca.pem"
	strmSSLCert = "/tmp/cert.pem"
	strmSSLKey = "/tmp/key.pem"
	strmFormat = "json"
	strmReset = true
	strmNoGapFill = true
	strmGapTimeout = 99

	got := streamConfigFromFlags()
	// The Deps seam must be wired (else streamrun.One nil-panics at runtime, and
	// only the Docker-gated monitor integration test would catch a dropped
	// streamDeps() call). Spot-check two fields here so a unit run flags it.
	if got.Deps.BuildIndexFilters == nil || got.Deps.OutputJSON == nil {
		t.Error("streamConfigFromFlags did not wire Deps (streamDeps())")
	}
	// Deps holds func values (not comparable / not == ); this test checks the
	// strm* → field snapshot, so zero it before the struct comparison.
	got.Deps = streamrun.Deps{}
	want := streamrun.Config{
		IndexDSN:    "ix:pw@tcp(127.0.0.1:3306)/binlog_index",
		SourceDSN:   "user:pass@tcp(source.example.com:3306)/src",
		ServerID:    424242,
		StartFile:   "mysql-bin.000777",
		StartPos:    1234,
		StartGTID:   "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		BatchSize:   333,
		Schemas:     "mydb,otherdb",
		Tables:      "mydb.orders",
		Checkpoint:  17,
		MetricsAddr: ":9191",
		SSLMode:     "verify-ca",
		SSLCA:       "/tmp/ca.pem",
		SSLCert:     "/tmp/cert.pem",
		SSLKey:      "/tmp/key.pem",
		Format:      "json",
		Reset:       true,
		NoGapFill:   true,
		GapTimeout:  99,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("streamConfigFromFlags mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}

// TestStreamOneRejectsBadConfig: streamrun.One owns its own validation (it must
// not depend on cobra/flag-layer checks once a supervisor builds configs
// programmatically).
func TestStreamOneRejectsBadConfig(t *testing.T) {
	if err := streamrun.One(t.Context(), streamrun.Config{Format: "bogus", GapTimeout: 30}); err == nil ||
		!strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("bad format: err = %v, want invalid --format", err)
	}
	if err := streamrun.One(t.Context(), streamrun.Config{Format: "text", GapTimeout: 0}); err == nil ||
		!strings.Contains(err.Error(), "gap-timeout") {
		t.Errorf("bad gap-timeout: err = %v, want gap-timeout error", err)
	}
}
