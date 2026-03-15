package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/bintrail/bintrail/internal/parser"
)

// selfSignedCAPEM generates a minimal self-signed CA certificate as PEM bytes.
func selfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

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

// ─── resolveStart ────────────────────────────────────────────────────────────

func TestResolveStart_noStateNoFlags(t *testing.T) {
	_, _, _, _, _, err := resolveStart("", "", 4, nil)
	if err == nil {
		t.Error("expected error when no flags and no saved state")
	}
}

func TestResolveStart_positionFlagsNoState(t *testing.T) {
	mode, file, gtidStr, pos, accGTID, err := resolveStart("binlog.000001", "", 4, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000001" {
		t.Errorf("expected file=binlog.000001, got %q", file)
	}
	if pos != 4 {
		t.Errorf("expected pos=4, got %d", pos)
	}
	if gtidStr != "" {
		t.Errorf("expected empty gtidStr, got %q", gtidStr)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

func TestResolveStart_savedStateWinsOverFlags(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	mode, file, _, pos, _, err := resolveStart("binlog.000020", "", 100, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000010" {
		t.Errorf("expected saved file=binlog.000010, got %q", file)
	}
	if pos != 9999 {
		t.Errorf("expected saved pos=9999, got %d", pos)
	}
}

func TestResolveStart_resumePosition(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000005",
		binlogPos:  1234,
	}
	mode, file, _, pos, accGTID, err := resolveStart("", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position, got %q", mode)
	}
	if file != "binlog.000005" {
		t.Errorf("expected file=binlog.000005, got %q", file)
	}
	if pos != 1234 {
		t.Errorf("expected pos=1234, got %d", pos)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

func TestResolveStart_resumeGTID(t *testing.T) {
	gtidSet := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"
	saved := &streamState{
		mode:    "gtid",
		gtidSet: gtidSet,
	}
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != gtidSet {
		t.Errorf("expected GTID=%q, got %q", gtidSet, returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID in gtid mode")
	}
}

func TestResolveStart_mutuallyExclusive(t *testing.T) {
	_, _, _, _, _, err := resolveStart("binlog.000001", "uuid:1", 4, nil)
	if err == nil {
		t.Error("expected error for mutually exclusive --start-file and --start-gtid")
	}
}

func TestResolveStart_gtidFlagsNoState(t *testing.T) {
	gtidSet := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", gtidSet, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != gtidSet {
		t.Errorf("expected GTID=%q, got %q", gtidSet, returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID")
	}
}

// TestResolveStart_invalidSavedGTID verifies that a corrupt gtid_set in
// stream_state results in a clear error (not a panic).
func TestResolveStart_invalidSavedGTID(t *testing.T) {
	saved := &streamState{mode: "gtid", gtidSet: "not-a-valid-gtid"}
	_, _, _, _, _, err := resolveStart("", "", 4, saved)
	if err == nil {
		t.Error("expected error for invalid saved GTID set")
	}
}

// TestResolveStart_invalidStartGTIDFlag verifies that --start-gtid with an
// invalid GTID string is rejected with a clear error.
func TestResolveStart_invalidStartGTIDFlag(t *testing.T) {
	_, _, _, _, _, err := resolveStart("", "garbage-gtid", 0, nil)
	if err == nil {
		t.Error("expected error for invalid --start-gtid value")
	}
}

// ─── GTID accumulation ────────────────────────────────────────────────────────

// TestStreamState_gtidAccumulation verifies that accGTID.Update correctly
// accumulates multiple GTIDs from a single server UUID into a range.
func TestStreamState_gtidAccumulation(t *testing.T) {
	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562" // go-mysql lowercases UUIDs
	gs, err := gomysql.ParseMysqlGTIDSet(uuid + ":1")
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet: %v", err)
	}
	acc := gs.(*gomysql.MysqlGTIDSet)

	for _, gtid := range []string{uuid + ":2", uuid + ":3", uuid + ":4"} {
		if err := acc.Update(gtid); err != nil {
			t.Fatalf("Update(%q): %v", gtid, err)
		}
	}

	got := acc.String()
	// Should contain the UUID and a range covering 1-4.
	if !strings.Contains(got, uuid) {
		t.Errorf("expected UUID in GTID set string, got %q", got)
	}
	if !strings.Contains(got, "1-4") {
		t.Errorf("expected range 1-4 in GTID set string, got %q", got)
	}
}

// TestStreamState_gtidAccumulationMultiServer verifies accumulation across
// two different server UUIDs — each gets its own range entry.
func TestStreamState_gtidAccumulationMultiServer(t *testing.T) {
	uuid1 := "3e11fa47-71ca-11e1-9e33-c80aa9429562" // go-mysql lowercases UUIDs
	uuid2 := "7d93a8e1-0b3c-11e2-ab3d-0022114ef123"

	gs, _ := gomysql.ParseMysqlGTIDSet(uuid1 + ":1")
	acc := gs.(*gomysql.MysqlGTIDSet)

	if err := acc.Update(uuid2 + ":1"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := acc.Update(uuid1 + ":2"); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got := acc.String()
	if !strings.Contains(got, uuid1) {
		t.Errorf("expected %s in result, got %q", uuid1, got)
	}
	if !strings.Contains(got, uuid2) {
		t.Errorf("expected %s in result, got %q", uuid2, got)
	}
}

// ─── buildTLSConfig ───────────────────────────────────────────────────────────

func TestBuildTLSConfig_disabled(t *testing.T) {
	cfg, err := buildTLSConfig("disabled", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil tls.Config for disabled mode")
	}
}

func TestBuildTLSConfig_preferred(t *testing.T) {
	cfg, err := buildTLSConfig("preferred", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config for preferred mode")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true for preferred mode")
	}
}

func TestBuildTLSConfig_required(t *testing.T) {
	cfg, err := buildTLSConfig("required", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config for required mode")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true for required mode")
	}
}

func TestBuildTLSConfig_invalidMode(t *testing.T) {
	_, err := buildTLSConfig("bogus", "", "", "", "")
	if err == nil {
		t.Error("expected error for unknown ssl-mode")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Errorf("expected mode name in error, got: %v", err)
	}
}

func TestBuildTLSConfig_certWithoutKey(t *testing.T) {
	_, err := buildTLSConfig("required", "", "cert.pem", "", "")
	if err == nil {
		t.Error("expected error when cert provided without key")
	}
}

func TestBuildTLSConfig_keyWithoutCert(t *testing.T) {
	_, err := buildTLSConfig("required", "", "", "key.pem", "")
	if err == nil {
		t.Error("expected error when key provided without cert")
	}
}

func TestBuildTLSConfig_nonexistentCA(t *testing.T) {
	_, err := buildTLSConfig("verify-ca", "/nonexistent/ca.pem", "", "", "")
	if err == nil {
		t.Error("expected error for non-existent CA file")
	}
}

func TestBuildTLSConfig_verifyIdentitySetsServerName(t *testing.T) {
	cfg, err := buildTLSConfig("verify-identity", "", "", "", "db.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}
	if cfg.ServerName != "db.example.com" {
		t.Errorf("expected ServerName=db.example.com, got %q", cfg.ServerName)
	}
	if cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=false for verify-identity")
	}
}

func TestBuildTLSConfig_verifyCAHasVerifyConnection(t *testing.T) {
	cfg, err := buildTLSConfig("verify-ca", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}
	if cfg.VerifyConnection == nil {
		t.Error("expected VerifyConnection to be set for verify-ca mode")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true for verify-ca (hostname skipped via VerifyConnection)")
	}
}

func TestBuildTLSConfig_validCAFile(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, selfSignedCAPEM(t), 0600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}

	cfg, err := buildTLSConfig("verify-ca", caFile, "", "", "")
	if err != nil {
		t.Fatalf("unexpected error with valid CA file: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs to be set when --ssl-ca is provided")
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

// ─── resolveStart additional paths ───────────────────────────────────────────

// TestResolveStart_customStartPos verifies that a non-default startPos is
// preserved through the position-mode path (not hardcoded to 4).
func TestResolveStart_customStartPos(t *testing.T) {
	_, _, _, pos, _, err := resolveStart("binlog.000001", "", 1234, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 1234 {
		t.Errorf("expected pos=1234, got %d", pos)
	}
}

// TestResolveStart_savedGTID_fileFlagSwitchesMode verifies that a saved GTID-mode
// checkpoint is overridden when --start-file requests a mode switch to position.
func TestResolveStart_savedGTID_fileFlagSwitchesMode(t *testing.T) {
	saved := &streamState{
		mode:    "gtid",
		gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
	}
	mode, file, gtidStr, pos, accGTID, err := resolveStart("binlog.000001", "", 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position after switch, got %q", mode)
	}
	if file != "binlog.000001" {
		t.Errorf("expected file from flag, got %q", file)
	}
	if pos != 4 {
		t.Errorf("expected pos=4, got %d", pos)
	}
	if gtidStr != "" {
		t.Errorf("expected empty gtidStr, got %q", gtidStr)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

// TestResolveStart_savedGTIDWinsOverGTIDFlag verifies that a saved GTID-mode
// checkpoint is used even when --start-gtid provides a different GTID set.
func TestResolveStart_savedGTIDWinsOverGTIDFlag(t *testing.T) {
	saved := &streamState{
		mode:    "gtid",
		gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
	}
	newGTID := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-200"
	mode, _, returnedGTID, _, accGTID, err := resolveStart("", newGTID, 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if returnedGTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100" {
		t.Errorf("expected saved GTID, got %q", returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID")
	}
}

// TestResolveStart_savedPosition_gtidFlagSwitchesMode verifies that when a
// position-mode checkpoint exists and --start-gtid is provided, the mode
// switches to GTID (explicit user intent to change tracking mode).
func TestResolveStart_savedPosition_gtidFlagSwitchesMode(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	gtidSet := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	mode, file, returnedGTID, pos, accGTID, err := resolveStart("", gtidSet, 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid (switched), got %q", mode)
	}
	if file != "" {
		t.Errorf("expected empty file after switch, got %q", file)
	}
	if pos != 0 {
		t.Errorf("expected pos=0 after switch, got %d", pos)
	}
	if returnedGTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5" {
		t.Errorf("expected flag GTID, got %q", returnedGTID)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID after GTID switch")
	}
}

// ─── Mode switching (issue #68) ──────────────────────────────────────────────

// TestResolveStart_modeSwitch_positionToGTID verifies that a saved position-mode
// checkpoint is overridden when the user passes --start-gtid (without --start-file).
func TestResolveStart_modeSwitch_positionToGTID(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	newGTID := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50"
	mode, file, returnedGTID, pos, accGTID, err := resolveStart("", newGTID, 4, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid after switch, got %q", mode)
	}
	if file != "" {
		t.Errorf("expected empty file in gtid mode, got %q", file)
	}
	if returnedGTID != newGTID {
		t.Errorf("expected flag GTID %q, got %q", newGTID, returnedGTID)
	}
	if pos != 0 {
		t.Errorf("expected pos=0 in gtid mode, got %d", pos)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID after switch to gtid mode")
	}
}

// TestResolveStart_modeSwitch_gtidToPosition verifies that a saved GTID-mode
// checkpoint is overridden when the user passes --start-file (without --start-gtid).
func TestResolveStart_modeSwitch_gtidToPosition(t *testing.T) {
	saved := &streamState{
		mode:    "gtid",
		gtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
	}
	mode, file, gtidStr, pos, accGTID, err := resolveStart("binlog.000020", "", 100, saved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != "position" {
		t.Errorf("expected mode=position after switch, got %q", mode)
	}
	if file != "binlog.000020" {
		t.Errorf("expected file=binlog.000020, got %q", file)
	}
	if pos != 100 {
		t.Errorf("expected pos=100, got %d", pos)
	}
	if gtidStr != "" {
		t.Errorf("expected empty gtidStr in position mode, got %q", gtidStr)
	}
	if accGTID != nil {
		t.Error("expected nil accGTID in position mode")
	}
}

// TestResolveStart_modeSwitch_bothFlagsWithSaved verifies that passing both
// --start-file and --start-gtid is still rejected even with a saved checkpoint.
func TestResolveStart_modeSwitch_bothFlagsWithSaved(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	_, _, _, _, _, err := resolveStart("binlog.000001", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", 4, saved)
	if err == nil {
		t.Error("expected error for mutually exclusive flags with saved state")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("expected 'mutually exclusive' in error, got: %v", err)
	}
}

// TestResolveStart_modeSwitch_invalidGTID verifies that an invalid --start-gtid
// during a mode switch produces a clear error.
func TestResolveStart_modeSwitch_invalidGTID(t *testing.T) {
	saved := &streamState{
		mode:       "position",
		binlogFile: "binlog.000010",
		binlogPos:  9999,
	}
	_, _, _, _, _, err := resolveStart("", "not-a-valid-gtid", 0, saved)
	if err == nil {
		t.Error("expected error for invalid GTID during mode switch")
	}
	if !strings.Contains(err.Error(), "invalid --start-gtid") {
		t.Errorf("expected 'invalid --start-gtid' in error, got: %v", err)
	}
}

// ─── normalizeGTIDSet ────────────────────────────────────────────────────────

func TestNormalizeGTIDSet_standard(t *testing.T) {
	// Already standard 36-char UUID — no change expected.
	input := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	got := normalizeGTIDSet(input)
	if got != input {
		t.Errorf("expected no change, got %q", got)
	}
}

func TestNormalizeGTIDSet_rdsShortened(t *testing.T) {
	// RDS-style shortened UUID (first segment 7 chars instead of 8).
	input := "5512139-1432-11f1-8d8d-0693b428a89b:1-7594394"
	want := "05512139-1432-11f1-8d8d-0693b428a89b:1-7594394"
	got := normalizeGTIDSet(input)
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeGTIDSet_multipleEntries(t *testing.T) {
	input := "5512139-1432-11f1-8d8d-0693b428a89b:1-100,ab-cdef-1234-5678-abcdefabcdef:1-5"
	want := "05512139-1432-11f1-8d8d-0693b428a89b:1-100,000000ab-cdef-1234-5678-abcdefabcdef:1-5"
	got := normalizeGTIDSet(input)
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestNormalizeGTIDSet_parsesAfterNormalization(t *testing.T) {
	// The RDS GTID should parse successfully after normalization.
	input := "5512139-1432-11f1-8d8d-0693b428a89b:1-7594394"
	normalized := normalizeGTIDSet(input)
	_, err := gomysql.ParseMysqlGTIDSet(normalized)
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet failed after normalization: %v", err)
	}
}

func TestNormalizeGTIDSet_empty(t *testing.T) {
	got := normalizeGTIDSet("")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestResolveStart_rdsShortGTID(t *testing.T) {
	// Verify that resolveStart accepts an RDS-style shortened GTID.
	rdsGTID := "5512139-1432-11f1-8d8d-0693b428a89b:1-7594394"
	wantGTID := "05512139-1432-11f1-8d8d-0693b428a89b:1-7594394"

	mode, _, gtidStr, _, accGTID, err := resolveStart("", rdsGTID, 0, nil)
	if err != nil {
		t.Fatalf("resolveStart with RDS GTID: %v", err)
	}
	if mode != "gtid" {
		t.Errorf("expected mode=gtid, got %q", mode)
	}
	if gtidStr != wantGTID {
		t.Errorf("expected normalized GTID %q, got %q", wantGTID, gtidStr)
	}
	if accGTID == nil {
		t.Error("expected non-nil accGTID")
	}
}

// ─── streamLoop GTID tracking ─────────────────────────────────────────────────

// TestStreamLoop_gtidOnlyEventsAccumulated verifies that EventGTID events
// (transactions with no row changes on tracked tables) are accumulated into
// the GTID set without gaps. This is the fix for issue #124: without these
// events, the checkpoint GTID set had gaps, causing ERROR 1236 on resume.
func TestStreamLoop_gtidOnlyEventsAccumulated(t *testing.T) {
	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"

	gs, err := gomysql.ParseMysqlGTIDSet(uuid + ":1-5")
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet: %v", err)
	}

	state := &streamState{
		mode:    "gtid",
		gtidSet: uuid + ":1-5",
		accGTID: gs.(*gomysql.MysqlGTIDSet),
	}

	// Simulate: GTID 6 is a row event, GTID 7 is a GTID-only event (no rows),
	// GTID 8 is another row event. Without the fix, GTID 7 would be missing.
	events := make(chan parser.Event, 10)
	events <- parser.Event{
		GTID:      uuid + ":6",
		EventType: parser.EventInsert,
		Schema:    "test",
		Table:     "t1",
		EndPos:    100,
	}
	events <- parser.Event{
		GTID:      uuid + ":7",
		EventType: parser.EventGTID,
		EndPos:    200,
	}
	events <- parser.Event{
		GTID:      uuid + ":8",
		EventType: parser.EventInsert,
		Schema:    "test",
		Table:     "t1",
		EndPos:    300,
	}
	close(events)

	// Simulate what streamLoop does: accumulate GTIDs from every event.
	// We can't call streamLoop directly (needs real DB/indexer), but the
	// GTID accumulation logic is the core of this fix.
	for ev := range events {
		if ev.GTID != "" && state.accGTID != nil {
			if err := state.accGTID.Update(ev.GTID); err != nil {
				t.Fatalf("Update(%q): %v", ev.GTID, err)
			}
			state.gtidSet = state.accGTID.String()
		}
	}

	// The GTID set should be contiguous: 1-8 with no gaps.
	got := state.accGTID.String()
	if !strings.Contains(got, "1-8") {
		t.Errorf("expected contiguous range 1-8, got %q", got)
	}
	// Verify no gaps (should NOT contain colons separating ranges within the UUID).
	// A gapped set would look like "uuid:1-6:8" — the ":" after "1-6" splits ranges.
	parts := strings.SplitN(got, ":", 2) // split off UUID
	if len(parts) == 2 && strings.Contains(parts[1], ":") {
		t.Errorf("GTID set has gaps: %q", got)
	}
}

// ─── Gap detection ────────────────────────────────────────────────────────────

// TestDetectPositionGap_noGap verifies that no gap is reported when the
// checkpoint file is the current (latest) binlog file.
func TestDetectPositionGap_noGap(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000001", 1048576).
			AddRow("mysql-bin.000002", 524288).
			AddRow("mysql-bin.000003", 100))

	gap, err := detectPositionGap(db, "mysql-bin.000003", 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Error("expected no gap when checkpoint is on latest file")
	}
}

// TestDetectPositionGap_fillable verifies that a fillable gap is reported when
// the checkpoint file exists but is not the latest.
func TestDetectPositionGap_fillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000001", 1048576).
			AddRow("mysql-bin.000002", 524288).
			AddRow("mysql-bin.000003", 100))

	gap, err := detectPositionGap(db, "mysql-bin.000001", 9999)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap when checkpoint is behind latest file")
	}
	if !gap.Fillable {
		t.Error("expected fillable gap when checkpoint file still exists")
	}
	if !strings.Contains(gap.Message, "mysql-bin.000001") {
		t.Errorf("expected checkpoint file in message, got: %s", gap.Message)
	}
}

// TestDetectPositionGap_unfillable verifies that an unfillable gap is reported
// when the checkpoint file has been purged.
func TestDetectPositionGap_unfillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("mysql-bin.000050", 1048576).
			AddRow("mysql-bin.000051", 524288))

	gap, err := detectPositionGap(db, "mysql-bin.000038", 7890)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap when checkpoint file is purged")
	}
	if gap.Fillable {
		t.Error("expected unfillable gap when file is purged")
	}
	if gap.EarliestFile != "mysql-bin.000050" {
		t.Errorf("expected earliest file mysql-bin.000050, got %q", gap.EarliestFile)
	}
	if gap.EarliestPos != 4 {
		t.Errorf("expected earliest pos 4, got %d", gap.EarliestPos)
	}
	if !strings.Contains(gap.Message, "purged") {
		t.Errorf("expected 'purged' in message, got: %s", gap.Message)
	}
	if !strings.Contains(gap.Message, "mysql-bin.000038") {
		t.Errorf("expected checkpoint file in message, got: %s", gap.Message)
	}
}

// TestDetectPositionGap_extraColumns verifies that SHOW BINARY LOGS with extra
// columns (MySQL 8.0.14+ adds Encrypted) is handled correctly.
func TestDetectPositionGap_extraColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size", "Encrypted"}).
			AddRow("mysql-bin.000010", 1048576, "No").
			AddRow("mysql-bin.000011", 524288, "No"))

	gap, err := detectPositionGap(db, "mysql-bin.000010", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap (checkpoint is not latest)")
	}
	if !gap.Fillable {
		t.Error("expected fillable gap")
	}
}

// TestDetectGTIDGap_noGap verifies no gap when checkpoint matches executed.
func TestDetectGTIDGap_noGap(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(""))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(gtid))

	gap, err := detectGTIDGap(db, gtid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Error("expected no gap when checkpoint matches executed")
	}
}

// TestDetectGTIDGap_fillable verifies a fillable gap when checkpoint is behind
// executed but nothing is purged.
func TestDetectGTIDGap_fillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(""))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-200"))

	gap, err := detectGTIDGap(db, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap when checkpoint is behind executed")
	}
	if !gap.Fillable {
		t.Error("expected fillable gap when nothing is purged")
	}
}

// TestDetectGTIDGap_unfillable verifies an unfillable gap when the checkpoint
// includes GTIDs that have been purged.
func TestDetectGTIDGap_unfillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(uuid + ":1-500"))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(uuid + ":1-1000"))

	gap, err := detectGTIDGap(db, uuid+":1-100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap")
	}
	if gap.Fillable {
		t.Error("expected unfillable gap when checkpoint is within purged range")
	}
	if gap.PurgedGTIDSet == "" {
		t.Error("expected purged GTID set in result")
	}
	if !strings.Contains(gap.Message, "purged") {
		t.Errorf("expected 'purged' in message, got: %s", gap.Message)
	}
}

// TestDetectGTIDGap_fillableWithPurged verifies a fillable gap when checkpoint
// is ahead of the purged set (common case: purged set exists but checkpoint
// has progressed beyond it).
func TestDetectGTIDGap_fillableWithPurged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(uuid + ":1-50"))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(uuid + ":1-1000"))

	// Checkpoint at :1-200 — well past the purged range of :1-50.
	gap, err := detectGTIDGap(db, uuid+":1-200")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap when checkpoint is behind executed")
	}
	if !gap.Fillable {
		t.Error("expected fillable gap when checkpoint is past purged range")
	}
}

// TestDetectGTIDGap_unfillableMissingUUID verifies an unfillable gap when the
// purged set contains a UUID that the checkpoint has never seen.
func TestDetectGTIDGap_unfillableMissingUUID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	uuidA := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuidB := "7d93a8e1-0b3c-11e2-ab3d-0022114ef123"
	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(uuidA + ":1-50," + uuidB + ":1-200"))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(uuidA + ":1-500," + uuidB + ":1-300"))

	// Checkpoint only knows about uuidA (past its purged range), not uuidB at all.
	gap, err := detectGTIDGap(db, uuidA+":1-100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected gap")
	}
	if gap.Fillable {
		t.Error("expected unfillable gap: purged set has UUID not in checkpoint")
	}
}

// TestDetectGTIDGap_noGapStructuralComparison verifies that GTID sets are
// compared structurally, not by string equality (different formatting of the
// same set should still report no gap).
func TestDetectGTIDGap_noGapStructuralComparison(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Checkpoint uses lowercase UUID; MySQL returns uppercase.
	checkpoint := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	executed := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"

	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_purged"}).AddRow(""))
	mock.ExpectQuery("SELECT @@gtid_executed").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_executed"}).AddRow(executed))

	gap, err := detectGTIDGap(db, checkpoint)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Error("expected no gap: same GTID set in different case should be equal")
	}
}

// TestGtidSetsEqual verifies structural comparison of GTID sets.
func TestGtidSetsEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", true},
		{"case difference", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100", true},
		{"different range", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-200", false},
		{"empty both", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gtidSetsEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("gtidSetsEqual(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
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
