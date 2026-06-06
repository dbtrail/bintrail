package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// This file mutates up*/strm* package globals via save-and-restore. DO NOT
// add t.Parallel() to any test here or to sibling tests that read strm*
// globals — concurrent runs would cross-write each other's state. The whole
// package effectively serializes on these globals already (runStream/runUp
// read them at start), so parallelism is not a real benefit anyway.

// TestPopulateStreamFlags asserts the up* → strm* fan-out so a flag added to
// up but not wired into runStream fails CI rather than silently dropping the
// value.
func TestPopulateStreamFlags(t *testing.T) {
	// ── Save originals ─────────────────────────────────────────────────────
	orig := struct {
		// up* side
		uSrc, uIdx, uSch, uTbl, uMet, uFmt string
		uSID                               uint32
		uBatch, uChk                       int
		// strm* side
		sSrc, sIdx, sFile, sGTID, sSch, sTbl, sMet, sFmt, sSSL, sCA, sCert, sKey string
		sSID                                                                     uint32
		sPos                                                                     uint32
		sBatch, sChk, sGap                                                       int
		sReset, sNoGap                                                           bool
	}{
		uSrc: upSourceDSN, uIdx: upIndexDSN, uSID: upServerID,
		uSch: upSchemas, uTbl: upTables, uBatch: upBatchSize,
		uChk: upCheckpoint, uMet: upMetricsAddr, uFmt: upFormat,
		sSrc: strmSourceDSN, sIdx: strmIndexDSN, sFile: strmStartFile,
		sGTID: strmStartGTID, sSch: strmSchemas, sTbl: strmTables,
		sMet: strmMetricsAddr, sFmt: strmFormat, sSSL: strmSSLMode,
		sCA: strmSSLCA, sCert: strmSSLCert, sKey: strmSSLKey,
		sSID: strmServerID, sPos: strmStartPos,
		sBatch: strmBatchSize, sChk: strmCheckpoint, sGap: strmGapTimeout,
		sReset: strmReset, sNoGap: strmNoGapFill,
	}
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN, upServerID = orig.uSrc, orig.uIdx, orig.uSID
		upSchemas, upTables, upBatchSize = orig.uSch, orig.uTbl, orig.uBatch
		upCheckpoint, upMetricsAddr, upFormat = orig.uChk, orig.uMet, orig.uFmt
		strmSourceDSN, strmIndexDSN, strmStartFile = orig.sSrc, orig.sIdx, orig.sFile
		strmStartGTID, strmSchemas, strmTables = orig.sGTID, orig.sSch, orig.sTbl
		strmMetricsAddr, strmFormat, strmSSLMode = orig.sMet, orig.sFmt, orig.sSSL
		strmSSLCA, strmSSLCert, strmSSLKey = orig.sCA, orig.sCert, orig.sKey
		strmServerID, strmStartPos = orig.sSID, orig.sPos
		strmBatchSize, strmCheckpoint, strmGapTimeout = orig.sBatch, orig.sChk, orig.sGap
		strmReset, strmNoGapFill = orig.sReset, orig.sNoGap
	})

	// ── Set up* to known values ─────────────────────────────────────────────
	upSourceDSN = "user:pass@tcp(source.example.com:3306)/src"
	upIndexDSN = "ix:pw@tcp(127.0.0.1:3306)/binlog_index"
	upSchemas = "mydb,otherdb"
	upTables = "mydb.orders"
	upBatchSize = 2500
	upCheckpoint = 15
	upMetricsAddr = ":9091"
	upFormat = "json"

	const passedServerID uint32 = 4242424242

	// ── Act ─────────────────────────────────────────────────────────────────
	populateStreamFlags(passedServerID)

	// ── Assert mapped fields ────────────────────────────────────────────────
	assertStr(t, "strmIndexDSN", strmIndexDSN, upIndexDSN)
	assertStr(t, "strmSourceDSN", strmSourceDSN, upSourceDSN)
	assertStr(t, "strmSchemas", strmSchemas, upSchemas)
	assertStr(t, "strmTables", strmTables, upTables)
	assertStr(t, "strmMetricsAddr", strmMetricsAddr, upMetricsAddr)
	assertStr(t, "strmFormat", strmFormat, upFormat)
	if strmServerID != passedServerID {
		t.Errorf("strmServerID = %d, want %d", strmServerID, passedServerID)
	}
	if strmBatchSize != upBatchSize {
		t.Errorf("strmBatchSize = %d, want %d", strmBatchSize, upBatchSize)
	}
	if strmCheckpoint != upCheckpoint {
		t.Errorf("strmCheckpoint = %d, want %d", strmCheckpoint, upCheckpoint)
	}

	// ── Assert defaults set by populateStreamFlags ──────────────────────────
	// These are not user-configurable from `up` — populateStreamFlags pins
	// them. Asserting on them locks in the intentional defaults so a future
	// "let me delete this unused field" refactor can't silently change behavior.
	assertStr(t, "strmStartFile", strmStartFile, "")
	assertStr(t, "strmStartGTID", strmStartGTID, "")
	assertStr(t, "strmSSLMode", strmSSLMode, "preferred")
	assertStr(t, "strmSSLCA", strmSSLCA, "")
	assertStr(t, "strmSSLCert", strmSSLCert, "")
	assertStr(t, "strmSSLKey", strmSSLKey, "")
	if strmStartPos != 4 {
		t.Errorf("strmStartPos = %d, want 4 (binlog magic-number header end)", strmStartPos)
	}
	if strmGapTimeout != 30 {
		t.Errorf("strmGapTimeout = %d, want 30", strmGapTimeout)
	}
	if strmReset || strmNoGapFill {
		t.Errorf("strmReset=%v strmNoGapFill=%v, want both false", strmReset, strmNoGapFill)
	}
}

func assertStr(t *testing.T, name, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q, want %q", name, got, want)
	}
}

func TestUpConsoleConfig(t *testing.T) {
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", "127.0.0.1:8090", "tok", "/baselines", "s3://bucket/prefix/")
	if err != nil {
		t.Fatalf("upConsoleConfig: %v", err)
	}
	if cfg.DBName != "binlog_index" {
		t.Errorf("DBName = %q, want binlog_index", cfg.DBName)
	}
	if cfg.Listen != "127.0.0.1:8090" || cfg.Token != "tok" {
		t.Errorf("Listen=%q Token=%q, want 127.0.0.1:8090 / tok", cfg.Listen, cfg.Token)
	}
	// Baseline sources thread through verbatim (#379) — dir-over-s3 precedence
	// is owned by console.New, not here, so both must pass through raw.
	if cfg.BaselineDir != "/baselines" || cfg.BaselineS3 != "s3://bucket/prefix/" {
		t.Errorf("BaselineDir=%q BaselineS3=%q, want /baselines / s3://bucket/prefix/", cfg.BaselineDir, cfg.BaselineS3)
	}
	// up has no --profile/--no-archive, so NoArchive must stay false — setting
	// it would silently disable the reconstruct gate this wiring enables.
	if cfg.NoArchive {
		t.Errorf("up console config must not set NoArchive: %+v", cfg)
	}

	// Without baseline flags the Phase 1 default is preserved: empty baselines
	// keep the reconstruct surface gated off.
	cfg, err = upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", "127.0.0.1:8090", "tok", "", "")
	if err != nil {
		t.Fatalf("upConsoleConfig (no baseline): %v", err)
	}
	if cfg.BaselineDir != "" || cfg.BaselineS3 != "" {
		t.Errorf("empty baseline args should pass through empty: %+v", cfg)
	}

	// Invalid DSN (no '/') must error, not silently produce an empty dbName.
	if _, err := upConsoleConfig(nil, "invalid", "127.0.0.1:8090", "", "", ""); err == nil {
		t.Error("invalid --index-dsn should error")
	}
	// A DSN with no database name must error (parity with runConsole) rather
	// than starting a console that feeds an empty schema to the planner.
	if _, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/", "127.0.0.1:8090", "", "", ""); err == nil {
		t.Error("--index-dsn without a database name should error")
	}
}

// TestResolveUpConsoleEnv locks the flag > env > default precedence for the
// console-specific env vars. The trap this guards: the StringVar flag name and
// the Changed("<name>") string must match exactly — a mismatch compiles fine
// but makes Changed() return false for the real flag, so the env var would
// silently override an explicitly-passed flag.
func TestResolveUpConsoleEnv(t *testing.T) {
	saved := [4]string{upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3}
	t.Cleanup(func() {
		upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3 = saved[0], saved[1], saved[2], saved[3]
	})

	// The four flag names below must exist on the REAL upCmd, or this test
	// would happily pass against a synthetic clone while production drifted
	// (a rename in init() not mirrored in resolveUpConsoleEnv would make
	// Changed() return false for the real flag → env silently overriding an
	// explicit flag). Pin the names to upCmd before exercising the resolver.
	for _, name := range []string{"console-listen", "console-token", "baseline-dir", "baseline-s3"} {
		if upCmd.Flags().Lookup(name) == nil {
			t.Fatalf("flag --%s not registered on upCmd; resolveUpConsoleEnv's Changed(%q) would always be false", name, name)
		}
	}

	// newCmd registers the same flag names as upCmd, bound to the same
	// globals, so Changed() reflects what cobra would see in a real run.
	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().StringVar(&upConsoleListen, "console-listen", "127.0.0.1:8090", "")
		cmd.Flags().StringVar(&upConsoleToken, "console-token", "", "")
		cmd.Flags().StringVar(&upConsoleBaselineDir, "baseline-dir", "", "")
		cmd.Flags().StringVar(&upConsoleBaselineS3, "baseline-s3", "", "")
		return cmd
	}

	t.Setenv("BINTRAIL_CONSOLE_LISTEN", "0.0.0.0:9999")
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "env-tok")
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_DIR", "/env/baselines")
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_S3", "s3://env-bucket/p/")

	// No flags set → env wins over defaults.
	upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3 = "127.0.0.1:8090", "", "", ""
	resolveUpConsoleEnv(newCmd())
	assertStr(t, "upConsoleListen (env)", upConsoleListen, "0.0.0.0:9999")
	assertStr(t, "upConsoleToken (env)", upConsoleToken, "env-tok")
	assertStr(t, "upConsoleBaselineDir (env)", upConsoleBaselineDir, "/env/baselines")
	assertStr(t, "upConsoleBaselineS3 (env)", upConsoleBaselineS3, "s3://env-bucket/p/")

	// Explicit flags → flag beats env.
	cmd := newCmd()
	for flag, val := range map[string]string{
		"console-listen": "127.0.0.1:7070",
		"console-token":  "flag-tok",
		"baseline-dir":   "/flag/baselines",
		"baseline-s3":    "s3://flag-bucket/p/",
	} {
		if err := cmd.Flags().Set(flag, val); err != nil {
			t.Fatalf("set --%s: %v", flag, err)
		}
	}
	resolveUpConsoleEnv(cmd)
	assertStr(t, "upConsoleListen (flag)", upConsoleListen, "127.0.0.1:7070")
	assertStr(t, "upConsoleToken (flag)", upConsoleToken, "flag-tok")
	assertStr(t, "upConsoleBaselineDir (flag)", upConsoleBaselineDir, "/flag/baselines")
	assertStr(t, "upConsoleBaselineS3 (flag)", upConsoleBaselineS3, "s3://flag-bucket/p/")

	// Exported-but-EMPTY env vars must be a no-op (the `v != ""` guard):
	// a refactor to os.LookupEnv-with-ok would make an empty export clobber
	// a non-empty default like console-listen's 127.0.0.1:8090 with "".
	// NOTE: newCmd() must run BEFORE seeding the globals — StringVar resets
	// each bound variable to its default at registration time.
	cmd = newCmd()
	t.Setenv("BINTRAIL_CONSOLE_LISTEN", "")
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "")
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_DIR", "")
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_S3", "")
	upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3 = "127.0.0.1:8090", "tok", "/dir", "s3://b/p/"
	resolveUpConsoleEnv(cmd)
	assertStr(t, "upConsoleListen (empty env)", upConsoleListen, "127.0.0.1:8090")
	assertStr(t, "upConsoleToken (empty env)", upConsoleToken, "tok")
	assertStr(t, "upConsoleBaselineDir (empty env)", upConsoleBaselineDir, "/dir")
	assertStr(t, "upConsoleBaselineS3 (empty env)", upConsoleBaselineS3, "s3://b/p/")
}

// TestRunUpSourceDSNValidation: --source-dsn is required for the classic
// single-stream up, but --console may start source-less (the zero-config
// install — sources are then added from the UI).
func TestRunUpSourceDSNValidation(t *testing.T) {
	origSrc, origIdx, origConsole, origFmt := upSourceDSN, upIndexDSN, upConsole, upFormat
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN, upConsole, upFormat = origSrc, origIdx, origConsole, origFmt
	})
	upFormat = "text"

	// Without --console: refused up front with the actionable hint.
	upSourceDSN, upConsole = "", false
	upIndexDSN = "u:p@tcp(127.0.0.1:3306)/idx"
	err := runUp(upCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--source-dsn is required") {
		t.Fatalf("source-less up without --console: err = %v, want the --source-dsn requirement", err)
	}

	// With --console: validation passes; the run proceeds into init and fails
	// THERE on the deliberately unparseable index DSN — proving the
	// source-dsn gate did not fire.
	upSourceDSN, upConsole = "", true
	upIndexDSN = "not-a-dsn"
	err = runUp(upCmd, nil)
	if err == nil || strings.Contains(err.Error(), "--source-dsn is required") {
		t.Fatalf("source-less up WITH --console must pass the source gate, got: %v", err)
	}
}
