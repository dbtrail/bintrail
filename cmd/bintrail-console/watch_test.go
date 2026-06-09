package main

import (
	"testing"

	"github.com/spf13/cobra"
)

// This file mutates up* package globals via save-and-restore. DO NOT add
// t.Parallel() to any test here — concurrent runs would cross-write each
// other's state (runWatch reads the globals at start).

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
	// watch has no --profile/--no-archive, so NoArchive must stay false —
	// setting it would silently disable the reconstruct gate this wiring
	// enables.
	if cfg.NoArchive {
		t.Errorf("watch console config must not set NoArchive: %+v", cfg)
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
	// A DSN with no database name must error (parity with runServe) rather
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

	// The four flag names below must exist on the REAL watchCmd, or this test
	// would happily pass against a synthetic clone while production drifted
	// (a rename in init() not mirrored in resolveUpConsoleEnv would make
	// Changed() return false for the real flag → env silently overriding an
	// explicit flag). Pin the names to watchCmd before exercising the resolver.
	for _, name := range []string{"console-listen", "console-token", "baseline-dir", "baseline-s3"} {
		if watchCmd.Flags().Lookup(name) == nil {
			t.Fatalf("flag --%s not registered on watchCmd; resolveUpConsoleEnv's Changed(%q) would always be false", name, name)
		}
	}

	// newCmd registers the same flag names as watchCmd, bound to the same
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

// TestWatchStreamConfig asserts the up* → streamrun.Config fan-out (the watch
// equivalent of core up's TestPopulateStreamFlags): a flag added to watch but
// not wired into watchStreamConfig fails here rather than silently dropping
// the value, and the pinned non-configurable defaults are locked in so a
// future "let me delete this unused field" refactor can't change behavior.
func TestWatchStreamConfig(t *testing.T) {
	orig := struct {
		src, idx, sch, tbl, fmtv string
		batch, chk               int
	}{upSourceDSN, upIndexDSN, upSchemas, upTables, upFormat, upBatchSize, upCheckpoint}
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN, upSchemas, upTables, upFormat = orig.src, orig.idx, orig.sch, orig.tbl, orig.fmtv
		upBatchSize, upCheckpoint = orig.batch, orig.chk
	})

	upSourceDSN = "user:pass@tcp(source.example.com:3306)/src"
	upIndexDSN = "ix:pw@tcp(127.0.0.1:3306)/binlog_index"
	upSchemas = "mydb,otherdb"
	upTables = "mydb.orders"
	upBatchSize = 2500
	upCheckpoint = 15
	upFormat = "json"

	const passedServerID uint32 = 4242424242
	cfg := watchStreamConfig(passedServerID)

	assertStr(t, "IndexDSN", cfg.IndexDSN, upIndexDSN)
	assertStr(t, "SourceDSN", cfg.SourceDSN, upSourceDSN)
	assertStr(t, "Schemas", cfg.Schemas, upSchemas)
	assertStr(t, "Tables", cfg.Tables, upTables)
	assertStr(t, "Format", cfg.Format, upFormat)
	if cfg.ServerID != passedServerID {
		t.Errorf("ServerID = %d, want %d", cfg.ServerID, passedServerID)
	}
	if cfg.BatchSize != upBatchSize {
		t.Errorf("BatchSize = %d, want %d", cfg.BatchSize, upBatchSize)
	}
	if cfg.Checkpoint != upCheckpoint {
		t.Errorf("Checkpoint = %d, want %d", cfg.Checkpoint, upCheckpoint)
	}

	// Pinned defaults, not user-configurable from watch.
	assertStr(t, "StartFile", cfg.StartFile, "")
	assertStr(t, "StartGTID", cfg.StartGTID, "")
	assertStr(t, "SSLMode", cfg.SSLMode, "preferred")
	// The daemon serves ONE /metrics endpoint for all streams; a per-stream
	// bind here would conflict with it.
	assertStr(t, "MetricsAddr", cfg.MetricsAddr, "")
	if cfg.StartPos != 4 {
		t.Errorf("StartPos = %d, want 4 (binlog magic-number header end)", cfg.StartPos)
	}
	if cfg.GapTimeout != 30 {
		t.Errorf("GapTimeout = %d, want 30", cfg.GapTimeout)
	}
	if cfg.Reset || cfg.NoGapFill {
		t.Errorf("Reset=%v NoGapFill=%v, want both false", cfg.Reset, cfg.NoGapFill)
	}
	if cfg.Deps.ValidateBinlogFormat == nil {
		t.Error("Deps must be wired (streamdeps.Default()), got zero-value Deps")
	}
}
