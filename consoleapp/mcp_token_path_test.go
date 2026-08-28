package consoleapp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
)

// TestServeMCPTokenPathEnvFallback covers the serve-side env fallback for the
// managed MCP token file (#1493), the same way TestServeAuthTLSEnvFallback
// covers the auth file: let runServe error on its earliest guard, then check
// the env value landed in the global (the fallback block runs first).
func TestServeMCPTokenPathEnvFallback(t *testing.T) {
	conIndexDSN, conProfile, conBaselineDir, conBaselineS3 = "", "", "", ""
	conAuthFile, conMCPTokenFile = "", ""
	conServersFile = filepath.Join(t.TempDir(), "servers.yaml") // empty registry
	t.Cleanup(func() {
		conIndexDSN, conServersFile, conAuthFile, conMCPTokenFile = "", "", "", ""
	})
	t.Setenv("BINTRAIL_INDEX_DSN", "")
	t.Setenv("BINTRAIL_CONSOLE_SERVERS", "")
	t.Setenv("BINTRAIL_CONSOLE_MCP_TOKEN", "/env/mcp-token.yaml")

	if err := runServe(serveCmd, nil); err == nil {
		t.Fatal("expected the empty-DSN guard to fire")
	}
	assertStr(t, "conMCPTokenFile (env)", conMCPTokenFile, "/env/mcp-token.yaml")
}

// TestResolveUpConsoleEnvMCPTokenPath is the watch-side tripwire for the same
// var: serve and watch read it in duplicated direct-read blocks, which is the
// established silent-breakage trap for env-only installs such as the compose
// stack.
func TestResolveUpConsoleEnvMCPTokenPath(t *testing.T) {
	saved := upConsoleMCPTokenFile
	t.Cleanup(func() { upConsoleMCPTokenFile = saved })

	if watchCmd.Flags().Lookup("console-mcp-token-file") == nil {
		t.Fatal("flag --console-mcp-token-file not registered on watchCmd; resolveUpConsoleEnv's Changed() call would always be false")
	}
	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().StringVar(&upConsoleMCPTokenFile, "console-mcp-token-file", "", "")
		return cmd
	}

	t.Setenv("BINTRAIL_CONSOLE_MCP_TOKEN", "/env/mcp-token.yaml")

	upConsoleMCPTokenFile = ""
	resolveUpConsoleEnv(newCmd())
	assertStr(t, "upConsoleMCPTokenFile (env)", upConsoleMCPTokenFile, "/env/mcp-token.yaml")

	cmd := newCmd()
	if err := cmd.Flags().Set("console-mcp-token-file", "/flag/mcp-token.yaml"); err != nil {
		t.Fatalf("set --console-mcp-token-file: %v", err)
	}
	resolveUpConsoleEnv(cmd)
	assertStr(t, "upConsoleMCPTokenFile (flag)", upConsoleMCPTokenFile, "/flag/mcp-token.yaml")
}

// TestManagedTokenLandsAtTheConfiguredPath drives the whole watch-side chain
// the compose stack uses (env -> resolveUpConsoleEnv -> upConsoleConfig) and
// then writes a token where the server would write it.
//
// The assertion that matters is the NEGATIVE one. Checking only that the file
// appeared at the configured path passes for an unrelated reason as soon as
// the fixture creates that directory, so this also asserts the file did NOT
// appear at DefaultMCPTokenPath() under HOME, which is where it landed before
// #1493 and, in a container, is the writable layer that a recreation destroys.
func TestManagedTokenLandsAtTheConfiguredPath(t *testing.T) {
	saved := upConsoleMCPTokenFile
	t.Cleanup(func() { upConsoleMCPTokenFile = saved })

	home := t.TempDir()
	t.Setenv("HOME", home)
	state := t.TempDir() // stands in for the compose state volume
	configured := filepath.Join(state, "console-mcp-token.yaml")
	t.Setenv("BINTRAIL_CONSOLE_MCP_TOKEN", configured)

	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&upConsoleMCPTokenFile, "console-mcp-token-file", "", "")
	upConsoleMCPTokenFile = ""
	if err := resolveUpConsoleEnv(cmd); err != nil {
		t.Fatalf("resolveUpConsoleEnv: %v", err)
	}
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", upConsoleOpts())
	if err != nil {
		t.Fatalf("upConsoleConfig: %v", err)
	}

	// Mirror the resolution in console.New: an empty MCPTokenPath falls back
	// to the home-anchored default. Reproducing it here is what makes the
	// negative assertion below fail on unwired config instead of erroring on
	// an empty path.
	path := cfg.MCPTokenPath
	if path == "" {
		path = console.DefaultMCPTokenPath()
	}
	if _, _, err := console.GenerateMCPToken(path, nil); err != nil {
		t.Fatalf("GenerateMCPToken(%s): %v", path, err)
	}

	if _, err := os.Stat(configured); err != nil {
		t.Errorf("no token file at the configured path %s: %v", configured, err)
	}
	underHome := console.DefaultMCPTokenPath()
	if _, err := os.Stat(underHome); err == nil {
		t.Errorf("the token was written to %s instead of the configured %s; in the compose stack that path is the container's writable layer and does not survive a restart", underHome, configured)
	}
}
