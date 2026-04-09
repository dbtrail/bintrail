package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/agent"
	"github.com/dbtrail/bintrail/internal/observe"
)

var (
	logLevel  string
	logFormat string
)

// Build-time variables injected via -ldflags.
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "bintrail",
	Short: "MySQL binlog indexer and recovery tool",
	Long: `Bintrail parses MySQL ROW-format binary logs, indexes every row event into a
MySQL table with full before/after images, and provides query and recovery
capabilities. The index is self-contained — recovery does not depend on
binlog files still existing on disk.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		observe.Setup(os.Stderr, logFormat, logLevel)
		return nil
	},
	SilenceErrors: true, // we handle error output ourselves in main()
	SilenceUsage:  true, // don't print usage/help on errors — users can use --help
}

func init() {
	rootCmd.Version = fmt.Sprintf("%s (commit %s, built %s)", Version, CommitSHA, BuildDate)
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log format: text or json")
}

func main() {
	err := rootCmd.Execute()
	if err == nil {
		return
	}
	// Map permanent agent WebSocket rejections to distinct exit codes so
	// systemd (RestartPreventExitStatus=64 65) can stop respawning on
	// permanent failures. Transient errors fall through to the default
	// exit-1 path and are safe to respawn. See issue #201.
	//
	// This runs AFTER Cobra's RunE returns, which means every defer in
	// runAgent (buffer flush, S3 writers, source DB close, metrics
	// shutdown) has already executed — unlike calling os.Exit from
	// within runAgent directly.
	var fce *agent.FatalCloseError
	if errors.As(err, &fce) {
		code, msg := exitCodeFor(fce.Reason)
		slog.Error("agent exit",
			"reason", fce.Reason.String(),
			"exit_code", code,
			"message", msg,
			"error", fce.Err)
		os.Exit(code)
	}
	if wantsJSON(rootCmd) {
		json.NewEncoder(os.Stderr).Encode(map[string]string{"error": err.Error()})
	} else {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(1)
}

// exitCodeFor maps a fatal agent close reason to a process exit code and
// an operator-facing message. Exit codes match the contract documented in
// README.md (Agent exit codes): 64 for auth/config failures, 65 for rate
// limiting. systemd units should list these in RestartPreventExitStatus.
func exitCodeFor(reason agent.FatalReason) (int, string) {
	switch reason {
	case agent.FatalRateLimited:
		return 65, "agent rate-limited by server — contact support"
	case agent.FatalMissingCredentials:
		return 64, "missing credentials — set --api-key or BINTRAIL_API_KEY"
	case agent.FatalWrongTenantMode:
		return 64, "tenant is not in BYOS mode — WebSocket channel unavailable"
	case agent.FatalInvalidKey:
		return 64, "invalid or revoked API key"
	default:
		return 64, "agent rejected by server — fix credentials/config and restart manually"
	}
}

// outputJSON encodes v as indented JSON to stdout.
func outputJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// wantsJSON reports whether the active command has a --format flag set to "json".
func wantsJSON(root *cobra.Command) bool {
	// CalledAs returns the command that was actually invoked.
	// Walk the command tree to find the leaf command.
	cmd, _, _ := root.Find(os.Args[1:])
	if cmd == nil {
		return false
	}
	f := cmd.Flags().Lookup("format")
	if f == nil {
		return false
	}
	return f.Value.String() == "json"
}
