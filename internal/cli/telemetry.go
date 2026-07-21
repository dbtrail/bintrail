package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

var telFormat string

// AddTelemetryCommand registers the telemetry control surface on root. Every
// binary that can report registers it, so `telemetry off` works from whichever
// one the operator happens to have on PATH.
func AddTelemetryCommand(root *cobra.Command) {
	root.AddCommand(telemetryCmd)
}

var telemetryCmd = &cobra.Command{
	Use:   "telemetry",
	Short: "Inspect and control usage telemetry",
	Long: `Inspect and control bintrail's usage telemetry.

Telemetry is metadata-only: command names, version, OS/arch and a bounded
error class. No identifier is stored or sent, and never your data, schemas,
tables, DSNs, hostnames, IPs or file paths. Run "telemetry show" to see the
exact bytes that would be sent.

Precedence, highest first: DO_NOT_TRACK, --telemetry=on|off,
BINTRAIL_TELEMETRY, the config file, then the default (on).`,
}

var telemetryStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show whether telemetry is on, and what decided it",
	RunE: func(cmd *cobra.Command, args []string) error {
		if !cliutil.IsValidOutputFormat(telFormat) {
			return fmt.Errorf("invalid --format %q: must be text or json", telFormat)
		}
		dir, dirErr := telemetry.ConfigDir()
		decision := telemetry.Resolve(telemetryFlagValue(cmd), dir)
		isCI := telemetry.IsCI()
		ep := telemetry.Endpoint()

		// Reporting requires consent AND a build that can send AND not being in
		// CI. Keep the reasons separate so "off" is never mysterious.
		reporting := decision.Enabled && !isCI && ep != "" && dirErr == nil

		if telFormat == "json" {
			// With no home directory there is no spool; emit empty rather than
			// a bare relative path that looks like a real location.
			spool := ""
			if dirErr == nil {
				spool = telemetry.SpoolDir(dir)
			}
			return cliutil.OutputJSON(map[string]any{
				"reporting":      reporting,
				"consent":        decision.Enabled,
				"decided_by":     string(decision.Source),
				"endpoint_set":   ep != "",
				"ci_detected":    isCI,
				"config_dir":     dir,
				"spool_dir":      spool,
				"schema_version": telemetry.SchemaVersion,
			})
		}

		state := "OFF"
		if reporting {
			state = "ON"
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Telemetry:    %s\n", state)
		fmt.Fprintf(cmd.OutOrStdout(), "Consent:      %s (decided by: %s)\n",
			onOff(decision.Enabled), decision.Source)
		if ep == "" {
			fmt.Fprintln(cmd.OutOrStdout(), "Endpoint:     not compiled in — this build cannot send anything")
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Endpoint:     %s\n", ep)
		}
		if isCI {
			fmt.Fprintln(cmd.OutOrStdout(), "CI detected:  yes — reporting suppressed regardless of consent")
		}
		if dirErr != nil {
			fmt.Fprintln(cmd.OutOrStdout(), "Config dir:   unavailable (no home directory) — telemetry disabled")
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Spool:        %s\n", telemetry.SpoolDir(dir))
		}
		return nil
	},
}

var telemetryShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Print the exact JSON that would be sent (sends nothing)",
	Long: `Print a representative event, byte for byte as it would be transmitted.

This command performs no network access. It exists so the payload can be
inspected without trusting the documentation: what you see here is the
complete set of fields the wire format permits.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		out, err := json.MarshalIndent(telemetry.SampleEvent(), "", "  ")
		if err != nil {
			return fmt.Errorf("render sample event: %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\nFields permitted on the wire: %v\n",
			out, telemetry.AllowedFields)
		fmt.Fprintf(cmd.OutOrStdout(), "Sent to: %s\n", endpointDescription())
		return nil
	},
}

var telemetryOnCmd = &cobra.Command{
	Use:   "on",
	Short: "Enable usage telemetry",
	RunE:  func(cmd *cobra.Command, args []string) error { return setTelemetry(cmd, true) },
}

var telemetryOffCmd = &cobra.Command{
	Use:   "off",
	Short: "Disable usage telemetry",
	RunE:  func(cmd *cobra.Command, args []string) error { return setTelemetry(cmd, false) },
}

func setTelemetry(cmd *cobra.Command, enabled bool) error {
	dir, err := telemetry.ConfigDir()
	if err != nil {
		return fmt.Errorf("locate config directory: %w", err)
	}
	if err := telemetry.SetEnabled(dir, enabled); err != nil {
		return err
	}
	if !enabled {
		// Events spooled before this decision would otherwise sit on disk
		// forever: the drain only ever runs while telemetry is ENABLED, so
		// nothing would ever send them or age them out. Reported as an error
		// rather than swallowed — telling an operator "nothing will be sent"
		// while their events are still on disk is the kind of half-truth this
		// whole feature cannot afford.
		if err := telemetry.PurgeSpool(dir); err != nil {
			return fmt.Errorf("telemetry is now off, but discarding already-spooled events failed: %w", err)
		}
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Telemetry %s (recorded in %s).\n",
		onOff(enabled), telemetry.StatePath(dir))
	if !enabled {
		fmt.Fprintln(cmd.OutOrStdout(),
			"Nothing further will be recorded or sent, and anything already\n"+
				"spooled locally has been discarded.\n"+
				"To disable telemetry for every tool on this machine, set DO_NOT_TRACK=1.")
	}
	return nil
}

// telemetryFlagValue reads the root --telemetry flag if the binary defines one.
// Absent flag means "unset", which falls through to the next control.
func telemetryFlagValue(cmd *cobra.Command) string {
	if f := cmd.Root().PersistentFlags().Lookup("telemetry"); f != nil {
		return f.Value.String()
	}
	return ""
}

func endpointDescription() string {
	if ep := telemetry.Endpoint(); ep != "" {
		return ep
	}
	return "nowhere — this build has no endpoint compiled in"
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func init() {
	telemetryStatusCmd.Flags().StringVar(&telFormat, "format", "text", "Output format: text or json")
	telemetryCmd.AddCommand(telemetryStatusCmd, telemetryShowCmd, telemetryOnCmd, telemetryOffCmd)
}
