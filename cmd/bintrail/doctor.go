package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/doctor"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Diagnose source MySQL prerequisites and emit copy-pasteable remediation",
	Long: `Runs preflight checks against the source MySQL server and (optionally)
the index database. Reports each check as PASS, FAIL, WARN, or SKIP with
copy-pasteable remediation SQL when a check fails.

Run this before 'bintrail up', 'bintrail stream', 'bintrail index', or
'bintrail agent' to catch the common onboarding gotchas (wrong binlog_format,
missing GRANTs, missing log_bin) before they cost you a debugging cycle.

Exit code is 0 only when every required check passes. Warnings do not affect
the exit code so 'doctor' is safe to run in CI as a smoke test.

Examples:

  bintrail doctor --source-dsn "user:pass@tcp(source:3306)/"
  bintrail doctor --source-dsn "$SRC" --index-dsn "$IDX" --schemas mydb`,
	RunE: runDoctor,
}

var (
	docSourceDSN string
	docIndexDSN  string
	docSchemas   string
	docFormat    string
	docRetain    string
)

func init() {
	doctorCmd.Flags().StringVar(&docSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	doctorCmd.Flags().StringVar(&docIndexDSN, "index-dsn", "", "DSN for the index MySQL database (optional; verifies write access when provided)")
	doctorCmd.Flags().StringVar(&docSchemas, "schemas", "", "Comma-separated schemas to check (default: all user schemas)")
	doctorCmd.Flags().StringVar(&docFormat, "format", "text", "Output format: text or json")
	doctorCmd.Flags().StringVar(&docRetain, "retain", "30d", "Retention window assumed by the index capacity projection (Nd/Nh; \"off\" if you don't rotate)")
	_ = doctorCmd.MarkFlagRequired("source-dsn")
	bindCommandEnv(doctorCmd)
	rootCmd.AddCommand(doctorCmd)
}

func runDoctor(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(docFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", docFormat)
	}
	retain, err := parseDocRetain(docRetain)
	if err != nil {
		return err
	}
	return runDoctorTo(cmd.Context(), os.Stdout, docFormat, docSourceDSN, docIndexDSN, docSchemas, retain)
}

// parseDocRetain maps doctor's --retain value to the capacity projection's
// window: "off", "0", and "" mean no rotation (0 — the check reports
// unbounded growth); anything else must parse as Nd/Nh.
func parseDocRetain(s string) (time.Duration, error) {
	switch s {
	case "off", "0", "":
		return 0, nil
	}
	retain, err := cliutil.ParseRetain(s)
	if err != nil {
		return 0, fmt.Errorf("--retain: %w (or \"off\" if you don't rotate)", err)
	}
	return retain, nil
}

// runDoctorTo is the testable core of the doctor command. It runs every check
// against sourceDSN (and optionally indexDSN), renders the report to w using
// format ("text" or "json"), and returns a non-nil error iff any required
// check failed. indexRetain is the rotation window the capacity projection
// assumes (0 = no rotation). Callers wanting to route output (e.g. `bintrail
// up` sending preflight output to stderr to keep stdout clean for streaming)
// pass their own writer here instead of going through the cobra entry point.
func runDoctorTo(parent context.Context, w io.Writer, format, sourceDSN, indexDSN, schemasCSV string, indexRetain time.Duration) error {
	report := doctor.Build(parent, sourceDSN, indexDSN, schemasCSV, indexRetain)
	if err := report.Write(w, format); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return report.Err()
}
