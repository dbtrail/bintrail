package cliapp

import (
	"errors"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/accessprofiles"
	"github.com/dbtrail/dbtrail/internal/config"
)

// ─── Parent command ───────────────────────────────────────────────────────────

var flagCmd = &cobra.Command{
	Use:   "flag",
	Short: "Manage flags on tables and columns",
	Long: `Add, remove, or list named flags on tables or columns in the index database.

Flags are the building block for RBAC: a flag labels a table or column with a
name (e.g. "billing", "pii"). Access rules then define which profiles may or
may not access data carrying each flag.

Table-level flag (omit --column):
  bintrail flag add billing --schema mydb --table orders --index-dsn "..."

Column-level flag (include --column):
  bintrail flag add pii --schema mydb --table customers --column email --index-dsn "..."`,
}

var (
	flgIndexDSN string
	flgSchema   string
	flgTable    string
	flgColumn   string
)

// connectIndex opens the index database for the flag, profile and access
// verbs. A variable so a unit test can hand the verbs a mocked database and
// drive the real RunE functions (the not-found-is-exit-0 mapping lives
// there, not in the shared package).
var connectIndex = config.Connect

func init() {
	// --index-dsn is inherited by all subcommands via PersistentFlags.
	flagCmd.PersistentFlags().StringVar(&flgIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	_ = flagCmd.MarkPersistentFlagRequired("index-dsn")
	bindCommandEnv(flagCmd)

	flagCmd.AddCommand(flagAddCmd, flagRemoveCmd, flagListCmd)
	rootCmd.AddCommand(flagCmd)
}

// ─── flag add ────────────────────────────────────────────────────────────────

var flagAddCmd = &cobra.Command{
	Use:   "add <flag-name>",
	Short: "Add a flag to a table or column",
	Args:  cobra.ExactArgs(1),
	RunE:  runFlagAdd,
}

func init() {
	flagAddCmd.Flags().StringVar(&flgSchema, "schema", "", "Schema (database) name (required)")
	flagAddCmd.Flags().StringVar(&flgTable, "table", "", "Table name (required)")
	flagAddCmd.Flags().StringVar(&flgColumn, "column", "", "Column name (omit for a table-level flag)")
	_ = flagAddCmd.MarkFlagRequired("schema")
	_ = flagAddCmd.MarkFlagRequired("table")
}

func runFlagAdd(cmd *cobra.Command, args []string) error {
	// The same code the console's Access profiles page runs (#1445). Trimmed
	// here as well so the line printed below shows the stored value.
	f := accessprofiles.Flag{Schema: flgSchema, Table: flgTable, Column: flgColumn, Name: args[0]}.Trimmed()

	db, err := connectIndex(flgIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	if err := accessprofiles.AddFlag(cmd.Context(), db, f); err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	if f.Column != "" {
		fmt.Fprintf(out, "Flag %q added to %s.%s (%s)\n", f.Name, f.Schema, f.Table, f.Column)
	} else {
		fmt.Fprintf(out, "Flag %q added to %s.%s\n", f.Name, f.Schema, f.Table)
	}
	return nil
}

// ─── flag remove ─────────────────────────────────────────────────────────────

var flagRemoveCmd = &cobra.Command{
	Use:   "remove <flag-name>",
	Short: "Remove a flag from a table or column",
	Args:  cobra.ExactArgs(1),
	RunE:  runFlagRemove,
}

func init() {
	flagRemoveCmd.Flags().StringVar(&flgSchema, "schema", "", "Schema (database) name (required)")
	flagRemoveCmd.Flags().StringVar(&flgTable, "table", "", "Table name (required)")
	flagRemoveCmd.Flags().StringVar(&flgColumn, "column", "", "Column name (omit for a table-level flag)")
	_ = flagRemoveCmd.MarkFlagRequired("schema")
	_ = flagRemoveCmd.MarkFlagRequired("table")
}

func runFlagRemove(cmd *cobra.Command, args []string) error {
	f := accessprofiles.Flag{Schema: flgSchema, Table: flgTable, Column: flgColumn, Name: args[0]}.Trimmed()

	db, err := connectIndex(flgIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	out := cmd.OutOrStdout()
	err = accessprofiles.RemoveFlag(cmd.Context(), db, f)
	var notFound *accessprofiles.FlagNotFoundError
	if errors.As(err, &notFound) {
		// Not an error on the command line (exit 0): the state asked for is
		// the state there is.
		fmt.Fprintf(out, "Flag %q not found on %s.%s", f.Name, f.Schema, f.Table)
		if f.Column != "" {
			fmt.Fprintf(out, " (%s)", f.Column)
		}
		fmt.Fprintln(out)
		return nil
	}
	if err != nil {
		return err
	}

	if f.Column != "" {
		fmt.Fprintf(out, "Flag %q removed from %s.%s (%s)\n", f.Name, f.Schema, f.Table, f.Column)
	} else {
		fmt.Fprintf(out, "Flag %q removed from %s.%s\n", f.Name, f.Schema, f.Table)
	}
	return nil
}

// ─── flag list ───────────────────────────────────────────────────────────────

var flagListCmd = &cobra.Command{
	Use:   "list",
	Short: "List flags stored in the index",
	Args:  cobra.NoArgs,
	RunE:  runFlagList,
}

var (
	flgListSchema string
	flgListTable  string
)

func init() {
	flagListCmd.Flags().StringVar(&flgListSchema, "schema", "", "Filter by schema name")
	flagListCmd.Flags().StringVar(&flgListTable, "table", "", "Filter by table name")
}

func runFlagList(cmd *cobra.Command, args []string) error {
	db, err := connectIndex(flgIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	flags, err := accessprofiles.ListFlags(cmd.Context(), db, flgListSchema, flgListTable)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "SCHEMA\tTABLE\tCOLUMN\tFLAG\tCREATED")
	fmt.Fprintln(tw, "──────\t─────\t──────\t────\t───────")

	for _, f := range flags {
		level := f.Column
		if level == "" {
			level = "(table)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", f.Schema, f.Table, level, f.Name, f.CreatedAt.UTC().Format("2006-01-02 15:04:05"))
	}
	if len(flags) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No flags found.")
	}
	return nil
}
