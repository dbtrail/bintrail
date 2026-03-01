package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/config"
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

func init() {
	// --index-dsn is inherited by all subcommands via PersistentFlags.
	flagCmd.PersistentFlags().StringVar(&flgIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	_ = flagCmd.MarkPersistentFlagRequired("index-dsn")

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
	flagName := args[0]

	db, err := config.Connect(flgIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	_, err = db.ExecContext(cmd.Context(), `
		INSERT INTO table_flags (schema_name, table_name, column_name, flag)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE flag = flag`,
		flgSchema, flgTable, flgColumn, flagName,
	)
	if err != nil {
		return fmt.Errorf("failed to add flag: %w", err)
	}

	if flgColumn != "" {
		fmt.Printf("Flag %q added to %s.%s (%s)\n", flagName, flgSchema, flgTable, flgColumn)
	} else {
		fmt.Printf("Flag %q added to %s.%s\n", flagName, flgSchema, flgTable)
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
	flagName := args[0]

	db, err := config.Connect(flgIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	res, err := db.ExecContext(cmd.Context(), `
		DELETE FROM table_flags
		WHERE schema_name = ? AND table_name = ? AND column_name = ? AND flag = ?`,
		flgSchema, flgTable, flgColumn, flagName,
	)
	if err != nil {
		return fmt.Errorf("failed to remove flag: %w", err)
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		fmt.Printf("Flag %q not found on %s.%s", flagName, flgSchema, flgTable)
		if flgColumn != "" {
			fmt.Printf(" (%s)", flgColumn)
		}
		fmt.Println()
		return nil
	}

	if flgColumn != "" {
		fmt.Printf("Flag %q removed from %s.%s (%s)\n", flagName, flgSchema, flgTable, flgColumn)
	} else {
		fmt.Printf("Flag %q removed from %s.%s\n", flagName, flgSchema, flgTable)
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
	db, err := config.Connect(flgIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	q := `SELECT schema_name, table_name, column_name, flag, created_at
	      FROM table_flags`
	var where []string
	var qargs []any
	if flgListSchema != "" {
		where = append(where, "schema_name = ?")
		qargs = append(qargs, flgListSchema)
	}
	if flgListTable != "" {
		where = append(where, "table_name = ?")
		qargs = append(qargs, flgListTable)
	}
	if len(where) > 0 {
		q += " WHERE "
		for i, w := range where {
			if i > 0 {
				q += " AND "
			}
			q += w
		}
	}
	q += " ORDER BY schema_name, table_name, column_name, flag"

	rows, err := db.QueryContext(cmd.Context(), q, qargs...)
	if err != nil {
		return fmt.Errorf("failed to list flags: %w", err)
	}
	defer rows.Close()

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "SCHEMA\tTABLE\tCOLUMN\tFLAG\tCREATED")
	fmt.Fprintln(tw, "──────\t─────\t──────\t────\t───────")

	n := 0
	for rows.Next() {
		var schema, table, column, flag string
		var created time.Time
		if err := rows.Scan(&schema, &table, &column, &flag, &created); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		level := column
		if level == "" {
			level = "(table)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", schema, table, level, flag, created.UTC().Format("2006-01-02 15:04:05"))
		n++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	if n == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No flags found.")
	}
	return nil
}
