package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/config"
)

// ─── Parent command ───────────────────────────────────────────────────────────

var profileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Manage access profiles",
	Long: `Add, remove, or list named access profiles in the index database.

Profiles are named groups (e.g. "dev", "marketing") used by RBAC access rules.
Each profile can have any number of access rules that allow or deny specific flags.`,
}

var (
	proIndexDSN    string
	proDescription string
)

func init() {
	profileCmd.PersistentFlags().StringVar(&proIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	_ = profileCmd.MarkPersistentFlagRequired("index-dsn")
	bindCommandEnv(profileCmd)

	profileCmd.AddCommand(profileAddCmd, profileRemoveCmd, profileListCmd)
	rootCmd.AddCommand(profileCmd)
}

// ─── profile add ─────────────────────────────────────────────────────────────

var profileAddCmd = &cobra.Command{
	Use:   "add <name>",
	Short: "Add a profile",
	Args:  cobra.ExactArgs(1),
	RunE:  runProfileAdd,
}

func init() {
	profileAddCmd.Flags().StringVar(&proDescription, "description", "", "Optional description for the profile")
}

func runProfileAdd(cmd *cobra.Command, args []string) error {
	name := args[0]

	db, err := config.Connect(proIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	_, err = db.ExecContext(cmd.Context(), `
		INSERT INTO profiles (name, description)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE description = VALUES(description)`,
		name, proDescription,
	)
	if err != nil {
		return fmt.Errorf("failed to add profile: %w", err)
	}

	fmt.Printf("Profile %q added.\n", name)
	return nil
}

// ─── profile remove ───────────────────────────────────────────────────────────

var profileRemoveCmd = &cobra.Command{
	Use:   "remove <name>",
	Short: "Remove a profile and its access rules",
	Args:  cobra.ExactArgs(1),
	RunE:  runProfileRemove,
}

func runProfileRemove(cmd *cobra.Command, args []string) error {
	name := args[0]

	db, err := config.Connect(proIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	res, err := db.ExecContext(cmd.Context(), `DELETE FROM profiles WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("failed to remove profile: %w", err)
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		fmt.Printf("Profile %q not found.\n", name)
		return nil
	}

	fmt.Printf("Profile %q removed.\n", name)
	return nil
}

// ─── profile list ─────────────────────────────────────────────────────────────

var profileListCmd = &cobra.Command{
	Use:   "list",
	Short: "List profiles",
	Args:  cobra.NoArgs,
	RunE:  runProfileList,
}

func runProfileList(cmd *cobra.Command, args []string) error {
	db, err := config.Connect(proIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(cmd.Context(), `
		SELECT name, COALESCE(description, ''), created_at
		FROM profiles
		ORDER BY name`)
	if err != nil {
		return fmt.Errorf("failed to list profiles: %w", err)
	}
	defer rows.Close()

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "NAME\tDESCRIPTION\tCREATED")
	fmt.Fprintln(tw, "────\t───────────\t───────")

	n := 0
	for rows.Next() {
		var name, description string
		var created time.Time
		if err := rows.Scan(&name, &description, &created); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\n", name, description, created.UTC().Format("2006-01-02 15:04:05"))
		n++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	if n == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No profiles found.")
	}
	return nil
}
