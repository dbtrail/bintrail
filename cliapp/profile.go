package cliapp

import (
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/accessprofiles"
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
	// The same code the console's Access profiles page runs (#1445). Trimmed
	// here as well so the line printed below shows the stored value.
	p := accessprofiles.Profile{Name: args[0], Description: proDescription}.Trimmed()

	db, err := connectIndex(proIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	if err := accessprofiles.AddProfile(cmd.Context(), db, p); err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Profile %q added.\n", p.Name)
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
	name := strings.TrimSpace(args[0])

	db, err := connectIndex(proIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	out := cmd.OutOrStdout()
	err = accessprofiles.RemoveProfile(cmd.Context(), db, name)
	var notFound *accessprofiles.ProfileNotFoundError
	if errors.As(err, &notFound) {
		// Exit 0, as for a flag: the state asked for is the state there is.
		fmt.Fprintf(out, "Profile %q not found.\n", name)
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "Profile %q removed.\n", name)
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
	db, err := connectIndex(proIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	profiles, err := accessprofiles.ListProfiles(cmd.Context(), db)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "NAME\tDESCRIPTION\tCREATED")
	fmt.Fprintln(tw, "────\t───────────\t───────")

	for _, p := range profiles {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", p.Name, p.Description, p.CreatedAt.UTC().Format("2006-01-02 15:04:05"))
	}
	if len(profiles) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No profiles found.")
	}
	return nil
}
