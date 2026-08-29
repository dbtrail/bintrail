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

var accessCmd = &cobra.Command{
	Use:   "access",
	Short: "Manage access rules for profiles",
	Long: `Add, remove, or list access rules in the index database.

Access rules map a profile to a flag with allow or deny permission:
  bintrail access add --profile dev --flag billing --permission deny --index-dsn "..."
  bintrail access add --profile marketing --flag pii --permission deny --index-dsn "..."`,
}

var (
	aclIndexDSN    string
	aclProfile     string
	aclFlag        string
	aclPermission  string
	aclListProfile string
)

func init() {
	accessCmd.PersistentFlags().StringVar(&aclIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	_ = accessCmd.MarkPersistentFlagRequired("index-dsn")
	bindCommandEnv(accessCmd)

	accessCmd.AddCommand(accessAddCmd, accessRemoveCmd, accessListCmd)
	rootCmd.AddCommand(accessCmd)
}

// ─── access add ──────────────────────────────────────────────────────────────

var accessAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add an access rule",
	Args:  cobra.NoArgs,
	RunE:  runAccessAdd,
}

func init() {
	accessAddCmd.Flags().StringVar(&aclProfile, "profile", "", "Profile name (required)")
	accessAddCmd.Flags().StringVar(&aclFlag, "flag", "", "Flag name (required)")
	accessAddCmd.Flags().StringVar(&aclPermission, "permission", "", "Permission: allow or deny (required)")
	_ = accessAddCmd.MarkFlagRequired("profile")
	_ = accessAddCmd.MarkFlagRequired("flag")
	_ = accessAddCmd.MarkFlagRequired("permission")
}

func runAccessAdd(cmd *cobra.Command, args []string) error {
	// Validated by the shared code before any connection is opened, so a bad
	// --permission is refused without a database (and without one in tests).
	// Trimmed first, as the writer would, so the line printed below shows
	// the stored value.
	rule := accessprofiles.Rule{Profile: aclProfile, Flag: aclFlag, Permission: aclPermission}.Trimmed()
	if err := accessprofiles.ValidateRule(rule); err != nil {
		return cliRuleError(err)
	}

	db, err := connectIndex(aclIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	// The same code the console's Access profiles page runs (#1445).
	if err := accessprofiles.AddRule(cmd.Context(), db, rule); err != nil {
		return cliRuleError(err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Access rule added: profile=%q flag=%q permission=%s\n", rule.Profile, rule.Flag, rule.Permission)
	return nil
}

// cliRuleError spells the shared permission refusal the way the command line
// names the field: "--permission must be ...". The message is the shared one;
// only the dashes are the CLI's.
func cliRuleError(err error) error {
	var bad *accessprofiles.InvalidPermissionError
	if errors.As(err, &bad) {
		return fmt.Errorf("--%w", err)
	}
	return err
}

// ─── access remove ────────────────────────────────────────────────────────────

var accessRemoveCmd = &cobra.Command{
	Use:   "remove",
	Short: "Remove an access rule",
	Args:  cobra.NoArgs,
	RunE:  runAccessRemove,
}

func init() {
	accessRemoveCmd.Flags().StringVar(&aclProfile, "profile", "", "Profile name (required)")
	accessRemoveCmd.Flags().StringVar(&aclFlag, "flag", "", "Flag name (required)")
	_ = accessRemoveCmd.MarkFlagRequired("profile")
	_ = accessRemoveCmd.MarkFlagRequired("flag")
}

func runAccessRemove(cmd *cobra.Command, args []string) error {
	profile, flag := strings.TrimSpace(aclProfile), strings.TrimSpace(aclFlag)

	db, err := connectIndex(aclIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	out := cmd.OutOrStdout()
	err = accessprofiles.RemoveRule(cmd.Context(), db, profile, flag)
	var notFound *accessprofiles.RuleNotFoundError
	if errors.As(err, &notFound) {
		// Exit 0, as for a flag: the state asked for is the state there is.
		fmt.Fprintf(out, "Access rule not found: profile=%q flag=%q\n", profile, flag)
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "Access rule removed: profile=%q flag=%q\n", profile, flag)
	return nil
}

// ─── access list ─────────────────────────────────────────────────────────────

var accessListCmd = &cobra.Command{
	Use:   "list",
	Short: "List access rules",
	Args:  cobra.NoArgs,
	RunE:  runAccessList,
}

func init() {
	accessListCmd.Flags().StringVar(&aclListProfile, "profile", "", "Filter by profile name")
}

func runAccessList(cmd *cobra.Command, args []string) error {
	db, err := connectIndex(aclIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	rules, err := accessprofiles.ListRules(cmd.Context(), db, aclListProfile)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "PROFILE\tFLAG\tPERMISSION\tCREATED")
	fmt.Fprintln(tw, "───────\t────\t──────────\t───────")

	for _, r := range rules {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Profile, r.Flag, r.Permission, r.CreatedAt.UTC().Format("2006-01-02 15:04:05"))
	}
	if len(rules) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No access rules found.")
	}
	return nil
}
