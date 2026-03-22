package main

import (
	"database/sql"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/config"
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
	if aclPermission != "allow" && aclPermission != "deny" {
		return fmt.Errorf("--permission must be \"allow\" or \"deny\", got %q", aclPermission)
	}

	db, err := config.Connect(aclIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	var profileID int64
	err = db.QueryRowContext(cmd.Context(), `SELECT id FROM profiles WHERE name = ?`, aclProfile).Scan(&profileID)
	if err == sql.ErrNoRows {
		return fmt.Errorf("profile %q not found", aclProfile)
	}
	if err != nil {
		return fmt.Errorf("failed to look up profile: %w", err)
	}

	_, err = db.ExecContext(cmd.Context(), `
		INSERT INTO access_rules (profile_id, flag, permission)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE permission = VALUES(permission)`,
		profileID, aclFlag, aclPermission,
	)
	if err != nil {
		return fmt.Errorf("failed to add access rule: %w", err)
	}

	fmt.Printf("Access rule added: profile=%q flag=%q permission=%s\n", aclProfile, aclFlag, aclPermission)
	return nil
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
	db, err := config.Connect(aclIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	res, err := db.ExecContext(cmd.Context(), `
		DELETE ar FROM access_rules ar
		JOIN profiles p ON ar.profile_id = p.id
		WHERE p.name = ? AND ar.flag = ?`,
		aclProfile, aclFlag,
	)
	if err != nil {
		return fmt.Errorf("failed to remove access rule: %w", err)
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		fmt.Printf("Access rule not found: profile=%q flag=%q\n", aclProfile, aclFlag)
		return nil
	}

	fmt.Printf("Access rule removed: profile=%q flag=%q\n", aclProfile, aclFlag)
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
	db, err := config.Connect(aclIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer db.Close()

	q := `SELECT p.name, ar.flag, ar.permission, ar.created_at
	      FROM access_rules ar
	      JOIN profiles p ON ar.profile_id = p.id`
	var qargs []any
	if aclListProfile != "" {
		q += " WHERE p.name = ?"
		qargs = append(qargs, aclListProfile)
	}
	q += " ORDER BY p.name, ar.flag"

	rows, err := db.QueryContext(cmd.Context(), q, qargs...)
	if err != nil {
		return fmt.Errorf("failed to list access rules: %w", err)
	}
	defer rows.Close()

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintln(tw, "PROFILE\tFLAG\tPERMISSION\tCREATED")
	fmt.Fprintln(tw, "───────\t────\t──────────\t───────")

	n := 0
	for rows.Next() {
		var profile, flag, permission string
		var created time.Time
		if err := rows.Scan(&profile, &flag, &permission, &created); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", profile, flag, permission, created.UTC().Format("2006-01-02 15:04:05"))
		n++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	if n == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No access rules found.")
	}
	return nil
}
