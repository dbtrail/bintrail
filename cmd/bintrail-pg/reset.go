package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Tear down a PostgreSQL capture: drop the replication slot and clear the index checkpoint",
	Long: `Cleanly tears down a PostgreSQL capture so you can re-baseline and start fresh.

It performs the two-system teardown that otherwise has to be done by hand:

  1. drops the replication slot on the SOURCE (--query-dsn) — a slot left behind
     keeps pinning WAL and can fill the source disk;
  2. clears the durable checkpoint in the INDEX (--index-dsn): DELETE the
     stream_state row, so the next 'bintrail-pg stream' starts from a fresh slot.

The slot is dropped first: if the run is interrupted between the two steps, the
safe state is "slot gone, checkpoint stale" (the next stream fails loud) rather
than "checkpoint cleared, slot live" (which would silently skip data).

After a lost-slot invalidation the slot may already be gone or unusable; pass
--index-only to clear just the checkpoint without touching the source.

This is destructive and irreversible — pass --force to confirm. The index's
recovery data is NOT affected (recovery is index-only); only capture-resume state
is removed.

Examples:

  bintrail-pg reset --query-dsn "$PG" --index-dsn "$IDX" --slot bintrail_slot --force
  bintrail-pg reset --index-dsn "$IDX" --index-only --force   # slot already gone`,
	RunE: runPGReset,
}

var (
	pgResetQueryDSN  string
	pgResetIndexDSN  string
	pgResetSlot      string
	pgResetForce     bool
	pgResetIndexOnly bool
)

func init() {
	resetCmd.Flags().StringVar(&pgResetQueryDSN, "query-dsn", "", "PostgreSQL ordinary connection string, to drop the slot (required unless --index-only; env BINTRAIL_PG_QUERY_DSN)")
	resetCmd.Flags().StringVar(&pgResetIndexDSN, "index-dsn", "", "DSN for the index MySQL database, to clear the checkpoint (required; env BINTRAIL_INDEX_DSN)")
	resetCmd.Flags().StringVar(&pgResetSlot, "slot", "", "Replication slot to drop (required unless --index-only; env BINTRAIL_PG_SLOT)")
	resetCmd.Flags().BoolVar(&pgResetForce, "force", false, "Confirm this destructive, irreversible teardown")
	resetCmd.Flags().BoolVar(&pgResetIndexOnly, "index-only", false, "Only clear the index checkpoint; do not touch the source slot (use when the slot is already gone or lost)")
	// index-dsn is in cli.EnvBindings, so BindCommandEnv sets it from BINTRAIL_INDEX_DSN;
	// the PG-specific flags use the BINTRAIL_PG_* fallback applied in runPGReset.
	cli.BindCommandEnv(resetCmd)
	rootCmd.AddCommand(resetCmd)
}

func runPGReset(cmd *cobra.Command, args []string) error {
	applyEnvFallback(&pgResetQueryDSN, "BINTRAIL_PG_QUERY_DSN")
	applyEnvFallback(&pgResetSlot, "BINTRAIL_PG_SLOT")

	if pgResetIndexDSN == "" {
		return fmt.Errorf("missing required --index-dsn (or BINTRAIL_INDEX_DSN)")
	}
	if !pgResetIndexOnly {
		var missing []string
		if pgResetQueryDSN == "" {
			missing = append(missing, "--query-dsn (or BINTRAIL_PG_QUERY_DSN)")
		}
		if pgResetSlot == "" {
			missing = append(missing, "--slot (or BINTRAIL_PG_SLOT)")
		}
		if len(missing) > 0 {
			return fmt.Errorf("missing required settings: %s (or pass --index-only to clear just the checkpoint)", strings.Join(missing, ", "))
		}
	}
	if !pgResetForce {
		return errors.New("refusing to run a destructive teardown without --force — it drops the replication slot and clears the index checkpoint (recovery data is unaffected)")
	}

	ctx := cmd.Context()

	// 1. Drop the slot on the source FIRST (unless --index-only). Ordering matters: a
	//    half-failure then leaves "slot gone, checkpoint stale → next stream fails loud",
	//    not "checkpoint cleared, slot live → silent skip".
	if pgResetIndexOnly {
		fmt.Fprintln(os.Stdout, "--index-only: leaving the source replication slot untouched.")
	} else {
		conn, err := pgx.Connect(ctx, pgResetQueryDSN)
		if err != nil {
			return fmt.Errorf("connect to source: %w", err)
		}
		dropped, dropErr := pgcapture.DropSlot(ctx, conn, pgResetSlot)
		conn.Close(ctx)
		if dropErr != nil {
			return fmt.Errorf("%w\n(if the slot is active, stop the running `bintrail-pg stream` first, or use --index-only if the slot is already gone)", dropErr)
		}
		if dropped {
			fmt.Fprintf(os.Stdout, "Dropped replication slot %q on the source.\n", pgResetSlot)
		} else {
			fmt.Fprintf(os.Stdout, "Replication slot %q was already absent.\n", pgResetSlot)
		}
	}

	// 2. Clear the index checkpoint.
	idx, err := config.Connect(pgResetIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer idx.Close()
	res, err := idx.ExecContext(ctx, "DELETE FROM stream_state WHERE id = 1")
	if err != nil {
		if isTableMissingErr(err) {
			// A never-streamed index: nothing to clear.
			fmt.Fprintln(os.Stdout, "No index checkpoint to clear (stream_state table absent).")
			fmt.Fprintln(os.Stdout, "Done.")
			return nil
		}
		return fmt.Errorf("clearing the index checkpoint: %w", err)
	}
	n, _ := res.RowsAffected()
	fmt.Fprintf(os.Stdout, "Cleared %d index checkpoint row(s).\n", n)
	fmt.Fprintln(os.Stdout, "Done. Re-seed the baseline, then run `bintrail-pg stream` to start fresh.")
	return nil
}

// isTableMissingErr reports whether err is MySQL error 1146 (ER_NO_SUCH_TABLE), i.e.
// the index has no stream_state table yet (never streamed) — nothing to reset.
func isTableMissingErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1146
}
