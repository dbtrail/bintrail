package main

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
)

// addDuckDBTuningFlags registers the shared DuckDB resource flags on the
// offline query/recover/reconstruct commands. They lift the conservative,
// container-safe DuckDB budget (threads=2, memory_limit=4GB) that
// parquetquery.Fetch applies by default — useful on a dedicated box with
// plenty of RAM where the small-container defaults leave performance on the
// table (#510). The long-lived shim/console daemons never register these, so
// their archive reads keep the safe default (#509 non-goal).
//
// --duckdb-threads default -1 (not 0) so 0 stays a meaningful explicit value:
// "let DuckDB pick one thread per core". --duckdb-memory-limit "" means unset.
func addDuckDBTuningFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("ultrafast", false,
		"trade DuckDB memory-safety for speed: let DuckDB self-tune to the host (all cores, ~80% RAM) instead of the container-safe 2 threads / 4GB cap — for big boxes, not small containers")
	cmd.Flags().Int("duckdb-threads", -1,
		"override DuckDB thread count (0 = one per CPU core); -1 keeps the mode default")
	cmd.Flags().String("duckdb-memory-limit", "",
		"override DuckDB memory limit, e.g. 16GB (empty keeps the mode default)")
}

// duckDBTuningFromFlags resolves the effective DuckDB tuning for a command.
// Precedence: an explicit --duckdb-* flag wins over --ultrafast, which wins
// over the conservative default. The granular flags let an operator tune to
// their box without the all-or-nothing --ultrafast switch.
func duckDBTuningFromFlags(cmd *cobra.Command) duckdbutil.Tuning {
	t := duckdbutil.DefaultTuning()
	if ultrafast, _ := cmd.Flags().GetBool("ultrafast"); ultrafast {
		t = duckdbutil.Ultrafast()
	}
	if threads, _ := cmd.Flags().GetInt("duckdb-threads"); threads >= 0 {
		t.Threads = threads
	}
	if mem, _ := cmd.Flags().GetString("duckdb-memory-limit"); mem != "" {
		t.MemoryLimit = mem
	}
	return t
}

// tunedArchiveFetcher adapts a DuckDB Tuning into a query.ArchiveFetcher so the
// CLI commands can inject their budget without changing parquetquery.Fetch's
// signature (which other packages pass as a function value).
func tunedArchiveFetcher(t duckdbutil.Tuning) query.ArchiveFetcher {
	return func(ctx context.Context, opts query.Options, source string) ([]query.ResultRow, error) {
		return parquetquery.FetchWithTuning(ctx, opts, source, t)
	}
}
