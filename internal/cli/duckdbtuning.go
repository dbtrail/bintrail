// Package cli holds command-layer building blocks intended to be shared across
// bintrail binaries. It exists so a second binary (the planned PostgreSQL-native
// bintrail-pg, #527/#529) can register the same source-agnostic read/recover
// commands and their shared flag/helper infrastructure without duplicating the
// command layer that lives in package main today.
//
// Today it holds only shared flag/helper infrastructure — this first file
// extracts the DuckDB resource-tuning flags shared by the offline
// query/recover/reconstruct commands (#510/#511), and cmd/bintrail is its only
// importer. Subsequent slices of #529 move the env-binding helpers and the
// source-agnostic commands themselves, at which point bintrail-pg becomes a
// second importer.
package cli

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
)

// duckDBMemoryLimitRE matches a number with a REQUIRED unit from the exact set
// the linked DuckDB accepts as a memory_limit — decimal B/KB/MB/GB/TB or binary
// KiB/MiB/GiB/TiB (verified empirically; DuckDB rejects PB/PiB, '%', and a
// bare unitless number). Matching only what DuckDB honors is the whole point:
// a value the regex accepts but DuckDB rejects (e.g. '80%') would pass this CLI
// gate and then hit Apply's best-effort SET, which logs a WARN and silently
// falls back to the default — the exact silent-fallback this validation exists
// to prevent. A leading '-' has no match (DuckDB SILENTLY accepts a negative
// limit as effectively unlimited), and a zero magnitude is rejected separately
// in validateDuckDBMemoryLimit (DuckDB accepts e.g. '0GB' and uncaps).
var duckDBMemoryLimitRE = regexp.MustCompile(`(?i)^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb|kib|mib|gib|tib)$`)

// validateDuckDBMemoryLimit rejects an operator-supplied --duckdb-memory-limit
// that DuckDB would mishandle, turning a typo or a footgun into a clear CLI
// error instead of a silent fall-back to the default (or a silent uncap).
func validateDuckDBMemoryLimit(s string) error {
	m := duckDBMemoryLimitRE.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return fmt.Errorf("invalid --duckdb-memory-limit %q: expected a positive size with a unit, e.g. 16GB, 512MB, or 16GiB (units: B/KB/MB/GB/TB or KiB/MiB/GiB/TiB)", s)
	}
	if v, err := strconv.ParseFloat(m[1], 64); err != nil || v <= 0 {
		return fmt.Errorf("invalid --duckdb-memory-limit %q: must be greater than zero (DuckDB treats a zero limit as unlimited)", s)
	}
	return nil
}

// AddDuckDBTuningFlags registers the shared DuckDB resource flags on the
// offline query/recover/reconstruct commands. They lift the conservative,
// container-safe DuckDB budget (threads=2, memory_limit=4GB) that
// parquetquery.Fetch applies by default — useful on a dedicated box with
// plenty of RAM where the small-container defaults leave performance on the
// table (#510). The long-lived shim/console daemons never register these, so
// their archive reads keep the safe default (#509 non-goal).
//
// --duckdb-threads default -1 (not 0) so 0 stays a meaningful explicit value:
// "let DuckDB pick one thread per core". --duckdb-memory-limit "" means unset.
func AddDuckDBTuningFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("ultrafast", false,
		"trade DuckDB memory-safety for speed: let DuckDB self-tune to the host (all cores, ~80% RAM) instead of the container-safe 2 threads / 4GB cap — for big boxes, not small containers")
	cmd.Flags().Int("duckdb-threads", -1,
		"override DuckDB thread count (0 = one per CPU core); -1 keeps the mode default")
	cmd.Flags().String("duckdb-memory-limit", "",
		"override DuckDB memory limit, e.g. 16GB (empty keeps the mode default)")
}

// DuckDBTuningFromFlags resolves the effective DuckDB tuning for a command.
// Precedence: an explicit --duckdb-* flag wins over --ultrafast, which wins
// over the conservative default. The granular flags let an operator tune to
// their box without the all-or-nothing --ultrafast switch.
//
// An operator-supplied --duckdb-memory-limit is validated here, at the CLI
// boundary, and a bad value is a hard error — DuckDB's SET memory_limit is
// best-effort downstream and would either silently fall back to the default
// (a typo) or silently uncap on a negative value, neither of which an operator
// who explicitly asked for a budget should get without being told.
func DuckDBTuningFromFlags(cmd *cobra.Command) (duckdbutil.Tuning, error) {
	t := duckdbutil.DefaultTuning()
	if ultrafast, _ := cmd.Flags().GetBool("ultrafast"); ultrafast {
		t = duckdbutil.Ultrafast()
	}
	if threads, _ := cmd.Flags().GetInt("duckdb-threads"); threads >= 0 {
		t.Threads = threads
	}
	if mem, _ := cmd.Flags().GetString("duckdb-memory-limit"); mem != "" {
		if err := validateDuckDBMemoryLimit(mem); err != nil {
			return duckdbutil.Tuning{}, err
		}
		t.MemoryLimit = mem
	}
	return t, nil
}

// TunedArchiveFetcher adapts a DuckDB Tuning into a query.ArchiveFetcher so the
// CLI commands can inject their budget without changing parquetquery.Fetch's
// signature (which other packages pass as a function value).
func TunedArchiveFetcher(t duckdbutil.Tuning) query.ArchiveFetcher {
	return func(ctx context.Context, opts query.Options, source string) ([]query.ResultRow, error) {
		return parquetquery.FetchWithTuning(ctx, opts, source, t)
	}
}
