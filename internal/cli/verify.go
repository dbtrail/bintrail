package cli

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/verify"
)

var (
	vfySourceDSN   string
	vfyIndexDSN    string
	vfyBaselineDir string
	vfyBaselineS3  string
	vfyTables      string
	vfyNoArchive   bool
	vfyExplain     bool
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify that a recovery would reproduce the source",
	Long: `Prove the recovery chain (baseline + indexed binlog) faithfully reproduces
the data. Two modes:

  Baseline-anchored (default, drift-free) — omit --source-dsn. Compares the two
  most recent baselines: reconstructs the previous baseline forward to the new
  baseline's exact binlog anchor and fingerprints it against the new baseline.
  Both sides are at-rest, so it reads no live source — run it any time after a
  baseline (e.g. right after "bintrail baseline", or on a schedule). No
  production impact.

  Live-source — pass --source-dsn. Reconstructs each table to a consistent
  snapshot of the live source and compares. Reads the whole table off the live
  server, so run it off-peak.

Results are per table: match, mismatch, or inconclusive (no predecessor
baseline, index behind, unsupported PK, coverage gap, or a value class this
version can't yet compare — never reported as a failure). The run exits non-zero
on any mismatch or error, or when comparable tables existed but none could be
proven (all inconclusive). A source with only one baseline — no predecessor yet
— is reported and exits zero.

Add --explain (baseline-anchored mode) to print, below the report, a row-level
drill-down of each mismatch: which primary keys diverged and, for changed rows,
the differing columns with the reconstructed value vs the new baseline's. It
re-runs the same reconstruction the verdict came from (byte-identical by
construction) — no live source, scratch database, or external tool.

Examples:
  # Baseline-anchored (drift-free), all tables
  bintrail verify --index-dsn "..." --baseline-dir /data/baselines

  # Baseline-anchored with a row-level drill-down on any mismatch
  bintrail verify --index-dsn "..." --baseline-dir /data/baselines --explain

  # Live-source, specific tables, S3 baselines
  bintrail verify --source-dsn "..." --index-dsn "..." \
    --baseline-s3 s3://bucket/baselines --tables mydb.orders,mydb.users`,
	RunE: runVerify,
}

func init() {
	verifyCmd.Flags().StringVar(&vfySourceDSN, "source-dsn", "", "DSN for the live source MySQL database; pass it for live-source mode, omit for baseline-anchored mode")
	verifyCmd.Flags().StringVar(&vfyIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	verifyCmd.Flags().StringVar(&vfyBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots")
	verifyCmd.Flags().StringVar(&vfyBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline Parquet snapshots (e.g. s3://bucket/baselines/)")
	verifyCmd.Flags().StringVar(&vfyTables, "tables", "", "Comma-separated schema.table list (default: all tables in the latest schema snapshot)")
	verifyCmd.Flags().BoolVar(&vfyNoArchive, "no-archive", false, "Query live MySQL partitions only; skip Parquet archive discovery")
	verifyCmd.Flags().BoolVar(&vfyExplain, "explain", false, "On a baseline-anchored mismatch, print a row-level drill-down (which primary keys diverged and how) below the report")
	AddDuckDBTuningFlags(verifyCmd)
}

func runVerify(cmd *cobra.Command, _ []string) error {
	if vfyIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required")
	}
	baselineSrc := vfyBaselineDir
	if baselineSrc == "" {
		baselineSrc = vfyBaselineS3
	}
	if baselineSrc == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required")
	}

	indexDB, err := config.Connect(vfyIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index database: %w", err)
	}
	defer indexDB.Close()

	// Idempotent schema migration (CLI-typed DSN — the one-DDL boundary):
	// the shared query engine SELECTs post-initial-schema binlog_events
	// columns (connection_id, query_text/query_hash #699), so verify against
	// an index last written by an older binary would otherwise fail with
	// MySQL error 1054 — a false drift alert.
	if err := indexer.EnsureSchema(indexDB); err != nil {
		return fmt.Errorf("ensure index schema: %w", err)
	}

	var indexDBName string
	if cfg, parseErr := mysqldriver.ParseDSN(vfyIndexDSN); parseErr == nil {
		indexDBName = cfg.DBName
	}
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		return fmt.Errorf("load schema snapshot from index: %w", err)
	}
	duckTuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}

	if vfySourceDSN != "" {
		return runVerifyLive(cmd, indexDB, resolver, indexDBName, baselineSrc, duckTuning)
	}
	return runVerifyBaselinePair(cmd, indexDB, resolver, indexDBName, baselineSrc, duckTuning)
}

// runVerifyBaselinePair is the default, drift-free mode: compare the two most
// recent baselines (#642). It reads no live source.
func runVerifyBaselinePair(cmd *cobra.Command, indexDB *sql.DB, resolver *metadata.Resolver, indexDBName, baselineSrc string, duckTuning duckdbutil.Tuning) error {
	pairs, unpaired, prevOnly, err := verify.FindBaselinePair(cmd.Context(), baselineSrc)
	if err != nil {
		return fmt.Errorf("discover baseline pair: %w", err)
	}
	if len(pairs) == 0 && len(unpaired) == 0 {
		// Two physically different causes reach here as "fewer than two
		// baselines". A source with NO baselines at all is almost always a
		// misconfiguration or a broken baseline job — exiting 0 there would let a
		// CI/cron gate go permanently green while verifying nothing, the exact
		// false-assurance this command exists to prevent. A source with exactly
		// one baseline is a legitimate first run with no predecessor to compare.
		any, err := verify.AnyBaseline(cmd.Context(), baselineSrc)
		if err != nil {
			return fmt.Errorf("list baselines under source: %w", err)
		}
		if !any {
			return fmt.Errorf("no baselines found under %q; nothing to verify (check --baseline-dir/--baseline-s3 and that the baseline job is producing complete snapshots)", baselineSrc)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "only one baseline under the source; nothing to verify yet (no predecessor to compare against)")
		return nil
	}

	want, err := verifyTableFilter()
	if err != nil {
		return err
	}
	cfg := verify.BaselineConfig{
		IndexDB:        indexDB,
		Resolver:       resolver,
		IndexDBName:    indexDBName,
		NoArchive:      vfyNoArchive,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
	}

	results := make([]verify.TableResult, 0, len(pairs)+len(unpaired))
	var toExplain []verify.BaselinePair // mismatched pairs to drill into when --explain
	for _, p := range pairs {
		if want != nil && !want[p.Schema+"."+p.Table] {
			continue
		}
		res, err := verify.VerifyBaselinePair(cmd.Context(), cfg, p)
		if err != nil {
			res = verify.TableResult{Schema: p.Schema, Table: p.Table, Status: verify.StatusError, Detail: err.Error()}
		}
		results = append(results, res)
		if vfyExplain && res.Status == verify.StatusMismatch {
			toExplain = append(toExplain, p)
		}
	}
	for _, u := range unpaired {
		if want != nil && !want[u.Schema+"."+u.Table] {
			continue
		}
		results = append(results, verify.TableResult{
			Schema: u.Schema, Table: u.Table, Status: verify.StatusInconclusive,
			Detail: "no predecessor baseline (new since the previous snapshot)",
		})
	}
	// Tables in the previous baseline the newest snapshot no longer carries —
	// dropped, or skipped by a subset ("--tables") re-baseline. Reported as
	// inconclusive (not verified) so they appear instead of silently vanishing
	// from a default all-tables run; the exit code is unchanged (inconclusive,
	// like unpaired, does not by itself fail the run).
	for _, d := range prevOnly {
		if want != nil && !want[d.Schema+"."+d.Table] {
			continue
		}
		results = append(results, verify.TableResult{
			Schema: d.Schema, Table: d.Table, Status: verify.StatusInconclusive,
			Detail: "present in the previous baseline but absent from the newest snapshot (dropped, or the newest baseline was run with --tables); not verified",
		})
	}
	// A table named in --tables that is absent from BOTH the paired and unpaired
	// sets was never iterated above, so it would silently vanish from the report
	// while the run still exited 0 on the other tables' matches — the exact
	// silent-omission this command exists to prevent (and asymmetric with live
	// mode, where a bogus --tables entry reaches VerifyTable and gates the exit).
	// Surface each unseen request as an error so it appears and fails the run.
	if want != nil {
		seen := make(map[string]bool, len(results))
		for _, r := range results {
			seen[r.Schema+"."+r.Table] = true
		}
		for key := range want {
			if seen[key] {
				continue
			}
			schema, table, _ := strings.Cut(key, ".")
			results = append(results, verify.TableResult{
				Schema: schema, Table: table, Status: verify.StatusError,
				Detail: "requested via --tables but not present in the latest baseline pair",
			})
		}
	}
	if len(results) == 0 {
		return fmt.Errorf("no tables matched --tables in the baseline pair")
	}
	reportErr := printVerifyReport(cmd, results)
	// Drill-downs print AFTER the summary table so the verdict reads first, then
	// the per-row detail for each mismatch. A drill-down failure is non-fatal — it
	// must not mask the report's own (mismatch) exit status.
	for _, p := range toExplain {
		ex, err := verify.ExplainBaselinePairMismatch(cmd.Context(), cfg, p)
		if err != nil {
			fmt.Fprintf(cmd.OutOrStdout(), "\n--- mismatch drill-down: %s.%s — unavailable: %v ---\n", p.Schema, p.Table, err)
			continue
		}
		ex.Write(cmd.OutOrStdout())
	}
	return reportErr
}

// runVerifyLive is the secondary mode: reconstruct each table to a consistent
// snapshot of the live source and compare (#634).
func runVerifyLive(cmd *cobra.Command, indexDB *sql.DB, resolver *metadata.Resolver, indexDBName, baselineSrc string, duckTuning duckdbutil.Tuning) error {
	sourceDB, err := config.Connect(vfySourceDSN)
	if err != nil {
		return fmt.Errorf("connect to source database: %w", err)
	}
	defer sourceDB.Close()

	tables, err := verifyTargetTables(cmd, indexDB)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("no tables to verify (empty --tables and no schema snapshot)")
	}

	cfg := verify.Config{
		SourceDB:       sourceDB,
		IndexDB:        indexDB,
		Resolver:       resolver,
		BaselineSource: baselineSrc,
		IndexDBName:    indexDBName,
		NoArchive:      vfyNoArchive,
		ArchiveFetcher: TunedArchiveFetcher(duckTuning),
	}

	results := make([]verify.TableResult, 0, len(tables))
	for _, st := range tables {
		res, err := verify.VerifyTable(cmd.Context(), cfg, st.schema, st.table)
		if err != nil {
			// One table's hard error must not abort the run and hide the other
			// tables' results (including real mismatches). Record it and continue.
			res = verify.TableResult{Schema: st.schema, Table: st.table, Status: verify.StatusError, Detail: err.Error()}
		}
		results = append(results, res)
	}
	return printVerifyReport(cmd, results)
}

// verifyTableFilter parses --tables into a "schema.table" set, or nil for all.
func verifyTableFilter() (map[string]bool, error) {
	if vfyTables == "" {
		return nil, nil
	}
	want := map[string]bool{}
	for _, entry := range splitAndTrim(vfyTables, ",") {
		parts := strings.SplitN(entry, ".", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid --tables entry %q (want schema.table)", entry)
		}
		want[parts[0]+"."+parts[1]] = true
	}
	return want, nil
}

type schemaTable struct{ schema, table string }

// verifyTargetTables returns the tables to verify: the explicit --tables list,
// or every table in the latest schema snapshot.
func verifyTargetTables(cmd *cobra.Command, indexDB *sql.DB) ([]schemaTable, error) {
	if vfyTables != "" {
		var out []schemaTable
		for _, entry := range splitAndTrim(vfyTables, ",") {
			parts := strings.SplitN(entry, ".", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				return nil, fmt.Errorf("invalid --tables entry %q (want schema.table)", entry)
			}
			out = append(out, schemaTable{parts[0], parts[1]})
		}
		return out, nil
	}
	rows, err := indexDB.QueryContext(cmd.Context(), `
		SELECT DISTINCT schema_name, table_name FROM schema_snapshots
		WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM schema_snapshots)
		ORDER BY schema_name, table_name`)
	if err != nil {
		return nil, fmt.Errorf("list tables from schema snapshot: %w", err)
	}
	defer rows.Close()
	var out []schemaTable
	for rows.Next() {
		var s, t string
		if err := rows.Scan(&s, &t); err != nil {
			return nil, fmt.Errorf("scan table row: %w", err)
		}
		out = append(out, schemaTable{s, t})
	}
	return out, rows.Err()
}

// printVerifyReport writes a per-table table and a summary, and returns a
// non-nil error (non-zero exit) when any table mismatched.
func printVerifyReport(cmd *cobra.Command, results []verify.TableResult) error {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Schema != results[j].Schema {
			return results[i].Schema < results[j].Schema
		}
		return results[i].Table < results[j].Table
	})

	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "TABLE\tSTATUS\tROWS(src/recon)\tDETAIL")
	var match, mismatch, inconclusive, errored int
	for _, r := range results {
		switch r.Status {
		case verify.StatusMatch:
			match++
		case verify.StatusMismatch:
			mismatch++
		case verify.StatusError:
			errored++
		case verify.StatusInconclusive:
			inconclusive++
		default:
			// An unrecognized status (incl. the zero value) must not be filed
			// under the benign inconclusive bucket — a verify tool's job is to
			// not hand out false assurance. Count it as an error.
			errored++
		}
		fmt.Fprintf(w, "%s.%s\t%s\t%d/%d\t%s\n",
			r.Schema, r.Table, r.Status, r.SourceRows, r.ReconstructRows, r.Detail)
	}
	w.Flush()
	fmt.Fprintf(cmd.OutOrStdout(), "\n%d match, %d mismatch, %d inconclusive, %d error\n",
		match, mismatch, inconclusive, errored)

	// Fail the run on any divergence or hard error. Also fail when nothing was
	// proven (zero matches) — an all-inconclusive run must not read as success
	// ("recovery verified") to an operator or CI gate.
	switch {
	case mismatch > 0:
		return fmt.Errorf("%d table(s) diverged from the source", mismatch)
	case errored > 0:
		return fmt.Errorf("%d table(s) could not be verified due to errors", errored)
	case match == 0:
		return fmt.Errorf("no tables were verified (%d inconclusive); nothing proven", inconclusive)
	}
	return nil
}
