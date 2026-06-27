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
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify that a recovery would reproduce the live source",
	Long: `Reconstruct each table's current state from its baseline + indexed binlog
events, fingerprint it, and compare to a consistent snapshot of the live source.
A match proves a recovery would reproduce the source byte-for-byte; a mismatch
flags a real divergence between the recovery chain and the source.

The source fingerprint reads the whole table at a consistent snapshot, so run
this off-peak. Results are per table: match, mismatch, or inconclusive (index
behind the snapshot, no baseline, unsupported PK, coverage gap, or a value class
this version can't yet compare — never reported as a failure).

Examples:
  # All tables in the latest schema snapshot
  bintrail verify --source-dsn "..." --index-dsn "..." \
    --baseline-dir /data/baselines

  # Specific tables, S3 baselines
  bintrail verify --source-dsn "..." --index-dsn "..." \
    --baseline-s3 s3://bucket/baselines --tables mydb.orders,mydb.users`,
	RunE: runVerify,
}

func init() {
	verifyCmd.Flags().StringVar(&vfySourceDSN, "source-dsn", "", "DSN for the live source MySQL database (required)")
	verifyCmd.Flags().StringVar(&vfyIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	verifyCmd.Flags().StringVar(&vfyBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots")
	verifyCmd.Flags().StringVar(&vfyBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline Parquet snapshots (e.g. s3://bucket/baselines/)")
	verifyCmd.Flags().StringVar(&vfyTables, "tables", "", "Comma-separated schema.table list (default: all tables in the latest schema snapshot)")
	verifyCmd.Flags().BoolVar(&vfyNoArchive, "no-archive", false, "Query live MySQL partitions only; skip Parquet archive discovery")
	AddDuckDBTuningFlags(verifyCmd)
}

func runVerify(cmd *cobra.Command, _ []string) error {
	if vfySourceDSN == "" {
		return fmt.Errorf("--source-dsn is required")
	}
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

	sourceDB, err := config.Connect(vfySourceDSN)
	if err != nil {
		return fmt.Errorf("connect to source database: %w", err)
	}
	defer sourceDB.Close()

	indexDB, err := config.Connect(vfyIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index database: %w", err)
	}
	defer indexDB.Close()

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
