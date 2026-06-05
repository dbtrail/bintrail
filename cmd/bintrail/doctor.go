package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/config"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Diagnose source MySQL prerequisites and emit copy-pasteable remediation",
	Long: `Runs preflight checks against the source MySQL server and (optionally)
the index database. Reports each check as PASS, FAIL, WARN, or SKIP with
copy-pasteable remediation SQL when a check fails.

Run this before 'bintrail up', 'bintrail stream', 'bintrail index', or
'bintrail agent' to catch the common onboarding gotchas (wrong binlog_format,
missing GRANTs, missing log_bin) before they cost you a debugging cycle.

Exit code is 0 only when every required check passes. Warnings do not affect
the exit code so 'doctor' is safe to run in CI as a smoke test.

Examples:

  bintrail doctor --source-dsn "user:pass@tcp(source:3306)/"
  bintrail doctor --source-dsn "$SRC" --index-dsn "$IDX" --schemas mydb`,
	RunE: runDoctor,
}

var (
	docSourceDSN string
	docIndexDSN  string
	docSchemas   string
	docFormat    string
)

func init() {
	doctorCmd.Flags().StringVar(&docSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	doctorCmd.Flags().StringVar(&docIndexDSN, "index-dsn", "", "DSN for the index MySQL database (optional; verifies write access when provided)")
	doctorCmd.Flags().StringVar(&docSchemas, "schemas", "", "Comma-separated schemas to check (default: all user schemas)")
	doctorCmd.Flags().StringVar(&docFormat, "format", "text", "Output format: text or json")
	_ = doctorCmd.MarkFlagRequired("source-dsn")
	bindCommandEnv(doctorCmd)
	rootCmd.AddCommand(doctorCmd)
}

// checkStatus is the outcome of a single preflight check. Constrained to the
// four constants below; unknown values are rejected by (*doctorReport).add.
type checkStatus string

const (
	statusPass checkStatus = "pass"
	statusFail checkStatus = "fail"
	statusWarn checkStatus = "warn"
	statusSkip checkStatus = "skip"
)

type checkResult struct {
	Name string      `json:"name"`
	Status      checkStatus `json:"status"`
	// Detail and Remediation are typically empty for statusPass/statusSkip and
	// populated for statusFail/statusWarn.
	Detail      string `json:"detail,omitempty"`
	Remediation string `json:"remediation,omitempty"`
}

type doctorReport struct {
	Checks   []checkResult `json:"checks"`
	Passed   int           `json:"passed"`
	Failed   int           `json:"failed"`
	Warnings int           `json:"warnings"`
	Skipped  int           `json:"skipped"`
}

func runDoctor(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(docFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", docFormat)
	}
	return runDoctorTo(cmd.Context(), os.Stdout, docFormat, docSourceDSN, docIndexDSN, docSchemas)
}

// binlogRetentionMinSeconds is the minimum binlog retention bintrail asks for
// (docs/streaming.md:279). Hoisted so the check function and any test referring
// to the threshold share a single source of truth.
const binlogRetentionMinSeconds = 172800

// queryErrorRemediation returns the generic remediation block for "the SELECT
// itself failed" FAIL paths. Connection drops outnumber permission gaps in
// practice, so retry is bullet 1 and grants are bullet 2.
func queryErrorRemediation(query string) string {
	return "The check could not query " + query + ". Common causes (most likely first):\n" +
		"  - Connection dropped or timed out: retry once before investigating further\n" +
		"  - User lacks required SELECT privilege on the relevant system table or variable\n" +
		"  - Server overloaded — raise the per-check timeout or check server load"
}

// isUnknownDatabaseErr reports whether err (possibly wrapped) is MySQL error
// 1049 (ER_BAD_DB, "Unknown database ..."): the DSN names a database that
// doesn't exist yet. `bintrail init` (and therefore `up`) creates the index
// database itself, so the index checks must treat 1049 as "probe the server
// instead", not as a hard failure — the remediation text already promised as
// much before the behavior matched it (#384).
func isUnknownDatabaseErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1049
}

// connectWithoutDB reconnects to the DSN's server with the database name
// stripped (the same DBName="" + FormatDSN pattern as init.go's
// ensureDatabase, but routed through config.Connect to keep parseTime and
// the connect timeout), for checks that must run before the index database
// exists.
func connectWithoutDB(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.DBName = ""
	return config.Connect(cfg.FormatDSN())
}

// runDoctorTo is the testable core of the doctor command. It runs every check
// against sourceDSN (and optionally indexDSN), renders the report to w using
// format ("text" or "json"), and returns a non-nil error iff any required
// check failed. Callers wanting to route output (e.g. `bintrail up` sending
// preflight output to stderr to keep stdout clean for streaming) pass their
// own writer here instead of going through the cobra entry point.
func runDoctorTo(parent context.Context, w io.Writer, format, sourceDSN, indexDSN, schemasCSV string) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()

	report := &doctorReport{}
	schemas := parseSchemaList(schemasCSV)

	// ── Source MySQL checks ──────────────────────────────────────────────────
	sourceDB, err := config.Connect(sourceDSN)
	if err != nil {
		report.add(checkResult{
			Name:   "Source MySQL connection",
			Status: statusFail,
			Detail: err.Error(),
			Remediation: "Verify --source-dsn is reachable: try `mysql -h<host> -P<port> -u<user> -p<pass>`.\n" +
				"For RDS/Aurora: ensure the security group allows ingress from bintrail's IP on port 3306.",
		})
		_ = report.Write(w, format)
		return report.Err()
	}
	defer sourceDB.Close()

	report.add(checkSourceConnection(ctx, sourceDB))
	report.add(checkLogBin(sourceDB))
	report.add(checkBinlogFormat(sourceDB))
	report.add(checkBinlogRowImage(sourceDB))
	report.add(checkBinlogRetention(sourceDB))
	report.add(checkReplicationGrants(ctx, sourceDB))
	report.add(checkFKCascades(sourceDB, schemas))
	report.add(checkSchemaVisibility(ctx, sourceDB, schemas))

	// ── Index MySQL checks (optional) ─────────────────────────────────────────
	if indexDSN != "" {
		indexCfg, parseErr := mysql.ParseDSN(indexDSN)
		if parseErr != nil {
			report.add(checkResult{
				Name:        "Index DSN parse",
				Status:      statusFail,
				Detail:      parseErr.Error(),
				Remediation: `Expected DSN format: user:pass@tcp(host:port)/binlog_index`,
			})
		} else if indexCfg.DBName == "" {
			report.add(checkResult{
				Name:        "Index DSN database name",
				Status:      statusFail,
				Detail:      "DSN does not include a database name",
				Remediation: "Add a database name to the DSN, e.g. user:pass@tcp(host:3306)/binlog_index",
			})
		} else {
			report.add(checkIndexConnection(ctx, indexDSN, indexCfg.DBName))
			report.add(checkIndexWriteAccess(ctx, indexDSN, indexCfg.DBName))
		}
	} else {
		report.add(checkResult{
			Name:   "Index database",
			Status: statusSkip,
			Detail: "--index-dsn not provided",
		})
	}

	if err := report.Write(w, format); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return report.Err()
}

func checkSourceConnection(ctx context.Context, db *sql.DB) checkResult {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return checkResult{
			Name:   "Source MySQL connection",
			Status: statusFail,
			Detail: err.Error(),
			Remediation: "The connection opened but SELECT VERSION() failed. Common causes:\n" +
				"  - Permission denied: ensure the user has at least SELECT on *.*\n" +
				"  - Transient network issue: retry once before investigating further\n" +
				"  - Server restarted mid-handshake: wait and retry",
		}
	}
	return checkResult{
		Name:   "Source MySQL connection",
		Status: statusPass,
		Detail: "MySQL " + version,
	}
}

func checkLogBin(db *sql.DB) checkResult {
	var val string
	err := db.QueryRow("SELECT @@log_bin").Scan(&val)
	if err != nil {
		return checkResult{
			Name:        "log_bin enabled",
			Status:      statusFail,
			Detail:      err.Error(),
			Remediation: queryErrorRemediation("@@log_bin"),
		}
	}
	if val != "1" && !strings.EqualFold(val, "ON") {
		return checkResult{
			Name:   "log_bin enabled",
			Status: statusFail,
			Detail: fmt.Sprintf("log_bin=%q (binary logging is OFF)", val),
			Remediation: "Binary logging is disabled. Set in my.cnf and restart MySQL:\n\n" +
				"  [mysqld]\n" +
				"  log_bin = mysql-bin\n" +
				"  server_id = 1\n\n" +
				"Then restart MySQL. log_bin cannot be enabled at runtime.",
		}
	}
	return checkResult{Name: "log_bin enabled", Status: statusPass, Detail: "ON"}
}

func checkBinlogFormat(db *sql.DB) checkResult {
	err := validateBinlogFormat(db)
	if err != nil {
		return checkResult{
			Name:   "binlog_format=ROW",
			Status: statusFail,
			Detail: err.Error(),
			Remediation: "Set on the source MySQL (MySQL 8.0+ — survives restart without editing my.cnf):\n\n" +
				"  SET PERSIST binlog_format = 'ROW';\n\n" +
				"On MySQL 5.7 use SET GLOBAL and also add to my.cnf:\n\n" +
				"  [mysqld]\n" +
				"  binlog_format = ROW",
		}
	}
	return checkResult{Name: "binlog_format=ROW", Status: statusPass, Detail: "ROW"}
}

func checkBinlogRowImage(db *sql.DB) checkResult {
	err := validateBinlogRowImage(db)
	if err != nil {
		return checkResult{
			Name:   "binlog_row_image=FULL",
			Status: statusFail,
			Detail: err.Error(),
			Remediation: "Set on the source MySQL (MySQL 8.0+ — survives restart):\n\n" +
				"  SET PERSIST binlog_row_image = 'FULL';\n\n" +
				"On MySQL 5.7 use SET GLOBAL and also add to my.cnf:\n\n" +
				"  [mysqld]\n" +
				"  binlog_row_image = FULL\n\n" +
				"Without FULL, UPDATE/DELETE binlog events omit unchanged columns, " +
				"so bintrail cannot reconstruct full before/after images for recovery.",
		}
	}
	return checkResult{Name: "binlog_row_image=FULL", Status: statusPass, Detail: "FULL"}
}

// checkBinlogRetention emits a WARN (not FAIL) when binlog retention is below
// the 2-day recommendation in docs/streaming.md — short windows can leave
// bintrail unable to fill gaps after a restart.
func checkBinlogRetention(db *sql.DB) checkResult {
	var raw string
	// binlog_expire_logs_seconds is MySQL 8.0+; older servers use expire_logs_days.
	err := db.QueryRow("SELECT @@binlog_expire_logs_seconds").Scan(&raw)
	if err != nil {
		// Try the legacy variable.
		var days string
		if dErr := db.QueryRow("SELECT @@expire_logs_days").Scan(&days); dErr == nil {
			d, parseErr := strconv.Atoi(days)
			if parseErr != nil {
				return checkResult{
					Name:   "Binlog retention >= 2 days",
					Status: statusWarn,
					Detail: fmt.Sprintf("could not parse expire_logs_days=%q: %v", days, parseErr),
				}
			}
			if d < 2 {
				return checkResult{
					Name:   "Binlog retention >= 2 days",
					Status: statusWarn,
					Detail: fmt.Sprintf("expire_logs_days=%d (legacy variable)", d),
					Remediation: "Set retention to at least 2 days so bintrail can fill gaps after a restart:\n\n" +
						"  SET PERSIST expire_logs_days = 2;",
				}
			}
			return checkResult{Name: "Binlog retention >= 2 days", Status: statusPass, Detail: days + " days"}
		}
		return checkResult{
			Name:   "Binlog retention >= 2 days",
			Status: statusWarn,
			Detail: "could not read binlog retention setting: " + err.Error(),
		}
	}
	seconds, parseErr := strconv.Atoi(raw)
	if parseErr != nil {
		return checkResult{
			Name:   "Binlog retention >= 2 days",
			Status: statusWarn,
			Detail: fmt.Sprintf("could not parse binlog_expire_logs_seconds=%q: %v", raw, parseErr),
		}
	}
	if seconds == 0 {
		// 0 = never expire; harmless for bintrail (retention is effectively infinite).
		return checkResult{
			Name:   "Binlog retention >= 2 days",
			Status: statusWarn,
			Detail: "binlog_expire_logs_seconds=0 (no automatic expiration)",
		}
	}
	if seconds < binlogRetentionMinSeconds {
		return checkResult{
			Name:   "Binlog retention >= 2 days",
			Status: statusWarn,
			Detail: fmt.Sprintf("binlog_expire_logs_seconds=%d (%dh)", seconds, seconds/3600),
			Remediation: fmt.Sprintf("Set retention to at least 2 days (%ds) so bintrail can fill gaps after a restart:\n\n"+
				"  SET PERSIST binlog_expire_logs_seconds = %d;", binlogRetentionMinSeconds, binlogRetentionMinSeconds),
		}
	}
	return checkResult{
		Name:   "Binlog retention >= 2 days",
		Status: statusPass,
		Detail: fmt.Sprintf("%dh", seconds/3600),
	}
}

func checkReplicationGrants(ctx context.Context, db *sql.DB) checkResult {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return checkResult{
			Name:        "REPLICATION SLAVE + CLIENT grants",
			Status:      statusFail,
			Detail:      err.Error(),
			Remediation: queryErrorRemediation("SHOW GRANTS"),
		}
	}
	defer rows.Close()

	var grants []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return checkResult{
				Name:        "REPLICATION SLAVE + CLIENT grants",
				Status:      statusFail,
				Detail:      err.Error(),
				Remediation: queryErrorRemediation("SHOW GRANTS"),
			}
		}
		grants = append(grants, g)
	}

	slave, client := hasReplPrivileges(grants)
	if slave && client {
		return checkResult{
			Name:   "REPLICATION SLAVE + CLIENT grants",
			Status: statusPass,
			Detail: "REPLICATION SLAVE, REPLICATION CLIENT",
		}
	}

	var missing []string
	if !slave {
		missing = append(missing, "REPLICATION SLAVE")
	}
	if !client {
		missing = append(missing, "REPLICATION CLIENT")
	}

	// Try to extract the user from the first grant line to make remediation concrete.
	user := "<bintrail-user>"
	if len(grants) > 0 {
		if u := extractGrantUser(grants[0]); u != "" {
			user = u
		}
	}

	return checkResult{
		Name:   "REPLICATION SLAVE + CLIENT grants",
		Status: statusFail,
		Detail: "missing: " + strings.Join(missing, ", "),
		Remediation: fmt.Sprintf("Run on the source MySQL as a privileged user (e.g. root):\n\n"+
			"  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO %s;\n"+
			"  FLUSH PRIVILEGES;\n\n"+
			"REPLICATION SLAVE lets bintrail stream binlog events.\n"+
			"REPLICATION CLIENT lets it run SHOW BINARY LOGS / SHOW MASTER STATUS for gap detection.", user),
	}
}

// extractGrantUser pulls the "user@host" out of a SHOW GRANTS line like
// `GRANT USAGE ON *.* TO 'bintrail'@'%'` so we can render the GRANT remediation
// with the actual user instead of a placeholder. Returns empty when the format
// is unfamiliar — the caller falls back to a placeholder.
func extractGrantUser(grant string) string {
	upper := strings.ToUpper(grant)
	i := strings.Index(upper, " TO ")
	if i < 0 {
		return ""
	}
	rest := strings.TrimSpace(grant[i+4:])
	// Strip trailing clauses like " IDENTIFIED BY ..." (older MySQL).
	if j := strings.IndexAny(rest, " "); j > 0 {
		// Keep only the user@host token, unless it contains spaces inside quotes.
		// Safe heuristic: if the first char is a quote, take until matching close + @host.
		if rest[0] == '\'' || rest[0] == '`' {
			// Find the next quote after position 1, then the @, then take until end-of-token.
			closing := rest[0]
			k := strings.IndexByte(rest[1:], closing)
			if k > 0 {
				end := 1 + k + 1
				// Append @host segment if present.
				if end < len(rest) && rest[end] == '@' {
					rest2 := rest[end+1:]
					if len(rest2) > 0 && (rest2[0] == '\'' || rest2[0] == '`') {
						closing2 := rest2[0]
						m := strings.IndexByte(rest2[1:], closing2)
						if m > 0 {
							return rest[:end+1+m+2]
						}
					}
				}
				return rest[:end]
			}
		}
		return rest[:j]
	}
	return rest
}

func checkFKCascades(db *sql.DB, schemas []string) checkResult {
	err := validateNoFKCascades(db, schemas)
	if err == nil {
		return checkResult{Name: "No FK CASCADE constraints", Status: statusPass}
	}
	return checkResult{
		Name:   "No FK CASCADE constraints",
		Status: statusWarn,
		Detail: err.Error(),
		Remediation: "Foreign keys with ON DELETE CASCADE or ON UPDATE CASCADE produce side-effect row changes\n" +
			"that bintrail's reversal SQL cannot reliably undo. Two options:\n\n" +
			"  1. Drop or change the cascade rules:\n" +
			"     ALTER TABLE <child> DROP FOREIGN KEY <fk_name>;\n" +
			"     ALTER TABLE <child> ADD CONSTRAINT <fk_name> FOREIGN KEY (...) REFERENCES <parent>(...)\n" +
			"         ON DELETE RESTRICT ON UPDATE RESTRICT;\n\n" +
			"  2. Accept that reversal across cascades requires manual review.\n\n" +
			"This is a WARN, not a hard fail — bintrail will index events fine, but `recover` may\n" +
			"produce incomplete SQL for tables involved in cascades.",
	}
}

// checkSchemaVisibility queries information_schema to ensure bintrail can see
// at least one table in the target schemas. A pass here means the snapshot
// step will succeed.
func checkSchemaVisibility(ctx context.Context, db *sql.DB, schemas []string) checkResult {
	var query string
	var args []any
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`SELECT COUNT(*), COUNT(DISTINCT TABLE_SCHEMA)
			FROM information_schema.TABLES
			WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN (%s)`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	} else {
		query = `SELECT COUNT(*), COUNT(DISTINCT TABLE_SCHEMA)
			FROM information_schema.TABLES
			WHERE TABLE_TYPE = 'BASE TABLE'
			  AND TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')`
	}

	var tableCount, schemaCount int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&tableCount, &schemaCount); err != nil {
		return checkResult{
			Name:        "Schema visibility",
			Status:      statusFail,
			Detail:      err.Error(),
			Remediation: queryErrorRemediation("information_schema.TABLES"),
		}
	}

	if tableCount == 0 {
		filter := "user schemas"
		if len(schemas) > 0 {
			filter = strings.Join(schemas, ", ")
		}
		return checkResult{
			Name:   "Schema visibility",
			Status: statusFail,
			Detail: "no tables visible in " + filter,
			Remediation: "Bintrail needs at least SELECT on information_schema to read column metadata.\n" +
				"Grant minimum read access:\n\n" +
				"  GRANT SELECT ON *.* TO <bintrail-user>;\n\n" +
				"Or scope to the schemas you want indexed:\n\n" +
				"  GRANT SELECT ON <schema>.* TO <bintrail-user>;",
		}
	}
	return checkResult{
		Name:   "Schema visibility",
		Status: statusPass,
		Detail: fmt.Sprintf("%d tables across %d schemas", tableCount, schemaCount),
	}
}

func checkIndexConnection(ctx context.Context, dsn, dbName string) checkResult {
	db, err := config.Connect(dsn)
	if err != nil {
		// Error 1049 means only the DATABASE is absent — init creates it,
		// so verify the SERVER is reachable instead of failing (#384).
		if isUnknownDatabaseErr(err) {
			serverDB, serverErr := connectWithoutDB(dsn)
			if serverErr == nil {
				defer serverDB.Close()
				var version string
				vErr := serverDB.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
				if vErr == nil {
					return checkResult{
						Name:   "Index MySQL connection",
						Status: statusPass,
						Detail: fmt.Sprintf("MySQL %s, database=%s (does not exist yet — `bintrail init` will create it)", version, dbName),
					}
				}
				serverErr = vErr
			}
			// The database is absent AND the server-level probe failed too.
			// Surface BOTH errors: 1049 alone would mislead — its remediation
			// says "init will create it" — when the real, newer problem is
			// e.g. max_user_connections, a timeout, or the server going away
			// between the two connects. A diagnostic must not swallow the
			// diagnostic.
			err = fmt.Errorf("%w (server-level probe also failed: %v)", err, serverErr)
		}
		return checkResult{
			Name:   "Index MySQL connection",
			Status: statusFail,
			Detail: err.Error(),
			Remediation: "Verify --index-dsn is reachable. The database does not need to exist yet — " +
				"`bintrail init` will create it. But the user needs CREATE DATABASE if so.",
		}
	}
	defer db.Close()
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return checkResult{
			Name:        "Index MySQL connection",
			Status:      statusFail,
			Detail:      err.Error(),
			Remediation: queryErrorRemediation("VERSION()"),
		}
	}
	return checkResult{
		Name:   "Index MySQL connection",
		Status: statusPass,
		Detail: fmt.Sprintf("MySQL %s, database=%s", version, dbName),
	}
}

// checkIndexWriteAccess verifies the user can CREATE TABLE in the target index
// database. Opens its own connection; delegates to checkIndexWriteAccessOn for
// the actual probing so the IO and the logic can be tested independently.
func checkIndexWriteAccess(ctx context.Context, dsn, dbName string) checkResult {
	db, err := config.Connect(dsn)
	if err != nil {
		// Error 1049: the database is absent. Probe at the server level —
		// checkIndexWriteAccessOn's SCHEMATA lookup then exercises the
		// CREATE DATABASE privilege path this check exists to verify (#384).
		if isUnknownDatabaseErr(err) {
			serverDB, serverErr := connectWithoutDB(dsn)
			if serverErr == nil {
				defer serverDB.Close()
				return checkIndexWriteAccessOn(ctx, serverDB, dbName)
			}
			// Surface both errors — see checkIndexConnection for why 1049
			// alone would mislead here.
			err = fmt.Errorf("%w (server-level probe also failed: %v)", err, serverErr)
		}
		return checkResult{
			Name:   "Index write access",
			Status: statusFail,
			Detail: err.Error(),
			Remediation: "Could not connect to --index-dsn. Verify the host/port/user are correct " +
				"and that the user has connect privileges. The database itself does not need to " +
				"exist yet — `bintrail init` (or `bintrail up`) will create it given CREATE DATABASE.",
		}
	}
	defer db.Close()
	return checkIndexWriteAccessOn(ctx, db, dbName)
}

// checkIndexWriteAccessOn runs the actual SCHEMATA/CREATE/DROP probe sequence
// against an already-open *sql.DB.
func checkIndexWriteAccessOn(ctx context.Context, db *sql.DB, dbName string) checkResult {
	// First ensure the database exists. If it doesn't, we need CREATE DATABASE privilege.
	createdByProbe := false
	var dbExists string
	dbErr := db.QueryRowContext(ctx,
		"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
		dbName).Scan(&dbExists)
	if errors.Is(dbErr, sql.ErrNoRows) {
		// Attempt CREATE DATABASE to see if we have the privilege.
		_, createErr := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
		if createErr != nil {
			return checkResult{
				Name:   "Index write access",
				Status: statusFail,
				Detail: fmt.Sprintf("database %q does not exist and user cannot CREATE DATABASE: %v", dbName, createErr),
				Remediation: fmt.Sprintf("Either create the database manually as a privileged user:\n\n"+
					"  CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n\n"+
					"Or grant CREATE on it to the bintrail user:\n\n"+
					"  GRANT ALL PRIVILEGES ON `%s`.* TO <bintrail-user>;", dbName, dbName),
			}
		}
		createdByProbe = true
		// A diagnostic must not leave server state behind: best-effort drop
		// of the probe-created database on exit (#384). A drop failure is
		// logged, not surfaced — it can only happen when the user also
		// lacks DROP, which the table probe below already reports as FAIL —
		// and init re-creates the database for real (CREATE DATABASE IF NOT
		// EXISTS), so a leftover costs nothing.
		defer func() {
			if _, dropErr := db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName)); dropErr != nil {
				slog.Warn("doctor: could not drop probe-created database", "database", dbName, "error", dropErr)
			}
		}()
	} else if dbErr != nil {
		return checkResult{
			Name:   "Index write access",
			Status: statusFail,
			Detail: dbErr.Error(),
			Remediation: queryErrorRemediation("information_schema.SCHEMATA"),
		}
	}

	// Test CREATE TABLE / DROP TABLE in the target database.
	probeTable := fmt.Sprintf("`%s`.`_bintrail_doctor_probe`", dbName)
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+probeTable+" (id INT)"); err != nil {
		return checkResult{
			Name:   "Index write access",
			Status: statusFail,
			Detail: "cannot CREATE TABLE: " + err.Error(),
			Remediation: fmt.Sprintf("Grant table-creation privileges on the index database:\n\n"+
				"  GRANT ALL PRIVILEGES ON `%s`.* TO <bintrail-user>;\n"+
				"  FLUSH PRIVILEGES;", dbName),
		}
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE "+probeTable); err != nil {
		// FAIL, not WARN: bintrail's rotate/partition-management code needs DROP
		// (drop p_future, drop old partitions). A user with CREATE but no DROP
		// passes here and then fails at runtime during `rotate` — worse than
		// failing now.
		return checkResult{
			Name:   "Index write access",
			Status: statusFail,
			Detail: "user has CREATE but not DROP TABLE: " + err.Error(),
			Remediation: fmt.Sprintf("Grant DROP — bintrail rotates partitions and needs it at runtime:\n\n"+
				"  GRANT ALL PRIVILEGES ON `%s`.* TO <bintrail-user>;\n"+
				"  FLUSH PRIVILEGES;\n\n"+
				"Also clean up the probe table left behind:\n\n"+
				"  DROP TABLE %s;", dbName, probeTable),
		}
	}
	// Deliberately no "(probe database dropped)" claim here: the deferred
	// drop runs AFTER this return value is built, so the Detail cannot
	// honestly assert the drop happened.
	detail := "CREATE/DROP TABLE OK"
	if createdByProbe {
		detail = fmt.Sprintf("database %q does not exist yet — CREATE DATABASE privilege verified; CREATE/DROP TABLE OK", dbName)
	}
	return checkResult{
		Name:   "Index write access",
		Status: statusPass,
		Detail: detail,
	}
}

// ─── Report tabulation and output ────────────────────────────────────────────

func (r *doctorReport) add(c checkResult) {
	r.Checks = append(r.Checks, c)
	switch c.Status {
	case statusPass:
		r.Passed++
	case statusFail:
		r.Failed++
	case statusWarn:
		r.Warnings++
	case statusSkip:
		r.Skipped++
	default:
		// Unknown status — log so a regression in a caller is visible instead of
		// silently miscounting. We still append to Checks so the JSON output is
		// complete and operators can see the malformed entry.
		slog.Warn("doctor: check produced unknown status", "name", c.Name, "status", c.Status)
	}
}

// Write renders the report to w. format is "text" (default) or "json".
func (r *doctorReport) Write(w io.Writer, format string) error {
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}

	for _, c := range r.Checks {
		var mark string
		switch c.Status {
		case statusPass:
			mark = "✓"
		case statusFail:
			mark = "✗"
		case statusWarn:
			mark = "!"
		case statusSkip:
			mark = "-"
		default:
			mark = "?"
		}
		if c.Detail != "" {
			fmt.Fprintf(w, "%s %s (%s)\n", mark, c.Name, c.Detail)
		} else {
			fmt.Fprintf(w, "%s %s\n", mark, c.Name)
		}
		if c.Remediation != "" {
			for _, line := range strings.Split(c.Remediation, "\n") {
				fmt.Fprintf(w, "    %s\n", line)
			}
		}
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Passed: %d  Failed: %d  Warnings: %d  Skipped: %d\n",
		r.Passed, r.Failed, r.Warnings, r.Skipped)
	if r.Failed == 0 {
		fmt.Fprintln(w, "\nReady to stream. Run `bintrail up --source-dsn ... --index-dsn ...` to start.")
	} else {
		fmt.Fprintln(w, "\nFix the failures above, then re-run `bintrail doctor` to verify.")
	}
	return nil
}

// Err returns a non-nil error when any required check failed, so the CLI exits
// non-zero for CI/scripting use cases. Warnings do not cause failure.
func (r *doctorReport) Err() error {
	if r.Failed > 0 {
		return fmt.Errorf("%d preflight check(s) failed", r.Failed)
	}
	return nil
}
