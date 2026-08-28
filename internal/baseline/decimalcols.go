package baseline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for the footer read below

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// MaxDuckDBDecimalPrecision is the widest DECIMAL DuckDB can represent. MySQL
// allows up to 65 digits, so a column past this ceiling has no DuckDB DECIMAL
// to be cast to and stays text. Measured against the linked engine, not assumed:
// DECIMAL(39,2) is refused with "Width must be between 1 and 38".
const MaxDuckDBDecimalPrecision = 38

// DecimalColumnsFor reports the decimal and numeric columns of each baseline
// Parquet file, read from the CREATE TABLE the writer embedded in the file's
// footer. The result is keyed by the path as it was passed in.
//
// PRESENCE in the map means the file's schema was read, and is deliberately
// distinct from the value being empty. A table with no decimal column maps to
// an empty slice; a file whose footer carries no CREATE TABLE at all (a
// baseline written before that key existed) is ABSENT. Collapsing the two would
// let a caller report "this table has no decimal columns" about a table it
// never managed to look at.
//
// One DuckDB session reads every footer in a single parquet_kv_metadata() call.
// That is the reason this does not loop over ReadParquetMetadataAny: on S3 that
// helper validates the whole object against its snapshot manifest and opens its
// own session per file, so a per-table loop would download an entire baseline
// to learn its column types. parquet_kv_metadata reads footers only.
//
// Callers are expected to treat a failure as "no type information available"
// and carry on — knowing a column's precision is an improvement to the output,
// never a precondition for producing it. The error is returned rather than
// swallowed so the caller can say so in its own voice.
func DecimalColumnsFor(ctx context.Context, paths []string) (map[string][]DecimalColumn, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if anyS3(paths) {
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return nil, fmt.Errorf("load httpfs extension: %w", err)
		}
		if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
			return nil, err
		}
	}

	// file_name comes back as the string DuckDB was handed, which is what lets
	// the result be keyed by the caller's own path. A path DuckDB reports
	// differently would simply not be found by the caller and lose its casts,
	// which is the same output this function existed to improve on.
	q := "SELECT file_name, value FROM parquet_kv_metadata(" + fileListLiteral(paths) + ") WHERE key = " +
		sqlQuoteLiteral(MetaKeyCreateTableSQL)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("read baseline schemas from Parquet footers: %w", err)
	}
	defer rows.Close()

	out := make(map[string][]DecimalColumn)
	for rows.Next() {
		var file string
		// parquet_kv_metadata types both key and value as BLOB.
		var createSQL []byte
		if err := rows.Scan(&file, &createSQL); err != nil {
			return nil, fmt.Errorf("scan Parquet footer metadata: %w", err)
		}
		cols, err := ParseSchemaText(string(createSQL))
		if err != nil {
			// One unparseable CREATE TABLE costs that table its casts, not the
			// whole run: the other tables' schemas are still good, and the
			// caller reports an absent table as "type unknown".
			continue
		}
		decs := DecimalColumns(cols)
		if decs == nil {
			// Non-nil so the entry is unambiguously "schema read, no decimal
			// columns" rather than a nil that reads like an absent key.
			decs = []DecimalColumn{}
		}
		out[file] = decs
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read Parquet footer metadata: %w", err)
	}
	return out, nil
}

// DecimalColumns picks the decimal and numeric columns out of a parsed schema.
func DecimalColumns(cols []Column) []DecimalColumn {
	var out []DecimalColumn
	for _, c := range cols {
		if c.MySQLType != "decimal" && c.MySQLType != "numeric" {
			continue
		}
		out = append(out, DecimalColumn{
			Name:      c.Name,
			Precision: c.DecimalPrecision,
			Scale:     c.DecimalScale,
		})
	}
	return out
}

func anyS3(paths []string) bool {
	for _, p := range paths {
		if strings.HasPrefix(p, "s3://") {
			return true
		}
	}
	return false
}

// fileListLiteral renders paths as a DuckDB list literal ['a', 'b'].
func fileListLiteral(paths []string) string {
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = sqlQuoteLiteral(p)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func sqlQuoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
