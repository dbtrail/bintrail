package views

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// DecimalColumn names one decimal or numeric column of a baseline table, with
// the precision and scale it was declared with. It is the baseline package's
// own type: the command layer reads these out of the Parquet footer and hands
// them straight over, and a second definition here would be one more place for
// the two to drift.
type DecimalColumn = baseline.DecimalColumn

// The baseline writer stores every MySQL DECIMAL and NUMERIC as text, because
// MySQL allows 65 digits of precision and DuckDB stops at 38
// (internal/baseline.MysqlToParquetNode has the full reasoning). That
// choice is right for storage and wrong for the reader: a state view built with
// a bare SELECT * hands DuckDB a VARCHAR, and the first aggregate anyone writes
// against a money column fails with
//
//	Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'
//
// which reads like the data is wrong rather than like a storage choice (#1486).
//
// So the generated view casts those columns back, using the precision and scale
// the column was declared with rather than anything inferred from the stored
// text. The file is where the reader is standing when they hit this, so the
// file is where it gets fixed.

// BaselinePaths lists the Parquet files of the tables this file will describe,
// for a caller about to resolve their schemas.
func (in Input) BaselinePaths() []string {
	paths := make([]string, 0, len(in.Baselines))
	for _, t := range in.Baselines {
		paths = append(paths, t.Path)
	}
	return paths
}

// ApplyDecimals records each table's decimal columns from a map keyed by
// Parquet path, as baseline.DecimalColumnsFor returns it. Pure, so the two
// command layers that resolve the map share one rule for reading it.
//
// A table missing from the map keeps SchemaKnown false and is reported in the
// generated file as a table whose types could not be read. That is the same
// answer a caller gets by skipping this entirely after a failed resolution, so
// the degraded path and the partly-degraded path say the same thing.
func (in *Input) ApplyDecimals(decimals map[string][]DecimalColumn) {
	for i := range in.Baselines {
		decs, ok := decimals[in.Baselines[i].Path]
		if !ok {
			continue
		}
		in.Baselines[i].Decimals = decs
		in.Baselines[i].SchemaKnown = true
	}
}

// writeDecimalNote explains the casts once, above the views, rather than
// repeating it in every one of them. A reader who finds a CAST in their own
// state view should be able to learn here why it is there, instead of assuming
// bintrail decided their money column needed rounding.
func writeDecimalNote(b *strings.Builder, in Input) {
	var cast, uncastable, unknown bool
	for _, t := range in.Baselines {
		if !t.SchemaKnown {
			unknown = true
			continue
		}
		for _, d := range t.Decimals {
			if castableDecimal(d) {
				cast = true
			} else {
				uncastable = true
			}
		}
	}
	if !cast && !uncastable && !unknown {
		return
	}
	b.WriteString("--\n")
	if cast || uncastable {
		b.WriteString("-- DECIMAL and NUMERIC columns are stored as text, so that a value MySQL can\n")
		b.WriteString("-- hold is never rounded to fit a narrower type. The views below cast them\n")
		b.WriteString("-- back to DECIMAL with the precision and scale the column was declared with,\n")
		b.WriteString("-- so sum() and the rest work on them directly.\n")
	}
	if uncastable {
		fmt.Fprintf(b, "-- Columns wider than %d digits have no DuckDB DECIMAL to be cast to, so they\n",
			baseline.MaxDuckDBDecimalPrecision)
		b.WriteString("-- stay text. They are named below. Cast them yourself when you need\n")
		b.WriteString("-- arithmetic; DOUBLE works if an approximate result is acceptable.\n")
	}
	if unknown {
		b.WriteString("-- Some tables' column types could not be read from their Parquet footer, so\n")
		b.WriteString("-- their views cast nothing. Those tables are named below. If one of their\n")
		b.WriteString("-- columns reads as text where you expected a number, cast it yourself.\n")
	}
}

// decimalComments returns the per-view notes: the columns this table could not
// have cast, and why. Silence would be the same bug in miniature, a column that
// reads as text with nothing anywhere saying so.
func decimalComments(t BaselineTable) []string {
	if !t.SchemaKnown {
		return []string{"column types could not be read from the Parquet footer, so nothing is cast"}
	}
	var wide []string
	for _, d := range t.Decimals {
		if !castableDecimal(d) {
			wide = append(wide, fmt.Sprintf("%s is DECIMAL(%d,%d)", d.Name, d.Precision, d.Scale))
		}
	}
	if len(wide) == 0 {
		return nil
	}
	return []string{fmt.Sprintf("%s, wider than DuckDB's %d digits, so it is left as text",
		strings.Join(wide, ", "), baseline.MaxDuckDBDecimalPrecision)}
}

// decimalReplaceClause builds the `* REPLACE (...)` list that re-types this
// table's decimal columns. Empty when there is nothing to cast, in which case
// the caller emits the plain `SELECT *` it always did.
//
// CAST, not TRY_CAST: the values were written out of a column of exactly this
// precision, so a value that will not fit means the file and the schema
// disagree. TRY_CAST would turn that into NULLs nobody would notice.
func decimalReplaceClause(t BaselineTable) string {
	if !t.SchemaKnown {
		return ""
	}
	var parts []string
	for _, d := range t.Decimals {
		if !castableDecimal(d) {
			continue
		}
		parts = append(parts, fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d)) AS %s",
			quoteIdent(d.Name), d.Precision, d.Scale, quoteIdent(d.Name)))
	}
	return strings.Join(parts, ", ")
}

// castableDecimal reports whether DuckDB has a DECIMAL that can hold this
// column. MySQL allows 65 digits and DuckDB stops at 38, and a scale wider than
// the precision is not a DuckDB type either. Both stay text rather than being
// cast to something that would drop digits.
func castableDecimal(d DecimalColumn) bool {
	return d.Precision > 0 &&
		d.Precision <= baseline.MaxDuckDBDecimalPrecision &&
		d.Scale >= 0 && d.Scale <= d.Precision
}
