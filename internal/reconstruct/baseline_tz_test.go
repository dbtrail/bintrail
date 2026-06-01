package reconstruct

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/dbtrail/bintrail/internal/baseline"
)

// TestReadBaselineRow_TemporalPKUnderNonUTCHost is the discriminating test for
// #359: a DATETIME/TIMESTAMP/DATE PK lookup through ReadBaselineRow must hit
// the baseline row regardless of the host OS timezone.
//
// DuckDB caches its session TimeZone from the process TZ env at the first
// connection open (ICU default), process-wide — so a t.Setenv("TZ", …) inside
// a test that runs after some other test already opened DuckDB has no effect.
// The only order-independent way to force ReadBaselineRow's connection default
// non-UTC is to re-exec this test in a fresh child process with TZ set to a
// non-UTC zone. Under that default, without the SET TimeZone='UTC' pin in
// ReadBaselineRow the string→TIMESTAMPTZ cast resolves '2020-01-01 00:00:00'
// to a different UTC instant than the stored (UTC-anchored) micros, the row
// silently misses, the child t.Fatalf's, and the parent sees a non-zero exit.
// With the pin, every temporal PK matches and the child exits 0.
//
// Verified to fail without the pin and pass with it (the whole point of the
// re-exec: a green run under UTC CI would otherwise prove nothing).
func TestReadBaselineRow_TemporalPKUnderNonUTCHost(t *testing.T) {
	if os.Getenv("BINTRAIL_TZ_CHILD") == "1" {
		runTemporalPKBaselineChild(t)
		return
	}
	// Parent: re-exec just this test in a child process pinned to a non-UTC
	// host timezone so the child's DuckDB session default is America/Los_Angeles.
	cmd := exec.Command(os.Args[0], "-test.run=^TestReadBaselineRow_TemporalPKUnderNonUTCHost$", "-test.v")
	cmd.Env = append(os.Environ(), "BINTRAIL_TZ_CHILD=1", "TZ=America/Los_Angeles")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("temporal-PK baseline lookup failed under TZ=America/Los_Angeles "+
			"(missing SET TimeZone='UTC' pin in ReadBaselineRow?):\n%s", out)
	}
}

// runTemporalPKBaselineChild runs in the re-exec'd child process. It writes a
// one-row baseline Parquet for each temporal PK type and asserts ReadBaselineRow
// finds the row by binding the PK as a string parameter — the exact path the
// shim single-row _snapshot and `bintrail reconstruct --pk` both use.
func runTemporalPKBaselineChild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cases := []struct {
		name      string
		mysqlType string
		pkVal     string
	}{
		{"datetime", "datetime", "2020-01-01 00:00:00"},
		{"timestamp", "timestamp", "2020-06-15 12:34:56"},
		{"date", "date", "2020-01-01"},
	}
	for _, tc := range cases {
		cols := []baseline.Column{
			{Name: "k", MySQLType: tc.mysqlType, ParquetType: baseline.MysqlToParquetNode(tc.mysqlType)},
			{Name: "v", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		}
		path := filepath.Join(dir, tc.name+".parquet")
		w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
		if err != nil {
			t.Fatalf("%s NewWriter: %v", tc.name, err)
		}
		if err := w.WriteRow([]string{tc.pkVal, "found"}, []bool{false, false}); err != nil {
			t.Fatalf("%s WriteRow: %v", tc.name, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("%s Close: %v", tc.name, err)
		}

		row, err := ReadBaselineRow(ctx, path, map[string]string{"k": tc.pkVal})
		if err != nil {
			t.Fatalf("%s ReadBaselineRow: %v", tc.name, err)
		}
		if row == nil {
			t.Fatalf("%s PK %q: baseline row not found under TZ=%s — the temporal string→timestamp cast missed without a UTC-pinned session",
				tc.name, tc.pkVal, os.Getenv("TZ"))
		}
		if !valueEquals(row["v"], "found") {
			t.Fatalf("%s PK %q: v=%v (%T), want \"found\"", tc.name, tc.pkVal, row["v"], row["v"])
		}
	}
}

func valueEquals(got any, want string) bool {
	switch v := got.(type) {
	case string:
		return v == want
	case []byte:
		return string(v) == want
	default:
		return false
	}
}
