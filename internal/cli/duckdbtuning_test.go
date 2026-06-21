package cli

import (
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// TestDuckDBTuningFromFlags pins the precedence contract: an explicit --duckdb-*
// flag wins over --ultrafast, which wins over the conservative default. The
// no-flags row guards the "don't break anything" invariant — with the feature
// off, callers get exactly the budget parquetquery.Fetch used before #510.
func TestDuckDBTuningFromFlags(t *testing.T) {
	tests := []struct {
		name    string
		flags   map[string]string
		want    duckdbutil.Tuning
		wantErr bool
	}{
		{
			name:  "no flags → conservative default",
			flags: nil,
			want:  duckdbutil.Tuning{Threads: 2, MemoryLimit: "4GB"},
		},
		{
			name:  "ultrafast → DuckDB self-tunes (both unset) + S3Direct",
			flags: map[string]string{"ultrafast": "true"},
			want:  duckdbutil.Tuning{S3Direct: true},
		},
		{
			name:  "explicit threads overrides the default base",
			flags: map[string]string{"duckdb-threads": "8"},
			want:  duckdbutil.Tuning{Threads: 8, MemoryLimit: "4GB"},
		},
		{
			name:  "explicit memory-limit overrides the default base",
			flags: map[string]string{"duckdb-memory-limit": "16GB"},
			want:  duckdbutil.Tuning{Threads: 2, MemoryLimit: "16GB"},
		},
		{
			name:  "binary-unit memory-limit accepted",
			flags: map[string]string{"duckdb-memory-limit": "16GiB"},
			want:  duckdbutil.Tuning{Threads: 2, MemoryLimit: "16GiB"},
		},
		{
			name:  "decimal memory-limit accepted",
			flags: map[string]string{"duckdb-memory-limit": "2.5GB"},
			want:  duckdbutil.Tuning{Threads: 2, MemoryLimit: "2.5GB"},
		},
		{
			name:  "explicit threads=0 means one-per-core, overriding default 2",
			flags: map[string]string{"duckdb-threads": "0"},
			want:  duckdbutil.Tuning{Threads: 0, MemoryLimit: "4GB"},
		},
		{
			name:  "ultrafast + explicit threads → threads applied, memory unset, S3Direct kept",
			flags: map[string]string{"ultrafast": "true", "duckdb-threads": "8"},
			want:  duckdbutil.Tuning{Threads: 8, MemoryLimit: "", S3Direct: true},
		},
		{
			name:  "ultrafast + explicit memory-limit → memory applied, threads unset, S3Direct kept",
			flags: map[string]string{"ultrafast": "true", "duckdb-memory-limit": "16GB"},
			want:  duckdbutil.Tuning{Threads: 0, MemoryLimit: "16GB", S3Direct: true},
		},
		{
			// Negative is a dangerous case: DuckDB silently accepts it and
			// uncaps memory, so the CLI must reject it up front.
			name:    "negative memory-limit → error",
			flags:   map[string]string{"duckdb-memory-limit": "-4GB"},
			wantErr: true,
		},
		{
			// Zero is the other silent-uncap: DuckDB accepts e.g. '0GB' and
			// treats it as unlimited.
			name:    "zero memory-limit → error",
			flags:   map[string]string{"duckdb-memory-limit": "0GB"},
			wantErr: true,
		},
		{
			// A percentage / a bare unitless number are NOT accepted by the
			// linked DuckDB — rejecting them at the CLI avoids a silent
			// fall-back to the default at Apply time.
			name:    "percentage memory-limit → error",
			flags:   map[string]string{"duckdb-memory-limit": "80%"},
			wantErr: true,
		},
		{
			name:    "bare unitless number → error",
			flags:   map[string]string{"duckdb-memory-limit": "1024"},
			wantErr: true,
		},
		{
			name:    "garbage memory-limit → error",
			flags:   map[string]string{"duckdb-memory-limit": "lots"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: "test"}
			AddDuckDBTuningFlags(cmd)
			for flag, val := range tt.flags {
				if err := cmd.Flags().Set(flag, val); err != nil {
					t.Fatalf("set --%s=%s: %v", flag, val, err)
				}
			}
			got, err := DuckDBTuningFromFlags(cmd)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("DuckDBTuningFromFlags() = %+v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("DuckDBTuningFromFlags() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("DuckDBTuningFromFlags() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestTunedArchiveFetcherNonNil: the adapter must always return a usable
// fetcher (it is passed where a nil ArchiveFetcher is a hard error).
func TestTunedArchiveFetcherNonNil(t *testing.T) {
	if TunedArchiveFetcher(duckdbutil.DefaultTuning()) == nil {
		t.Fatal("TunedArchiveFetcher returned nil")
	}
}
