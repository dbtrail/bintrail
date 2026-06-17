package main

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
		name  string
		flags map[string]string
		want  duckdbutil.Tuning
	}{
		{
			name:  "no flags → conservative default",
			flags: nil,
			want:  duckdbutil.Tuning{Threads: 2, MemoryLimit: "4GB"},
		},
		{
			name:  "ultrafast → DuckDB self-tunes (both unset)",
			flags: map[string]string{"ultrafast": "true"},
			want:  duckdbutil.Tuning{},
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
			name:  "explicit threads=0 means one-per-core, overriding default 2",
			flags: map[string]string{"duckdb-threads": "0"},
			want:  duckdbutil.Tuning{Threads: 0, MemoryLimit: "4GB"},
		},
		{
			name:  "ultrafast + explicit threads → threads applied, memory stays unset",
			flags: map[string]string{"ultrafast": "true", "duckdb-threads": "8"},
			want:  duckdbutil.Tuning{Threads: 8, MemoryLimit: ""},
		},
		{
			name:  "ultrafast + explicit memory-limit → memory applied, threads stays unset",
			flags: map[string]string{"ultrafast": "true", "duckdb-memory-limit": "16GB"},
			want:  duckdbutil.Tuning{Threads: 0, MemoryLimit: "16GB"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: "test"}
			addDuckDBTuningFlags(cmd)
			for flag, val := range tt.flags {
				if err := cmd.Flags().Set(flag, val); err != nil {
					t.Fatalf("set --%s=%s: %v", flag, val, err)
				}
			}
			if got := duckDBTuningFromFlags(cmd); got != tt.want {
				t.Fatalf("duckDBTuningFromFlags() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestTunedArchiveFetcherNonNil: the adapter must always return a usable
// fetcher (it is passed where a nil ArchiveFetcher is a hard error).
func TestTunedArchiveFetcherNonNil(t *testing.T) {
	if tunedArchiveFetcher(duckdbutil.DefaultTuning()) == nil {
		t.Fatal("tunedArchiveFetcher returned nil")
	}
}
