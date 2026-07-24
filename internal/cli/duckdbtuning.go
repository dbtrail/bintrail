// Package cli holds command-layer building blocks intended to be shared across
// bintrail binaries. It exists so a second binary (the PostgreSQL-native
// bintrail-pg, #527/#529) can register the same source-agnostic read/recover
// commands and their shared flag/helper infrastructure without duplicating the
// command layer that lives in package main today.
package cli

import (
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/duckdbtuning"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The DuckDB resource-tuning helpers used to live here. They now live in the
// leaf package internal/duckdbtuning so the public read-plane facade
// (indexquery) can expose them without importing this package — which imports
// ext for the audit sink, and would otherwise close an
// ext → indexquery → internal/cli → ext import cycle when an ext seam consumes
// the facade. These thin forwarders keep the command layer and existing callers
// (query/recover/reconstruct/verify and their tests) referencing the same
// unqualified names.

// AddDuckDBTuningFlags registers the shared --ultrafast / --duckdb-threads /
// --duckdb-memory-limit flags. See duckdbtuning.AddDuckDBTuningFlags.
func AddDuckDBTuningFlags(cmd *cobra.Command) { duckdbtuning.AddDuckDBTuningFlags(cmd) }

// DuckDBTuningFromFlags resolves the effective DuckDB tuning for a command
// carrying those flags. See duckdbtuning.DuckDBTuningFromFlags.
func DuckDBTuningFromFlags(cmd *cobra.Command) (duckdbutil.Tuning, error) {
	return duckdbtuning.DuckDBTuningFromFlags(cmd)
}

// TunedArchiveFetcher adapts a DuckDB Tuning into a query.ArchiveFetcher. See
// duckdbtuning.TunedArchiveFetcher.
func TunedArchiveFetcher(t duckdbutil.Tuning) query.ArchiveFetcher {
	return duckdbtuning.TunedArchiveFetcher(t)
}
