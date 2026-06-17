package duckdbutil

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

// Tuning holds the two DuckDB resource knobs bintrail sets on its Parquet query
// session (threads + memory budget). A zero-valued field means "leave it
// unset", so DuckDB applies its own native default: threads = one per CPU core,
// memory_limit = ~80% of physical RAM. Both spill to temp_directory when
// exceeded, so leaving them unset is "let DuckDB use more RAM before spilling",
// not "unbounded" — the temp_directory backstop still prevents the OOM-killer.
//
// bintrail's shipped default (DefaultTuning) caps both low for small-container
// safety. Ultrafast lifts the cap by leaving them unset and letting DuckDB
// self-tune to the host (#509). Apply only emits a SET for a non-zero field, so
// these two constructors are not special-cased anywhere — the same struct
// expresses "default", "ultrafast", and any explicit operator override.
//
// Note the polarity: the whole-struct zero value Tuning{} equals Ultrafast()
// (host-greedy), NOT the container-safe budget. A caller that wants the safe
// default must call DefaultTuning() explicitly rather than rely on a zero value.
//
// temp_directory and preserve_insertion_order are deliberately NOT in this
// struct: they are not memory-for-speed trade-offs (the former is the spill
// backstop, the latter is a win-win that is safe under our explicit ORDER BY),
// so parquetquery sets them unconditionally regardless of Tuning.
type Tuning struct {
	// Threads is the DuckDB thread count (SET threads = N). 0 leaves it unset,
	// so DuckDB defaults to one thread per CPU core.
	Threads int
	// MemoryLimit is the DuckDB memory budget (SET memory_limit = '...'), e.g.
	// "4GB". Empty leaves it unset, so DuckDB defaults to ~80% of physical RAM.
	MemoryLimit string
}

// DefaultTuning is bintrail's conservative, container-safe DuckDB budget — the
// exact values parquetquery.Fetch hardcoded before this struct existed (#510).
// Used by every long-lived/shared caller (shim, console, agent) and as the
// base the CLI tuning flags build on.
func DefaultTuning() Tuning { return Tuning{Threads: 2, MemoryLimit: "4GB"} }

// Ultrafast trades the container-safe cap for speed: both knobs unset, so
// DuckDB self-tunes to the host (all cores, ~80% RAM, spilling to
// temp_directory). For big boxes running the offline query/recover/reconstruct
// commands, not the small shared containers the shim runs in. See #509.
func Ultrafast() Tuning { return Tuning{} }

// Apply emits the SET statements for the non-zero fields on db. Best-effort,
// matching the rest of duckdbutil: a failed SET is logged at WARN and the
// session falls back to DuckDB's default for that knob rather than aborting the
// query. Call it on a freshly opened DuckDB session.
func (t Tuning) Apply(ctx context.Context, db *sql.DB) {
	if t.Threads > 0 {
		applyTuningStmt(ctx, db, fmt.Sprintf("SET threads = %d", t.Threads))
	}
	if t.MemoryLimit != "" {
		// Mirror parquetquery's temp_directory concatenation; escape single
		// quotes so an operator-supplied value can't break out of the literal.
		applyTuningStmt(ctx, db, "SET memory_limit = '"+strings.ReplaceAll(t.MemoryLimit, "'", "''")+"'")
	}
}

func applyTuningStmt(ctx context.Context, db *sql.DB, stmt string) {
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		slog.Warn("could not configure DuckDB", "statement", stmt, "error", err)
	}
}
