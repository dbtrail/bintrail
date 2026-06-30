// Package cascaderecover assembles the cascade-recovery SQL script (the
// documented preamble, the SET FOREIGN_KEY_CHECKS=0/1 wrapper, the CASCADE
// re-INSERTs, and the idempotent SET NULL restorations) from a synthesized
// cascade result.
//
// It is the binary-neutral home for the emission logic that used to live in the
// `recover-cascade` CLI command: factoring it out lets the CLI and the console
// (#577) produce BYTE-IDENTICAL scripts that cannot drift. The package depends
// on cascade (for the SetNullRestore rows), recovery (the reversal-SQL
// generator), metadata (the child PK columns), and query (the row type); nothing
// it depends on imports it back, so it stays a leaf composition layer (no import
// cycle) and keeps the cascade synthesis engine free of any emission/recovery
// coupling.
//
// It owns no cobra flags and no command globals. Callers keep their own flag
// parsing and exit-code mapping; the structured coverage (cascade.Result's
// Incomplete/Complete()) stays with the caller so each surface can present it.
package cascaderecover

import (
	"fmt"
	"io"
	"strings"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// Header carries the values rendered into the SQL preamble. Parents and Children
// cannot be derived from EmitSQL's flattened rows (it cannot split the
// parents++victims concat back apart), so the caller supplies them; the SET NULL
// count is NOT carried here — EmitSQL derives it from len(setNullRows) so a
// caller can never desync the header count from the statements emitted.
type Header struct {
	Schema, Table     string
	Parents, Children int
	Caveats           []string
	BaselineActive    bool
	// Combined switches the preamble to the cascade-AWARE recover wording used
	// when the console auto-detects a cascade parent inside a normal recover: the
	// base reversal of the selected change(s) is composed with the synthesized
	// children in ONE script, so the parent count and the "recover-cascade"
	// framing no longer fit. The zero value (false) keeps the byte-identical
	// `recover-cascade` preamble used by the CLI and the explicit endpoint.
	Combined bool
}

// EmitSQL writes the documented preamble, the FK-checks-off wrapper, the CASCADE
// reversal statements (DELETE→INSERT via the generator), and the SET NULL FK
// restorations (idempotent guarded UPDATEs). Returns the total statement count.
// resolver supplies child PK columns for the SET NULL WHERE clauses; gen must be
// built from the same (or an equivalent) resolver (recovery.New(db, resolver)).
func EmitSQL(w io.Writer, gen *recovery.Generator, rows []query.ResultRow, setNullRows []cascade.SetNullRestore, resolver *metadata.Resolver, hdr Header) (int, error) {
	// Enforce the recover script-size budget BEFORE writing a byte (#654): EmitSQL
	// emits its preamble (including `SET FOREIGN_KEY_CHECKS=0`) ahead of the
	// generator, so without an up-front check a budget refusal would leave that
	// dangling FK-disable on the writer. Refusing here keeps "emit nothing on
	// refusal" consistent with the plain recover path.
	if err := gen.CheckScriptBudget(rows); err != nil {
		return 0, err
	}

	// Build every SET NULL restoration BEFORE writing a byte (all-or-nothing): a
	// missing resolver, an unresolvable table, or an absent PK column must abort
	// the whole emit cleanly — returning mid-script would leave the parent/child
	// INSERTs written but drop the closing `SET FOREIGN_KEY_CHECKS=1`, handing the
	// operator a script that re-enables nothing.
	var setNullStmts []string
	if len(setNullRows) > 0 {
		if resolver == nil {
			return 0, fmt.Errorf("a schema snapshot is required to restore SET NULL foreign keys (run `bintrail snapshot`)")
		}
		for _, sr := range setNullRows {
			tm, terr := resolver.Resolve(sr.Schema, sr.Table)
			if terr != nil {
				return 0, fmt.Errorf("resolve %s.%s for SET NULL restore: %w", sr.Schema, sr.Table, terr)
			}
			stmt, ferr := recovery.FormatSetNullRestore(sr.Schema, sr.Table, sr.Column, sr.Value, tm.PKColumnMetas(), sr.Row)
			if ferr != nil {
				return 0, ferr
			}
			setNullStmts = append(setNullStmts, stmt)
		}
	}

	var b strings.Builder
	if hdr.Combined {
		fmt.Fprintf(&b, "-- bintrail recover (cascade-aware): undo %s.%s, including the foreign-key ON DELETE\n", hdr.Schema, hdr.Table)
		b.WriteString("-- CASCADE / SET NULL side effects InnoDB ran below the binlog (MySQL Bug #32506).\n")
		fmt.Fprintf(&b, "-- Re-creates %d cascade-deleted child row(s) and restores %d SET NULL'd FK(s) alongside\n", hdr.Children, len(setNullRows))
		b.WriteString("-- the reversal of the selected change(s). NEVER auto-applied.\n")
	} else {
		fmt.Fprintf(&b, "-- bintrail recover-cascade: reverse ON DELETE CASCADE / SET NULL side effects on %s.%s\n", hdr.Schema, hdr.Table)
		fmt.Fprintf(&b, "-- Re-inserts %d deleted parent row(s) and %d cascade-deleted child row(s); restores %d SET NULL'd FK(s)\n", hdr.Parents, hdr.Children, len(setNullRows))
		b.WriteString("-- that InnoDB removed/nulled below the binlog (MySQL Bug #32506). NEVER auto-applied.\n")
	}
	b.WriteString("--\n")
	if hdr.BaselineActive {
		b.WriteString("-- Phase-2 baseline fallback ACTIVE: children present in a covered baseline are\n")
		b.WriteString("-- reconstructed even if untouched within the window. Tables NOT covered by a\n")
		b.WriteString("-- baseline are flagged above. \"Complete\" means everything DETECTABLE was recovered.\n")
	} else {
		b.WriteString("-- Phase-1 (binlog-window) recovery: a child untouched within --lookback and not\n")
		b.WriteString("-- in a baseline is NOT reconstructed — pass --baseline-dir/--baseline-s3 to enable\n")
		b.WriteString("-- Phase-2 fallback. \"Complete\" means everything DETECTABLE was recovered.\n")
	}
	b.WriteString("--\n")
	b.WriteString("-- If you have already re-created a deleted parent, delete its INSERT below:\n")
	b.WriteString("-- SET FOREIGN_KEY_CHECKS=0 does NOT suppress PRIMARY KEY violations.\n")
	if len(hdr.Caveats) > 0 {
		b.WriteString("--\n-- !!! INCOMPLETE RECOVERY — the result is provably partial:\n")
		for _, c := range hdr.Caveats {
			fmt.Fprintf(&b, "--   - %s\n", c)
		}
	}
	b.WriteString("\nSET FOREIGN_KEY_CHECKS=0;\n\n")
	if _, err := io.WriteString(w, b.String()); err != nil {
		return 0, err
	}

	n, err := gen.GenerateSQLFromRows(rows, w)
	if err != nil {
		return 0, err
	}

	// SET NULL restorations: idempotent UPDATEs (… AND fk IS NULL) that only
	// touch rows still in the post-cascade nulled state, so a re-run or a later
	// re-point of the child is never clobbered. Pre-built above, so nothing here
	// can fail after the INSERTs are already on disk.
	if len(setNullStmts) > 0 {
		if _, err := io.WriteString(w, "\n-- SET NULL FK restorations (idempotent: only rows whose FK is still NULL):\n"); err != nil {
			return n, err
		}
		for _, stmt := range setNullStmts {
			if _, werr := io.WriteString(w, stmt+";\n"); werr != nil {
				return n, werr
			}
			n++
		}
	}

	if _, err := io.WriteString(w, "\nSET FOREIGN_KEY_CHECKS=1;\n"); err != nil {
		return n, err
	}
	return n, nil
}
