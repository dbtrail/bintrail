package cli

// sourceEmptyHint is the command-layer remediation for a
// *query.SourceEmptyError, shared verbatim by every CLI call site (recover,
// reconstruct single-row, reconstruct full-table) so the advice cannot drift
// per-site — #1274 happened because three hand-copied hints outlived two
// wording fixes. --repair re-registers archive files that exist; --prune (a
// separate, deliberate flag) clears registrations whose files are gone for
// good; a flagless reconcile only reports the drift.
const sourceEmptyHint = "run `bintrail archive reconcile --repair` to re-sync archive_state with storage " +
	"(--repair re-registers files that exist; add --prune if the files are gone for good; flagless it only reports), " +
	"or pass --allow-gaps to proceed without that source"
