// Package assurance exposes read-only access to the backup-assurance signals
// — restore coverage, the capture-continuity verdict, baseline staleness, and
// the scheduled-verify run history — for tooling and embedding distributions
// that import the core as a module.
//
// It is a thin facade, the same shape as indexquery: type aliases to the
// internal types (usable across module boundaries through the alias) plus
// one-line wrappers over the internal entry points. It adds no computation of
// its own, and deliberately so — every verdict here has exactly one
// implementation in the core, and an embedder that rendered its own would
// contradict `bintrail status` about the same index. Aliases, not wrapper
// structs, are what make that structural rather than a matter of discipline.
//
// Read-only: nothing here writes to the index, the archives, or the console's
// local files.
//
// Note for callers inside this repo: this package imports internal/console
// (the verify history is console-local state on disk), so importing assurance
// from a core command would trip the decouple guard in cliapp/uifree_test.go
// — cmd/bintrail must not link the console, however indirectly. This facade
// is for embedders, not for the core CLI.
package assurance

import (
	"context"
	"database/sql"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// Aliases to the internal assurance types.
type (
	// CoverageSummary is the live restore-coverage window (#1194).
	CoverageSummary = status.CoverageSummary
	// StreamStateInfo is the capture checkpoint ContinuityStatus grades.
	StreamStateInfo = status.StreamStateInfo
	// DeltaFloor is the delta-coverage floor and whether values below it can
	// be attributed to a source at all (#1213/#1219). Grade THROUGH it; using
	// Hour alone on a multi-source index manufactures false "broken".
	DeltaFloor = status.DeltaFloor
	// BaselineInfo is one baseline snapshot as graded by staleness (#1193).
	BaselineInfo = status.BaselineInfo
	// BaselineStalenessVerdict is ok / aging / broken / unknown.
	BaselineStalenessVerdict = status.BaselineStalenessVerdict
	// BaselineFile is one table's Parquet file in a baseline listing.
	BaselineFile = reconstruct.BaselineFile

	// VerifyHistory is the persisted scheduled-verify run history (#1191).
	VerifyHistory = console.VerifyHistory
	// VerifyRunRecord is one completed or skipped run.
	VerifyRunRecord = console.VerifyRunRecord
	// VerifyStatus is the per-run verdict embedded in VerifyRunRecord.
	VerifyStatus = console.VerifyStatus
	// VerifyTableResult is one table's outcome within a run.
	VerifyTableResult = console.VerifyTableResult
	// VerifySummary is a run's match/mismatch/inconclusive tally.
	VerifySummary = console.VerifySummary
	// VerifyMode is the verify mode a run used.
	VerifyMode = console.VerifyMode
)

// Baseline staleness verdicts. Unknown means the check could not be
// evaluated — never treat it as either healthy or broken.
const (
	BaselineOK      = status.BaselineOK
	BaselineAging   = status.BaselineAging
	BaselineBroken  = status.BaselineBroken
	BaselineUnknown = status.BaselineUnknown
)

// Verify run triggers, plus the history-only "skipped" state.
const (
	VerifyTriggerManual    = console.VerifyTriggerManual
	VerifyTriggerScheduled = console.VerifyTriggerScheduled
	VerifyStateSkipped     = console.VerifyStateSkipped
)

// CollectCoverageSummary computes the restore-coverage window for one index:
// the delta floor, the newest indexed event, capture lag, and the continuity
// verdict. Cheap by construction (no COUNT(*), no whole-table MAX).
func CollectCoverageSummary(ctx context.Context, db *sql.DB, dbName string, now time.Time) (*CoverageSummary, error) {
	return status.CollectCoverageSummary(ctx, db, dbName, now)
}

// ContinuityStatus is the single rule that turns a stream checkpoint into a
// continuity verdict: ok / gap_lost / unknown / unavailable / none. Pass the
// error from LoadStreamState alongside the value — a read failure is
// "unavailable", never a silent "ok".
func ContinuityStatus(stream *StreamStateInfo, streamErr error) string {
	return status.ContinuityStatus(stream, streamErr)
}

// LoadStreamState reads the single-row capture checkpoint. A nil value with a
// nil error means no stream row at all (a file-mode index).
func LoadStreamState(ctx context.Context, db *sql.DB) (*StreamStateInfo, error) {
	return status.LoadStreamState(ctx, db)
}

// OldestDeltaFromDB determines how far back the index can restore: the oldest
// live partition, extended backwards only across CONTIGUOUS archived hours,
// and only when those archives can be attributed to a single source.
func OldestDeltaFromDB(ctx context.Context, db *sql.DB, dbName string) (DeltaFloor, error) {
	return status.OldestDeltaFromDB(ctx, db, dbName)
}

// AnnotateBaselineStaleness grades each baseline in place against the floor.
func AnnotateBaselineStaleness(baselines []BaselineInfo, floor DeltaFloor, now time.Time) {
	status.AnnotateBaselineStaleness(baselines, floor, now)
}

// OverallBaselineStaleness reduces per-baseline verdicts to one, worst-first
// (broken > unknown > aging > ok).
func OverallBaselineStaleness(baselines []BaselineInfo) BaselineStalenessVerdict {
	return status.OverallBaselineStaleness(baselines)
}

// ListBaselines enumerates the baseline snapshot files under source (a local
// directory or an s3:// prefix), newest snapshot first. Path-derived only —
// no Parquet contents are read.
func ListBaselines(ctx context.Context, source string) ([]BaselineFile, error) {
	return reconstruct.ListBaselines(ctx, source)
}

// DefaultRegistryPath returns the console server-registry path the console
// uses when --servers-file is not set. Re-exported because the history is
// located relative to it: without this a caller would hardcode the path, and
// a relocated or later-moved registry would silently read no history at all.
func DefaultRegistryPath() string {
	return console.DefaultRegistryPath()
}

// DefaultVerifyHistoryPath returns the verify-history file path for a given
// console server-registry path (the history lives beside the registry). For
// a console running on defaults that is
// DefaultVerifyHistoryPath(DefaultRegistryPath()); pass the operator's
// --servers-file / --console-servers-file value when one is configured.
func DefaultVerifyHistoryPath(serversPath string) string {
	return console.DefaultVerifyHistoryPath(serversPath)
}

// OpenVerifyHistory loads the verify-run history at path for reading. A
// missing file yields an empty history whose Found reports false — check it:
// the history is written only by `bintrail-console watch`, so "absent" and
// "present with no failures" are different facts and must not render alike.
// A corrupt or newer-versioned file is an error rather than a silent empty.
func OpenVerifyHistory(path string) (*VerifyHistory, error) {
	return console.OpenVerifyHistory(path)
}
