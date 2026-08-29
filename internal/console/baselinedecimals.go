package console

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// negativeDecimalTTL bounds how long a FAILED footer resolution is remembered,
// for the same reason negativeRegionTTL exists: a failure is not a property of
// the snapshot, so a blip must not cost every file this daemon hands out its
// casts until someone restarts it.
const negativeDecimalTTL = 5 * time.Minute

type baselineDecimalEntry struct {
	decimals map[string][]baseline.DecimalColumn
	at       time.Time
	failed   bool
}

// resolveBaselineDecimals fills in the state views' decimal casts, memoized per
// snapshot.
//
// The memoization is not an optimization, it is the rule this path is under.
// Server.bucketRegions carries it in so many words: buildViewsInput runs on
// EVERY SQL panel query, so a network round trip does not belong there in the
// steady state. Reading a footer per table would be N of them, unbounded in the
// table count, inside the panel's setup deadline and behind its single-flight
// latch, which turns one slow bucket into 429s for every other panel user.
//
// A snapshot directory is immutable once complete, so a SUCCESSFUL answer never
// expires: the cache key carries the snapshot timestamp, and taking a new
// baseline produces a new key rather than a stale hit. A resolution that FAILED
// expires after negativeDecimalTTL.
//
// A table that is merely ABSENT from a successful answer is cached with it, and
// deliberately so: that means the file carries no embedded schema, which for an
// old baseline or a PostgreSQL-source one is a permanent fact about an
// immutable file, not a fault to retry on every query.
func (s *Server) resolveBaselineDecimals(ctx context.Context, in *views.Input) {
	if len(in.Baselines) == 0 {
		return
	}
	key := fmt.Sprintf("%s@%s", in.BaselineSource, in.BaselineSnapshot.UTC().Format(time.RFC3339))

	s.baselineDecimalMu.Lock()
	e, ok := s.baselineDecimals[key]
	s.baselineDecimalMu.Unlock()
	if ok && (!e.failed || time.Since(e.at) < negativeDecimalTTL) {
		in.ApplyDecimals(e.decimals)
		return
	}

	// The lock is held only around the map, never across the read: sync.Mutex
	// is not context-aware, so a goroutine blocked on it could not be released
	// by the panel's setup deadline. Two callers racing the same snapshot just
	// do the same read twice, which is harmless.
	decimals, err := baseline.DecimalColumnsFor(ctx, in.BaselinePaths())
	if err != nil {
		slog.Warn("console: could not read baseline column types from the Parquet footers; "+
			"the state views will not cast decimal columns", "error", err)
	} else {
		in.ApplyDecimals(decimals)
	}
	s.rememberBaselineDecimals(key, decimals, err != nil)
}

func (s *Server) rememberBaselineDecimals(key string, decimals map[string][]baseline.DecimalColumn, failed bool) {
	s.baselineDecimalMu.Lock()
	defer s.baselineDecimalMu.Unlock()
	if s.baselineDecimals == nil {
		s.baselineDecimals = map[string]baselineDecimalEntry{}
	}
	s.baselineDecimals[key] = baselineDecimalEntry{decimals: decimals, at: time.Now(), failed: failed}
}

// sqlPanelDecimalNote is what the panel says when some of the layout's tables
// carry no column types.
//
// The panel executes the generated views and throws the text away, so every
// explanation the downloadable file carries is invisible here. Without this a
// panel user runs sum() on a money column, gets the raw
// "No function matches ... 'sum(VARCHAR)'", and has nowhere to read why: it is
// the failure #1486 exists to remove, reproduced on the one surface whose
// output is rows rather than a file. Same shape as sqlPanelRegistryNote.
//
// On a PostgreSQL-source console this fires on EVERY panel query, forever, and
// that is the considered answer rather than an oversight. A standing warning on
// a healthy system is normally the shape this repo treats as a defect, but the
// test is whether the thing being reported is real, and here it is: a PG
// baseline stores every value as text (pgbaseline writes RawText columns) and
// deliberately omits the embedded CREATE TABLE, so its DECIMAL columns really
// do read as text and sum() over one really does fail. Suppressing the note
// there would remove the only available explanation from the one deployment
// where the limitation is permanent. So it stays, worded as a property of the
// files with the action attached.
//
// It names all THREE causes and attributes none of them, which is the same rule
// decimalComments follows in the generated file and for the same reason, run in
// the other direction. That function refuses to blame an unreadable footer
// because two of the three shapes that reach it are fine; this one must not
// claim the two benign shapes either, because only the SchemaKnown bit is
// available here and a whole-batch failure (an S3 403, no httpfs) clears it for
// every table at once. Naming only the harmless causes would tell an operator
// with a real credentials fault that nothing is wrong, so the sentence ends by
// pointing at the log, which is where that one and only that one shows up.
func sqlPanelDecimalNote(in views.Input) string {
	var untyped int
	for _, t := range in.Baselines {
		if !t.SchemaKnown {
			untyped++
		}
	}
	if untyped == 0 {
		return ""
	}
	noun, verb := "files", "carry"
	if len(in.Baselines) == 1 {
		noun = "file"
	}
	if untyped == 1 {
		verb = "carries"
	}
	return fmt.Sprintf("%d of %d baseline %s %s no column types, so DECIMAL columns there read as "+
		"text and an aggregate over one needs an explicit CAST. A PostgreSQL source never stores "+
		"column types, and neither did baselines taken before bintrail began recording them; if a "+
		"footer could not be read instead, the console log has the error",
		untyped, len(in.Baselines), noun, verb)
}
