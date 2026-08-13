//go:build integration

package cli

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// `bintrail query --profile` skips archive discovery entirely: archive reads
// enforce no redaction rules, so a profiled query provably opens NO archives.
// The planner was still handed a nil scope, which it reads as "every archive in
// the index" — so a rotated hour the query cannot reach was credited as
// coverage and the gap warning never fired.
//
// That is the same false OK #1232 removes, reachable from the command line with
// one flag, at a call site the fix already touched.
func TestRunQuery_profileScopesToNoArchives(t *testing.T) {
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	// An hour that is neither live nor reachable by this query.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO archive_state (partition_name, bintrail_id, local_path)
		 VALUES ('p_2020010203', 'src-a', '/archives/bintrail_id=src-a/x.parquet')`); err != nil {
		t.Fatalf("seed archive_state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO profiles (name) VALUES ('analyst')`); err != nil {
		t.Fatalf("seed profile: %v", err)
	}

	saved := struct{ dsn, format, profile, since, until string }{qIndexDSN, qFormat, qProfile, qSince, qUntil}
	t.Cleanup(func() {
		qIndexDSN, qFormat, qProfile, qSince, qUntil = saved.dsn, saved.format, saved.profile, saved.since, saved.until
	})
	qIndexDSN = testutil.IntegrationDSN(name)
	qFormat = "table"
	qSince = "2020-01-02 03:00:00"
	qUntil = "2020-01-02 03:30:00"
	queryCmd.SetContext(ctx)

	// The gap verdict is emitted through slog, so swap the default handler.
	gapWarned := func(t *testing.T) bool {
		t.Helper()
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		captureStdout(t, func() {
			if err := runQuery(queryCmd, nil); err != nil {
				t.Fatalf("runQuery: %v", err)
			}
		})
		return strings.Contains(buf.String(), "rotated and not archived")
	}

	// Control: without a profile the query DOES open the archive, so the hour
	// is covered and no gap is warned. Without this the assertion below could
	// pass for the wrong reason.
	qProfile = ""
	if gapWarned(t) {
		t.Error("an unprofiled query warned about a gap over an hour it can fetch")
	}

	// The regression: profiled, the query opens nothing, so the hour is a gap.
	qProfile = "analyst"
	if !gapWarned(t) {
		t.Error("a --profile query credited an archive it never opens as coverage; the gap warning is silent over an unreachable hour")
	}
}
