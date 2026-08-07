//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRefresh_gapMarkerIsStampedAndInherited is the durable half of #1170's
// fail-closed rule.
//
// Refusing a gapped refresh is only half the contract: --allow-gaps exists
// because an operator sometimes genuinely wants the incomplete result. What must
// never happen is that the resulting baseline becomes indistinguishable from a
// clean one — least of all after being refreshed again, which is exactly when
// the human memory of the override is gone.
//
// So this asserts both halves against a real gapped stream_state: the first
// refresh refuses without --allow-gaps and stamps a marker with it, and the
// SECOND refresh — a clean window, no gap of its own — still carries the
// ancestor's marker forward.
func TestRefresh_gapMarkerIsStampedAndInherited(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	dsn := testutil.BaseDSN() + "/" + dbName
	const schema = "shop"

	base := time.Now().UTC().Truncate(time.Hour)
	cut1 := base.Add(30 * time.Second)
	cut2 := base.Add(60 * time.Second)

	seedOrdersSnapshot(t, db, schema, base)
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		base.Add(10*time.Second).Format("2006-01-02 15:04:05"), nil,
		schema, "orders", 2, "1", nil, nil, []byte(`{"id":1,"status":"A"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300,
		base.Add(40*time.Second).Format("2006-01-02 15:04:05"), nil,
		schema, "orders", 2, "2", nil, nil, []byte(`{"id":2,"status":"B"}`))

	// A permanent capture loss stamped INSIDE the first refresh's window.
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, gtid_set, events_indexed, last_checkpoint, server_id, gap_lost_at, gap_lost_detail)
		VALUES (1, 'position', 'binlog.000001', 300, '', 2, NOW(), 1, ?, ?)`,
		base.Add(5*time.Second).Format("2006-01-02 15:04:05"),
		"source binlogs purged before the stream caught up")

	root := t.TempDir()
	seedSourceBaseline(t, root, base, schema)

	cfg := func(at time.Time, allowGaps bool) reconstruct.FullTableConfig {
		return reconstruct.FullTableConfig{
			IndexDSN:     dsn,
			BaselineSrc:  root,
			Tables:       []string{schema + ".orders"},
			At:           at,
			OutputDir:    root,
			OutputFormat: reconstruct.OutputFormatParquet,
			AllowGaps:    allowGaps,
		}
	}

	// Strict: refuse, and refuse in a way the summary can classify.
	_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, cfg(cut1, false))
	if err == nil {
		t.Fatal("a refresh over a stamped capture gap was published under the default (strict) policy")
	}
	if len(failures) != 1 || !errorsIsCaptureGap(failures[0].Err) {
		t.Fatalf("failure is not classified as a capture gap: %+v", failures)
	}

	// Overridden: publish, and mark it forever.
	if _, err := reconstruct.ReconstructTables(ctx, cfg(cut1, true)); err != nil {
		t.Fatalf("refresh with AllowGaps: %v", err)
	}
	mid, _, _, err := reconstruct.FindBaseline(ctx, root, schema, "orders", cut2)
	if err != nil {
		t.Fatalf("FindBaseline: %v", err)
	}
	midMeta, err := baseline.ReadParquetMetadata(mid)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if midMeta.CaptureGap == "" {
		t.Fatal("the published snapshot carries no capture-gap marker: nothing distinguishes a knowingly " +
			"incomplete baseline from a clean one once the terminal is closed")
	}
	if !strings.Contains(midMeta.CaptureGap, "purged") {
		t.Errorf("marker does not carry the recorded detail: %q", midMeta.CaptureGap)
	}

	// Second refresh: its OWN window is clean (the gap is before it), so the
	// only thing that can keep the fact alive is inheritance.
	if _, err := reconstruct.ReconstructTables(ctx, cfg(cut2, false)); err != nil {
		t.Fatalf("second refresh over a clean window: %v", err)
	}
	final, _, _, err := reconstruct.FindBaseline(ctx, root, schema, "orders", cut2.Add(time.Second))
	if err != nil {
		t.Fatalf("FindBaseline after the second refresh: %v", err)
	}
	finalMeta, err := baseline.ReadParquetMetadata(final)
	if err != nil {
		t.Fatalf("ReadParquetMetadata: %v", err)
	}
	if finalMeta.CaptureGap == "" {
		t.Fatal("the refreshed snapshot dropped its ancestor's capture-gap marker — one refresh would " +
			"launder a knowingly-incomplete baseline into a clean-looking one")
	}
}

// errorsIsCaptureGap classifies via the exported sentinel, not the message —
// the same way `baseline refresh` does, so this test fails if that tagging is
// ever dropped.
func errorsIsCaptureGap(err error) bool { return errors.Is(err, reconstruct.ErrCaptureGap) }
