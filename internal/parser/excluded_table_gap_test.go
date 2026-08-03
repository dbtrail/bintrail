package parser

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── Validation-excluded tables vs the file-mode gap tracker (#1199) ─────────
//
// After #1051 a degraded snapshot (TakeSnapshotExcludingInvalid) can durably
// exclude a no-PK / non-InnoDB table. Its row events are absent from the
// snapshot ON PURPOSE: re-snapshotting excludes the table again, so the #778
// "stale snapshot → fail the file → run `bintrail snapshot`" escalation can
// never converge for it. handleRows must therefore treat a validation-excluded
// table like the system schemas — warn-and-skip, visible in the skip ledger,
// never a file failure — while keeping the #778 escalation intact for tables
// that are absent for any OTHER reason.

// excludedScratchResolver is timedOrdersResolver's sibling with shop.scratch
// recorded as a validation exclusion, built through the exported constructor
// production code shares (NewResolver populates the same field from
// snapshot_exclusions — pinned by TestNewResolver_loadsSnapshotExclusions).
func excludedScratchResolver(snapshotTime time.Time) *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop",
		Table:  "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "amount", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTablesAtExcluding(9, snapshotTime,
		map[string]*metadata.TableMeta{"shop.orders": tm},
		map[string]string{"shop.scratch": "no primary key"})
}

func TestHandleRows_validationExcludedTableFileMode(t *testing.T) {
	snapT := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	afterTS := uint32(snapT.Add(time.Hour).Unix())

	t.Run("excluded table at-or-after snapshot never records a gap and tells the truth", func(t *testing.T) {
		ev := gapRowsEvent("shop", "scratch", 2, afterTS)
		rowsEv := ev.Event.(*replication.RowsEvent)
		var logBuf bytes.Buffer
		tracker := &schemaGapTracker{}
		out := make(chan Event, 4)
		err := handleRows(context.Background(), newTestLogger(&logBuf), excludedScratchResolver(snapT),
			&Filters{}, ev, rowsEv, "binlog.000007", "", 0, 0, "", 9, out, tracker, nil)
		close(out)
		if err != nil {
			t.Fatalf("handleRows returned a hard error on an excluded-table skip: %v", err)
		}
		if tracker.count != 0 {
			t.Fatalf("validation-excluded table recorded a gap (count=%d) — the file would be marked failed with a remediation that cannot converge (#1199)", tracker.count)
		}
		logs := logBuf.String()
		if !strings.Contains(logs, "excluded from snapshot 9 (no primary key)") {
			t.Errorf("warn must carry the exclusion diagnosis, got logs: %s", logs)
		}
		if !strings.Contains(logs, "not capturable as-is") {
			t.Errorf("warn must state the table is not capturable as-is, got logs: %s", logs)
		}
		// The stale-snapshot diagnosis and its remediation must NOT appear:
		// "consider re-running `bintrail snapshot`" is Resolve's absent-table
		// message and "schema gap" is the #778 escalation.
		if strings.Contains(logs, "consider re-running") || strings.Contains(logs, "schema gap") {
			t.Errorf("excluded-table skip must not be diagnosed as a stale snapshot, got logs: %s", logs)
		}
	})

	t.Run("other absent table still records a gap with the same resolver", func(t *testing.T) {
		// Guard against an over-broad carve-out: exclusions are per-table,
		// so a DIFFERENT table absent at-or-after snapshot time keeps the
		// #778 fail-loud escalation.
		ev := gapRowsEvent("shop", "widgets", 2, afterTS)
		rowsEv := ev.Event.(*replication.RowsEvent)
		tracker := &schemaGapTracker{}
		out := make(chan Event, 4)
		err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), excludedScratchResolver(snapT),
			&Filters{}, ev, rowsEv, "binlog.000007", "", 0, 0, "", 9, out, tracker, nil)
		close(out)
		if err != nil {
			t.Fatalf("handleRows: %v", err)
		}
		if tracker.count != 1 {
			t.Fatalf("non-excluded absent table must still record a gap, count=%d", tracker.count)
		}
	})

	t.Run("excluded table still counts in the skip tally under the file-mode wiring", func(t *testing.T) {
		// Visibility is not negotiable (#1034/#1199): the diagnosis changes
		// and the file no longer fails, but the skip stays tallied. This
		// drives the REAL file-mode combination — gapTracker AND skips both
		// non-nil (the Parser.SetSkipCounters wiring) — so a regression that
		// silences either half (re-nils the counters, or re-fails the file)
		// goes red here. The stream wiring (nil tracker, non-nil skips) hits
		// the same RecordSkip site.
		ev := gapRowsEvent("shop", "scratch", 2, afterTS)
		rowsEv := ev.Event.(*replication.RowsEvent)
		var logBuf bytes.Buffer
		skips := NewSkipCounters(newTestLogger(&logBuf))
		tracker := &schemaGapTracker{}
		out := make(chan Event, 4)
		err := handleRows(context.Background(), newTestLogger(&logBuf), excludedScratchResolver(snapT),
			&Filters{}, ev, rowsEv, "binlog.000007", "", 0, 0, "", 9, out, tracker, skips)
		close(out)
		if err != nil {
			t.Fatalf("handleRows: %v", err)
		}
		if got := skips.Total(); got != 1 {
			t.Fatalf("excluded-table skip must stay visible in the run tally, Total()=%d, want 1", got)
		}
		if tracker.count != 0 {
			t.Fatalf("excluded-table skip must not fail the file, gap count=%d", tracker.count)
		}
	})
}
