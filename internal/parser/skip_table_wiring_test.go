package parser

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// ─── The skip ledger as PRODUCTION fills it (#1296) ──────────────────────────
//
// skips_tables_test.go drives SkipCounters directly; these drive handleRows,
// the only caller that decides WHICH reason a dropped table lands under. That
// decision is the whole point of the split — a test of the counters alone would
// stay green if handleRows recorded every absent table under one reason again.

func skipLedgerFromHandleRows(t *testing.T, schema, table string) map[string]SkipStat {
	t.Helper()
	snapT := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ev := gapRowsEvent(schema, table, 2, uint32(snapT.Add(time.Hour).Unix()))
	rowsEv := ev.Event.(*replication.RowsEvent)
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), excludedScratchResolver(snapT),
		&Filters{}, ev, rowsEv, "binlog.000007", "", 0, 0, "", 9, emitTo(out), nil, skips)
	close(out)
	if err != nil {
		t.Fatalf("handleRows: %v", err)
	}
	return decodeLedger(t, skips)
}

// A table the snapshot simply never saw: fixable by a fresh snapshot.
func TestHandleRows_absentTableRecordsMissingReasonWithName(t *testing.T) {
	m := skipLedgerFromHandleRows(t, "shop", "plugin_log")
	st, ok := m[SkipTableNotInSnapshot]
	if !ok {
		t.Fatalf("absent table not counted under %s: %v", SkipTableNotInSnapshot, m)
	}
	if len(st.Tables) != 1 || st.Tables[0] != "shop.plugin_log" {
		t.Errorf("ledger does not name the skipped table: %v", st.Tables)
	}
	if st.LastFile != "binlog.000007" {
		t.Errorf("last_file = %q, want the binlog the drop happened in", st.LastFile)
	}
	if _, wrong := m[SkipTableExcludedFromSnapshot]; wrong {
		t.Error("an absent table must not be reported as validation-excluded — its fix IS a fresh snapshot")
	}
}

// A table validation excluded on purpose: a fresh snapshot excludes it again,
// so it must never be counted under the reason whose remedy is re-snapshotting.
func TestHandleRows_excludedTableRecordsItsOwnReason(t *testing.T) {
	m := skipLedgerFromHandleRows(t, "shop", "scratch")
	st, ok := m[SkipTableExcludedFromSnapshot]
	if !ok {
		t.Fatalf("excluded table not counted under %s: %v", SkipTableExcludedFromSnapshot, m)
	}
	if len(st.Tables) != 1 || st.Tables[0] != "shop.scratch" {
		t.Errorf("ledger does not name the excluded table: %v", st.Tables)
	}
	if st.LastDetail != "no primary key" {
		t.Errorf("last_detail = %q, want the validator's exclusion reason", st.LastDetail)
	}
	if _, wrong := m[SkipTableNotInSnapshot]; wrong {
		t.Error("a validation-excluded table under table_not_in_snapshot sends the operator to a remedy that can never converge (#1199)")
	}
}

// A snapshot-excluded system schema stays a routine permanent skip: counting it
// would mark capture degraded forever on any RDS source.
func TestHandleRows_systemSchemaStillNotCounted(t *testing.T) {
	m := skipLedgerFromHandleRows(t, "mysql", "rds_heartbeat2")
	if len(m) != 0 {
		t.Errorf("system-schema skip must not enter the ledger: %v", m)
	}
}
