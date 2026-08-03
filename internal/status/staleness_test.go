package status

import (
	"bytes"
	"context"
	"database/sql"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestBaselineStalenessFor(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-100 * time.Hour) // coverage span = 100h; aging floor = 80h ago
	cases := []struct {
		name     string
		snapshot time.Time
		oldest   time.Time
		want     BaselineStalenessVerdict
	}{
		{"fresh snapshot", now.Add(-1 * time.Hour), oldest, BaselineOK},
		{"just inside the aging floor", now.Add(-79 * time.Hour), oldest, BaselineOK},
		{"exactly at 80% of the span", now.Add(-80 * time.Hour), oldest, BaselineAging},
		{"old but still covered", now.Add(-99 * time.Hour), oldest, BaselineAging},
		{"anchor equals the floor", oldest, oldest, BaselineAging},
		{"anchor predates coverage", oldest.Add(-time.Minute), oldest, BaselineBroken},
		{"no evaluable floor", now.Add(-1 * time.Hour), time.Time{}, BaselineUnknown},
		{"zero snapshot time", time.Time{}, oldest, BaselineUnknown},
		// Degenerate: a non-positive span must not be graded against (it
		// would mark everything aging).
		{"coverage starts now", now, now, BaselineOK},
	}
	for _, tc := range cases {
		if got := BaselineStalenessFor(tc.snapshot, tc.oldest, now); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestOldestLivePartitionHour(t *testing.T) {
	if got := OldestLivePartitionHour(nil); !got.IsZero() {
		t.Fatalf("no partitions must be unknown, got %v", got)
	}
	// Only p_future / malformed names: still unknown, never a fabricated floor.
	parts := []PartitionStat{{Name: "p_future"}, {Name: "garbage"}}
	if got := OldestLivePartitionHour(parts); !got.IsZero() {
		t.Fatalf("unparseable-only partitions must be unknown, got %v", got)
	}
	parts = append(parts, PartitionStat{Name: "p_2026080103"}, PartitionStat{Name: "p_2026080101"})
	want := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	if got := OldestLivePartitionHour(parts); !got.Equal(want) {
		t.Fatalf("oldest live partition hour = %v, want %v", got, want)
	}
}

func TestDeltaFloorGrade(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	live := now.Add(-10 * time.Hour)
	older := live.Add(-time.Hour)

	// Attributable floor: below it is broken, the verdict callers act on.
	if got := (DeltaFloor{Hour: live}).Grade(older, now); got != BaselineBroken {
		t.Fatalf("attributable floor: got %s, want broken", got)
	}
	// Unattributable floor: the same snapshot may still be covered by its own
	// source's archives, so it is unknown — reporting broken would cry wolf.
	if got := (DeltaFloor{Hour: live, BelowIsUnknown: true}).Grade(older, now); got != BaselineUnknown {
		t.Fatalf("unattributable floor: got %s, want unknown", got)
	}
	// Above the floor the ambiguity is irrelevant: the live partitions are
	// shared by every source, so those verdicts need no attribution.
	if got := (DeltaFloor{Hour: live, BelowIsUnknown: true}).Grade(now.Add(-time.Hour), now); got != BaselineOK {
		t.Fatalf("in-window snapshot: got %s, want ok", got)
	}
	// Aging is deliberately NOT demoted: it is a true statement about the
	// window that IS provable, and demoting it would erase the only signal
	// left on a multi-source index.
	if got := (DeltaFloor{Hour: live, BelowIsUnknown: true}).Grade(now.Add(-9*time.Hour), now); got != BaselineAging {
		t.Fatalf("in-window aging snapshot: got %s, want aging", got)
	}
}

// TestOverallBaselineStalenessRanksUnknownOverAging: #1219 makes unknown the
// routine below-floor verdict, and aging is the one verdict the codebase
// treats as ignorable (it never alerts). A table that CANNOT be evaluated
// must not hide behind a merely-old one in the headline.
func TestOverallBaselineStalenessRanksUnknownOverAging(t *testing.T) {
	baselines := []BaselineInfo{
		{Database: "shop", Table: "orders", SnapshotTime: time.Unix(2000, 0), Staleness: BaselineAging},
		{Database: "shop", Table: "legacy", SnapshotTime: time.Unix(1000, 0), Staleness: BaselineUnknown},
	}
	if got := OverallBaselineStaleness(baselines); got != BaselineUnknown {
		t.Fatalf("got %s, want unknown", got)
	}
	// Broken still outranks everything: a known-broken table is actionable.
	baselines[0].Staleness = BaselineBroken
	if got := OverallBaselineStaleness(baselines); got != BaselineBroken {
		t.Fatalf("got %s, want broken", got)
	}
}

func TestOldestDeltaFromDB(t *testing.T) {
	partsQ := regexp.QuoteMeta("SELECT PARTITION_NAME FROM information_schema.PARTITIONS")
	archQ := regexp.QuoteMeta("SELECT MIN(partition_name), MAX(partition_name), COUNT(DISTINCT bintrail_id) FROM archive_state")
	srvQ := regexp.QuoteMeta("SELECT COUNT(*) FROM bintrail_servers")
	partRows := func(names ...string) *sqlmock.Rows {
		r := sqlmock.NewRows([]string{"PARTITION_NAME"})
		for _, n := range names {
			r.AddRow(n)
		}
		return r
	}
	// Two mechanisms pin the QUERY SET, not just the result: sqlmock's
	// ordered mode rejects an UNEXPECTED query at call time (that is what
	// catches a new round trip on the single-source path), and
	// ExpectationsWereMet catches the converse — a query the code stopped
	// issuing, e.g. the sources probe silently skipped.
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet or unexpected queries: %v", err)
			}
			db.Close()
		})
		return db, mock
	}

	archRows := func(min, max any, sources int) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(min, max, sources)
	}
	srvRows := func(n int) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"n"}).AddRow(n)
	}

	t.Run("contiguous archive floor wins when earlier", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110", "p_future"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080103", "p_2026080109", 1))
		mock.ExpectQuery(srvQ).WillReturnRows(srvRows(1))
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil || !got.Hour.Equal(time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)) || got.BelowIsUnknown {
			t.Fatalf("got %+v, %v", got, err)
		}
	})

	t.Run("non-contiguous archives do not extend the floor", func(t *testing.T) {
		// Archives end at 05:00, live partitions start at 10:00: the 4-hour
		// hole breaks every restore anchored before the live floor, so
		// extending the floor to 01:00 would grade those baselines with an
		// unearned "ok".
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080101", "p_2026080105", 1))
		mock.ExpectQuery(srvQ).WillReturnRows(srvRows(1))
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil || !got.Hour.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) {
			t.Fatalf("got %+v, %v — want the live floor, not the archive one", got, err)
		}
	})

	t.Run("archive-only coverage counts when no live partitions exist", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows())
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080103", "p_2026080105", 1))
		mock.ExpectQuery(srvQ).WillReturnRows(srvRows(1))
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil || !got.Hour.Equal(time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)) {
			t.Fatalf("got %+v, %v", got, err)
		}
	})

	t.Run("archives from several sources never extend the floor", func(t *testing.T) {
		// #1219: archive_state rows are per-source. Source A archived back to
		// 01:00; a baseline of a table that lives on source B must not inherit
		// A's coverage. The live floor stands and everything below it is
		// unknowable — not "ok" (missed alarm) and not "broken" (false alarm).
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080101", "p_2026080109", 2))
		// No bintrail_servers probe: the archive rows already answered it.
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil {
			t.Fatal(err)
		}
		if !got.Hour.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) || !got.BelowIsUnknown {
			t.Fatalf("got %+v — want the live floor with BelowIsUnknown", got)
		}
	})

	t.Run("one archived source but several known ones is still unattributable", func(t *testing.T) {
		// The half the archive rows cannot see: source B exists and simply has
		// not archived yet, so a B baseline would inherit A's floor.
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080101", "p_2026080109", 1))
		mock.ExpectQuery(srvQ).WillReturnRows(srvRows(2))
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil {
			t.Fatal(err)
		}
		if !got.Hour.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) || !got.BelowIsUnknown {
			t.Fatalf("got %+v — want the live floor with BelowIsUnknown", got)
		}
	})

	t.Run("missing bintrail_servers table is tolerated (legacy/file-mode index)", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080103", "p_2026080109", 1))
		mock.ExpectQuery(srvQ).WillReturnError(&mysql.MySQLError{Number: 1146, Message: "Table 'binlog_index.bintrail_servers' doesn't exist"})
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil || !got.Hour.Equal(time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)) || got.BelowIsUnknown {
			t.Fatalf("got %+v, %v — a legacy index keeps single-source semantics", got, err)
		}
	})

	t.Run("any other bintrail_servers error propagates", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080103", "p_2026080109", 1))
		mock.ExpectQuery(srvQ).WillReturnError(&mysql.MySQLError{Number: 1045, Message: "access denied"})
		if _, err := OldestDeltaFromDB(context.Background(), db, "binlog_index"); err == nil {
			t.Fatal("an unreadable source registry must propagate: it decides whether the floor is attributable")
		}
	})

	t.Run("missing archive_state table is tolerated (older index)", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnError(&mysql.MySQLError{Number: 1146, Message: "Table 'binlog_index.archive_state' doesn't exist"})
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil || !got.Hour.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) {
			t.Fatalf("got %+v, %v", got, err)
		}
	})

	t.Run("unparseable archive MAX propagates", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080103", "weird", 1))
		if _, err := OldestDeltaFromDB(context.Background(), db, "binlog_index"); err == nil {
			t.Fatal("unparseable MAX must propagate — contiguity cannot be judged")
		}
	})

	t.Run("any other archive_state error propagates", func(t *testing.T) {
		// The anti-cry-wolf direction: a swallowed archive error would make the
		// floor read LATER than reality and fabricate "broken" on healthy
		// archives. The caller must degrade to unknown instead.
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnError(&mysql.MySQLError{Number: 1045, Message: "access denied"})
		if _, err := OldestDeltaFromDB(context.Background(), db, "binlog_index"); err == nil {
			t.Fatal("non-1146 archive error must propagate")
		}
	})

	t.Run("unparseable archive MIN propagates", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("weird", "p_2026080105", 1))
		if _, err := OldestDeltaFromDB(context.Background(), db, "binlog_index"); err == nil {
			t.Fatal("unparseable archive floor must propagate, not silently drop")
		}
	})

	t.Run("archived hours naming no source are unattributable", func(t *testing.T) {
		// COUNT(DISTINCT bintrail_id) == 0 with a valid MIN means every
		// archive row has a NULL id (the column is nullable). Rows naming no
		// source are the strongest case of "cannot attribute" — reading the
		// zero as single-source would extend the floor with them.
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows("p_2026080110"))
		mock.ExpectQuery(archQ).WillReturnRows(archRows("p_2026080101", "p_2026080109", 0))
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil {
			t.Fatal(err)
		}
		if !got.Hour.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) || !got.BelowIsUnknown {
			t.Fatalf("got %+v — want the live floor with BelowIsUnknown", got)
		}
	})

	t.Run("empty index and empty archive is unknown, not an error", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(partsQ).WillReturnRows(partRows())
		mock.ExpectQuery(archQ).WillReturnRows(archRows(nil, nil, 0))
		got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
		if err != nil || !got.Hour.IsZero() || got.BelowIsUnknown {
			t.Fatalf("got %+v, %v — want zero, nil", got, err)
		}
	})
}

// TestWriteJSON_staleness pins the machine-consumed contract: per-baseline
// "staleness", the top-level "baseline_staleness", and the
// configured-but-unreadable case surfacing as "unknown" instead of vanishing.
func TestWriteJSON_staleness(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-100 * time.Hour)
	d := &StatusData{Baselines: []BaselineInfo{
		{Database: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour)},
		{Database: "shop", Table: "legacy", SnapshotTime: oldest.Add(-time.Hour)},
	}}
	AnnotateBaselineStaleness(d.Baselines, DeltaFloor{Hour: oldest}, now)
	var buf bytes.Buffer
	if err := d.WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `"staleness": "broken"`) || !strings.Contains(out, `"staleness": "ok"`) {
		t.Fatalf("per-baseline staleness missing:\n%s", out)
	}
	if !strings.Contains(out, `"baseline_staleness": "broken"`) {
		t.Fatalf("top-level baseline_staleness missing:\n%s", out)
	}

	// Configured-but-unreadable baseline dir: the field must read "unknown",
	// not be omitted — a monitor watching it would read absence as healthy.
	d = &StatusData{BaselinesUnavailable: true}
	buf.Reset()
	if err := d.WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"baseline_staleness": "unknown"`) {
		t.Fatalf("unreadable baseline dir must yield unknown:\n%s", buf.String())
	}
}

func TestOverallBaselineStaleness_newestPerTable(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-100 * time.Hour)
	baselines := []BaselineInfo{
		// A superseded broken snapshot must NOT drive the headline…
		{Database: "shop", Table: "orders", SnapshotTime: oldest.Add(-24 * time.Hour)},
		{Database: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour)},
		// …but a table whose NEWEST snapshot is broken must.
		{Database: "shop", Table: "legacy", SnapshotTime: oldest.Add(-time.Hour)},
	}
	AnnotateBaselineStaleness(baselines, DeltaFloor{Hour: oldest}, now)
	if baselines[0].Staleness != BaselineBroken || baselines[1].Staleness != BaselineOK {
		t.Fatalf("per-entry annotation wrong: %+v", baselines)
	}
	if got := OverallBaselineStaleness(baselines); got != BaselineBroken {
		t.Fatalf("overall = %s, want broken (legacy's newest is broken)", got)
	}

	// Without the broken table, the superseded broken snapshot is ignored.
	if got := OverallBaselineStaleness(baselines[:2]); got != BaselineOK {
		t.Fatalf("overall = %s, want ok (orders' newest is fresh)", got)
	}
	if got := OverallBaselineStaleness(nil); got != "" {
		t.Fatalf("empty list must have no verdict, got %q", got)
	}
}

func TestWriteBaselines_stalenessColumnAndBanner(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-100 * time.Hour)
	baselines := []BaselineInfo{
		{Database: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour)},
		{Database: "shop", Table: "orders", SnapshotTime: oldest.Add(-time.Hour)}, // superseded
		{Database: "shop", Table: "legacy", SnapshotTime: oldest.Add(-time.Hour)},
	}
	AnnotateBaselineStaleness(baselines, DeltaFloor{Hour: oldest}, now)
	var buf bytes.Buffer
	writeBaselines(&buf, baselines)
	out := buf.String()
	if !strings.Contains(out, "STALENESS") || !strings.Contains(out, "⚠ broken") {
		t.Fatalf("staleness column missing:\n%s", out)
	}
	// The ⚠ glyph is reserved for the newest-per-table rows the banner keys
	// on; the superseded orders row renders plain "broken" (routine on a
	// healthy retention cadence — the console's rule).
	if strings.Count(out, "⚠ broken") != 1 || strings.Count(out, "broken") != 2 {
		t.Fatalf("glyph must mark only the newest broken row (superseded rows plain):\n%s", out)
	}
	if !strings.Contains(out, "BASELINE STALE — FULL-TABLE RESTORE BROKEN") {
		t.Fatalf("broken banner missing:\n%s", out)
	}

	// All fresh: no banner, quiet "ok" verdicts.
	fresh := []BaselineInfo{{Database: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour)}}
	AnnotateBaselineStaleness(fresh, DeltaFloor{Hour: oldest}, now)
	buf.Reset()
	writeBaselines(&buf, fresh)
	if strings.Contains(buf.String(), "BASELINE STALE") {
		t.Fatalf("banner must not fire without a broken newest snapshot:\n%s", buf.String())
	}
}
