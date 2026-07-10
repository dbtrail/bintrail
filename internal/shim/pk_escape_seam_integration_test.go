//go:build integration

package shim

import (
	"encoding/json"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestPKValueEscapeSeam_BackslashAndPipe pins the regression both
// adversarial reviews of PR #928 found: stripQuotes/unescapeStringLiteral
// (#826) hands runPointInTime/runDiff a RAW, MySQL-unescaped PK literal, but
// binlog_events.pk_values is stored in event.BuildPKValues-ENCODED form
// (backslash doubled, pipe escaped). Without re-encoding the value at the
// match seam, a backslash- or pipe-containing PK silently misses —
// regressing the exact "false empty resultset" scenario #826 was filed to
// fix. This test seeds a REAL pk_values-encoded row and drives it through
// Parse() plus the real handler methods, so a fix that only pins the parser
// layer (as PR #928's own tests did) cannot go green over this bug.
func TestPKValueEscapeSeam_BackslashAndPipe(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	eventTS := hourTop.Add(5 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)

	cases := []struct {
		name    string
		rawPK   string // what stripQuotes/unescapeStringLiteral hands back
		literal string // the MySQL string literal as written in SQL
	}{
		{"backslash path", `C:\temp\new`, `'C:\\temp\\new'`},
		{"pipe-containing PK", `a|b`, `'a|b'`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// This is exactly what a real capture writes for this PK value —
			// event.BuildPKValues is the single source of truth for the
			// at-rest encoding.
			encodedPK := event.EscapePKValue(tc.rawPK)
			rowAfter, err := json.Marshal(map[string]any{"path": tc.rawPK, "name": "n"})
			if err != nil {
				t.Fatal(err)
			}
			testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200,
				eventTS.Format("2006-01-02 15:04:05"), nil,
				"myapp", "files", 1 /*insert*/, encodedPK, nil, nil, rowAfter)

			h := NewHandlerWithConfig(db, Config{
				AllowGaps:   true,
				NoArchive:   true,
				IndexDBName: dbName,
			}, slog.Default())

			sqlText := "SELECT * FROM _flashback.files AS OF '" +
				asOf.Format("2006-01-02 15:04:05") + "' WHERE path = " + tc.literal
			q, perr := Parse(sqlText, "myapp")
			if perr != nil {
				t.Fatalf("Parse: %v", perr)
			}
			if q.PKValue != tc.rawPK {
				t.Fatalf("sanity: Parse PKValue = %q, want %q (parser-layer contract from #826)", q.PKValue, tc.rawPK)
			}

			// _flashback and _diff go through independent match seams
			// (handler.go's two separate PKValues assignments) — run each
			// as its own subtest so a regression isolated to just one of
			// them still shows up, instead of one Fatalf hiding the other.
			t.Run("_flashback", func(t *testing.T) {
				res, err := h.runPointInTime(q)
				if err != nil {
					t.Fatalf("runPointInTime: %v", err)
				}
				if got := len(res.Resultset.RowDatas); got != 1 {
					t.Fatalf("_flashback: expected 1 row for PK %q (stored pk_values=%q), got %d — encoding seam regression", tc.rawPK, encodedPK, got)
				}
			})

			t.Run("_diff", func(t *testing.T) {
				diffQ := q
				diffQ.Type = TypeDiff
				diffQ.Since = eventTS.Add(-time.Minute)
				diffQ.Until = asOf
				diffRes, err := h.runDiff(diffQ)
				if err != nil {
					t.Fatalf("runDiff: %v", err)
				}
				if got := len(diffRes.Resultset.RowDatas); got != 1 {
					t.Fatalf("_diff: expected 1 row for PK %q, got %d — encoding seam regression", tc.rawPK, got)
				}
			})
		})
	}
}

// TestSnapshotBaseline_BackslashPK_DeltaAppliesAfterBaseline pins the
// second, more dangerous half of the regression: on _snapshot,
// ReadBaselineRow correctly receives the RAW PK value (baseline Parquet
// rows store actual column values, not the encoded pk_values form), so the
// baseline lookup matches — but the post-baseline DELTA fetch was, before
// this fix, ALSO issued with the raw value and therefore never matched the
// event.BuildPKValues-encoded pk_values a post-baseline UPDATE is stored
// under. That silently serves the (now-stale) baseline image as the answer
// instead of applying the delta — worse than an empty resultset, because it
// looks like a valid answer.
func TestSnapshotBaseline_BackslashPK_DeltaAppliesAfterBaseline(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")

	rawPK := `C:\temp\new`
	encodedPK := event.EscapePKValue(rawPK)

	snapTS := snapTime.UTC().Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "files", "path", 1, "PRI", "varchar", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "files", "owner", 2, "", "varchar", "YES")

	cols := []baseline.Column{
		{Name: "path", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		{Name: "owner", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "files", cols, [][]string{
		{rawPK, "alice"},
	})

	// Post-baseline UPDATE changes owner. Stored pk_values is the ENCODED
	// form — exactly what a real capture writes via event.BuildPKValues.
	rowBefore, err := json.Marshal(map[string]any{"path": rawPK, "owner": "alice"})
	if err != nil {
		t.Fatal(err)
	}
	rowAfter, err := json.Marshal(map[string]any{"path": rawPK, "owner": "bob"})
	if err != nil {
		t.Fatal(err)
	}
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "files", 2 /*update*/, encodedPK, nil, rowBefore, rowAfter)

	h := NewHandlerWithConfig(db, Config{
		AllowGaps:   true,
		NoArchive:   true,
		IndexDBName: dbName,
		BaselineDir: baselineDir,
	}, slog.Default())

	q := TimeTravelQuery{Type: TypeSnapshot, Schema: "myapp", Table: "files", PKColumn: "path", PKValue: rawPK, AsOf: asOf}
	res, err := h.runSnapshot(q)
	if err != nil {
		t.Fatalf("runSnapshot: %v", err)
	}
	cells := rowCells(t, res.Resultset)
	if len(cells) != 1 {
		t.Fatalf("expected 1 row, got %d", len(cells))
	}
	if want := []string{rawPK, "bob"}; !slices.Equal(cells[0], want) {
		t.Errorf("_snapshot row = %v, want %v — post-baseline UPDATE not applied (stale baseline served)", cells[0], want)
	}
}
