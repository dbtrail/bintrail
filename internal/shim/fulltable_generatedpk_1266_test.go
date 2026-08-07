package shim

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// svShimResolver stubs the MariaDB system-versioning PK shape (#1266): the PK
// silently extended with the STORED GENERATED ROW END period column.
func svShimResolver() func() (*metadata.Resolver, error) {
	return func() (*metadata.Resolver, error) {
		return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
			"app.orders": {Schema: "app", Table: "orders", PKColumns: []string{"id", "row_end"},
				Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
					{Name: "val", OrdinalPosition: 2, DataType: "varchar"},
					{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", ColumnType: "timestamp(6)", IsGenerated: true},
				}},
		}), nil
	}
}

func assertGeneratedPKWireError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("versioned-PK full-table view must refuse, got nil")
	}
	var myErr *gomysql.MyError
	if !errors.As(err, &myErr) {
		t.Fatalf("error = %T %v, want *mysql.MyError", err, err)
	}
	if myErr.Code != gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE {
		t.Errorf("wire code = %d, want %d (ER_NO_PARTITION_FOR_GIVEN_VALUE)", myErr.Code, gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE)
	}
	if !strings.Contains(myErr.Message, "generated column") || !strings.Contains(myErr.Message, "row_end") {
		t.Errorf("refusal must name the generated PK member: %q", myErr.Message)
	}
	// The sibling refusals steer to "use _flashback for a binlog-only view";
	// this one must NOT — the binlog-only view is exactly the corrupt one for
	// a versioned table (history rows as inserts, deletes as row_end updates).
	// The "resolve <type>: " prefix is stripped first: the query type itself
	// legitimately reads "_flashback" on that path, so a whole-message match
	// would either false-fail or have to settle for a phrase the message can
	// dodge by rewording.
	_, body, _ := strings.Cut(myErr.Message, ": ")
	if strings.Contains(body, "_flashback") || strings.Contains(body, "binlog-only view") {
		t.Errorf("versioned-PK refusal must not steer to _flashback or the binlog-only view: %q", myErr.Message)
	}
}

// TestRunSnapshotFullTable_generatedPKRefusal drives the baseline-merge path
// (#1266): the timestamp type passes the SupportedPKType loop, so without the
// gate the merge would die per-row with MissingPKColumnError surfaced as a
// generic wire error. A zero-value Handler with only a resolver suffices:
// Gate.Acquire is nil-safe by design (nil = unlimited), so deleting the gate
// does not panic — the run degrades past the empty baseline dir and the test
// fails on the message assertions instead.
func TestRunSnapshotFullTable_generatedPKRefusal(t *testing.T) {
	h := &Handler{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg:        Config{BaselineDir: t.TempDir()},
		resolverFn: svShimResolver(),
	}
	_, err := h.runSnapshotFullTable(TimeTravelQuery{
		Type: TypeSnapshot, Schema: "app", Table: "orders", AsOf: time.Now(),
	})
	assertGeneratedPKWireError(t, err)
}

// TestRunFullTable_generatedPKRefusal drives the binlog-only path (#1266) —
// the _flashback full table and _snapshot's no-baseline fallback. Before the
// gate this path was silent corruption: with no baseline probe to fail
// loudly, a versioned table's history-row inserts fold under their own full
// pk_values and render as duplicate live rows, and a versioned DELETE (an
// Update_rows tombstone, never a Delete_rows) stays live.
func TestRunFullTable_generatedPKRefusal(t *testing.T) {
	h := &Handler{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		resolverFn: svShimResolver(),
	}
	_, err := h.runFullTable(TimeTravelQuery{
		Type: TypeFlashback, Schema: "app", Table: "orders", AsOf: time.Now(),
	})
	assertGeneratedPKWireError(t, err)
}

// TestRunSnapshotFullTable_emptyDataTypeKeepsWrongPathVerdict pins the shim's
// gate ordering: a PK member with an empty DataType (the PG snapshot shape,
// #1009/#1198) that is ALSO marked generated must keep the wrong-path verdict,
// never the MariaDB-shaped generated-PK message.
func TestRunSnapshotFullTable_emptyDataTypeKeepsWrongPathVerdict(t *testing.T) {
	h := &Handler{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg:    Config{BaselineDir: t.TempDir()},
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"app.orders": {Schema: "app", Table: "orders", PKColumns: []string{"id"},
					Columns: []metadata.ColumnMeta{
						{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "", IsGenerated: true},
					}},
			}), nil
		},
	}
	_, err := h.runSnapshotFullTable(TimeTravelQuery{
		Type: TypeSnapshot, Schema: "app", Table: "orders", AsOf: time.Now(),
	})
	if err == nil {
		t.Fatal("expected the PG wrong-path refusal, got nil")
	}
	var myErr *gomysql.MyError
	if !errors.As(err, &myErr) {
		t.Fatalf("error = %T %v, want *mysql.MyError", err, err)
	}
	if strings.Contains(myErr.Message, "generated column") {
		t.Errorf("empty DataType must win the #1009 wrong-path verdict, got the generated-PK message: %q", myErr.Message)
	}
	if !strings.Contains(myErr.Message, "PG snapshot shape") {
		t.Errorf("want the PG wrong-path verdict, got: %q", myErr.Message)
	}
}
