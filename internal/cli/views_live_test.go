package cli

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/views"
)

// TestLiveIndexFromDSN_dropsThePassword guards the boundary the password
// actually crosses.
//
// The index DSN carries it, liveIndexFromDSN is the only place it is in scope,
// and everything downstream renders into a file the operator is meant to share.
// The views package has its own structural guard that LiveIndex grows no
// credential field; this one proves the value is dropped on the way in, which
// is the half that guard cannot see.
func TestLiveIndexFromDSN_dropsThePassword(t *testing.T) {
	const pw = "s3cr3t-index-password"
	li, err := liveIndexFromDSN("bintrail:" + pw + "@tcp(db.internal:3307)/bintrail_index")
	if err != nil {
		t.Fatalf("liveIndexFromDSN: %v", err)
	}

	// Rendered, not just inspected field by field: a future field would be
	// invisible to an enumeration written today, and the rendered file is what
	// actually reaches another person.
	out := views.Generate(views.Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: []string{"/a/bintrail_id=x"},
		LiveIndex:      li,
	})
	if strings.Contains(out, pw) {
		t.Fatal("the index password reached the generated file, which is meant to be shared")
	}

	// The non-secret half must survive, or the file cannot be used at all.
	if li.Host != "db.internal" || li.Port != 3307 || li.Database != "bintrail_index" || li.User != "bintrail" {
		t.Errorf("connection facts lost: %+v", li)
	}
}

// TestLiveIndexFromDSN_refusesWithoutADatabase: a DSN with no database would
// render an ATTACH naming nothing, which fails in the operator's DuckDB with an
// error about a catalog rather than about their flag.
func TestLiveIndexFromDSN_refusesWithoutADatabase(t *testing.T) {
	if _, err := liveIndexFromDSN("bintrail:pw@tcp(db.internal:3307)/"); err == nil {
		t.Error("expected a refusal for a DSN that names no database")
	}
}

// TestLiveIndexFromDSN_errorsNameTheFlagTheOperatorTyped.
//
// The refusals used to come from config.ParseSourceDSN, so a socket DSN passed
// to --index-dsn was refused with "--source-dsn uses a unix socket; binlog
// replication requires a TCP address": a flag this command does not have, and a
// justification about replication for a command that generates text. An
// operator cannot act on either.
func TestLiveIndexFromDSN_errorsNameTheFlagTheOperatorTyped(t *testing.T) {
	for _, dsn := range []string{
		"bintrail:pw@unix(/var/run/mysqld/mysqld.sock)/bintrail_index",
		"bintrail:pw@tcp(db.internal:3307)/",
	} {
		_, err := liveIndexFromDSN(dsn)
		if err == nil {
			t.Fatalf("%s: expected a refusal", dsn)
		}
		msg := err.Error()
		if !strings.Contains(msg, "--index-dsn") {
			t.Errorf("%s: the refusal does not name the flag that was passed: %s", dsn, msg)
		}
		for _, wrong := range []string{"--source-dsn", "binlog replication"} {
			if strings.Contains(msg, wrong) {
				t.Errorf("%s: the refusal talks about %q, which has nothing to do with this command: %s",
					dsn, wrong, msg)
			}
		}
	}
}

// TestLiveIndexFromDSN_defaultsToLoopbackVisibly documents the shape finding 5
// is about: a DSN with no address yields the driver's local default, which the
// generated file then carries as if it were a location.
func TestLiveIndexFromDSN_defaultsToLoopbackVisibly(t *testing.T) {
	li, err := liveIndexFromDSN("root:pw@/bintrail_index")
	if err != nil {
		t.Fatalf("liveIndexFromDSN: %v", err)
	}
	if li.Host != "127.0.0.1" {
		t.Fatalf("host = %q, want the driver's loopback default", li.Host)
	}
	out := views.Generate(views.Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: []string{"/a/bintrail_id=x"},
		LiveIndex:      li,
	})
	if !strings.Contains(out, "loopback address") {
		t.Error("the file carries a loopback host as a location, with nothing telling a reader " +
			"elsewhere that it names the generating machine")
	}
}

const columnsQuery = "SELECT COLUMN_NAME FROM information_schema.COLUMNS"

func columnRows() *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"COLUMN_NAME"})
	for _, c := range []string{"event_id", "event_timestamp", "event_type"} {
		rows.AddRow(c)
	}
	return rows
}

// TestAttributeLiveIndex_statesOnlyWhatItObserved is the test whose absence let
// one sentence stand for four different observations.
//
// The probe was `err == nil && n == 1`, so EVERY other outcome — a file-mode
// index that registers no server and serves exactly one source, an index too
// old to have the table, an account without SELECT on it, a dropped connection
// — produced the same file, claiming the index served more than one source.
// None of the error branches had a test at all, which is what let it ship.
func TestAttributeLiveIndex_statesOnlyWhatItObserved(t *testing.T) {
	cases := []struct {
		name   string
		expect func(sqlmock.Sqlmock)
		wantID string
		wantAt views.LiveAttribution
	}{
		{
			name: "exactly one registered source is attributable",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnRows(
					sqlmock.NewRows([]string{"n", "id"}).AddRow(1, "the-id"))
			},
			wantID: "the-id",
		},
		{
			name: "two sources cannot be told apart",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnRows(
					sqlmock.NewRows([]string{"n", "id"}).AddRow(2, "one-of-them"))
			},
			wantAt: views.AttributionMultiSource,
		},
		{
			// `bintrail index --binlog-dir` never registers a server, so the
			// index serves exactly ONE source and reports zero. Calling that
			// multi-source was the inversion.
			name: "no rows is a single unregistered source",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnRows(
					sqlmock.NewRows([]string{"n", "id"}).AddRow(0, ""))
			},
			wantAt: views.AttributionUnregistered,
		},
		{
			// The column is nullable (see internal/status/staleness.go): one
			// row with no id is still no id.
			name: "one row with no id has nothing to attribute with",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnRows(
					sqlmock.NewRows([]string{"n", "id"}).AddRow(1, ""))
			},
			wantAt: views.AttributionUnregistered,
		},
		{
			// Read exactly the way status.knownSourceCount reads it: a legacy
			// or file-mode index has zero known sources, not an unreadable
			// list. Identical input, opposite verdict, in the same repo.
			name: "no such table is zero sources, not a failure",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnError(
					&drivermysql.MySQLError{Number: 1146, Message: "Table 'idx.bintrail_servers' doesn't exist"})
			},
			wantAt: views.AttributionUnregistered,
		},
		{
			name: "no permission on the table is undetermined",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnError(
					&drivermysql.MySQLError{Number: 1142, Message: "SELECT command denied to user"})
			},
			wantAt: views.AttributionUndetermined,
		},
		{
			name: "a dropped connection is undetermined",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnError(errors.New("invalid connection"))
			},
			wantAt: views.AttributionUndetermined,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()
			tc.expect(mock)

			li := &views.LiveIndex{Database: "idx"}
			attributeLiveIndex(context.Background(), db, li)

			if li.BintrailID != tc.wantID {
				t.Errorf("BintrailID = %q, want %q", li.BintrailID, tc.wantID)
			}
			if li.Attribution != tc.wantAt {
				t.Errorf("Attribution = %d, want %d", li.Attribution, tc.wantAt)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}

// TestDescribeLiveIndex_explicitIDIsNotSecondGuessed: --bintrail-id also scoped
// the archive paths this same file reads, so the registry cannot be more
// authoritative about the id than the operator who named it. The attribution
// query is not run at all — sqlmock fails on a query nobody expected, which is
// the assertion.
func TestDescribeLiveIndex_explicitIDIsNotSecondGuessed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(columnsQuery).WillReturnRows(columnRows())

	li := &views.LiveIndex{Database: "idx"}
	if err := describeLiveIndex(context.Background(), db, li, "named-by-the-operator"); err != nil {
		t.Fatalf("describeLiveIndex: %v", err)
	}
	if li.BintrailID != "named-by-the-operator" {
		t.Errorf("BintrailID = %q, want the explicitly named id", li.BintrailID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestLiveTableColumns covers the probe that keeps the generated file BINDING.
//
// The hot leg names columns; an index migrated to an earlier point than this
// build's schema does not have all of them, and DuckDB answers a name it cannot
// resolve with a binder error that kills the whole statement — no events view
// at all. That is why this one is not best effort.
func TestLiveTableColumns(t *testing.T) {
	t.Run("returns what the index has", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(columnsQuery).WithArgs("idx").WillReturnRows(columnRows())

		cols, err := liveTableColumns(context.Background(), db, "idx")
		if err != nil {
			t.Fatalf("liveTableColumns: %v", err)
		}
		if len(cols) != 3 || cols[0] != "event_id" {
			t.Errorf("columns = %v", cols)
		}
	})

	t.Run("no table is a refusal that says which one", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(columnsQuery).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}))

		_, err = liveTableColumns(context.Background(), db, "idx")
		if err == nil {
			t.Fatal("expected a refusal when the index has no binlog_events")
		}
		for _, want := range []string{"binlog_events", "idx"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal does not name %q: %v", want, err)
			}
		}
	})

	t.Run("an unreadable column list fails the command", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(columnsQuery).WillReturnError(errors.New("connection refused"))

		if _, err := liveTableColumns(context.Background(), db, "idx"); err == nil {
			t.Fatal("an unknown column set was accepted; the file it generates may not bind at all")
		}
	})
}
