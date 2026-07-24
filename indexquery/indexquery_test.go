package indexquery

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// Compile-level pins for the wrappers that need a live database or write to
// stdout — the exported signatures are the contract embedding code builds
// against.
var (
	_ func(*sql.DB) *Engine = New
	_ func(*sql.DB) error   = EnsureSchema
	_ func(any) error       = OutputJSON
)

// TestFetchMergedValidatesBeforeAnyDBWork exercises the wrapper on the
// validation path: NoArchive=false with a nil ArchiveFetcher is rejected up
// front, so the nil db/engine prove no DB work happened.
func TestFetchMergedValidatesBeforeAnyDBWork(t *testing.T) {
	_, _, err := FetchMerged(context.Background(), nil, nil, FetchMergedOptions{})
	if err == nil || !strings.Contains(err.Error(), "ArchiveFetcher") {
		t.Fatalf("err = %v, want the ArchiveFetcher validation error", err)
	}
}

// TestFetchMergedOptionsConstructibleThroughAliases pins that every field an
// external module needs — including the ArchiveFetcher function type — is
// reachable through the aliases.
func TestFetchMergedOptionsConstructibleThroughAliases(t *testing.T) {
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	var fetcher ArchiveFetcher = func(context.Context, Options, string) ([]ResultRow, error) {
		return nil, nil
	}
	o := FetchMergedOptions{
		Opts:           Options{Schema: "shop", Table: "orders", Since: &since, Limit: 10},
		DBName:         "bintrail_index",
		AllowGaps:      true,
		ArchiveFetcher: fetcher,
	}
	if o.Opts.Schema != "shop" || o.ArchiveFetcher == nil {
		t.Fatal("alias construction lost fields")
	}
	if New(nil) == nil {
		t.Fatal("New returned nil engine")
	}
}

func TestConnectRejectsInvalidDSN(t *testing.T) {
	if _, err := Connect("this is not a dsn"); err == nil {
		t.Fatal("Connect accepted an invalid DSN")
	}
}

func TestParseSourceDSN(t *testing.T) {
	host, port, user, pass, err := ParseSourceDSN("u:p@tcp(db.example:3307)/shop")
	if err != nil {
		t.Fatal(err)
	}
	if host != "db.example" || port != 3307 || user != "u" || pass != "p" {
		t.Errorf("ParseSourceDSN = (%q, %d, %q, %q)", host, port, user, pass)
	}
	if _, _, _, _, err := ParseSourceDSN("not a dsn"); err == nil {
		t.Error("ParseSourceDSN accepted garbage")
	}
}

func TestDuckDBTuningWrappers(t *testing.T) {
	cmd := &cobra.Command{Use: "x"}
	AddDuckDBTuningFlags(cmd)
	for _, name := range []string{"ultrafast", "duckdb-threads", "duckdb-memory-limit"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("flag --%s not registered", name)
		}
	}
	tuning, err := DuckDBTuningFromFlags(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if TunedArchiveFetcher(tuning) == nil {
		t.Fatal("TunedArchiveFetcher returned nil")
	}
}

func TestFormatGapWarningNonEmpty(t *testing.T) {
	if FormatGapWarning([]time.Time{time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)}) == "" {
		t.Fatal("FormatGapWarning returned empty string")
	}
}

func TestWrapSchemaMigrationErr(t *testing.T) {
	if WrapSchemaMigrationErr(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	base := errors.New("boom")
	if wrapped := WrapSchemaMigrationErr(base); !errors.Is(wrapped, base) {
		t.Fatalf("wrapped = %v, want a wrap of the base error", wrapped)
	}
}
