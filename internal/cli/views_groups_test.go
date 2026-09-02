package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/views"
)

const groupTestID = "aaaa"

func groupTestBase() string { return "/arc/bintrail_id=" + groupTestID }

func groupTestFile(hour string) string {
	return groupTestBase() + "/event_date=2026-05-01/event_hour=" + hour + "/e.parquet"
}

// The whole point of #1535 reaching the downloadable file: `bintrail views`
// must carry the groups, or the file it writes still binds one footer per
// archived file in the operator's own DuckDB.
func TestResolveArchiveGroupsFrom_carriesTheGroups(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "local_path", "s3_key", "column_set"}).
			AddRow(groupTestID, groupTestFile("03"), nil, "event_id,query_text").
			AddRow(groupTestID, groupTestFile("04"), nil, "event_id"))

	in := views.Input{ArchiveSources: []string{groupTestBase()}}
	if err := resolveArchiveGroupsFrom(context.Background(), db, &in); err != nil {
		t.Fatal(err)
	}
	if in.UngroupedPartitions != 0 {
		t.Fatalf("UngroupedPartitions = %d, want 0", in.UngroupedPartitions)
	}
	if len(in.ArchiveGroups) != 2 {
		t.Fatalf("%d group(s) reached the generator, want 2", len(in.ArchiveGroups))
	}
	// Rendered, not just carried: the field is only worth anything if the file
	// it produces actually names the files instead of the glob.
	sql := views.Generate(views.Input{
		ArchiveSources:      in.ArchiveSources,
		ArchiveGroups:       in.ArchiveGroups,
		UngroupedPartitions: in.UngroupedPartitions,
	})
	if strings.Contains(sql, "union_by_name = true") {
		t.Errorf("the generated file still asks DuckDB to unify every footer:\n%s", sql)
	}
	if !strings.Contains(sql, groupTestFile("03")) {
		t.Errorf("the generated file does not name the archived partitions:\n%s", sql)
	}
}

// The all-or-nothing rule. One unrecorded partition and the file keeps the
// globbed leg, because the group list comes from the registry: grouping a
// partial registry would leave the unrecorded partitions out of the view
// entirely, which is a wrong answer rather than a slow one.
func TestResolveArchiveGroupsFrom_partialRegistryKeepsTheGlob(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "local_path", "s3_key", "column_set"}).
			AddRow(groupTestID, groupTestFile("03"), nil, "event_id,query_text").
			AddRow(groupTestID, groupTestFile("04"), nil, nil))

	in := views.Input{ArchiveSources: []string{groupTestBase()}}
	if err := resolveArchiveGroupsFrom(context.Background(), db, &in); err != nil {
		t.Fatal(err)
	}
	if len(in.ArchiveGroups) != 0 {
		t.Fatalf("grouping was used over a partial registry: %d group(s)", len(in.ArchiveGroups))
	}
	if in.UngroupedPartitions != 1 {
		t.Fatalf("UngroupedPartitions = %d, want 1", in.UngroupedPartitions)
	}

	// And the file says so, with the command that fixes it. An operator who
	// cannot see why the wait is still there has no way to act on it.
	sql := views.Generate(views.Input{
		ArchiveSources:      in.ArchiveSources,
		UngroupedPartitions: in.UngroupedPartitions,
	})
	if !strings.Contains(sql, "no recorded column set") {
		t.Errorf("the file does not explain why it still binds every footer:\n%s", sql)
	}
	if !strings.Contains(sql, "archive reconcile --repair") {
		t.Errorf("the file names no way to fix it:\n%s", sql)
	}
}
