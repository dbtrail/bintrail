package metadata

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func implicitPeriodFixture() []columnRow {
	return []columnRow{
		{schemaName: "svdb", tableName: "imp", columnName: "id", ordinalPosition: 1, columnKey: "PRI", dataType: "int", columnType: "int(11)", isNullable: "NO"},
		{schemaName: "svdb", tableName: "imp", columnName: "val", ordinalPosition: 2, dataType: "varchar", columnType: "varchar(20)", isNullable: "YES"},
		{schemaName: "svdb", tableName: "plain", columnName: "id", ordinalPosition: 1, columnKey: "PRI", dataType: "int", columnType: "int(11)", isNullable: "NO"},
		{schemaName: "svdb", tableName: "expl", columnName: "id", ordinalPosition: 1, columnKey: "PRI", dataType: "int", columnType: "int(11)", isNullable: "NO"},
		{schemaName: "svdb", tableName: "expl", columnName: "row_start", ordinalPosition: 2, dataType: "timestamp", columnType: "timestamp(6)", isNullable: "NO",
			generationExpression: sql.NullString{Valid: true, String: "ROW START"}},
		{schemaName: "svdb", tableName: "expl", columnName: "row_end", ordinalPosition: 3, columnKey: "PRI", dataType: "timestamp", columnType: "timestamp(6)", isNullable: "NO",
			generationExpression: sql.NullString{Valid: true, String: "ROW END"}},
	}
}

// TestAddImplicitPeriodColumns_synthesizesHiddenColumns pins the #1272
// synthesis shape: an implicitly-versioned table gains row_start/row_end at
// the END of its ordinal range, TIMESTAMP(6) NOT NULL, with row_end a PK
// member and BOTH carrying the ROW START/ROW END generation expressions so
// the existing is_generated derivation (and the #1266 gates downstream) treat
// them exactly like the explicit form. An EXPLICITLY-versioned table and a
// plain table pass through untouched.
func TestAddImplicitPeriodColumns_synthesizesHiddenColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SYSTEM VERSIONED").WithArgs("svdb").WillReturnRows(
		sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}).
			AddRow("svdb", "imp").
			AddRow("svdb", "expl"))

	out, err := addImplicitPeriodColumns(db, []string{"svdb"}, implicitPeriodFixture())
	if err != nil {
		t.Fatalf("addImplicitPeriodColumns: %v", err)
	}
	if len(out) != len(implicitPeriodFixture())+2 {
		t.Fatalf("got %d columns, want %d (exactly two synthetic rows for the implicit table)", len(out), len(implicitPeriodFixture())+2)
	}

	byTable := make(map[string][]columnRow)
	for _, c := range out {
		byTable[c.tableName] = append(byTable[c.tableName], c)
	}
	if len(byTable["plain"]) != 1 || len(byTable["expl"]) != 3 {
		t.Fatalf("plain/explicit tables must pass through untouched: plain=%d expl=%d", len(byTable["plain"]), len(byTable["expl"]))
	}

	imp := byTable["imp"]
	if len(imp) != 4 {
		t.Fatalf("implicit table has %d columns, want 4: %+v", len(imp), imp)
	}
	rs, re := imp[2], imp[3]
	if rs.columnName != "row_start" || rs.ordinalPosition != 3 || rs.columnKey != "" ||
		rs.columnType != "timestamp(6)" || rs.isNullable != "NO" ||
		!rs.generationExpression.Valid || rs.generationExpression.String != "ROW START" {
		t.Errorf("synthetic row_start has the wrong shape: %+v", rs)
	}
	if re.columnName != "row_end" || re.ordinalPosition != 4 || re.columnKey != "PRI" ||
		re.columnType != "timestamp(6)" || re.isNullable != "NO" ||
		!re.generationExpression.Valid || re.generationExpression.String != "ROW END" {
		t.Errorf("synthetic row_end has the wrong shape: %+v", re)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestAddImplicitPeriodColumns_noVersionedTablesIsNoOp pins the MySQL-source
// path: no table reports TABLE_TYPE 'SYSTEM VERSIONED', so the input passes
// through unchanged.
func TestAddImplicitPeriodColumns_noVersionedTablesIsNoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SYSTEM VERSIONED").WithArgs("svdb").WillReturnRows(
		sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}))

	in := implicitPeriodFixture()
	out, err := addImplicitPeriodColumns(db, []string{"svdb"}, in)
	if err != nil {
		t.Fatalf("addImplicitPeriodColumns: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("no-op path changed the column count: got %d, want %d", len(out), len(in))
	}
}
