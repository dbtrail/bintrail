package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestValidateBinlogRowImageRefusalIsClassed: `stream` refuses a MINIMAL
// source without the doctor in front, so the refusal itself must carry the
// config_invalid class (#1503). Driven through the real query, not a
// hand-built value.
func TestValidateBinlogRowImageRefusalIsClassed(t *testing.T) {
	for name, rows := range map[string]*sqlmock.Rows{
		"minimal": sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_row_image", "MINIMAL"),
		"absent":  sqlmock.NewRows([]string{"Variable_name", "Value"}),
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SHOW VARIABLES LIKE 'binlog_row_image'").WillReturnRows(rows)

			err = ValidateBinlogRowImageContext(context.Background(), db)
			var ri *RowImageError
			if !errors.As(err, &ri) {
				t.Fatalf("got %T (%v), want *RowImageError", err, err)
			}
			if got := telemetry.ClassifyError(err); got != telemetry.ClassConfigInvalid {
				t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassConfigInvalid)
			}
		})
	}

	// A query failure is not a refusal: it keeps its own cause.
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SHOW VARIABLES LIKE 'binlog_row_image'").WillReturnError(errors.New("boom"))
	var ri *RowImageError
	if err := ValidateBinlogRowImageContext(context.Background(), db); errors.As(err, &ri) {
		t.Errorf("a query error must not be reported as a row-image refusal: %v", err)
	}
}

// TestValidateBinlogFormatRefusalIsClassed: the format check runs AHEAD of the
// row-image check at every call site, so an untyped refusal here would have
// hidden the typed one below it (#1503 review).
func TestValidateBinlogFormatRefusalIsClassed(t *testing.T) {
	for name, rows := range map[string]*sqlmock.Rows{
		"statement": sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_format", "STATEMENT"),
		"absent":    sqlmock.NewRows([]string{"Variable_name", "Value"}),
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SHOW VARIABLES LIKE 'binlog_format'").WillReturnRows(rows)

			err = ValidateBinlogFormatContext(context.Background(), db)
			var bf *BinlogFormatError
			if !errors.As(err, &bf) {
				t.Fatalf("got %T (%v), want *BinlogFormatError", err, err)
			}
			if got := telemetry.ClassifyError(err); got != telemetry.ClassConfigInvalid {
				t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassConfigInvalid)
			}
		})
	}
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SHOW VARIABLES LIKE 'binlog_format'").WillReturnError(errors.New("boom"))
	var bf *BinlogFormatError
	if err := ValidateBinlogFormatContext(context.Background(), db); errors.As(err, &bf) {
		t.Errorf("a query error must not be reported as a format refusal: %v", err)
	}
}
