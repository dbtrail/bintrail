package console

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestWriteFetchErrorWrapped1054 pins the legacy-registry-index mapping for
// WRAPPED errors: the recover-cascade path wraps the fetch failure
// (fmt.Errorf %w), and writeFetchError must still unwrap the MySQL 1054 on a
// post-initial-schema column (query_text, #699) into the actionable 422
// instead of a cryptic 500.
func TestWriteFetchErrorWrapped1054(t *testing.T) {
	inner := &mysql.MySQLError{Number: 1054, Message: "Unknown column 'be.query_text' in 'field list'"}
	err := fmt.Errorf("fetch parent deletes: %w", inner)

	rec := httptest.NewRecorder()
	writeFetchError(rec, err)

	if rec.Code != 422 {
		t.Fatalf("status = %d, want 422; body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "query_text") || !strings.Contains(body, "never migrates") {
		t.Errorf("422 body must name the column and the remediation, got: %s", body)
	}
}
