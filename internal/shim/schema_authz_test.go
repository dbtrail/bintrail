package shim

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/query"
)

// wantSchemaDenied asserts err is the #824 denial: ER_DBACCESS_DENIED_ERROR
// (1044) — a proper wire error, not a connection drop or a generic 1105.
func wantSchemaDenied(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected ER_DBACCESS_DENIED_ERROR, got nil")
	}
	var myErr *gomysql.MyError
	if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_DBACCESS_DENIED_ERROR {
		t.Fatalf("expected ER_DBACCESS_DENIED_ERROR (1044), got %v", err)
	}
}

func TestUseDBEnforcesAllowedSchemas(t *testing.T) {
	h := NewHandler(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.BindActor("tenant_a")
	h.BindAllowedSchemas([]string{"myapp", "reporting"})

	if err := h.UseDB("myapp"); err != nil {
		t.Fatalf("USE of an in-set schema must pass: %v", err)
	}
	if err := h.UseDB("MyApp"); err != nil {
		t.Fatalf("schema match is case-insensitive (MySQL ident folding): %v", err)
	}
	err := h.UseDB("tenant_b_db")
	wantSchemaDenied(t, err)
	if !strings.Contains(err.Error(), "tenant_a") || !strings.Contains(err.Error(), "tenant_b_db") {
		t.Errorf("denial should name the user and the schema, got: %v", err)
	}
	// The rejected USE must not have replaced the selected schema.
	h.mu.Lock()
	got := h.db
	h.mu.Unlock()
	if got != "MyApp" {
		t.Errorf("rejected USE leaked into handler state: db=%q", got)
	}
}

// TestHandleQueryEnforcesAllowedSchemas drives the REAL command entry point
// (HandleQuery — what go-mysql/server dispatches COM_QUERY to), not a leaf
// validator, so deleting the enforcement call at the query chokepoint turns
// these red (mutation rule for #824). The current schema is planted directly
// on the handler — bypassing the UseDB gate on purpose — so this test stays
// red under a mutant that keeps the UseDB check but drops the query-time one.
func TestHandleQueryEnforcesAllowedSchemas(t *testing.T) {
	newRestricted := func() *Handler {
		h := NewHandler(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		h.BindActor("tenant_a")
		h.BindAllowedSchemas([]string{"myapp"})
		return h
	}

	t.Run("virtual_schema_with_out_of_set_current_db", func(t *testing.T) {
		h := newRestricted()
		h.mu.Lock()
		h.db = "tenant_b_db" // planted past the UseDB gate
		h.mu.Unlock()
		_, err := h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00' WHERE id = 1")
		wantSchemaDenied(t, err)
	})

	t.Run("fully_qualified_bare_as_of_without_use", func(t *testing.T) {
		h := newRestricted() // no USE at all: qualification names the schema
		_, err := h.HandleQuery("SELECT * FROM tenant_b_db.orders WHERE id = 1 AS OF '2026-01-01 00:00:00'")
		wantSchemaDenied(t, err)
	})

	t.Run("fully_qualified_hint_form_without_use", func(t *testing.T) {
		h := newRestricted()
		_, err := h.HandleQuery("SELECT /*+ DBTRAIL_AT='2026-01-01 00:00:00' */ * FROM tenant_b_db.orders WHERE id = 1")
		wantSchemaDenied(t, err)
	})

	t.Run("diff_with_out_of_set_current_db", func(t *testing.T) {
		h := newRestricted()
		h.mu.Lock()
		h.db = "tenant_b_db"
		h.mu.Unlock()
		_, err := h.HandleQuery("SELECT * FROM _diff.orders BETWEEN '2026-01-01 00:00:00' AND '2026-01-02 00:00:00' WHERE id = 1")
		wantSchemaDenied(t, err)
	})

	t.Run("show_tables_from_virtual_with_out_of_set_current_db", func(t *testing.T) {
		h := newRestricted()
		h.mu.Lock()
		h.db = "tenant_b_db"
		h.mu.Unlock()
		_, err := h.HandleQuery("SHOW TABLES FROM _flashback")
		wantSchemaDenied(t, err)
	})
}

// TestHandleQueryAllowedSchemaStillServes proves the gate lets an in-set
// query through to the real fetch path: the point-lookup SQL (pk_hash =
// SHA2, unique to that path) must reach the index DB.
func TestHandleQueryAllowedSchemaStillServes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	mock.ExpectQuery("pk_hash = SHA2").
		WillReturnRows(emptyBinlogEventsRows())

	h := &Handler{
		indexDB: db,
		cfg:     Config{AllowGaps: true, IndexDBName: "bintrail_index", NoArchive: true},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
			return nil, nil
		},
	}
	h.BindActor("tenant_a")
	h.BindAllowedSchemas([]string{"myapp"})
	if err := h.UseDB("myapp"); err != nil {
		t.Fatalf("in-set USE: %v", err)
	}
	if _, err := h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00' WHERE id = 42"); err != nil {
		t.Fatalf("in-set query must not be denied: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("in-set query never reached the point-lookup SQL: %v", err)
	}
}

// TestHandleQueryUnrestrictedTenantUntouched pins the opt-in contract: a
// tenant with NO allowed_schemas keeps the historical any-schema behaviour
// (issue #824's hard requirement of zero behavior change for existing
// configs).
func TestHandleQueryUnrestrictedTenantUntouched(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	mock.ExpectQuery("pk_hash = SHA2").
		WillReturnRows(emptyBinlogEventsRows())

	h := &Handler{
		indexDB: db,
		cfg:     Config{AllowGaps: true, IndexDBName: "bintrail_index", NoArchive: true},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
			return nil, nil
		},
	}
	// No BindAllowedSchemas: any schema is reachable, exactly as before.
	if err := h.UseDB("somebody_elses_db"); err != nil {
		t.Fatalf("unrestricted USE must pass: %v", err)
	}
	if _, err := h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00' WHERE id = 42"); err != nil {
		t.Fatalf("unrestricted query must not be denied: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unrestricted query never reached the point-lookup SQL: %v", err)
	}
}

func TestLoadTenantConfigsAllowedSchemas(t *testing.T) {
	write := func(t *testing.T, content string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "shim.yaml")
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	t.Run("parses_allowed_schemas", func(t *testing.T) {
		p := write(t, `
tenants:
  - mysql_user: tenant_a
    mysql_password: 'pw-a'
    allowed_schemas: [myapp, reporting]
  - mysql_user: tenant_b
    mysql_password: 'pw-b'
`)
		cfgs, err := LoadTenantConfigs(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(cfgs) != 2 {
			t.Fatalf("want 2 tenants, got %d", len(cfgs))
		}
		if got := cfgs[0].AllowedSchemas; len(got) != 2 || got[0] != "myapp" || got[1] != "reporting" {
			t.Errorf("tenant_a allowed_schemas = %v", got)
		}
		if cfgs[1].AllowedSchemas != nil {
			t.Errorf("tenant_b (no field) must stay nil = unrestricted, got %v", cfgs[1].AllowedSchemas)
		}
	})

	t.Run("rejects_empty_entry", func(t *testing.T) {
		p := write(t, `
tenants:
  - mysql_user: tenant_a
    mysql_password: 'pw-a'
    allowed_schemas: ['myapp', '']
`)
		_, err := LoadTenantConfigs(p)
		if err == nil || !strings.Contains(err.Error(), "allowed_schemas") {
			t.Fatalf("want an allowed_schemas empty-entry error, got %v", err)
		}
	})
}
