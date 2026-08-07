package shim

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// Per-tenant schema authorization (issue #824).
//
// shim.yaml models multiple tenants — each with its own credentials and
// source_dsn — but the virtual-schema query surface answers against ONE
// shared index, so before this gate any authenticated tenant could read
// any other tenant's entire indexed history (including deleted rows) by
// issuing `USE <other-schema>` or by fully qualifying the target table.
//
// The gate is OPT-IN: a tenant without allowed_schemas in shim.yaml keeps
// the historical unrestricted behaviour, so existing single-tenant
// deployments are untouched. The standalone `bintrail shim` warns at
// startup when a multi-tenant config leaves any tenant unrestricted.

// BindAllowedSchemas binds the authenticated tenant's allowed_schemas
// allowlist to this connection's handler. Call it once per connection,
// after the handshake and before serving commands — same lifecycle (and
// same no-lock-needed reasoning) as BindActor. nil or empty means
// unrestricted.
func (h *Handler) BindAllowedSchemas(schemas []string) {
	h.allowedSchemas = schemas
}

// schemaAllowed reports whether the connection may touch schema. An
// unbound (nil/empty) allowlist allows everything — the opt-in contract.
// Comparison is case-insensitive (strings.EqualFold), matching how MySQL
// folds database identifiers under its default lower_case_table_names
// packaging on the platforms bintrail ships for; an allowlist must not
// become bypassable by re-casing the schema name.
func (h *Handler) schemaAllowed(schema string) bool {
	if len(h.allowedSchemas) == 0 {
		return true
	}
	for _, s := range h.allowedSchemas {
		if strings.EqualFold(s, schema) {
			return true
		}
	}
	return false
}

// schemaDenied logs the denied cross-schema attempt and returns the
// wire error for it: ER_DBACCESS_DENIED_ERROR (1044), the code a real
// mysqld uses when a user has no grants on a database — a proper MySQL
// error the client library surfaces, never a connection drop. The slog
// warning is the denial's operator-visible trail; the shim has no
// denied-side audit action in the ext seam taxonomy (only successful
// timetravel.query emissions), and this deliberately does not invent
// one.
func (h *Handler) schemaDenied(schema string) error {
	actor := h.actor
	if actor == "" {
		actor = unboundActor
	}
	if h.logger != nil { // struct-literal handlers in tests may omit it
		h.logger.Warn("shim: cross-schema access denied by allowed_schemas",
			"tenant", actor, "schema", schema)
	}
	return mysql.NewError(mysql.ER_DBACCESS_DENIED_ERROR, fmt.Sprintf(
		"Access denied for user '%s' to database '%s'", actor, schema))
}
