package shim

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// BindAllowedSchemas binds the authenticated tenant's allowed_schemas
// allowlist to this connection's handler. Empty/nil = unrestricted (the
// pre-#824 behaviour). Call it BEFORE seeding the handler's schema, so a
// seed outside the allowlist is refused rather than silently trusted.
func (h *Handler) BindAllowedSchemas(schemas []string) {
	h.allowedSchemas = schemas
}

// schemaAllowed reports whether the connection may touch schema. An
// empty allowlist means unrestricted.
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

// logSchemaDenial emits the single denial log line and returns a
// protocol-neutral message. Both wire front-ends funnel through it so a
// denied access is logged exactly once, with the same fields, wherever it was
// refused.
func (h *Handler) logSchemaDenial(schema string) string {
	actor := h.actor
	if actor == "" {
		actor = unboundActor
	}
	if h.logger != nil {
		h.logger.Warn("shim: cross-schema access denied by allowed_schemas",
			"tenant", actor, "schema", schema)
	}
	return fmt.Sprintf("access denied for user %q to schema %q", actor, schema)
}

// SchemaAuthzCheck is the cross-protocol half of the allowed_schemas gate
// (#824, extended to the PostgreSQL front-end in #1261): it makes the
// DECISION, emits the denial log, and hands back a protocol-neutral message
// for the caller to render as its own wire error. Same shape and the same
// reason as PKColumnCheck — one rule enforced from two front-ends, each
// speaking its own protocol.
//
// The MySQL front-end does NOT use the returned message: its wording mimics
// mysqld's own ER_DBACCESS_DENIED_ERROR text, which clients and ProxySQL key
// on. Only the decision and the log are shared.
func (h *Handler) SchemaAuthzCheck(schema string) (msg string, deny bool) {
	if h.schemaAllowed(schema) {
		return "", false
	}
	return h.logSchemaDenial(schema), true
}

// schemaDenied builds the MySQL front-end's refusal: the same 1044 a real
// mysqld returns for a schema the user has no grants on.
func (h *Handler) schemaDenied(schema string) error {
	h.logSchemaDenial(schema)
	actor := h.actor
	if actor == "" {
		actor = unboundActor
	}
	return mysql.NewError(mysql.ER_DBACCESS_DENIED_ERROR, fmt.Sprintf(
		"Access denied for user '%s' to database '%s'", actor, schema))
}

// UserAllowedSchemas maps tenant username → allowed_schemas for every tenant
// that declares one. Tenants without an allowlist are ABSENT from the map, so
// a lookup yields nil, which BindAllowedSchemas treats as unrestricted (the
// exact pre-#824 behaviour).
//
// Both serving front-ends build their per-connection binding from this one
// function: an allowlist derived twice is an allowlist that can disagree with
// itself, which is how one protocol ends up enforcing what the other does not
// (#1261 was exactly that, on a larger scale).
func UserAllowedSchemas(cfgs []TenantConfig) map[string][]string {
	out := make(map[string][]string, len(cfgs))
	for _, t := range cfgs {
		if len(t.AllowedSchemas) > 0 {
			out[t.MySQLUser] = t.AllowedSchemas
		}
	}
	return out
}

// TenantsWithoutAllowedSchemas lists the tenant usernames that declare no
// allowlist. A serving front-end warns once at startup when more than one
// tenant is configured and any of them is unrestricted: with a single tenant
// there is nobody to be isolated from, but in a multi-tenant shim an operator
// who configured allowed_schemas for some tenants can reasonably believe the
// whole file is isolated.
func TenantsWithoutAllowedSchemas(cfgs []TenantConfig) []string {
	var out []string
	for _, t := range cfgs {
		if len(t.AllowedSchemas) == 0 {
			out = append(out, t.MySQLUser)
		}
	}
	return out
}
