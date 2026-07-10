package pgcapture

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Rendering-GUC pin (#593 slice D).
//
// The PostgreSQL baseline+delta design rests on one identity: the text a
// baseline COPY renders for a value must equal the text pgoutput renders for
// the same value on the delta path — reconstruct's PK join and last-write-wins
// merge are exact string operations over that text (see the raw-text contract
// in internal/pgbaseline). But BOTH sides render every datum through the
// type's server-side OUTPUT FUNCTION, whose text depends on session GUCs
// (TimeZone, DateStyle, extra_float_digits, bytea_output, IntervalStyle). Two
// sessions with different settings render the SAME logical value as DIFFERENT
// text — silently breaking the identity.
//
// Fix: every connection that renders row text — the logical-decoding
// (walsender) session and the baseline COPY connections — is pinned to the
// SAME canonical values via startup-packet parameters (RuntimeParams). Startup
// parameters are applied at backend start as PGC_S_CLIENT, which outranks
// ALTER DATABASE/ROLE defaults and is applied after the DSN's options string,
// so the pin deterministically wins; an invalid value fails the connection
// loudly (FATAL at connect) rather than leaving a session silently unpinned.
// Startup placement also means the pin is in effect BEFORE any transaction
// opens, so pgbaseline's REPEATABLE READ anchor / exported-snapshot workers
// are untouched.
//
// Catalog-only connections (the capturer's PK-lookup queryConn, health probes,
// doctor, reset) never render row text and are deliberately NOT pinned.
//
// renderGUCList is a fixed-order list (not a map) so RenderGUCsStamp is
// deterministic.
var renderGUCList = [5]struct{ name, value string }{
	{"TimeZone", "UTC"},
	{"DateStyle", "ISO"},
	{"extra_float_digits", "3"}, // any value >= 1 selects shortest-precise floats
	{"bytea_output", "hex"},
	{"IntervalStyle", "postgres"},
}

// PinRenderGUCs injects the pinned rendering GUCs into a connection's startup
// RuntimeParams, overriding any operator-supplied value for the same key (an
// unpinned or differently-pinned rendering session is exactly the text-
// mismatch corruption class this exists to kill; the override is logged).
// params must be non-nil.
//
// The purge below is CASE-INSENSITIVE on purpose: PostgreSQL GUC names are
// case-insensitive but Go map keys are not, so an operator DSN carrying
// `?timezone=...` lands under a DIFFERENT map key than the pinned "TimeZone".
// Both would then reach the startup packet in random map-iteration order, and
// the server applies them last-writer-wins at equal source priority — leaving
// roughly half of all connections silently unpinned, per connection. Every
// case-variant of a pinned key is deleted before the pin is inserted.
func PinRenderGUCs(params map[string]string) {
	for _, g := range renderGUCList {
		for k, old := range params {
			if strings.EqualFold(k, g.name) {
				if old != g.value {
					slog.Debug("pgcapture: overriding operator-supplied rendering GUC with the pinned value",
						"guc", k, "operator", old, "pinned", g.value)
				}
				delete(params, k)
			}
		}
		params[g.name] = g.value
	}
}

// RenderGUCsStamp returns the canonical serialization of the pinned set, e.g.
// "TimeZone=UTC;DateStyle=ISO;extra_float_digits=3;bytea_output=hex;IntervalStyle=postgres".
// pgbaseline embeds it in the baseline Parquet metadata
// (baseline.MetaKeyRenderGUCs) so readers can tell a pre-pin baseline (no
// stamp) from a pinned one and warn instead of silently mis-joining.
func RenderGUCsStamp() string {
	parts := make([]string, len(renderGUCList))
	for i, g := range renderGUCList {
		parts[i] = g.name + "=" + g.value
	}
	return strings.Join(parts, ";")
}

// ConnectReplPinned opens a replication connection (pgconn) with the rendering
// GUCs pinned in the startup packet. Use for every walsender session — its
// session GUCs determine pgoutput's tuple text.
func ConnectReplPinned(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: parse replication DSN: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string)
	}
	PinRenderGUCs(cfg.RuntimeParams)
	return pgconn.ConnectConfig(ctx, cfg)
}

// ConnectQueryPinned opens an ordinary connection (pgx) with the rendering
// GUCs pinned in the startup packet. Use for every connection that renders row
// text server-side (the baseline COPY anchor and its parallel workers).
func ConnectQueryPinned(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: parse query DSN: %w", err)
	}
	if cc.RuntimeParams == nil {
		cc.RuntimeParams = make(map[string]string)
	}
	PinRenderGUCs(cc.RuntimeParams)
	return pgx.ConnectConfig(ctx, cc)
}
