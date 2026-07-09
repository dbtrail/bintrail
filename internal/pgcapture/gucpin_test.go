package pgcapture

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestPinRenderGUCs_exactSet pins the five canonical values — the identity
// contract between the baseline COPY text and the pgoutput delta text (#593
// slice D). Changing any value silently breaks the text join against data
// captured under the old pin, so this test is a change detector on purpose.
func TestPinRenderGUCs_exactSet(t *testing.T) {
	params := map[string]string{}
	PinRenderGUCs(params)
	want := map[string]string{
		"TimeZone":           "UTC",
		"DateStyle":          "ISO",
		"extra_float_digits": "3",
		"bytea_output":       "hex",
		"IntervalStyle":      "postgres",
	}
	if len(params) != len(want) {
		t.Fatalf("pinned %d GUCs, want %d: %v", len(params), len(want), params)
	}
	for k, v := range want {
		if params[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, params[k], v)
		}
	}
}

// TestPinRenderGUCs_overridesOperatorValue: a same-key value the operator put
// in the DSN must LOSE to the pin (an unpinned rendering session is the
// corruption class the pin kills), while unrelated params survive.
func TestPinRenderGUCs_overridesOperatorValue(t *testing.T) {
	cfg, err := pgconn.ParseConfig("postgres://u@h/db?TimeZone=America/New_York&application_name=myapp")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	PinRenderGUCs(cfg.RuntimeParams)
	if got := cfg.RuntimeParams["TimeZone"]; got != "UTC" {
		t.Errorf("TimeZone = %q, want the pinned UTC (operator value must lose)", got)
	}
	if got := cfg.RuntimeParams["application_name"]; got != "myapp" {
		t.Errorf("application_name = %q, want myapp (unrelated params must survive)", got)
	}
}

// TestRenderGUCsStamp_canonical pins the stamp text: it is persisted into
// baseline Parquet metadata (baseline.MetaKeyRenderGUCs), so a wording change
// would make every existing pinned baseline read as unstamped.
func TestRenderGUCsStamp_canonical(t *testing.T) {
	const want = "TimeZone=UTC;DateStyle=ISO;extra_float_digits=3;bytea_output=hex;IntervalStyle=postgres"
	if got := RenderGUCsStamp(); got != want {
		t.Errorf("RenderGUCsStamp() = %q, want %q", got, want)
	}
}

// TestConnectPinned_badDSNFailsWithoutDialing: an unparseable DSN errors at
// parse time (no network).
func TestConnectPinned_badDSNFailsWithoutDialing(t *testing.T) {
	if _, err := ConnectReplPinned(context.Background(), "://not-a-dsn"); err == nil {
		t.Error("ConnectReplPinned: expected a parse error for a bad DSN")
	}
	if _, err := ConnectQueryPinned(context.Background(), "://not-a-dsn"); err == nil {
		t.Error("ConnectQueryPinned: expected a parse error for a bad DSN")
	}
}
