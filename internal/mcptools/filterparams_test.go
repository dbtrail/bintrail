package mcptools

import (
	"strings"
	"testing"
)

// Validation of the pks / limit_per_pk parameters added by #962. The
// cross-field rules mirror `bintrail recover`/`bintrail query` (see
// runRecover/runQuery in internal/cli): pks needs schema+table, pk and pks
// are mutually exclusive, limit_per_pk needs a PK scope and rejects
// negatives, and the pk list is trimmed + deduplicated with empty entries
// refused loudly.

func TestBuildQueryOptions_pksRequireSchemaTable(t *testing.T) {
	_, err := BuildQueryOptions(FilterParams{PKs: []string{"1"}}, DefaultQueryLimit)
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("expected schema/table error for pks without scope, got: %v", err)
	}
}

func TestBuildQueryOptions_pkAndPKsMutuallyExclusive(t *testing.T) {
	_, err := BuildQueryOptions(FilterParams{
		Schema: "app", Table: "orders", PK: "1", PKs: []string{"2"},
	}, DefaultQueryLimit)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected mutual-exclusion error, got: %v", err)
	}
}

func TestBuildQueryOptions_limitPerPKNegative(t *testing.T) {
	_, err := BuildQueryOptions(FilterParams{
		Schema: "app", Table: "orders", PK: "1", LimitPerPK: -1,
	}, DefaultQueryLimit)
	if err == nil || !strings.Contains(err.Error(), "limit_per_pk") {
		t.Fatalf("expected limit_per_pk error, got: %v", err)
	}
}

func TestBuildQueryOptions_limitPerPKRequiresPKScope(t *testing.T) {
	_, err := BuildQueryOptions(FilterParams{
		Schema: "app", Table: "orders", LimitPerPK: 2,
	}, DefaultQueryLimit)
	if err == nil || !strings.Contains(err.Error(), "requires pk or pks") {
		t.Fatalf("expected pk-scope error, got: %v", err)
	}
}

func TestBuildQueryOptions_pksEmptyEntryRefused(t *testing.T) {
	_, err := BuildQueryOptions(FilterParams{
		Schema: "app", Table: "orders", PKs: []string{"1", "  "},
	}, DefaultQueryLimit)
	if err == nil || !strings.Contains(err.Error(), "empty or whitespace-only") {
		t.Fatalf("expected empty-entry refusal, got: %v", err)
	}
}

func TestBuildQueryOptions_pksTrimmedDedupedAndPlumbed(t *testing.T) {
	opts, err := BuildQueryOptions(FilterParams{
		Schema: "app", Table: "orders",
		PKs:        []string{" 1 ", "2", "1"},
		LimitPerPK: 3,
	}, DefaultQueryLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := strings.Join(opts.PKValuesIn, ","); got != "1,2" {
		t.Errorf("PKValuesIn = %q, want \"1,2\" (trimmed, deduped, order kept)", got)
	}
	if opts.LimitPerPK != 3 {
		t.Errorf("LimitPerPK = %d, want 3", opts.LimitPerPK)
	}
}
