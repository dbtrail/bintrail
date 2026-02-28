package main

import (
	"strings"
	"testing"
)

func TestParseTableFilter(t *testing.T) {
	cases := []struct {
		input string
		want  []string
	}{
		{"", nil},
		{"mydb.orders", []string{"mydb.orders"}},
		{"mydb.orders, mydb.users", []string{"mydb.orders", "mydb.users"}},
		{"  mydb.orders  ,  ", []string{"mydb.orders"}},
	}
	for _, tc := range cases {
		got := parseTableFilter(tc.input)
		if len(got) != len(tc.want) {
			t.Errorf("parseTableFilter(%q) = %v, want %v", tc.input, got, tc.want)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("parseTableFilter(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.want[i])
			}
		}
	}
}

func TestRunBaselineTimestampParsing(t *testing.T) {
	origInput, origOutput, origTS := bslInput, bslOutput, bslTimestamp
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
	})

	// Use real directories so Run gets past DiscoverTables with 0 tables found,
	// avoiding any interaction with the filesystem beyond what's needed.
	bslInput = t.TempDir()
	bslOutput = t.TempDir()

	// Invalid format must return the "expected ISO 8601" error before calling Run.
	bslTimestamp = "not-a-timestamp"
	if err := runBaseline(baselineCmd, nil); err == nil || !strings.Contains(err.Error(), "expected ISO 8601") {
		t.Errorf("invalid timestamp: want ISO 8601 error, got: %v", err)
	}

	// Valid formats: each should parse without the ISO 8601 error.
	// With an empty input dir, DiscoverTables returns 0 tables → Run returns
	// Stats{} with no error, so runBaseline succeeds overall.
	validCases := []struct {
		name string
		ts   string
	}{
		{"RFC3339", "2025-02-28T00:00:00Z"},
		{"T-no-TZ", "2025-02-28T00:00:00"},
		{"space-fmt", "2025-02-28 00:00:00"},
	}
	for _, tc := range validCases {
		bslTimestamp = tc.ts
		err := runBaseline(baselineCmd, nil)
		if err != nil && strings.Contains(err.Error(), "expected ISO 8601") {
			t.Errorf("%s: timestamp should parse without ISO 8601 error, got: %v", tc.name, err)
		}
	}
}

func TestRunBaselineMissingInput(t *testing.T) {
	origInput, origOutput, origTS := bslInput, bslOutput, bslTimestamp
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
	})

	// Non-existent input directory should produce an error about reading the dir.
	bslInput = "/nonexistent/path-does-not-exist"
	bslOutput = t.TempDir()
	bslTimestamp = "2025-01-01T00:00:00Z" // valid timestamp, skips metadata parsing

	if err := runBaseline(baselineCmd, nil); err == nil {
		t.Error("expected error for nonexistent input directory, got nil")
	}
}
