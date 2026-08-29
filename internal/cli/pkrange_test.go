package cli

import (
	"strings"
	"testing"
)

// ─── #1440: --pk-min/--pk-max flag rules, before the index is opened ────────

func TestPKRangeFlagHelp_plainCopy(t *testing.T) {
	for _, cmdFlag := range []struct{ cmd, name string }{
		{"query", "pk-min"}, {"query", "pk-max"}, {"recover", "pk-min"}, {"recover", "pk-max"},
	} {
		var usage string
		switch cmdFlag.cmd {
		case "query":
			usage = queryCmd.Flag(cmdFlag.name).Usage
		default:
			usage = recoverCmd.Flag(cmdFlag.name).Usage
		}
		if strings.Contains(usage, "—") {
			t.Errorf("%s --%s help carries an em dash", cmdFlag.cmd, cmdFlag.name)
		}
		if cmdFlag.name == "pk-min" && !strings.Contains(usage, "--since") {
			t.Errorf("%s --pk-min help must state the scan cost and point at a time window: %q", cmdFlag.cmd, usage)
		}
	}
}

// TestPKRangeFlags_bindToTheirOwnVariables: a flag bound to the wrong
// variable (--pk-max writing qPKMin) passes every validation test that sets
// the globals directly, so parse the flags the way cobra does.
func TestPKRangeFlags_bindToTheirOwnVariables(t *testing.T) {
	saved := struct{ qmin, qmax, rmin, rmax string }{qPKMin, qPKMax, rPKMin, rPKMax}
	t.Cleanup(func() {
		qPKMin, qPKMax, rPKMin, rPKMax = saved.qmin, saved.qmax, saved.rmin, saved.rmax
		_ = queryCmd.ParseFlags([]string{"--pk-min", "", "--pk-max", ""})
		_ = recoverCmd.ParseFlags([]string{"--pk-min", "", "--pk-max", ""})
	})
	if err := queryCmd.ParseFlags([]string{"--pk-min", "1", "--pk-max", "2"}); err != nil {
		t.Fatal(err)
	}
	if qPKMin != "1" || qPKMax != "2" {
		t.Errorf("query: --pk-min/--pk-max bound to (%q, %q), want (1, 2)", qPKMin, qPKMax)
	}
	if err := recoverCmd.ParseFlags([]string{"--pk-min", "3", "--pk-max", "4"}); err != nil {
		t.Fatal(err)
	}
	if rPKMin != "3" || rPKMax != "4" {
		t.Errorf("recover: --pk-min/--pk-max bound to (%q, %q), want (3, 4)", rPKMin, rPKMax)
	}
}

func TestRunQuery_pkRangeRequiresSchemaTable(t *testing.T) {
	saved := struct{ min, max, s, tbl string }{qPKMin, qPKMax, qSchema, qTable}
	t.Cleanup(func() { qPKMin, qPKMax, qSchema, qTable = saved.min, saved.max, saved.s, saved.tbl })
	qPKMin, qPKMax, qSchema, qTable = "10", "", "db", ""
	err := runQuery(queryCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--pk-min/--pk-max require both --schema and --table") {
		t.Errorf("unexpected error: %v", err)
	}
	qPKMin, qPKMax, qSchema, qTable = "", "10", "", "t"
	err = runQuery(queryCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--pk-min/--pk-max require both --schema and --table") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunQuery_pkRangeExclusiveWithPKAndPKs(t *testing.T) {
	saved := struct {
		min, max, s, tbl, pk string
		pks                  []string
	}{qPKMin, qPKMax, qSchema, qTable, qPK, qPKs}
	t.Cleanup(func() {
		qPKMin, qPKMax, qSchema, qTable, qPK, qPKs = saved.min, saved.max, saved.s, saved.tbl, saved.pk, saved.pks
	})
	qSchema, qTable = "db", "t"
	qPKMin, qPK, qPKs = "10", "5", nil
	if err := runQuery(queryCmd, nil); err == nil || !strings.Contains(err.Error(), "cannot be combined with --pk or --pks") {
		t.Errorf("--pk-min with --pk: %v", err)
	}
	qPKMin, qPK, qPKs = "", "", []string{"1", "2"}
	qPKMax = "10"
	if err := runQuery(queryCmd, nil); err == nil || !strings.Contains(err.Error(), "cannot be combined with --pk or --pks") {
		t.Errorf("--pk-max with --pks: %v", err)
	}
}

func TestRunQuery_pkRangeBoundsMatrix(t *testing.T) {
	saved := struct{ min, max, s, tbl string }{qPKMin, qPKMax, qSchema, qTable}
	t.Cleanup(func() { qPKMin, qPKMax, qSchema, qTable = saved.min, saved.max, saved.s, saved.tbl })
	qSchema, qTable = "db", "t"
	for _, tc := range []struct{ min, max, want string }{
		{"abc", "", "--pk-min: \"abc\" is not an integer"},
		{"", "1.5", "--pk-max: \"1.5\" is not an integer"},
		{"10", "9", "--pk-min/--pk-max: lower bound 10 is above upper bound 9"},
	} {
		qPKMin, qPKMax = tc.min, tc.max
		err := runQuery(queryCmd, nil)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("--pk-min %q --pk-max %q: got %v, want %q", tc.min, tc.max, err, tc.want)
		}
	}
}

func TestRunQuery_pkRangeRejectedWithIncludeSnapshot(t *testing.T) {
	saved := struct {
		min, s, tbl, baseline string
		inc                   bool
	}{qPKMin, qSchema, qTable, qBaseline, qIncludeSnapshot}
	t.Cleanup(func() {
		qPKMin, qSchema, qTable, qBaseline, qIncludeSnapshot = saved.min, saved.s, saved.tbl, saved.baseline, saved.inc
	})
	qSchema, qTable, qPKMin = "db", "t", "1"
	qIncludeSnapshot, qBaseline = true, "/data/baseline.parquet"
	err := runQuery(queryCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--pk-min and --pk-max are not supported with --include-snapshot") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunRecover_pkRangeFlagRules(t *testing.T) {
	saved := struct {
		min, max, s, tbl, pk string
		dry                  bool
	}{rPKMin, rPKMax, rSchema, rTable, rPK, rDryRun}
	t.Cleanup(func() {
		rPKMin, rPKMax, rSchema, rTable, rPK, rDryRun = saved.min, saved.max, saved.s, saved.tbl, saved.pk, saved.dry
	})
	rDryRun = true
	rPKMin, rSchema, rTable = "10", "", ""
	if err := runRecover(recoverCmd, nil); err == nil || !strings.Contains(err.Error(), "--pk-min/--pk-max require both --schema and --table") {
		t.Errorf("recover --pk-min without scope: %v", err)
	}
	rSchema, rTable, rPK = "db", "t", "5"
	if err := runRecover(recoverCmd, nil); err == nil || !strings.Contains(err.Error(), "cannot be combined with --pk or --pks") {
		t.Errorf("recover --pk-min with --pk: %v", err)
	}
	rPK, rPKMin, rPKMax = "", "5", "1"
	if err := runRecover(recoverCmd, nil); err == nil || !strings.Contains(err.Error(), "lower bound 5 is above upper bound 1") {
		t.Errorf("recover inverted range: %v", err)
	}
}

func TestResolvePKRange_noSnapshotRefuses(t *testing.T) {
	r, err := validatePKRangeFlags("1", "", "db", "t", "", nil)
	if err != nil || r == nil {
		t.Fatalf("validatePKRangeFlags: %v", err)
	}
	err = resolvePKRange(nil, nil, "db", "t", r)
	if err == nil || !strings.Contains(err.Error(), "need the schema snapshot") {
		t.Errorf("no resolver must refuse, got %v", err)
	}
	if err := resolvePKRange(nil, nil, "db", "t", nil); err != nil {
		t.Errorf("no range, no resolver: must be a no-op, got %v", err)
	}
}
