package cliapp

import (
	"os/exec"
	"strings"
	"testing"
)

// TestCoreBinaryIsPostgresFree is the source-split guard (#534): the core
// bintrail binary captures MySQL and must NOT link the PostgreSQL capture stack
// (jackc/pgx + pglogrepl, pulled in via internal/pgcapture and internal/
// pgstreamrun). That stack lives only in the standalone bintrail-pg binary, so
// the MySQL binary's dependency surface stays free of pgx. A new import path
// from any core command back into either package, however indirect, fails this
// test.
//
// The shared read plane (internal/cli: status/query/recover/reconstruct/shim) is
// deliberately NOT banned — it is source-agnostic and linked by both binaries.
// Only the PostgreSQL CAPTURE packages are off-limits to the core binary.
func TestCoreBinaryIsPostgresFree(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps",
		"github.com/dbtrail/dbtrail/cmd/bintrail").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	banned := []string{
		"github.com/dbtrail/dbtrail/internal/pgcapture",
		"github.com/dbtrail/dbtrail/internal/pgstreamrun",
		"github.com/jackc/pglogrepl",
		"github.com/jackc/pgx",
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		pkg := strings.TrimSpace(line)
		for _, b := range banned {
			// Exact match or a subpackage — never a substring (so
			// github.com/jackc/pgx matches pgx/v5 and pgx/v5/pgconn but not an
			// unrelated package that merely contains the string).
			if pkg == b || strings.HasPrefix(pkg, b+"/") {
				t.Errorf("cmd/bintrail links %s — the PostgreSQL capture stack must only be linked by cmd/bintrail-pg", pkg)
			}
		}
	}
}
