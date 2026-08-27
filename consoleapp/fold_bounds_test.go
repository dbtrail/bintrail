package consoleapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
)

// TestEveryInDaemonFoldIsBounded is a structural guard, not a behavioural one.
//
// The bug it exists to stop is not a wrong value, it is an ABSENT field. Every
// reconstruct.FullTableConfig in this package configures a fold that runs
// inside the process that is also capturing, and two of that struct's fields
// invert the usual convention: zero means runtime.NumCPU() for Parallelism and
// "never warn" for WarnEventThreshold, while zero on every other budget there
// resolves to a container-safe default. So an omission is invisible by reading:
// it looks exactly like the deliberate omissions beside it.
//
// The per-call-site tests pin the two folds that exist today. They cannot see a
// THIRD one being added, which is precisely how the SQL export build spent its
// life unbounded while the refresh had a passing test one file over. This walks
// the package's own syntax tree so a new literal is caught by existing.
//
// go/ast rather than grep on purpose: grep cannot tell a composite literal from
// the same words in a comment, and reformatting breaks it.
func TestEveryInDaemonFoldIsBounded(t *testing.T) {
	const required = "Parallelism, WarnEventThreshold"

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	found := 0
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				sel, ok := lit.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "FullTableConfig" {
					return true
				}
				if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "reconstruct" {
					return true
				}
				found++

				set := map[string]bool{}
				for _, elt := range lit.Elts {
					if kv, ok := elt.(*ast.KeyValueExpr); ok {
						if k, ok := kv.Key.(*ast.Ident); ok {
							set[k.Name] = true
						}
					}
				}
				for _, field := range []string{"Parallelism", "WarnEventThreshold"} {
					if !set[field] {
						t.Errorf("%s: reconstruct.FullTableConfig does not set %s.\n"+
							"Every fold in this package runs inside the capture process, and this "+
							"field's ZERO value is the unsafe one (%s). Set both (%s) from the "+
							"daemonFold* constants, the way refreshFoldConfig and sqlExportFoldConfig do.",
							fset.Position(lit.Pos()), field,
							map[string]string{
								"Parallelism":        "zero means runtime.NumCPU(), tying peak memory to the host",
								"WarnEventThreshold": "zero means the volume warning never fires",
							}[field], required)
					}
				}
				return true
			})
		}
	}

	// Guard the guard: if the literals stop being found (renamed import, moved
	// package, a helper that builds the struct by reflection), this test would
	// pass by examining nothing at all.
	if found < 2 {
		t.Errorf("found %d reconstruct.FullTableConfig literals, want at least 2 "+
			"(the refresh fold and the SQL export fold). Zero or one means this "+
			"guard stopped looking at the code it is supposed to guard.", found)
	}
}
