package consoleapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This package used to carry a byte-for-byte copy of the core's env-file
// loader (#963). A copy is not a bug on the day it is made — it is a bug on
// the day one side changes, because env-file semantics then differ between
// `bintrail` and `bintrail-console` with nothing failing anywhere. That is the
// skew #529's extraction into internal/cli was meant to prevent, and the
// duplicate outlived it by living only in a code comment that named itself a
// "consolidation candidate".
//
// A comment cannot fail. This can: consoleapp must call cli.LoadEnvFile and
// must not grow its own loader back.
func TestConsoleAppHasNoDuplicateEnvLoader(t *testing.T) {
	banned := map[string]string{
		"loadEnvFile":    "call cli.LoadEnvFile() instead",
		"parseAndSetEnv": "the parser belongs to internal/cli; extend it there so both binaries move together",
	}
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse consoleapp: %v", err)
	}
	found := 0
	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			for _, d := range file.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Recv != nil {
					continue
				}
				if why, bad := banned[fn.Name.Name]; bad {
					t.Errorf("%s declares %s — %s", filepath.Base(path), fn.Name.Name, why)
				}
			}
			found++
		}
	}
	if found == 0 {
		t.Fatal("parsed no consoleapp files; this guard would pass by testing nothing")
	}
}
